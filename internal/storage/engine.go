package storage

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Options configures a DB.
type Options struct {
	// MemtableBytes is the approximate memtable size that triggers a flush
	// to a new SSTable. Defaults to 4 MiB.
	MemtableBytes int64
	// SyncWrites fsyncs the WAL before acknowledging each write. Defaults
	// to true; turning it off trades the last few writes in a crash for
	// throughput.
	SyncWrites bool
	// CompactAtTables triggers a full compaction when a flush leaves this
	// many SSTables. Defaults to 4.
	CompactAtTables int
}

func defaultOptions() Options {
	return Options{MemtableBytes: 4 << 20, SyncWrites: true, CompactAtTables: 4}
}

// DB is the single-node storage engine: an LSM tree.
//
// Write path: append to the WAL (durability), apply to the memtable (order).
// When the memtable passes the size threshold it is flushed to an immutable
// SSTable and the WAL is reset — the flushed table now carries that data.
//
// Read path: memtable first, then SSTables newest-first; the first version
// found wins, which is how overwrites and tombstones shadow older data.
type DB struct {
	mu     sync.RWMutex
	dir    string
	opts   Options
	wal    *WAL
	mem    *Memtable
	tables []*SSTable // newest first
	nextID uint64     // next SSTable file number
	closed bool
}

const walFileName = "wal.log"

// Open opens or creates a DB in dir. Any WAL left by a previous run is
// replayed into the memtable, so acknowledged writes survive a crash.
// Replaying writes that were already flushed to an SSTable (a crash between
// flush and WAL reset) is harmless: they re-apply the same versions, and the
// memtable shadows the table copy.
func Open(dir string, opts *Options) (*DB, error) {
	o := defaultOptions()
	if opts != nil {
		if opts.MemtableBytes > 0 {
			o.MemtableBytes = opts.MemtableBytes
		}
		if opts.CompactAtTables > 0 {
			o.CompactAtTables = opts.CompactAtTables
		}
		o.SyncWrites = opts.SyncWrites
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: o, mem: NewMemtable(), nextID: 1}

	ids, err := sstableIDs(dir)
	if err != nil {
		return nil, err
	}
	for _, id := range ids { // descending: newest first
		st, err := OpenSSTable(db.tablePath(id))
		if err != nil {
			db.closeTables()
			return nil, err
		}
		db.tables = append(db.tables, st)
	}
	if len(ids) > 0 {
		db.nextID = ids[0] + 1
	}

	db.wal, err = OpenWAL(filepath.Join(dir, walFileName))
	if err != nil {
		db.closeTables()
		return nil, err
	}
	err = db.wal.Replay(func(rec Record) error {
		switch rec.Op {
		case OpPut:
			db.mem.Put(rec.Key, rec.Value)
		case OpDelete:
			db.mem.Delete(rec.Key)
		default:
			return fmt.Errorf("storage: unknown op %d in WAL", rec.Op)
		}
		return nil
	})
	if err != nil {
		db.wal.Close()
		db.closeTables()
		return nil, err
	}
	return db, nil
}

// sstableIDs returns the numeric IDs of *.sst files in dir, descending.
func sstableIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sst") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ".sst"), 10, 64)
		if err != nil {
			continue // not one of ours
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return ids, nil
}

func (db *DB) tablePath(id uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", id))
}

// Put stores key -> value. It returns only after the write is durable per
// Options.SyncWrites.
func (db *DB) Put(key, value []byte) error {
	return db.apply(Record{Op: OpPut, Key: key, Value: value})
}

// Delete removes key. Deleting an absent key is not an error: the tombstone
// still shadows any copy in older SSTables.
func (db *DB) Delete(key []byte) error {
	return db.apply(Record{Op: OpDelete, Key: key})
}

func (db *DB) apply(rec Record) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.wal.Append(rec); err != nil {
		return err
	}
	if db.opts.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}
	if rec.Op == OpDelete {
		db.mem.Delete(rec.Key)
	} else {
		db.mem.Put(rec.Key, rec.Value)
	}
	if db.mem.Size() >= db.opts.MemtableBytes {
		if err := db.flushLocked(); err != nil {
			return err
		}
		if len(db.tables) >= db.opts.CompactAtTables {
			return db.compactLocked()
		}
	}
	return nil
}

// Get returns the value for key, or ok=false if the key does not exist (never
// written, or deleted).
func (db *DB) Get(key []byte) (value []byte, ok bool, err error) {
	db.mu.RLock()
	mem := db.mem
	tables := db.tables
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, false, ErrClosed
	}
	if ent, found := mem.Get(key); found {
		if ent.Tombstone {
			return nil, false, nil
		}
		return ent.Value, true, nil
	}
	for _, st := range tables {
		ent, found, err := st.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if ent.Tombstone {
				return nil, false, nil
			}
			return ent.Value, true, nil
		}
	}
	return nil, false, nil
}

// Flush forces the memtable to an SSTable regardless of size.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.flushLocked()
}

// flushLocked writes the memtable to a new SSTable, then resets the WAL.
// Order matters: the WAL may only shrink after the table is durably renamed
// into place, otherwise a crash loses the data entirely.
func (db *DB) flushLocked() error {
	if db.mem.Len() == 0 {
		return nil
	}
	id := db.nextID
	path := db.tablePath(id)
	if err := WriteSSTable(path, db.mem.All()); err != nil {
		return err
	}
	st, err := OpenSSTable(path)
	if err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		st.Close()
		return err
	}
	if err := os.Remove(filepath.Join(db.dir, walFileName)); err != nil {
		st.Close()
		return err
	}
	wal, err := OpenWAL(filepath.Join(db.dir, walFileName))
	if err != nil {
		st.Close()
		return err
	}
	db.wal = wal
	db.nextID = id + 1
	db.tables = append([]*SSTable{st}, db.tables...)
	db.mem = NewMemtable()
	return nil
}

// All yields every live key/value pair in ascending key order — tombstones
// and shadowed versions resolved — from a point-in-time snapshot of the
// memtable and tables. Yielded slices may alias internal buffers; copy them
// if retained. Read errors surface via errCheck after iteration.
func (db *DB) All() (seq iter.Seq2[[]byte, []byte], errCheck func() error) {
	db.mu.RLock()
	mem := db.mem
	tables := db.tables
	db.mu.RUnlock()

	seqs := make([]iter.Seq2[[]byte, Entry], 0, len(tables)+1)
	errPtrs := make([]*error, 0, len(tables))
	seqs = append(seqs, mem.All()) // newest source first
	for _, t := range tables {
		s, errp := t.All()
		seqs = append(seqs, s)
		errPtrs = append(errPtrs, errp)
	}
	merged := mergeSeqs(seqs)
	seq = func(yield func([]byte, []byte) bool) {
		for k, e := range merged {
			if e.Tombstone {
				continue
			}
			if !yield(k, e.Value) {
				return
			}
		}
	}
	errCheck = func() error {
		var errs []error
		for _, p := range errPtrs {
			errs = append(errs, *p)
		}
		return errors.Join(errs...)
	}
	return seq, errCheck
}

// TableCount returns the number of SSTables on disk (for tests, stats, and
// deciding when to compact).
func (db *DB) TableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.tables)
}

// MemtableSize returns the memtable's approximate memory usage in bytes.
func (db *DB) MemtableSize() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.mem.Size()
}

// Close syncs the WAL and releases all file handles. The memtable is not
// flushed: its contents are in the WAL and will be replayed on reopen.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	errs := []error{db.wal.Close()}
	for _, st := range db.tables {
		errs = append(errs, st.Close())
	}
	return errors.Join(errs...)
}

func (db *DB) closeTables() {
	for _, st := range db.tables {
		st.Close()
	}
}
