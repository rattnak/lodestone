package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rattnak/lodestone/internal/storage"
)

// HardState is everything Raft must find intact after a crash: losing a
// term/vote allows double voting; losing log entries un-commits data.
type HardState struct {
	Term     uint64
	VotedFor string
	Log      []Entry
	SnapData []byte
	SnapIndex uint64
	SnapTerm  uint64
}

// Storage persists Raft hard state. Every method must be durable when it
// returns.
type Storage interface {
	Load() (HardState, error)
	SetState(term uint64, votedFor string) error
	// Append stores entries; any previously stored entry with index >=
	// entries[0].Index is discarded first (suffix truncation).
	Append(entries []Entry) error
	// SetSnapshot stores the snapshot and discards log entries <= index.
	SetSnapshot(data []byte, index, term uint64) error
}

// MemoryStorage keeps hard state in memory. The simulator "crashes" nodes by
// discarding the Node but keeping its MemoryStorage, which models a machine
// reboot: RAM gone, disk intact.
type MemoryStorage struct {
	mu sync.Mutex
	hs HardState
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hs := m.hs
	hs.Log = append([]Entry(nil), m.hs.Log...)
	return hs, nil
}

func (m *MemoryStorage) SetState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs.Term, m.hs.VotedFor = term, votedFor
	return nil
}

func (m *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	first := entries[0].Index
	keep := m.hs.Log[:0]
	for _, e := range m.hs.Log {
		if e.Index < first {
			keep = append(keep, e)
		}
	}
	m.hs.Log = append(keep, entries...)
	return nil
}

func (m *MemoryStorage) SetSnapshot(data []byte, index, term uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs.SnapData = append([]byte(nil), data...)
	m.hs.SnapIndex, m.hs.SnapTerm = index, term
	keep := m.hs.Log[:0]
	for _, e := range m.hs.Log {
		if e.Index > index {
			keep = append(keep, e)
		}
	}
	m.hs.Log = keep
	return nil
}

// WALStorage persists Raft state through the Phase 1 write-ahead log: state
// changes, appends, and truncation markers are log records replayed on open.
// The Raft log stored in a log — compaction happens when a snapshot record
// supersedes everything before it, at which point the WAL is rewritten.
type WALStorage struct {
	mu   sync.Mutex
	dir  string
	wal  *storage.WAL
	hs   HardState
}

// WAL record keys (values carry the payload):
const (
	walKeyState    = "s" // value: term uvarint | votedFor
	walKeyEntry    = "e" // value: index | term uvarints | data
	walKeyTruncate = "t" // value: fromIndex uvarint — discard entries >= from
	walKeySnap     = "p" // value: index | term uvarints | snapshot data
)

func OpenWALStorage(dir string) (*WALStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WALStorage{dir: dir}
	wal, err := storage.OpenWAL(w.path())
	if err != nil {
		return nil, err
	}
	w.wal = wal
	err = wal.Replay(func(rec storage.Record) error {
		w.replay(string(rec.Key), append([]byte(nil), rec.Value...))
		return nil
	})
	if err != nil {
		wal.Close()
		return nil, err
	}
	return w, nil
}

func (w *WALStorage) path() string { return filepath.Join(w.dir, "raft.wal") }

func (w *WALStorage) replay(key string, val []byte) {
	switch key {
	case walKeyState:
		term, n := binary.Uvarint(val)
		w.hs.Term, w.hs.VotedFor = term, string(val[n:])
	case walKeyEntry:
		index, n1 := binary.Uvarint(val)
		term, n2 := binary.Uvarint(val[n1:])
		w.truncateFrom(index)
		w.hs.Log = append(w.hs.Log, Entry{Index: index, Term: term, Data: val[n1+n2:]})
	case walKeyTruncate:
		from, _ := binary.Uvarint(val)
		w.truncateFrom(from)
	case walKeySnap:
		index, n1 := binary.Uvarint(val)
		term, n2 := binary.Uvarint(val[n1:])
		w.hs.SnapData, w.hs.SnapIndex, w.hs.SnapTerm = val[n1+n2:], index, term
		keep := w.hs.Log[:0]
		for _, e := range w.hs.Log {
			if e.Index > index {
				keep = append(keep, e)
			}
		}
		w.hs.Log = keep
	}
}

func (w *WALStorage) truncateFrom(index uint64) {
	keep := w.hs.Log[:0]
	for _, e := range w.hs.Log {
		if e.Index < index {
			keep = append(keep, e)
		}
	}
	w.hs.Log = keep
}

func (w *WALStorage) appendRecord(key string, val []byte) error {
	if err := w.wal.Append(storage.Record{Op: storage.OpPut, Key: []byte(key), Value: val}); err != nil {
		return err
	}
	return w.wal.Sync()
}

func (w *WALStorage) Load() (HardState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	hs := w.hs
	hs.Log = append([]Entry(nil), w.hs.Log...)
	return hs, nil
}

func (w *WALStorage) SetState(term uint64, votedFor string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	val := binary.AppendUvarint(nil, term)
	val = append(val, votedFor...)
	if err := w.appendRecord(walKeyState, val); err != nil {
		return err
	}
	w.hs.Term, w.hs.VotedFor = term, votedFor
	return nil
}

func (w *WALStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		val := binary.AppendUvarint(nil, e.Index)
		val = binary.AppendUvarint(val, e.Term)
		val = append(val, e.Data...)
		if err := w.wal.Append(storage.Record{Op: storage.OpPut, Key: []byte(walKeyEntry), Value: val}); err != nil {
			return err
		}
	}
	if err := w.wal.Sync(); err != nil {
		return err
	}
	w.truncateFrom(entries[0].Index)
	w.hs.Log = append(w.hs.Log, entries...)
	return nil
}

// SetSnapshot rewrites the WAL from scratch: snapshot record, current state,
// then the surviving log suffix. This is the compaction step that keeps the
// Raft WAL from growing without bound.
func (w *WALStorage) SetSnapshot(data []byte, index, term uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.hs.SnapData = append([]byte(nil), data...)
	w.hs.SnapIndex, w.hs.SnapTerm = index, term
	keep := w.hs.Log[:0]
	for _, e := range w.hs.Log {
		if e.Index > index {
			keep = append(keep, e)
		}
	}
	w.hs.Log = keep

	fresh := w.path() + ".compact"
	os.Remove(fresh)
	nw, err := storage.OpenWAL(fresh)
	if err != nil {
		return err
	}
	write := func(key string, val []byte) {
		if err == nil {
			err = nw.Append(storage.Record{Op: storage.OpPut, Key: []byte(key), Value: val})
		}
	}
	snapVal := binary.AppendUvarint(nil, index)
	snapVal = binary.AppendUvarint(snapVal, term)
	write(walKeySnap, append(snapVal, data...))
	stateVal := binary.AppendUvarint(nil, w.hs.Term)
	write(walKeyState, append(stateVal, w.hs.VotedFor...))
	for _, e := range w.hs.Log {
		val := binary.AppendUvarint(nil, e.Index)
		val = binary.AppendUvarint(val, e.Term)
		write(walKeyEntry, append(val, e.Data...))
	}
	if err != nil {
		nw.Close()
		return err
	}
	if err := nw.Sync(); err != nil {
		nw.Close()
		return err
	}
	if err := nw.Close(); err != nil {
		return err
	}
	w.wal.Close()
	if err := os.Rename(fresh, w.path()); err != nil {
		return fmt.Errorf("raft: swapping compacted wal: %w", err)
	}
	w.wal, err = storage.OpenWAL(w.path())
	return err
}

func (w *WALStorage) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.wal.Close()
}
