package storage

import (
	"bytes"
	"container/heap"
	"errors"
	"iter"
	"os"
)

// Compaction merges every SSTable into one. Each flush adds a table, and
// every table is another place a read may have to look (and another copy of
// overwritten data on disk) — compaction pays that debt back.
//
// For equal keys the newest table wins and older versions are dropped.
// Tombstones are kept, not dropped: recovery may legitimately see old tables
// alongside the compacted one (a crash between rename and cleanup), and a
// dropped tombstone would let a deleted key resurrect from an old table.
// Dropping them safely needs a manifest of live tables — future work.

// Compact merges all SSTables into a single new table and deletes the old
// files. The memtable is untouched.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.compactLocked()
}

func (db *DB) compactLocked() error {
	if len(db.tables) < 2 {
		return nil
	}
	old := db.tables
	id := db.nextID
	path := db.tablePath(id)
	seq, errCheck := mergeTables(old)
	if err := WriteSSTable(path, seq); err != nil {
		return err
	}
	if err := errCheck(); err != nil {
		os.Remove(path)
		return err
	}
	st, err := OpenSSTable(path)
	if err != nil {
		os.Remove(path)
		return err
	}
	db.tables = []*SSTable{st}
	db.nextID = id + 1
	// Old tables are dead from this point; a crash before cleanup is fine
	// because the merged table (higher ID, hence newer) shadows them.
	var errs []error
	for _, t := range old {
		errs = append(errs, t.Close(), os.Remove(t.path))
	}
	return errors.Join(errs...)
}

// mergeTables yields the union of the tables (newest first, as db.tables is
// ordered) in ascending key order, one entry per key. Read errors from any
// table surface via errCheck after iteration.
func mergeTables(tables []*SSTable) (seq iter.Seq2[[]byte, Entry], errCheck func() error) {
	errPtrs := make([]*error, len(tables))
	seq = func(yield func([]byte, Entry) bool) {
		h := make(mergeHeap, 0, len(tables))
		for i, t := range tables {
			s, errp := t.All()
			errPtrs[i] = errp
			next, stop := iter.Pull2(s)
			defer stop()
			if k, e, ok := next(); ok {
				h = append(h, &mergeItem{key: k, ent: e, prio: i, next: next})
			}
		}
		heap.Init(&h)
		var last []byte
		for h.Len() > 0 {
			it := h[0]
			emit := last == nil || !bytes.Equal(last, it.key)
			key, ent := it.key, it.ent
			if k, e, ok := it.next(); ok {
				it.key, it.ent = k, e
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
			if emit {
				last = key // safe to retain: All yields fresh slices
				if !yield(key, ent) {
					return
				}
			}
		}
	}
	errCheck = func() error {
		var errs []error
		for _, p := range errPtrs {
			if p != nil {
				errs = append(errs, *p)
			}
		}
		return errors.Join(errs...)
	}
	return seq, errCheck
}

type mergeItem struct {
	key  []byte
	ent  Entry
	prio int // position in the tables slice; lower = newer, wins key ties
	next func() ([]byte, Entry, bool)
}

type mergeHeap []*mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	return h[i].prio < h[j].prio
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)         { *h = append(*h, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
