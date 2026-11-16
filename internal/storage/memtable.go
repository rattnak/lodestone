package storage

import (
	"bytes"
	"iter"
	"math/rand"
	"sync"
)

// Entry is a stored value: either live bytes or a tombstone, which marks a
// deletion that must shadow older versions of the key in SSTables below.
type Entry struct {
	Value     []byte
	Tombstone bool
}

const (
	maxHeight = 16
	// pUp is the probability a node is promoted one level; 0.25 gives
	// expected O(log n) search with ~1.33 pointers per node.
	pUp = 0.25
	// entryOverhead approximates per-entry bookkeeping so Size reflects
	// real memory pressure, not just payload bytes.
	entryOverhead = 32
)

type node struct {
	key  []byte
	ent  Entry
	next []*node
}

// Memtable is the sorted in-memory table that absorbs writes after they are
// logged to the WAL. It is a skiplist: ordered iteration is what lets a full
// memtable flush straight into a sorted SSTable. Safe for concurrent use.
type Memtable struct {
	mu     sync.RWMutex
	head   *node
	height int
	rng    *rand.Rand
	count  int
	size   int64
}

func NewMemtable() *Memtable {
	return &Memtable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}
}

// Put inserts or replaces key. The key and value are copied, so callers may
// reuse their buffers (WAL replay does).
func (m *Memtable) Put(key, value []byte) {
	m.set(key, Entry{Value: bytes.Clone(value)})
}

// Delete writes a tombstone for key. The key need not currently exist: the
// tombstone must survive to shadow copies of the key in older SSTables.
func (m *Memtable) Delete(key []byte) {
	m.set(key, Entry{Tombstone: true})
}

// Get returns the entry for key. ok is false when the memtable holds nothing
// for the key — callers must then consult older tables, whereas a returned
// tombstone means the key is definitively deleted.
func (m *Memtable) Get(key []byte) (ent Entry, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	x := m.findGE(key, nil)
	if x != nil && bytes.Equal(x.key, key) {
		return x.ent, true
	}
	return Entry{}, false
}

// All yields every entry, including tombstones, in ascending key order.
// Yielded slices are owned by the memtable and must not be mutated.
func (m *Memtable) All() iter.Seq2[[]byte, Entry] {
	return func(yield func([]byte, Entry) bool) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		for x := m.head.next[0]; x != nil; x = x.next[0] {
			if !yield(x.key, x.ent) {
				return
			}
		}
	}
}

// Len returns the number of entries, counting tombstones.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// Size returns approximate memory usage; the engine flushes when it passes a
// threshold.
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// findGE returns the first node with key >= target. When update is non-nil it
// records, per level, the rightmost node strictly before target — the splice
// points for an insert.
func (m *Memtable) findGE(target []byte, update []*node) *node {
	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && bytes.Compare(x.next[lvl].key, target) < 0 {
			x = x.next[lvl]
		}
		if update != nil {
			update[lvl] = x
		}
	}
	return x.next[0]
}

func (m *Memtable) set(key []byte, ent Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var update [maxHeight]*node
	cand := m.findGE(key, update[:])
	if cand != nil && bytes.Equal(cand.key, key) {
		m.size += int64(len(ent.Value)) - int64(len(cand.ent.Value))
		cand.ent = ent
		return
	}
	h := 1
	for h < maxHeight && m.rng.Float64() < pUp {
		h++
	}
	if h > m.height {
		for lvl := m.height; lvl < h; lvl++ {
			update[lvl] = m.head
		}
		m.height = h
	}
	n := &node{key: bytes.Clone(key), ent: ent, next: make([]*node, h)}
	for lvl := range n.next {
		n.next[lvl] = update[lvl].next[lvl]
		update[lvl].next[lvl] = n
	}
	m.count++
	m.size += int64(len(key)) + int64(len(ent.Value)) + entryOverhead
}
