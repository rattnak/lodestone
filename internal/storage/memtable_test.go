package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestMemtableBasicOps(t *testing.T) {
	m := NewMemtable()
	if _, ok := m.Get([]byte("missing")); ok {
		t.Fatal("empty memtable returned an entry")
	}

	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	if ent, ok := m.Get([]byte("a")); !ok || string(ent.Value) != "1" || ent.Tombstone {
		t.Fatalf("Get(a) = %+v, %v", ent, ok)
	}

	// Overwrite replaces in place.
	m.Put([]byte("a"), []byte("one"))
	if ent, _ := m.Get([]byte("a")); string(ent.Value) != "one" {
		t.Fatalf("after overwrite, Get(a) = %q", ent.Value)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d after overwrite, want 2", m.Len())
	}

	// Delete leaves a visible tombstone — distinct from absence.
	m.Delete([]byte("a"))
	ent, ok := m.Get([]byte("a"))
	if !ok || !ent.Tombstone {
		t.Fatalf("after delete, Get(a) = %+v, %v; want tombstone", ent, ok)
	}
	// Tombstone for a key never seen is legal and must be stored.
	m.Delete([]byte("ghost"))
	if ent, ok := m.Get([]byte("ghost")); !ok || !ent.Tombstone {
		t.Fatalf("tombstone for unseen key not stored: %+v, %v", ent, ok)
	}
}

func TestMemtableCopiesBuffers(t *testing.T) {
	m := NewMemtable()
	key := []byte("key")
	val := []byte("value")
	m.Put(key, val)
	key[0], val[0] = 'X', 'X' // caller reuses buffers, as WAL replay does
	if ent, ok := m.Get([]byte("key")); !ok || string(ent.Value) != "value" {
		t.Fatalf("memtable aliased caller buffers: %+v, %v", ent, ok)
	}
}

func TestMemtableOrderedIteration(t *testing.T) {
	m := NewMemtable()
	keys := []string{"delta", "alpha", "echo", "bravo", "charlie", ""}
	for _, k := range keys {
		m.Put([]byte(k), []byte("v-"+k))
	}
	var got []string
	for k := range m.All() {
		got = append(got, string(k))
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("iterated %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iteration order %v, want %v", got, want)
		}
	}
}

func TestMemtableAgainstReferenceMap(t *testing.T) {
	m := NewMemtable()
	ref := map[string]Entry{}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20_000; i++ {
		key := fmt.Sprintf("key-%04d", rng.Intn(3000))
		switch rng.Intn(3) {
		case 0:
			m.Delete([]byte(key))
			ref[key] = Entry{Tombstone: true}
		default:
			val := fmt.Sprintf("val-%d", i)
			m.Put([]byte(key), []byte(val))
			ref[key] = Entry{Value: []byte(val)}
		}
	}
	if m.Len() != len(ref) {
		t.Fatalf("Len = %d, want %d", m.Len(), len(ref))
	}
	for key, want := range ref {
		got, ok := m.Get([]byte(key))
		if !ok || got.Tombstone != want.Tombstone || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("Get(%s) = %+v, %v; want %+v", key, got, ok, want)
		}
	}
	// Full iteration must be sorted and match the reference exactly.
	var prev []byte
	n := 0
	for k, e := range m.All() {
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("iteration not strictly ascending: %q then %q", prev, k)
		}
		want := ref[string(k)]
		if e.Tombstone != want.Tombstone || !bytes.Equal(e.Value, want.Value) {
			t.Fatalf("iterated entry %s = %+v, want %+v", k, e, want)
		}
		prev = bytes.Clone(k)
		n++
	}
	if n != len(ref) {
		t.Fatalf("iterated %d entries, want %d", n, len(ref))
	}
}

func TestMemtableSizeAccounting(t *testing.T) {
	m := NewMemtable()
	if m.Size() != 0 {
		t.Fatalf("empty Size = %d", m.Size())
	}
	m.Put([]byte("k"), bytes.Repeat([]byte("v"), 100))
	s1 := m.Size()
	if s1 < 100 {
		t.Fatalf("Size %d does not reflect 100-byte value", s1)
	}
	m.Put([]byte("k"), nil) // shrink in place
	if s2 := m.Size(); s2 >= s1 || s2 <= 0 {
		t.Fatalf("Size after shrink = %d (was %d)", s2, s1)
	}
}

func TestMemtableConcurrentReadersAndWriter(t *testing.T) {
	m := NewMemtable()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5000; i++ {
			m.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("v"))
		}
	}()
	for i := 0; i < 4; i++ {
		go func() {
			for j := 0; j < 2000; j++ {
				m.Get([]byte(fmt.Sprintf("key-%d", j)))
				for range m.All() {
					break
				}
			}
		}()
	}
	<-done
}
