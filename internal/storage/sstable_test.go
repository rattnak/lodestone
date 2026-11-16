package storage

import (
	"bytes"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func buildTable(t *testing.T, entries iter.Seq2[[]byte, Entry]) *SSTable {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.sst")
	if err := WriteSSTable(path, entries); err != nil {
		t.Fatalf("write: %v", err)
	}
	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestSSTableRoundtripFromMemtable(t *testing.T) {
	m := NewMemtable()
	ref := map[string]Entry{}
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key-%05d", rng.Intn(1200))
		if rng.Intn(4) == 0 {
			m.Delete([]byte(key))
			ref[key] = Entry{Tombstone: true}
		} else {
			val := bytes.Repeat([]byte{byte(i)}, rng.Intn(100))
			m.Put([]byte(key), val)
			ref[key] = Entry{Value: val}
		}
	}
	st := buildTable(t, m.All())

	if st.Count() != uint64(len(ref)) {
		t.Fatalf("Count = %d, want %d", st.Count(), len(ref))
	}
	// Every written key must be found, with exact value and tombstone state.
	for key, want := range ref {
		got, ok, err := st.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !ok || got.Tombstone != want.Tombstone || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("Get(%s) = %+v, %v; want %+v", key, got, ok, want)
		}
	}
	// Absent keys: below the first key, above the last, and in gaps.
	for _, key := range []string{"aaa", "key-", "key-00000x", "zzz"} {
		if _, ok, err := st.Get([]byte(key)); err != nil || ok {
			t.Fatalf("Get(%s) = ok=%v err=%v; want absent", key, ok, err)
		}
	}
	// Full scan must be sorted and complete.
	seq, iterErr := st.All()
	var prev []byte
	n := 0
	for k, e := range seq {
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("scan not ascending: %q then %q", prev, k)
		}
		want := ref[string(k)]
		if e.Tombstone != want.Tombstone || !bytes.Equal(e.Value, want.Value) {
			t.Fatalf("scan entry %s = %+v, want %+v", k, e, want)
		}
		prev = bytes.Clone(k)
		n++
	}
	if *iterErr != nil {
		t.Fatalf("scan error: %v", *iterErr)
	}
	if n != len(ref) {
		t.Fatalf("scanned %d entries, want %d", n, len(ref))
	}
}

// Every key position relative to the sparse index matters: first key, keys at
// interval boundaries, keys just before and after boundaries, and the last key.
func TestSSTableLookupsAroundIndexBoundaries(t *testing.T) {
	m := NewMemtable()
	const n = sstIndexInterval*3 + 5
	for i := 0; i < n; i++ {
		m.Put([]byte(fmt.Sprintf("k%04d", i*2)), []byte(fmt.Sprintf("v%d", i)))
	}
	st := buildTable(t, m.All())
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%04d", i*2)
		ent, ok, err := st.Get([]byte(key))
		if err != nil || !ok || string(ent.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("Get(%s) = %+v ok=%v err=%v", key, ent, ok, err)
		}
		// Odd keys fall between stored keys and must be absent.
		gap := fmt.Sprintf("k%04d", i*2+1)
		if _, ok, _ := st.Get([]byte(gap)); ok {
			t.Fatalf("Get(%s) found a key that was never written", gap)
		}
	}
}

func TestSSTableSingleEntryAndLargeValue(t *testing.T) {
	m := NewMemtable()
	big := bytes.Repeat([]byte("x"), 1<<20)
	m.Put([]byte("only"), big)
	st := buildTable(t, m.All())
	ent, ok, err := st.Get([]byte("only"))
	if err != nil || !ok || !bytes.Equal(ent.Value, big) {
		t.Fatalf("1MB value roundtrip failed: ok=%v err=%v len=%d", ok, err, len(ent.Value))
	}
}

func TestSSTableRejectsUnsortedAndEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.sst")
	unsorted := func(yield func([]byte, Entry) bool) {
		yield([]byte("b"), Entry{Value: []byte("1")})
		yield([]byte("a"), Entry{Value: []byte("2")})
	}
	if err := WriteSSTable(path, unsorted); err == nil {
		t.Fatal("unsorted input accepted")
	}
	if err := WriteSSTable(path, func(func([]byte, Entry) bool) {}); err == nil {
		t.Fatal("empty input accepted")
	}
	// Failed writes must not leave files behind.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("table file exists after failed write: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind after failed write: %v", err)
	}
}

func TestSSTableRejectsCorruptFooter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.sst")
	m := NewMemtable()
	m.Put([]byte("a"), []byte("1"))
	if err := WriteSSTable(path, m.All()); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff // break the magic number
	bad := filepath.Join(dir, "bad.sst")
	os.WriteFile(bad, data, 0o644)
	if _, err := OpenSSTable(bad); err == nil {
		t.Fatal("corrupt footer accepted")
	}
	if _, err := OpenSSTable(filepath.Join(dir, "tiny.sst")); err == nil {
		t.Fatal("missing file accepted")
	}
	os.WriteFile(filepath.Join(dir, "tiny.sst"), []byte("short"), 0o644)
	if _, err := OpenSSTable(filepath.Join(dir, "tiny.sst")); err == nil {
		t.Fatal("undersized file accepted")
	}
}

func TestSSTableConcurrentGets(t *testing.T) {
	m := NewMemtable()
	for i := 0; i < 500; i++ {
		m.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%d", i)))
	}
	st := buildTable(t, m.All())
	done := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func(seed int64) {
			rng := rand.New(rand.NewSource(seed))
			for j := 0; j < 500; j++ {
				i := rng.Intn(500)
				ent, ok, err := st.Get([]byte(fmt.Sprintf("key-%04d", i)))
				if err != nil || !ok || string(ent.Value) != fmt.Sprintf("val-%d", i) {
					done <- fmt.Errorf("goroutine %d: Get(key-%04d) = %+v ok=%v err=%v", seed, i, ent, ok, err)
					return
				}
			}
			done <- nil
		}(int64(g))
	}
	for g := 0; g < 8; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
