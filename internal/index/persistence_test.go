package index

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func buildIndex(t *testing.T, n, dim int) (*Index, map[uint64][]float32) {
	t.Helper()
	rng := rand.New(rand.NewSource(13))
	ix := New(Options{Distance: L2, Seed: 13})
	vecs := map[uint64][]float32{}
	for i := uint64(0); i < uint64(n); i++ {
		v := randVec(rng, dim)
		vecs[i] = v
		if err := ix.Add(i, v); err != nil {
			t.Fatal(err)
		}
	}
	return ix, vecs
}

func TestSnapshotRoundtripPreservesResults(t *testing.T) {
	ix, _ := buildIndex(t, 1000, 16)
	for i := uint64(0); i < 100; i += 3 {
		ix.Delete(i)
	}
	path := filepath.Join(t.TempDir(), "index.snap")
	if err := ix.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path, Options{Distance: L2})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != ix.Len() {
		t.Fatalf("loaded Len = %d, want %d", loaded.Len(), ix.Len())
	}
	// The loaded graph must give byte-identical results: same structure,
	// same metric, same traversal.
	rng := rand.New(rand.NewSource(14))
	for q := 0; q < 50; q++ {
		query := randVec(rng, 16)
		want, err1 := ix.Search(query, 10, 64)
		got, err2 := loaded.Search(query, 10, 64)
		if err1 != nil || err2 != nil {
			t.Fatal(err1, err2)
		}
		if len(want) != len(got) {
			t.Fatalf("result lengths differ: %d vs %d", len(want), len(got))
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("query %d result %d: %+v vs %+v", q, i, want[i], got[i])
			}
		}
	}
}

func TestSnapshotSupportsFurtherAdds(t *testing.T) {
	ix, _ := buildIndex(t, 200, 8)
	var buf bytes.Buffer
	if err := ix.Save(&buf); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(&buf, Options{Distance: L2})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(15))
	for i := uint64(1000); i < 1200; i++ {
		if err := loaded.Add(i, randVec(rng, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if loaded.Len() != 400 {
		t.Fatalf("Len = %d after post-load adds, want 400", loaded.Len())
	}
	// New vectors must actually be findable.
	target := randVec(rng, 8)
	loaded.Add(5000, target)
	res, err := loaded.Search(target, 1, 64)
	if err != nil || len(res) != 1 || res[0].ID != 5000 {
		t.Fatalf("self-search after post-load add = %+v, %v", res, err)
	}
}

func TestSnapshotRejectsCorruption(t *testing.T) {
	ix, _ := buildIndex(t, 50, 4)
	path := filepath.Join(t.TempDir(), "index.snap")
	if err := ix.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)

	bad := bytes.Clone(data)
	bad[0] ^= 0xff // magic
	if _, err := Load(bytes.NewReader(bad), Options{}); err == nil {
		t.Fatal("corrupt magic accepted")
	}
	if _, err := Load(bytes.NewReader(data[:len(data)/2]), Options{}); err == nil {
		t.Fatal("truncated snapshot accepted")
	}
	if _, err := LoadFile(filepath.Join(t.TempDir(), "missing.snap"), Options{}); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestSearchFilter(t *testing.T) {
	ix, vecs := buildIndex(t, 1000, 16)
	rng := rand.New(rand.NewSource(16))
	query := randVec(rng, 16)
	// Only even IDs allowed; results must respect it and still be the best
	// evens (checked against filtered brute force).
	res, err := ix.SearchFilter(query, 10, 200, func(id uint64) bool { return id%2 == 0 })
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("filter returned nothing")
	}
	for _, r := range res {
		if r.ID%2 != 0 {
			t.Fatalf("filter leaked id %d", r.ID)
		}
	}
	evens := map[uint64][]float32{}
	for id, v := range vecs {
		if id%2 == 0 {
			evens[id] = v
		}
	}
	want := bruteForce(L2, evens, query, 10)
	wantSet := map[uint64]bool{}
	for _, id := range want {
		wantSet[id] = true
	}
	hits := 0
	for _, r := range res {
		if wantSet[r.ID] {
			hits++
		}
	}
	if hits < 8 { // allow slight approximation loss
		t.Fatalf("filtered recall %d/10 against brute force", hits)
	}
}
