package index

import (
	"math/rand"
	"sort"
	"testing"
)

func randVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

// bruteForce returns exact top-k IDs by the given distance.
func bruteForce(dist Distance, vecs map[uint64][]float32, query []float32, k int) []uint64 {
	type pair struct {
		id uint64
		d  float32
	}
	all := make([]pair, 0, len(vecs))
	for id, v := range vecs {
		all = append(all, pair{id, dist(query, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].d < all[j].d })
	if k > len(all) {
		k = len(all)
	}
	ids := make([]uint64, k)
	for i := range ids {
		ids[i] = all[i].id
	}
	return ids
}

func TestExactNeighborsOnTinySet(t *testing.T) {
	ix := New(Options{Distance: L2, Seed: 1})
	// Four well-separated points on a line.
	for i, x := range []float32{0, 10, 20, 30} {
		if err := ix.Add(uint64(i), []float32{x, 0}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := ix.Search([]float32{11, 0}, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || res[0].ID != 1 || res[1].ID != 2 {
		t.Fatalf("Search near 11 = %+v, want ids [1 2]", res)
	}
	if res[0].Score >= res[1].Score {
		t.Fatalf("results not sorted by distance: %+v", res)
	}
}

func TestEdgeCases(t *testing.T) {
	ix := New(Options{Seed: 1})
	if res, err := ix.Search([]float32{1}, 5, 0); err != nil || res != nil {
		t.Fatalf("empty index: %v %v", res, err)
	}
	ix.Add(1, []float32{1, 0, 0})
	// k larger than the collection.
	res, err := ix.Search([]float32{1, 0, 0}, 10, 0)
	if err != nil || len(res) != 1 || res[0].ID != 1 {
		t.Fatalf("k>n: %+v, %v", res, err)
	}
	// Dimension mismatches must error, not corrupt.
	if err := ix.Add(2, []float32{1, 0}); err == nil {
		t.Fatal("dimension mismatch on Add accepted")
	}
	if _, err := ix.Search([]float32{1, 0}, 1, 0); err == nil {
		t.Fatal("dimension mismatch on Search accepted")
	}
	if err := ix.Add(1, []float32{0, 1, 0}); err == nil {
		t.Fatal("duplicate id accepted")
	}
}

func TestDeleteExcludesFromResults(t *testing.T) {
	ix := New(Options{Distance: L2, Seed: 2})
	rng := rand.New(rand.NewSource(3))
	vecs := map[uint64][]float32{}
	for i := uint64(0); i < 500; i++ {
		v := randVec(rng, 16)
		vecs[i] = v
		ix.Add(i, v)
	}
	for i := uint64(0); i < 500; i += 2 {
		if err := ix.Delete(i); err != nil {
			t.Fatal(err)
		}
	}
	if ix.Len() != 250 {
		t.Fatalf("Len = %d after deletes, want 250", ix.Len())
	}
	if err := ix.Delete(0); err == nil {
		t.Fatal("double delete accepted")
	}
	for q := 0; q < 20; q++ {
		res, err := ix.Search(randVec(rng, 16), 10, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res {
			if r.ID%2 == 0 {
				t.Fatalf("deleted id %d returned from search", r.ID)
			}
		}
	}
}

func TestRecallAgainstBruteForce(t *testing.T) {
	const (
		n       = 2000
		dim     = 32
		queries = 100
		k       = 10
	)
	for _, metric := range []struct {
		name string
		d    Distance
	}{{"l2", L2}, {"cosine", Cosine}} {
		t.Run(metric.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			ix := New(Options{Distance: metric.d, Seed: 7})
			vecs := map[uint64][]float32{}
			for i := uint64(0); i < n; i++ {
				v := randVec(rng, dim)
				vecs[i] = v
				if err := ix.Add(i, v); err != nil {
					t.Fatal(err)
				}
			}
			hits, total := 0, 0
			for q := 0; q < queries; q++ {
				query := randVec(rng, dim)
				want := bruteForce(metric.d, vecs, query, k)
				got, err := ix.Search(query, k, 64)
				if err != nil {
					t.Fatal(err)
				}
				wantSet := map[uint64]bool{}
				for _, id := range want {
					wantSet[id] = true
				}
				for _, r := range got {
					if wantSet[r.ID] {
						hits++
					}
				}
				total += k
			}
			recall := float64(hits) / float64(total)
			t.Logf("recall@%d = %.3f over %d queries (n=%d, dim=%d)", k, recall, queries, n, dim)
			if recall < 0.9 {
				t.Fatalf("recall@%d = %.3f, want >= 0.9", k, recall)
			}
		})
	}
}

func TestConcurrentSearchDuringAdds(t *testing.T) {
	ix := New(Options{Distance: L2})
	rng := rand.New(rand.NewSource(5))
	seed := make([][]float32, 2000)
	for i := range seed {
		seed[i] = randVec(rng, 8)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i, v := range seed {
			ix.Add(uint64(i), v)
		}
	}()
	qrng := rand.New(rand.NewSource(6))
	for i := 0; i < 500; i++ {
		if _, err := ix.Search(randVec(qrng, 8), 5, 32); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}
