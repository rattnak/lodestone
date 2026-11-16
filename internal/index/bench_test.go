package index

import (
	"math/rand"
	"sync"
	"testing"
)

// The pairing that justifies HNSW: brute force is exact and O(n) per query;
// HNSW is approximate (recall measured in TestRecallAgainstBruteForce) and
// logarithmic.
//
//	go test ./internal/index/ -bench . -benchtime 2s -run ^$

const (
	benchN   = 20_000
	benchDim = 64
)

var benchSetup struct {
	once sync.Once
	ix   *Index
	vecs [][]float32
}

func benchIndex(b *testing.B) (*Index, [][]float32) {
	benchSetup.once.Do(func() {
		rng := rand.New(rand.NewSource(21))
		ix := New(Options{Distance: Cosine, Seed: 21})
		vecs := make([][]float32, benchN)
		for i := range vecs {
			vecs[i] = randVec(rng, benchDim)
			if err := ix.Add(uint64(i), vecs[i]); err != nil {
				b.Fatal(err)
			}
		}
		benchSetup.ix, benchSetup.vecs = ix, vecs
	})
	return benchSetup.ix, benchSetup.vecs
}

func BenchmarkHNSWSearch(b *testing.B) {
	ix, _ := benchIndex(b)
	rng := rand.New(rand.NewSource(22))
	queries := make([][]float32, 256)
	for i := range queries {
		queries[i] = randVec(rng, benchDim)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ix.Search(queries[i%len(queries)], 10, 64); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBruteForceSearch(b *testing.B) {
	_, vecs := benchIndex(b)
	rng := rand.New(rand.NewSource(22))
	queries := make([][]float32, 64)
	for i := range queries {
		queries[i] = randVec(rng, benchDim)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := queries[i%len(queries)]
		best, bestD := -1, float32(0)
		for j, v := range vecs {
			if d := Cosine(q, v); best < 0 || d < bestD {
				best, bestD = j, d
			}
		}
		_ = best
	}
}

func BenchmarkAdd(b *testing.B) {
	rng := rand.New(rand.NewSource(23))
	ix := New(Options{Distance: Cosine, Seed: 23})
	vecs := make([][]float32, b.N)
	for i := range vecs {
		vecs[i] = randVec(rng, benchDim)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ix.Add(uint64(i), vecs[i]); err != nil {
			b.Fatal(err)
		}
	}
}
