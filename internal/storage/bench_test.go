package storage

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// Benchmarks against bbolt (a B-tree store) ground the LSM vs B-tree
// trade-off from DDIA ch 3: LSM turns random writes into sequential log
// appends, B-trees pay for writes with in-place page updates but read
// without consulting multiple tables.
//
//	go test ./internal/storage/ -bench . -benchtime 2s -run ^$

const benchValueSize = 100

func benchKey(i int) []byte { return []byte(fmt.Sprintf("key-%012d", i)) }

func benchValue(i int) []byte {
	v := make([]byte, benchValueSize)
	for j := range v {
		v[j] = byte(i + j)
	}
	return v
}

func BenchmarkLodestonePut(b *testing.B) {
	for _, sync := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync=%v", sync), func(b *testing.B) {
			db, err := Open(b.TempDir(), &Options{SyncWrites: sync})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Put(benchKey(i), benchValue(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBboltPut(b *testing.B) {
	for _, sync := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync=%v", sync), func(b *testing.B) {
			db, err := bolt.Open(filepath.Join(b.TempDir(), "bolt.db"), 0o600, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.NoSync = !sync
			db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("b"))
				return err
			})
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte("b")).Put(benchKey(i), benchValue(i))
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

const benchReadSetSize = 100_000

func BenchmarkLodestoneGet(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{SyncWrites: false})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < benchReadSetSize; i++ {
		db.Put(benchKey(i), benchValue(i))
	}
	db.Flush()
	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := db.Get(benchKey(rng.Intn(benchReadSetSize))); err != nil || !ok {
			b.Fatalf("miss: ok=%v err=%v", ok, err)
		}
	}
}

func BenchmarkLodestoneGetAbsent(b *testing.B) {
	// Misses are the LSM worst case; the bloom filter is what keeps them
	// from searching every table.
	db, err := Open(b.TempDir(), &Options{SyncWrites: false})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < benchReadSetSize; i++ {
		db.Put(benchKey(i), benchValue(i))
	}
	db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, _ := db.Get([]byte(fmt.Sprintf("absent-%d", i))); ok {
			b.Fatal("found a key that was never written")
		}
	}
}

func BenchmarkBboltGet(b *testing.B) {
	db, err := bolt.Open(filepath.Join(b.TempDir(), "bolt.db"), 0o600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.NoSync = true
	db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < benchReadSetSize; i++ {
			if err := bkt.Put(benchKey(i), benchValue(i)); err != nil {
				return err
			}
		}
		return nil
	})
	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *bolt.Tx) error {
			if v := tx.Bucket([]byte("b")).Get(benchKey(rng.Intn(benchReadSetSize))); v == nil {
				return fmt.Errorf("miss")
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
