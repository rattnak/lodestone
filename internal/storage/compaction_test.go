package storage

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestCompactMergesToOneTable(t *testing.T) {
	db := openDB(t, t.TempDir(), &Options{SyncWrites: false, CompactAtTables: 100})
	defer db.Close()

	// Three generations of the same keys across three tables.
	for gen := 0; gen < 3; gen++ {
		for i := 0; i < 50; i++ {
			db.Put([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("gen-%d", gen)))
		}
		db.Flush()
	}
	db.Delete([]byte("key-07"))
	db.Flush()
	if n := db.TableCount(); n != 4 {
		t.Fatalf("TableCount = %d before compaction, want 4", n)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := db.TableCount(); n != 1 {
		t.Fatalf("TableCount = %d after compaction, want 1", n)
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if i == 7 {
			mustMiss(t, db, key)
			continue
		}
		mustGet(t, db, key, "gen-2") // only the newest version survives
	}

	// The merged table holds one version per key: 50 keys, one a tombstone.
	db.mu.RLock()
	count := db.tables[0].Count()
	db.mu.RUnlock()
	if count != 50 {
		t.Fatalf("merged table has %d entries, want 50 (one per key)", count)
	}
}

func TestCompactionSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, &Options{SyncWrites: false, CompactAtTables: 100})
	for i := 0; i < 30; i++ {
		db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("old"))
	}
	db.Flush()
	db.Delete([]byte("k00"))
	db.Put([]byte("k01"), []byte("new"))
	db.Flush()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db = openDB(t, dir, nil)
	defer db.Close()
	mustMiss(t, db, "k00") // the kept tombstone still shadows on reopen
	mustGet(t, db, "k01", "new")
	mustGet(t, db, "k29", "old")
}

func TestAutoCompactionTriggers(t *testing.T) {
	db := openDB(t, t.TempDir(), &Options{
		MemtableBytes:   1 << 10,
		SyncWrites:      false,
		CompactAtTables: 3,
	})
	defer db.Close()
	for i := 0; i < 2000; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%05d", i)), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if n := db.TableCount(); n >= 3 {
		t.Fatalf("TableCount = %d, auto-compaction never ran", n)
	}
	for i := 0; i < 2000; i++ {
		mustGet(t, db, fmt.Sprintf("key-%05d", i), fmt.Sprintf("val-%d", i))
	}
}

func TestCompactionAgainstReferenceMap(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, &Options{MemtableBytes: 2 << 10, SyncWrites: false, CompactAtTables: 3})
	ref := map[string]string{}
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 8000; i++ {
		key := fmt.Sprintf("key-%03d", rng.Intn(300))
		switch rng.Intn(12) {
		case 0:
			db.Delete([]byte(key))
			delete(ref, key)
		case 1:
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
		default:
			val := fmt.Sprintf("val-%d", i)
			db.Put([]byte(key), []byte(val))
			ref[key] = val
		}
	}
	db.Close()

	db = openDB(t, dir, nil)
	defer db.Close()
	for k, want := range ref {
		mustGet(t, db, k, want)
	}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if _, present := ref[k]; !present {
			mustMiss(t, db, k)
		}
	}
}
