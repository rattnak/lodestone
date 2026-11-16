package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func openAppend(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, walFileName), os.O_APPEND|os.O_WRONLY, 0)
}

func openDB(t *testing.T, dir string, opts *Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	v, ok, err := db.Get([]byte(key))
	if err != nil || !ok || string(v) != want {
		t.Fatalf("Get(%s) = %q ok=%v err=%v, want %q", key, v, ok, err, want)
	}
}

func mustMiss(t *testing.T, db *DB, key string) {
	t.Helper()
	if v, ok, err := db.Get([]byte(key)); err != nil || ok {
		t.Fatalf("Get(%s) = %q ok=%v err=%v, want miss", key, v, ok, err)
	}
}

func TestEngineAcrossFlushBoundaries(t *testing.T) {
	db := openDB(t, t.TempDir(), nil)
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")

	// Overwrite and delete after the flush: memtable must shadow the table.
	db.Put([]byte("a"), []byte("one"))
	db.Delete([]byte("b"))
	mustGet(t, db, "a", "one")
	mustMiss(t, db, "b")

	// Flush again: now the newer table must shadow the older one.
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := db.TableCount(); n != 2 {
		t.Fatalf("TableCount = %d, want 2", n)
	}
	mustGet(t, db, "a", "one")
	mustMiss(t, db, "b")
	mustMiss(t, db, "never-written")
}

func TestEngineReopenReplaysWAL(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, nil)
	db.Put([]byte("flushed"), []byte("on-disk"))
	db.Flush()
	// These live only in the WAL — Close does not flush the memtable.
	db.Put([]byte("logged"), []byte("wal-only"))
	db.Delete([]byte("flushed"))
	db.Close()

	db = openDB(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "logged", "wal-only")
	mustMiss(t, db, "flushed") // the replayed tombstone shadows the SSTable
}

func TestEngineThresholdFlush(t *testing.T) {
	db := openDB(t, t.TempDir(), &Options{MemtableBytes: 1 << 10, SyncWrites: false})
	defer db.Close()
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%04d", i)), bytes.Repeat([]byte("v"), 50)); err != nil {
			t.Fatal(err)
		}
	}
	if db.TableCount() == 0 {
		t.Fatal("no flush despite exceeding the memtable threshold")
	}
	for i := 0; i < 200; i++ {
		mustGet(t, db, fmt.Sprintf("key-%04d", i), string(bytes.Repeat([]byte("v"), 50)))
	}
}

// Fuzz the full engine against a reference map, with random flushes and a
// reopen at the end: the engine must agree with the map at every step.
func TestEngineAgainstReferenceMap(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, &Options{MemtableBytes: 2 << 10, SyncWrites: false})
	ref := map[string]string{}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key-%03d", rng.Intn(400))
		switch rng.Intn(10) {
		case 0:
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(ref, key)
		case 1:
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		default:
			val := fmt.Sprintf("val-%d", i)
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			ref[key] = val
		}
		if i%500 == 0 {
			for k, want := range ref {
				mustGet(t, db, k, want)
			}
		}
	}
	db.Close()

	db = openDB(t, dir, nil)
	defer db.Close()
	for k, want := range ref {
		mustGet(t, db, k, want)
	}
	for i := 0; i < 400; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if _, deleted := ref[k]; !deleted {
			mustMiss(t, db, k)
		}
	}
}

func TestEngineRecoversFromTornWAL(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, nil)
	db.Put([]byte("safe"), []byte("synced"))
	db.Close()

	// Simulate a crash mid-append: garbage at the WAL tail.
	f, err := openAppend(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xba, 0xad})
	f.Close()

	db = openDB(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "safe", "synced")
	if err := db.Put([]byte("after"), []byte("recovery")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "after", "recovery")
}
