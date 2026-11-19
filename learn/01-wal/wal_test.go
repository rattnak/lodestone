package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// collect replays the whole log into a slice (copying, since fn's slices may
// alias a per-record buffer).
func collect(t *testing.T, w *WAL) []Record {
	t.Helper()
	var out []Record
	err := w.Replay(func(r Record) error {
		out = append(out, Record{Op: r.Op, Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value)})
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out
}

func eq(a, b Record) bool {
	return a.Op == b.Op && bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

// 1. Round-trip: append, sync, replay — same records, in order, including
//    empty keys and nil values.
func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	want := []Record{
		{Op: OpPut, Key: []byte("alpha"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("beta"), Value: bytes.Repeat([]byte("x"), 5000)},
		{Op: OpDelete, Key: []byte("alpha")},
		{Op: OpPut, Key: []byte(""), Value: []byte("empty key is legal")},
		{Op: OpPut, Key: []byte("gamma"), Value: nil},
	}
	for _, r := range want {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	got := collect(t, w)
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !eq(got[i], want[i]) {
			t.Fatalf("record %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// 2. Durability across reopen, and appends continue afterward.
func TestReopenAndContinue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _ := OpenWAL(path)
	w.Append(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	w.Append(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := collect(t, w); len(got) != 2 || string(got[1].Key) != "b" {
		t.Fatalf("after reopen got %d records %v", len(got), got)
	}
	// Appends continue from the recovered end.
	w.Append(Record{Op: OpPut, Key: []byte("c"), Value: []byte("3")})
	w.Sync()
	if got := collect(t, w); len(got) != 3 || string(got[2].Key) != "c" {
		t.Fatalf("after continue got %d records", len(got))
	}
}

// Replaying must not disturb the append position.
func TestReplayThenAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _ := OpenWAL(path)
	defer w.Close()
	w.Append(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	w.Sync()
	collect(t, w) // replay in the middle
	w.Append(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})
	w.Sync()
	if got := collect(t, w); len(got) != 2 {
		t.Fatalf("got %d records after replay-then-append, want 2", len(got))
	}
}

// 3. Torn tail: garbage appended after a crash is discarded on reopen, and the
//    log is usable again.
func TestTornTailRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _ := OpenWAL(path)
	for i := 0; i < 3; i++ {
		w.Append(Record{Op: OpPut, Key: []byte{byte('a' + i)}, Value: []byte("v")})
	}
	w.Sync()
	good := w.Size()
	w.Close()

	// Simulate a crash mid-append: a partial record's worth of garbage.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x99})
	f.Close()

	w, err = OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := collect(t, w); len(got) != 3 {
		t.Fatalf("recovered %d records after torn tail, want 3", len(got))
	}
	if w.Size() != good {
		t.Fatalf("size %d after reopen, want truncated to %d", w.Size(), good)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("d"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	if got := collect(t, w); len(got) != 4 {
		t.Fatalf("after post-truncate append got %d records, want 4", len(got))
	}
}

// 4. The property test: a crash can cut a synced log at ANY byte offset.
//    Reopening must always recover exactly a prefix of the synced records —
//    never an error, never a partial or reordered record.
func TestCrashRecoveryProperty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "full.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	const n = 50
	var boundaries []int64 // byte offset of the end of each record
	for i := 0; i < n; i++ {
		rec := Record{
			Op:    OpPut,
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: bytes.Repeat([]byte{byte(i)}, rng.Intn(200)),
		}
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
		boundaries = append(boundaries, w.Size())
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	recordsIntactAt := func(cut int64) int {
		k := 0
		for k < n && boundaries[k] <= cut {
			k++
		}
		return k
	}

	for trial := 0; trial < 200; trial++ {
		cut := int64(rng.Intn(len(full) + 1))
		crashPath := filepath.Join(t.TempDir(), "crash.wal")
		if err := os.WriteFile(crashPath, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		cw, err := OpenWAL(crashPath)
		if err != nil {
			t.Fatalf("cut=%d: open after crash: %v", cut, err)
		}
		got := collect(t, cw)
		cw.Close()
		want := recordsIntactAt(cut)
		if len(got) != want {
			t.Fatalf("cut=%d: recovered %d records, want %d", cut, len(got), want)
		}
		for j, r := range got {
			if string(r.Key) != fmt.Sprintf("key-%03d", j) {
				t.Fatalf("cut=%d: record %d has key %q — not a clean prefix", cut, j, r.Key)
			}
		}
	}
}
