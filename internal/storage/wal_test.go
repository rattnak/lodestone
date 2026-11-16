package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func collect(t *testing.T, w *WAL) []Record {
	t.Helper()
	var out []Record
	err := w.Replay(func(r Record) error {
		out = append(out, Record{
			Op:    r.Op,
			Key:   bytes.Clone(r.Key),
			Value: bytes.Clone(r.Value),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out
}

func TestAppendReplayRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Op: OpPut, Key: []byte("alpha"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("beta"), Value: bytes.Repeat([]byte("x"), 10_000)},
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

	// Replay from the live handle, then again after reopen.
	for i := 0; i < 2; i++ {
		got := collect(t, w)
		if len(got) != len(want) {
			t.Fatalf("pass %d: replayed %d records, want %d", i, len(got), len(want))
		}
		for j := range want {
			if got[j].Op != want[j].Op || !bytes.Equal(got[j].Key, want[j].Key) || !bytes.Equal(got[j].Value, want[j].Value) {
				t.Fatalf("pass %d: record %d mismatch: got %+v want %+v", i, j, got[j], want[j])
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if w, err = OpenWAL(path); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
}

func TestAppendAfterReplayContinuesLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	w.Sync()
	collect(t, w) // replay must not corrupt the append position
	w.Append(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})
	w.Sync()
	w.Close()

	w, err = OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := collect(t, w)
	if len(got) != 2 || string(got[1].Key) != "b" {
		t.Fatalf("got %d records %v, want [a b]", len(got), got)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		w.Append(Record{Op: OpPut, Key: []byte{byte('a' + i)}, Value: []byte("v")})
	}
	w.Sync()
	goodSize := w.Size()
	w.Close()

	// Simulate a crash mid-write: garbage partial record at the tail.
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
		t.Fatalf("replayed %d records after torn tail, want 3", len(got))
	}
	if w.Size() != goodSize {
		t.Fatalf("size %d after reopen, want truncated to %d", w.Size(), goodSize)
	}
	// The log must accept appends cleanly after truncation.
	if err := w.Append(Record{Op: OpPut, Key: []byte("d"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, w); len(got) != 4 {
		t.Fatalf("replayed %d records after post-truncate append, want 4", len(got))
	}
}

func TestCorruptMiddleRecordStopsReplayAtPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("first"), Value: []byte("v")})
	w.Sync()
	firstSize := w.Size()
	w.Append(Record{Op: OpPut, Key: []byte("second"), Value: []byte("v")})
	w.Sync()
	w.Close()

	// Flip a byte inside the second record's payload.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[firstSize+walHeaderSize+2] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	w, err = OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := collect(t, w)
	if len(got) != 1 || string(got[0].Key) != "first" {
		t.Fatalf("got %v, want only the first record", got)
	}
	if w.Size() != firstSize {
		t.Fatalf("size %d, want %d", w.Size(), firstSize)
	}
}

// TestCrashRecoveryProperty simulates kill -9 at arbitrary byte offsets: for
// any truncation point, reopening must recover exactly a prefix of the synced
// records — never an error, never a partial or reordered record.
func TestCrashRecoveryProperty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "full.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	const n = 50
	var boundaries []int64 // end offset of each record
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
