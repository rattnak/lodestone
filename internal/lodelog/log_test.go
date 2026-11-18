package lodelog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReadRoundtrip(t *testing.T) {
	l, err := Open(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Interleave appends across partitions; each partition's offsets must be
	// dense and independent.
	for i := 0; i < 30; i++ {
		part := i % 3
		off, err := l.Append(part, fmt.Appendf(nil, "p%d-msg%d", part, i))
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(i / 3); off != want {
			t.Fatalf("append %d to p%d got offset %d, want %d", i, part, off, want)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	for part := 0; part < 3; part++ {
		msgs, err := l.Read(part, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 10 {
			t.Fatalf("p%d has %d messages, want 10", part, len(msgs))
		}
		for j, m := range msgs {
			if m.Offset != uint64(j) {
				t.Fatalf("p%d msg %d offset %d", part, j, m.Offset)
			}
		}
	}
}

func TestReadFromOffsetAndTail(t *testing.T) {
	l, _ := Open(t.TempDir(), 1)
	defer l.Close()
	for i := 0; i < 20; i++ {
		l.Append(0, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()

	msgs, _ := l.Read(0, 15, 100)
	if len(msgs) != 5 || msgs[0].Offset != 15 {
		t.Fatalf("read from 15 = %d msgs starting %d", len(msgs), msgs[0].Offset)
	}
	// Reading at the end yields nothing, no error — the consumer polls again.
	if msgs, err := l.Read(0, 20, 10); err != nil || len(msgs) != 0 {
		t.Fatalf("read at end = %v %v", msgs, err)
	}
	// maxRecords caps the batch.
	if msgs, _ := l.Read(0, 0, 7); len(msgs) != 7 {
		t.Fatalf("max=7 returned %d", len(msgs))
	}
}

func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 2)
	for i := 0; i < 50; i++ {
		l.AppendSync(i%2, fmt.Appendf(nil, "durable-%d", i))
	}
	end0, _ := l.EndOffset(0)
	l.Close()

	l, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got, _ := l.EndOffset(0); got != end0 {
		t.Fatalf("end offset %d after reopen, want %d", got, end0)
	}
	msgs, _ := l.Read(0, 0, 100)
	if len(msgs) != 25 {
		t.Fatalf("recovered %d messages, want 25", len(msgs))
	}
	// Appends continue from the recovered offset.
	off, _ := l.Append(0, []byte("after-reopen"))
	if off != 25 {
		t.Fatalf("post-reopen append offset %d, want 25", off)
	}
}

func TestTornTailRecovered(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 1)
	for i := 0; i < 5; i++ {
		l.AppendSync(0, fmt.Appendf(nil, "msg-%d", i))
	}
	l.Close()

	// Simulate a crash mid-append: garbage at the tail.
	path := filepath.Join(dir, "part-0000.log")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x11, 0x22})
	f.Close()

	l, err := Open(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	msgs, _ := l.Read(0, 0, 100)
	if len(msgs) != 5 {
		t.Fatalf("recovered %d after torn tail, want 5", len(msgs))
	}
	// A clean append after truncation must land at offset 5.
	off, err := l.AppendSync(0, []byte("clean"))
	if err != nil || off != 5 {
		t.Fatalf("post-truncate append = %d, %v", off, err)
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	l, _ := Open(t.TempDir(), 1)
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			l.Append(0, fmt.Appendf(nil, "m%d", i))
		}
		l.Sync()
	}()
	for i := 0; i < 500; i++ {
		if _, err := l.Read(0, 0, 50); err != nil {
			t.Fatal(err)
		}
	}
	<-done
	msgs, _ := l.Read(0, 0, 5000)
	if len(msgs) != 2000 {
		t.Fatalf("final count %d, want 2000", len(msgs))
	}
}

func TestLogContentsAgainstReference(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 4)
	ref := map[int][][]byte{}
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 4000; i++ {
		part := rng.Intn(4)
		val := fmt.Appendf(nil, "val-%d-%d", part, i)
		l.Append(part, val)
		ref[part] = append(ref[part], val)
	}
	l.Sync()
	l.Close()

	l, _ = Open(dir, 4)
	defer l.Close()
	for part := 0; part < 4; part++ {
		msgs, _ := l.Read(part, 0, 100000)
		if len(msgs) != len(ref[part]) {
			t.Fatalf("p%d: %d msgs, want %d", part, len(msgs), len(ref[part]))
		}
		for j, m := range msgs {
			if !bytes.Equal(m.Value, ref[part][j]) {
				t.Fatalf("p%d msg %d mismatch", part, j)
			}
		}
	}
}
