package lodelog

import (
	"fmt"
	"testing"
)

func TestConsumerStreamsAndCommits(t *testing.T) {
	l, _ := Open(t.TempDir(), 2)
	defer l.Close()
	for i := 0; i < 20; i++ {
		l.Append(i%2, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()

	c, err := NewConsumer(l, "embedder")
	if err != nil {
		t.Fatal(err)
	}
	// Poll advances a live cursor; commit records progress.
	var seen int
	for {
		msgs, err := c.Poll(0, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		seen += len(msgs)
		c.Commit(0, msgs[len(msgs)-1].Offset+1)
	}
	if seen != 10 {
		t.Fatalf("consumed %d from p0, want 10", seen)
	}
	if c.Committed(0) != 10 {
		t.Fatalf("committed %d, want 10", c.Committed(0))
	}
}

func TestConsumerResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 1)
	for i := 0; i < 30; i++ {
		l.Append(0, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()

	c, _ := NewConsumer(l, "g1")
	msgs, _ := c.Poll(0, 10)
	c.Commit(0, msgs[len(msgs)-1].Offset+1) // committed through offset 9
	l.Close()

	// A new process with the same group resumes where it left off.
	l2, _ := Open(dir, 1)
	defer l2.Close()
	c2, _ := NewConsumer(l2, "g1")
	if c2.Committed(0) != 10 {
		t.Fatalf("resumed at %d, want 10", c2.Committed(0))
	}
	msgs, _ = c2.Poll(0, 100)
	if len(msgs) != 20 || msgs[0].Offset != 10 {
		t.Fatalf("resumed poll got %d msgs starting %d", len(msgs), msgs[0].Offset)
	}
}

func TestAtLeastOnceReplaysUncommitted(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 1)
	for i := 0; i < 10; i++ {
		l.Append(0, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()

	// Consumer polls and "handles" but crashes before committing.
	c, _ := NewConsumer(l, "g")
	c.Poll(0, 6) // handled 0..5, no commit
	l.Close()

	// After restart the uncommitted batch is redelivered — at-least-once.
	l2, _ := Open(dir, 1)
	defer l2.Close()
	c2, _ := NewConsumer(l2, "g")
	msgs, _ := c2.Poll(0, 100)
	if len(msgs) != 10 || msgs[0].Offset != 0 {
		t.Fatalf("expected full replay from 0, got %d msgs starting %d", len(msgs), msgs[0].Offset)
	}
}

func TestSeekReplaysPartition(t *testing.T) {
	l, _ := Open(t.TempDir(), 1)
	defer l.Close()
	for i := 0; i < 15; i++ {
		l.Append(0, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()
	c, _ := NewConsumer(l, "batch")
	c.Poll(0, 15)
	c.Commit(0, 15)

	// Seek back to 0 — the batch re-index job's replay-from-scratch.
	if err := c.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	msgs, _ := c.Poll(0, 100)
	if len(msgs) != 15 || msgs[0].Offset != 0 {
		t.Fatalf("after seek got %d msgs starting %d", len(msgs), msgs[0].Offset)
	}
}

func TestIndependentGroups(t *testing.T) {
	l, _ := Open(t.TempDir(), 1)
	defer l.Close()
	for i := 0; i < 10; i++ {
		l.Append(0, fmt.Appendf(nil, "m%d", i))
	}
	l.Sync()
	a, _ := NewConsumer(l, "a")
	b, _ := NewConsumer(l, "b")
	a.Poll(0, 10)
	a.Commit(0, 10)
	// b's offsets are untouched by a.
	if b.Committed(0) != 0 {
		t.Fatalf("group b committed %d, want 0", b.Committed(0))
	}
	msgs, _ := b.Poll(0, 100)
	if len(msgs) != 10 {
		t.Fatalf("group b read %d, want 10", len(msgs))
	}
}
