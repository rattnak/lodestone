package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rattnak/lodestone/internal/raft"
)

// groupHarness drives one partition replicated across three in-process
// members with a real (threaded, lossless) transport and real WAL storage —
// the integration seam between cluster and raft, one level above the
// deterministic sim.
//
// The groups map is write-once during construction and read-only afterwards
// (ticker and transport goroutines start only after it is complete), so the
// harness needs no locking — and must not take any: the transport runs while
// raft's internal lock is held, so a harness lock would order against it.
type groupHarness struct {
	groups map[string]*Group // member id -> group
	stop   chan struct{}
	done   chan struct{}
}

type harnessTransport struct {
	h *groupHarness
}

func (t harnessTransport) SendGroup(gid uint32, m raft.Message) {
	g := t.h.groups[m.To]
	if g == nil {
		return
	}
	// Deliver asynchronously as a network would; Step is thread-safe.
	go g.Step(m)
}

func openHarness(t *testing.T, base string, members []string) *groupHarness {
	t.Helper()
	h := &groupHarness{groups: map[string]*Group{}, stop: make(chan struct{}), done: make(chan struct{})}
	for _, id := range members {
		peers := []string{}
		for _, other := range members {
			if other != id {
				peers = append(peers, other)
			}
		}
		g, err := OpenGroup(1, filepath.Join(base, id), id, peers, harnessTransport{h})
		if err != nil {
			t.Fatal(err)
		}
		h.groups[id] = g
	}
	go func() {
		defer close(h.done)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-tick.C:
				for _, g := range h.groups {
					g.Tick()
				}
			}
		}
	}()
	return h
}

func (h *groupHarness) shutdown() {
	close(h.stop)
	<-h.done
	for _, g := range h.groups {
		g.Close()
	}
}

func newGroupHarness(t *testing.T, members ...string) *groupHarness {
	t.Helper()
	h := openHarness(t, t.TempDir(), members)
	t.Cleanup(h.shutdown)
	return h
}

func (h *groupHarness) leader(t *testing.T) *Group {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, g := range h.groups {
			if g.Status().State == raft.Leader {
				return g
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no group leader elected")
	return nil
}

func TestGroupReplicatesKVAndDocs(t *testing.T) {
	h := newGroupHarness(t, "m1", "m2", "m3")
	lead := h.leader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := lead.Propose(ctx, Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := lead.Propose(ctx, Command{Op: OpUpsertDoc, DocID: 7, Vector: []float32{1, 2, 3}, Payload: []byte("doc7")}); err != nil {
		t.Fatal(err)
	}

	v, ok, err := lead.Get(ctx, []byte("k"))
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	hits, err := lead.Search(ctx, []float32{1, 2, 3}, 1, 0)
	if err != nil || len(hits) != 1 || hits[0].ID != 7 {
		t.Fatalf("Search = %+v, %v", hits, err)
	}
	if p, ok := lead.Payload(7); !ok || string(p) != "doc7" {
		t.Fatalf("Payload = %q %v", p, ok)
	}

	// Followers converge (eventually readable directly from their DBs).
	deadline := time.Now().Add(5 * time.Second)
	for id, g := range h.groups {
		for {
			v, ok, _ := g.DB().Get([]byte("k"))
			if ok && string(v) == "v" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("follower %s never converged", id)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestGroupFollowerRejectsAndHints(t *testing.T) {
	h := newGroupHarness(t, "m1", "m2", "m3")
	lead := h.leader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var follower *Group
	for _, g := range h.groups {
		if g != lead {
			follower = g
			break
		}
	}

	// The follower may briefly not know the leader yet; retry until the
	// hint names it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := follower.Propose(ctx, Command{Op: OpPut, Key: []byte("x"), Value: []byte("y")})
		var nl ErrNotLeader
		if !errors.As(err, &nl) {
			t.Fatalf("follower Propose error = %v, want ErrNotLeader", err)
		}
		if nl.LeaderID == lead.Status().ID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader hint = %q, want %q", nl.LeaderID, lead.Status().ID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var nl ErrNotLeader
	if _, _, err := follower.Get(ctx, []byte("x")); !errors.As(err, &nl) {
		t.Fatalf("follower Get error, want ErrNotLeader")
	}
}

func TestGroupRestartRecoversFromRaftLog(t *testing.T) {
	base := t.TempDir()
	members := []string{"m1", "m2", "m3"}
	h := openHarness(t, base, members)

	lead := h.leader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		if err := lead.Propose(ctx, Command{Op: OpPut, Key: fmt.Appendf(nil, "k%d", i), Value: fmt.Appendf(nil, "v%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	// Hard-stop everything (engine is unsynced; the raft WAL is the
	// durability story) and reopen from disk.
	h.shutdown()
	h2 := openHarness(t, base, members)
	t.Cleanup(h2.shutdown)

	lead2 := h2.leader(t)
	// A fresh leader's read lease needs a round of follower acks; wait for
	// the first read to clear, then the rest must hold.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok, err := lead2.Get(ctx, []byte("k0")); err == nil && ok {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("read lease never established after restart: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		v, ok, err := lead2.Get(ctx, fmt.Appendf(nil, "k%d", i))
		if err != nil || !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("after restart, Get(k%d) = %q %v %v", i, v, ok, err)
		}
	}
}
