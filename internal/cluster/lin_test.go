package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/rattnak/lodestone/internal/raft"
)

// This is the capstone correctness test: it runs a real replicated key-value
// register across three nodes, hammers it with concurrent clients while
// injecting leader-killing faults, records the exact history of every
// operation, and checks with porcupine that the history is linearizable —
// that some single sequential order of the operations is consistent with
// every client's observations. This is the Jepsen-style property that finds
// the subtle consensus bugs example-based tests miss.

// --- a fault-injecting, partitionable in-process cluster ---

type linHarness struct {
	t       *testing.T
	base    string
	members []string

	mu     sync.RWMutex
	groups map[string]*Group
	paused map[string]bool  // "crashed": not ticked, mail dropped
	part   map[string]int   // partition group id per node; equal => connected

	stop chan struct{}
	wg   sync.WaitGroup
}

func newLinHarness(t *testing.T, members ...string) *linHarness {
	h := &linHarness{
		t: t, base: t.TempDir(), members: members,
		groups: map[string]*Group{}, paused: map[string]bool{}, part: map[string]int{},
		stop: make(chan struct{}),
	}
	for _, id := range members {
		h.openMember(id)
	}
	h.wg.Add(1)
	go h.tickLoop()
	t.Cleanup(func() {
		close(h.stop)
		h.wg.Wait()
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, g := range h.groups {
			g.Close()
		}
	})
	return h
}

func (h *linHarness) openMember(id string) {
	peers := []string{}
	for _, m := range h.members {
		if m != id {
			peers = append(peers, m)
		}
	}
	g, err := OpenGroup(1, filepath.Join(h.base, id), id, peers, false, h)
	if err != nil {
		h.t.Fatal(err)
	}
	h.groups[id] = g
}

// SendGroup is the transport: deliver unless the target is paused or the two
// nodes are on different sides of a partition.
func (h *linHarness) SendGroup(gid uint32, m raft.Message) {
	h.mu.RLock()
	g := h.groups[m.To]
	drop := h.paused[m.To] || h.paused[m.From] || (len(h.part) > 0 && h.part[m.From] != h.part[m.To])
	h.mu.RUnlock()
	if g == nil || drop {
		return
	}
	go g.Step(m)
}

func (h *linHarness) tickLoop() {
	defer h.wg.Done()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-tick.C:
			h.mu.RLock()
			var live []*Group
			for id, g := range h.groups {
				if !h.paused[id] {
					live = append(live, g)
				}
			}
			h.mu.RUnlock()
			for _, g := range live {
				g.Tick()
			}
		}
	}
}

func (h *linHarness) pause(id string, down bool) {
	h.mu.Lock()
	h.paused[id] = down
	h.mu.Unlock()
}

func (h *linHarness) partition(sides ...[]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.part = map[string]int{}
	for i, side := range sides {
		for _, id := range side {
			h.part[id] = i + 1
		}
	}
}

func (h *linHarness) heal() {
	h.mu.Lock()
	h.part = map[string]int{}
	h.mu.Unlock()
}

func (h *linHarness) liveGroups() []*Group {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var gs []*Group
	for id, g := range h.groups {
		if !h.paused[id] {
			gs = append(gs, g)
		}
	}
	return gs
}

// put proposes key=value through whichever live node is the leader, retrying
// the same value until it commits or the deadline passes. Retrying the same
// value keeps the recorded operation linearizable even if an earlier attempt
// secretly committed.
func (h *linHarness) put(ctx context.Context, key, value string) bool {
	for ctx.Err() == nil {
		for _, g := range h.liveGroups() {
			if g.Status().State != raft.Leader {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := g.Propose(cctx, Command{Op: OpPut, Key: []byte(key), Value: []byte(value)})
			cancel()
			if err == nil {
				return true
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return false
}

// get performs a linearizable read via the leader, retrying until one serves.
func (h *linHarness) get(ctx context.Context, key string) (string, bool) {
	for ctx.Err() == nil {
		for _, g := range h.liveGroups() {
			if g.Status().State != raft.Leader {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			v, _, err := g.Get(cctx, []byte(key))
			cancel()
			if err == nil {
				return string(v), true
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return "", false
}

func (h *linHarness) waitLeader(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, g := range h.liveGroups() {
			if g.Status().State == raft.Leader {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// --- the porcupine key-value register model ---

type kvInput struct {
	op    string // "put" | "get"
	key   string
	value string
}

var kvModel = porcupine.Model{
	// Partition by key so porcupine checks each register independently —
	// the difference between tractable and exponential.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			m[k] = append(m[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range m {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "" }, // empty register
	Step: func(state, input, output any) (bool, any) {
		in := input.(kvInput)
		st := state.(string)
		switch in.op {
		case "put":
			return true, in.value
		default: // get: valid iff it returned the current value
			return st == output.(string), st
		}
	},
	Equal: func(a, b any) bool { return a.(string) == b.(string) },
}

func TestLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("linearizability run is slow; skipped under -short")
	}
	h := newLinHarness(t, "a", "b", "c")
	if !h.waitLeader(10 * time.Second) {
		t.Fatal("no initial leader")
	}

	const (
		clients  = 4
		duration = 6 * time.Second
		keys     = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var ops []porcupine.Operation
	var opsMu sync.Mutex
	var seq atomic.Int64
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	// Fault injector: alternately isolate the current leader (minority side)
	// and heal, forcing repeated leader changes while a quorum survives.
	var fault sync.WaitGroup
	fault.Add(1)
	go func() {
		defer fault.Done()
		rng := rand.New(rand.NewSource(99))
		for ctx.Err() == nil {
			time.Sleep(time.Duration(300+rng.Intn(400)) * time.Millisecond)
			var leader string
			for _, g := range h.liveGroups() {
				if g.Status().State == raft.Leader {
					leader = g.Status().ID
				}
			}
			if leader == "" {
				continue
			}
			switch rng.Intn(2) {
			case 0: // isolate the leader (it's a minority of 1 of 3)
				others := []string{}
				for _, m := range h.members {
					if m != leader {
						others = append(others, m)
					}
				}
				h.partition([]string{leader}, others)
				time.Sleep(time.Duration(200+rng.Intn(300)) * time.Millisecond)
				h.heal()
			case 1: // pause the leader briefly (crash-like)
				h.pause(leader, true)
				time.Sleep(time.Duration(200+rng.Intn(300)) * time.Millisecond)
				h.pause(leader, false)
			}
		}
	}()

	// Concurrent clients issuing puts and gets against a small key set.
	var clientsWG sync.WaitGroup
	for c := 0; c < clients; c++ {
		clientsWG.Add(1)
		go func(cid int) {
			defer clientsWG.Done()
			rng := rand.New(rand.NewSource(int64(cid) + 1))
			n := 0
			for ctx.Err() == nil {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				if rng.Intn(2) == 0 {
					val := fmt.Sprintf("c%d-%d", cid, n)
					n++
					call := now()
					if h.put(ctx, key, val) {
						record(&opsMu, &ops, cid, kvInput{"put", key, val}, "", call, now())
					}
				} else {
					call := now()
					if v, ok := h.get(ctx, key); ok {
						record(&opsMu, &ops, cid, kvInput{"get", key, ""}, v, call, now())
					}
				}
				_ = seq.Add(1)
			}
		}(c)
	}

	clientsWG.Wait()
	cancel()
	fault.Wait()

	if len(ops) < 20 {
		t.Fatalf("only %d operations completed — the workload barely ran", len(ops))
	}
	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, 10*time.Second)
	switch res {
	case porcupine.Ok:
		t.Logf("linearizable: %d operations across faults verified", len(ops))
	case porcupine.Illegal:
		t.Fatalf("LINEARIZABILITY VIOLATION over %d operations: %+v", len(ops), info)
	case porcupine.Unknown:
		t.Fatalf("porcupine timed out checking %d operations", len(ops))
	}
}

// TestLinearizabilityCheckerCatchesViolation is a negative control: it hands
// porcupine a history that is obviously NOT linearizable and asserts the
// checker rejects it. Without this, a passing TestLinearizableUnderFaults
// could mean "the system is correct" or "the checker never says no" — this
// pins it to the former.
func TestLinearizabilityCheckerCatchesViolation(t *testing.T) {
	// put k=A, then (strictly later) put k=B, then (strictly later) a get
	// that returns A. With no overlap the order is forced A→B→get, so the
	// get must observe B; returning A is a stale read — illegal.
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{"put", "k", "A"}, Output: "", Call: 0, Return: 10},
		{ClientId: 0, Input: kvInput{"put", "k", "B"}, Output: "", Call: 20, Return: 30},
		{ClientId: 1, Input: kvInput{"get", "k", ""}, Output: "A", Call: 40, Return: 50},
	}
	if res := porcupine.CheckOperations(kvModel, ops); res {
		t.Fatal("checker accepted a stale read — the linearizability test is vacuous")
	}

	// Sanity: the same history with the get returning B is linearizable.
	ops[2].Output = "B"
	if !porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("checker rejected a valid history")
	}
}

func record(mu *sync.Mutex, ops *[]porcupine.Operation, client int, in kvInput, out string, call, ret int64) {
	mu.Lock()
	*ops = append(*ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
	mu.Unlock()
}
