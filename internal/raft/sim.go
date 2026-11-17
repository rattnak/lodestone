package raft

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
)

// Cluster is a deterministic in-process Raft cluster for tests and chaos
// runs: simulated time (ticks), simulated network (seeded delays, drops,
// partitions), and crash/restart with storage intact. Every behavior is a
// pure function of the seed, so a failing scenario replays exactly.
//
// The cluster doubles as a safety checker. Each node's state machine is a
// log of applied entries; the cluster records what every node applied at
// every index and flags two violation classes:
//
//   - divergence: two nodes apply different (term, data) at one index
//   - regression: one node applies different content at an index it (or
//     anyone) already applied — a lost committed entry
type Cluster struct {
	rng    *rand.Rand
	ids    []string
	nodes  map[string]*Node
	stores map[string]*MemoryStorage
	seeds  map[string]int64

	now      uint64
	seq      uint64
	queue    msgHeap
	dropRate float64
	maxDelay int // extra delivery delay in ticks, uniform [0, maxDelay]

	group map[string]int // partition group per id; all 0 = connected

	applied    map[string][]Entry
	appliedIdx map[string]uint64
	committed  map[uint64]Entry
	violations []string
}

type scheduledMsg struct {
	at  uint64
	seq uint64
	msg Message
}

type msgHeap []scheduledMsg

func (h msgHeap) Len() int { return len(h) }
func (h msgHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h msgHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *msgHeap) Push(x any)        { *h = append(*h, x.(scheduledMsg)) }
func (h *msgHeap) Pop() any {
	old := *h
	m := old[len(old)-1]
	*h = old[:len(old)-1]
	return m
}

type clusterTransport struct {
	c *Cluster
}

func (t clusterTransport) Send(m Message) { t.c.enqueue(m) }

// NewCluster builds n nodes named n1..nN, fully connected, with 1-tick
// message delay and no drops.
func NewCluster(seed int64, n int) *Cluster {
	c := &Cluster{
		rng:        rand.New(rand.NewSource(seed)),
		nodes:      map[string]*Node{},
		stores:     map[string]*MemoryStorage{},
		seeds:      map[string]int64{},
		group:      map[string]int{},
		applied:    map[string][]Entry{},
		appliedIdx: map[string]uint64{},
		committed:  map[uint64]Entry{},
		maxDelay:   1,
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.stores[id] = NewMemoryStorage()
		c.seeds[id] = c.rng.Int63()
		c.start(id)
	}
	return c
}

func (c *Cluster) peersOf(id string) []string {
	var ps []string
	for _, other := range c.ids {
		if other != id {
			ps = append(ps, other)
		}
	}
	return ps
}

// start boots (or reboots) a node from its storage.
func (c *Cluster) start(id string) {
	cfg := Config{
		ID:    id,
		Peers: c.peersOf(id),
		Store: c.stores[id],
		Trans: clusterTransport{c},
		Apply: func(e Entry) { c.recordApply(id, e) },
		Restore: func(data []byte) { c.restoreApply(id, data) },
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           rand.New(rand.NewSource(c.seeds[id] ^ int64(c.now))),
	}
	// A rebooted state machine starts empty; Restore or re-application
	// rebuilds it. Clearing here models that.
	c.applied[id] = nil
	c.appliedIdx[id] = 0
	n, err := NewNode(cfg)
	if err != nil {
		panic(err)
	}
	c.nodes[id] = n
}

// --- fault controls ---

// Crash removes the node (RAM lost), keeping its storage (disk intact).
func (c *Cluster) Crash(id string) { delete(c.nodes, id) }

// AddNode boots a brand-new node (empty storage) that bootstraps against the
// current members. The caller must still commit the membership change via
// ProposeConfChange on the leader for voting rights.
func (c *Cluster) AddNode(id string) {
	c.ids = append(c.ids, id)
	c.stores[id] = NewMemoryStorage()
	c.seeds[id] = c.rng.Int63()
	c.start(id)
}

// Restart reboots a crashed node from its persisted state.
func (c *Cluster) Restart(id string) {
	if _, up := c.nodes[id]; up {
		panic("restart of a running node: " + id)
	}
	c.start(id)
}

// Partition splits the network into the given groups; nodes in different
// groups cannot exchange messages. Unlisted nodes land in group -1 together.
func (c *Cluster) Partition(groups ...[]string) {
	c.group = map[string]int{}
	for _, id := range c.ids {
		c.group[id] = -1
	}
	for g, members := range groups {
		for _, id := range members {
			c.group[id] = g
		}
	}
}

// Heal reconnects everything.
func (c *Cluster) Heal() { c.group = map[string]int{} }

// SetDropRate makes every delivery independently fail with probability p.
func (c *Cluster) SetDropRate(p float64) { c.dropRate = p }

// SetMaxDelay sets the maximum extra delivery delay in ticks.
func (c *Cluster) SetMaxDelay(d int) { c.maxDelay = d }

// --- the clock ---

// RunTicks advances simulated time: each tick, every live node ticks (in
// stable order) and due messages are delivered.
func (c *Cluster) RunTicks(n int) {
	for i := 0; i < n; i++ {
		c.now++
		for _, id := range c.ids {
			if node, up := c.nodes[id]; up {
				node.Tick()
			}
		}
		c.deliverDue()
	}
}

func (c *Cluster) enqueue(m Message) {
	delay := uint64(1)
	if c.maxDelay > 1 {
		delay += uint64(c.rng.Intn(c.maxDelay))
	}
	c.seq++
	heap.Push(&c.queue, scheduledMsg{at: c.now + delay, seq: c.seq, msg: m})
}

func (c *Cluster) deliverDue() {
	for c.queue.Len() > 0 && c.queue[0].at <= c.now {
		sm := heap.Pop(&c.queue).(scheduledMsg)
		m := sm.msg
		to, up := c.nodes[m.To]
		if !up {
			continue // crashed nodes lose their mail
		}
		if len(c.group) > 0 && c.group[m.From] != c.group[m.To] {
			continue // partitioned
		}
		if c.dropRate > 0 && c.rng.Float64() < c.dropRate {
			continue
		}
		to.Step(m)
	}
}

// --- cluster queries ---

// Leader returns the single live leader at the highest term, or "" if there
// is none (or more than one, which flags a violation).
func (c *Cluster) Leader() string {
	var leaders []string
	maxTerm := uint64(0)
	for _, id := range c.ids {
		n, up := c.nodes[id]
		if !up {
			continue
		}
		st := n.Status()
		if st.State == Leader {
			leaders = append(leaders, id)
			if st.Term > maxTerm {
				maxTerm = st.Term
			}
		}
	}
	// Stale leaders in lower terms can linger briefly (they step down on
	// first contact); two leaders in one term is the real violation.
	byTerm := map[uint64][]string{}
	for _, id := range leaders {
		t := c.nodes[id].Status().Term
		byTerm[t] = append(byTerm[t], id)
	}
	for term, ls := range byTerm {
		if len(ls) > 1 {
			c.violations = append(c.violations,
				fmt.Sprintf("two leaders in term %d: %v", term, ls))
		}
	}
	for _, id := range leaders {
		if c.nodes[id].Status().Term == maxTerm {
			return id
		}
	}
	return ""
}

// WaitLeader runs until a leader exists (up to limit ticks).
func (c *Cluster) WaitLeader(limit int) string {
	for i := 0; i < limit; i += 5 {
		if id := c.Leader(); id != "" {
			return id
		}
		c.RunTicks(5)
	}
	return c.Leader()
}

// Propose submits data through the current leader; ok=false if there is no
// leader or it rejected the proposal.
func (c *Cluster) Propose(data []byte) bool {
	id := c.Leader()
	if id == "" {
		return false
	}
	_, _, ok := c.nodes[id].Propose(data)
	return ok
}

// Node returns the live node or nil.
func (c *Cluster) Node(id string) *Node { return c.nodes[id] }

// Applied returns what id's state machine holds, in order.
func (c *Cluster) Applied(id string) []Entry { return c.applied[id] }

// Violations lists every safety violation observed so far; empty means the
// run upheld Raft's guarantees.
func (c *Cluster) Violations() []string { return c.violations }

// --- the safety checker / harness state machine ---

func (c *Cluster) recordApply(id string, e Entry) {
	if want := c.appliedIdx[id] + 1; e.Index != want {
		c.violations = append(c.violations,
			fmt.Sprintf("%s applied index %d, expected %d (gap or replay)", id, e.Index, want))
	}
	c.appliedIdx[id] = e.Index
	// Leader no-ops and conf changes consume indexes but aren't commands.
	if e.Type == EntryNormal && len(e.Data) > 0 {
		c.applied[id] = append(c.applied[id], e)
	}

	if prev, seen := c.committed[e.Index]; seen {
		if prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data) {
			c.violations = append(c.violations,
				fmt.Sprintf("divergence at index %d: %s applied term=%d %q, cluster had term=%d %q",
					e.Index, id, e.Term, e.Data, prev.Term, prev.Data))
		}
	} else {
		c.committed[e.Index] = Entry{Index: e.Index, Term: e.Term, Data: append([]byte(nil), e.Data...)}
	}
}

func (c *Cluster) restoreApply(id string, data []byte) {
	snap, err := DecodeSnapshot(data)
	if err != nil {
		c.violations = append(c.violations, fmt.Sprintf("%s restored corrupt snapshot: %v", id, err))
		return
	}
	entries := snap.Entries
	c.applied[id] = entries
	c.appliedIdx[id] = snap.AppliedTo
	for _, e := range entries {
		if prev, seen := c.committed[e.Index]; seen {
			if prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data) {
				c.violations = append(c.violations,
					fmt.Sprintf("snapshot divergence at index %d on %s", e.Index, id))
			}
		}
	}
}

// SnapshotNode compacts id's log through everything it has applied, using
// the harness state machine as the snapshot content.
func (c *Cluster) SnapshotNode(id string) error {
	n, up := c.nodes[id]
	if !up {
		return fmt.Errorf("%s is down", id)
	}
	appliedTo := c.appliedIdx[id]
	if appliedTo == 0 {
		return nil
	}
	data, err := EncodeSnapshot(SnapshotState{AppliedTo: appliedTo, Entries: c.applied[id]})
	if err != nil {
		return err
	}
	return n.CompactTo(appliedTo, data)
}

// SnapshotState is the harness state machine's serialized form: AppliedTo is
// the highest log index covered (it can exceed the last entry's index when
// leader no-ops were applied after it).
type SnapshotState struct {
	AppliedTo uint64
	Entries   []Entry
}

func EncodeSnapshot(s SnapshotState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeSnapshot(data []byte) (SnapshotState, error) {
	var s SnapshotState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return SnapshotState{}, err
	}
	return s, nil
}

// AgreedPrefix returns the longest prefix applied by every live node —
// convergence means it equals every node's full applied log.
func (c *Cluster) AgreedPrefix() int {
	shortest := -1
	for _, id := range c.ids {
		if _, up := c.nodes[id]; !up {
			continue
		}
		if l := len(c.applied[id]); shortest < 0 || l < shortest {
			shortest = l
		}
	}
	if shortest < 0 {
		return 0
	}
	return shortest
}

// LiveIDs returns running node ids, sorted.
func (c *Cluster) LiveIDs() []string {
	var out []string
	for id := range c.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
