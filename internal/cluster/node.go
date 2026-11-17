package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rattnak/lodestone/internal/raft"
)

// Node is a cluster member: it hosts the partition groups assigned to it,
// drives their Raft clocks, routes client requests to the owning partition,
// and reconciles group membership as the node set changes.
//
// Partition ownership is pure function of (partition, nodeSet, rf) via
// rendezvous hashing, so every node computes the same assignment with no
// coordination. The node set itself does need agreement; here it is seeded at
// start and extended by Join, with reconciliation converging each group's
// Raft membership toward the computed replica set. (A production system would
// replicate the node set through its own Raft group — a natural next step
// noted in the devlog.)
type Node struct {
	id         string // this node's address, host:port
	dir        string
	partitions uint32
	rf         int

	trans *grpcTransport

	mu     sync.RWMutex
	nodes  []string          // known cluster members (addresses)
	groups map[uint32]*Group // partition id -> group, for partitions we host
	// booted becomes true after the first reconcile. Before it, a bootstrap
	// node forms groups as a founding voter; after it (and always for
	// nodes that joined an existing cluster) fresh groups open as learners
	// that wait to be added by the partition's leader.
	bootstrap bool
	booted    bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// Options configures a cluster node.
type Options struct {
	ID         string   // this node's reachable address (host:port)
	Dir        string   // data directory
	Seeds      []string // other known members to contact at startup (may be empty)
	Partitions uint32   // fixed partition count; used only when bootstrapping
	RF         int      // replication factor (replicas per partition)
	// Bootstrap marks this node as part of the initial cluster formation,
	// so it forms its partitions as a founding voter. A node joining an
	// existing cluster leaves this false and is added to each partition by
	// that partition's current leader.
	Bootstrap bool
}

const (
	reconcileInterval = 500 * time.Millisecond
	compactThreshold  = 2000
	raftTick          = 20 * time.Millisecond
)

// NewNode creates (or reopens) a cluster node. It does not start serving
// until Run is called with a registered transport server.
func NewNode(opts Options) (*Node, error) {
	if opts.RF <= 0 {
		opts.RF = 3
	}
	if opts.Partitions == 0 {
		opts.Partitions = DefaultPartitions
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	n := &Node{
		id:         opts.ID,
		dir:        opts.Dir,
		partitions: opts.Partitions,
		rf:         opts.RF,
		bootstrap:  opts.Bootstrap,
		trans:      newGRPCTransport(),
		groups:     map[uint32]*Group{},
		stop:       make(chan struct{}),
	}
	// Seed the member set with self plus seeds; dedup and sort for
	// deterministic placement.
	set := map[string]bool{opts.ID: true}
	for _, s := range opts.Seeds {
		set[s] = true
	}
	for a := range set {
		n.nodes = append(n.nodes, a)
	}
	sort.Strings(n.nodes)
	return n, nil
}

// Transport exposes the Raft message handler for registration on the gRPC
// server (see RaftTransportServer).
func (n *Node) deliver(gid uint32, m raft.Message) {
	n.mu.RLock()
	g := n.groups[gid]
	n.mu.RUnlock()
	if g != nil {
		g.Step(m)
	}
}

// Run starts the node's background loops and returns immediately; Close stops
// them. The Raft ticker and the reconciliation loop run on separate
// goroutines: reconcile scans every partition and can be slow, and must never
// delay heartbeats — a stalled tick loop would let followers time out and
// trigger spurious elections.
func (n *Node) Run() {
	n.reconcile() // open our initial groups synchronously
	n.wg.Add(2)
	go n.tickLoop()
	go n.reconcileLoop()
}

func (n *Node) tickLoop() {
	defer n.wg.Done()
	tick := time.NewTicker(raftTick)
	defer tick.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-tick.C:
			n.tickGroups()
		}
	}
}

func (n *Node) reconcileLoop() {
	defer n.wg.Done()
	recon := time.NewTicker(reconcileInterval)
	defer recon.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-recon.C:
			n.reconcile()
			n.maybeCompact()
			n.sweepRemoved()
		}
	}
}

func (n *Node) tickGroups() {
	n.mu.RLock()
	gs := make([]*Group, 0, len(n.groups))
	for _, g := range n.groups {
		gs = append(gs, g)
	}
	n.mu.RUnlock()
	for _, g := range gs {
		g.Tick()
	}
}

func (n *Node) maybeCompact() {
	n.mu.RLock()
	gs := make([]*Group, 0, len(n.groups))
	for _, g := range n.groups {
		gs = append(gs, g)
	}
	n.mu.RUnlock()
	for _, g := range gs {
		_ = g.MaybeCompact(compactThreshold)
	}
}

// reconcile opens groups for partitions this node should host and, on the
// group leader, proposes membership changes to move each group's Raft voter
// set toward the computed replica set — one change at a time.
func (n *Node) reconcile() {
	n.mu.Lock()
	nodes := append([]string(nil), n.nodes...)
	n.mu.Unlock()

	for p := uint32(0); p < n.partitions; p++ {
		replicas := Replicas(p, nodes, n.rf)
		hostsSelf := false
		for _, r := range replicas {
			if r == n.id {
				hostsSelf = true
				break
			}
		}
		n.mu.RLock()
		g := n.groups[p]
		n.mu.RUnlock()

		if hostsSelf && g == nil {
			n.openGroup(p, replicas)
		} else if g != nil {
			n.reconcileMembership(g, replicas)
		}
	}
	n.mu.Lock()
	n.booted = true
	n.mu.Unlock()
}

func (n *Node) openGroup(p uint32, replicas []string) {
	// Found the group as a voter only during this node's initial bootstrap
	// pass; otherwise join as a learner and wait for the partition leader to
	// add us, so we never campaign against an established group.
	n.mu.RLock()
	founding := n.bootstrap && !n.booted
	n.mu.RUnlock()

	peers := []string{}
	if founding {
		for _, r := range replicas {
			if r != n.id {
				peers = append(peers, r)
			}
		}
	}
	dir := filepath.Join(n.dir, fmt.Sprintf("p%d", p))
	g, err := OpenGroup(p, dir, n.id, peers, !founding, n.trans)
	if err != nil {
		// A failed open is logged by the caller's environment; retried
		// next reconcile tick.
		return
	}
	n.mu.Lock()
	if _, exists := n.groups[p]; exists {
		n.mu.Unlock()
		g.Close()
		return
	}
	n.groups[p] = g
	n.mu.Unlock()
}

// reconcileMembership (leader only) steps the group's voting membership one
// member toward want: add a missing replica, or remove a surplus one once the
// replicas that will remain are caught up. Each ConfChange carries the full
// resulting set, so added nodes learn the whole membership from it.
func (n *Node) reconcileMembership(g *Group, want []string) {
	st := g.Status()
	if st.State != raft.Leader {
		return
	}
	rn := g.Raft()
	current := rn.Members()
	currentSet := map[string]bool{}
	for _, c := range current {
		currentSet[c] = true
	}
	wantSet := map[string]bool{}
	for _, w := range want {
		wantSet[w] = true
	}

	// Add the first wanted node not yet a voter.
	for _, w := range want {
		if !currentSet[w] {
			rn.ProposeConfChange(raft.ConfChange{Members: addTo(current, w)})
			return
		}
	}
	// Remove the first voter that should no longer host this partition, but
	// only once every replica that will stay is caught up (so the shrunken
	// quorum still holds all committed data).
	progress := rn.Progress()
	for _, c := range current {
		if wantSet[c] {
			continue
		}
		if !n.safeToRemove(st, progress, current, c) {
			return
		}
		rn.ProposeConfChange(raft.ConfChange{Members: removeFrom(current, c)})
		return
	}
}

func addTo(set []string, x string) []string {
	return append(append([]string(nil), set...), x)
}

func removeFrom(set []string, x string) []string {
	out := make([]string, 0, len(set))
	for _, s := range set {
		if s != x {
			out = append(out, s)
		}
	}
	return out
}

// safeToRemove requires that every voter staying in the group (besides the
// leader and the victim) is close enough to the leader's log that losing the
// victim cannot lose committed data.
func (n *Node) safeToRemove(st raft.Status, progress map[string]uint64, current []string, victim string) bool {
	const lagTolerance = 50
	for _, id := range current {
		if id == n.id || id == victim {
			continue
		}
		if progress[id]+lagTolerance < st.LastIndex {
			return false
		}
	}
	return true
}

// sweepRemoved closes and deletes any group whose Raft node has applied its
// own removal.
func (n *Node) sweepRemoved() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for p, g := range n.groups {
		if g.Status().Removed {
			g.Destroy()
			delete(n.groups, p)
		}
	}
}

// --- membership of the cluster itself ---

// AddMember inserts addr into the known node set (idempotent) and triggers
// reconciliation so placements shift toward including it.
func (n *Node) AddMember(addr string) []string {
	n.mu.Lock()
	for _, a := range n.nodes {
		if a == addr {
			nodes := append([]string(nil), n.nodes...)
			n.mu.Unlock()
			return nodes
		}
	}
	n.nodes = append(n.nodes, addr)
	sort.Strings(n.nodes)
	nodes := append([]string(nil), n.nodes...)
	n.mu.Unlock()
	go n.reconcile()
	return nodes
}

// Members returns the known cluster node addresses.
func (n *Node) Members() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]string(nil), n.nodes...)
}

// Partitions returns the fixed partition count.
func (n *Node) Partitions() uint32 { return n.partitions }

// HostedGroups returns the partitions this node currently hosts, sorted.
func (n *Node) HostedGroups() []*Group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	gs := make([]*Group, 0, len(n.groups))
	for _, g := range n.groups {
		gs = append(gs, g)
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].ID < gs[j].ID })
	return gs
}

// group returns the hosted group for partition p, or nil.
func (n *Node) group(p uint32) *Group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groups[p]
}

// leaderHint returns the address this node believes leads partition p, for
// redirecting clients. Empty if unknown.
func (n *Node) leaderHint(p uint32) string {
	if g := n.group(p); g != nil {
		return g.Status().LeaderID
	}
	return ""
}

// routeTargets returns addresses to try for partition p, best first: the
// believed leader (if this node hosts the group and sees one), then every
// replica that could host the partition as a fallback. Always including the
// replica set matters because a believed leader can be stale — relying on it
// alone would spin against a node that is no longer the leader. This is also
// what lets a node route to a partition it does not itself host, essential
// once rebalancing moves a partition off the node a client is talking to.
func (n *Node) routeTargets(p uint32) []string {
	n.mu.RLock()
	nodes := append([]string(nil), n.nodes...)
	n.mu.RUnlock()
	replicas := Replicas(p, nodes, n.rf)

	var hint string
	if g := n.group(p); g != nil {
		hint = g.Status().LeaderID
	}
	// Order: believed leader, then the computed replicas, then any other
	// member. The tail matters during rebalancing, when the partition's
	// actual Raft leader can briefly be a node the rendezvous set no longer
	// lists (a member being added before the outgoing one is removed).
	seen := map[string]bool{}
	var targets []string
	add := func(a string) {
		if a != "" && !seen[a] {
			seen[a] = true
			targets = append(targets, a)
		}
	}
	add(hint)
	for _, r := range replicas {
		add(r)
	}
	for _, m := range nodes {
		add(m)
	}
	return targets
}

func (n *Node) Close() error {
	close(n.stop)
	n.wg.Wait()
	n.trans.Close()
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, g := range n.groups {
		g.Close()
	}
	return nil
}

// context helper so callers don't import context just for a default timeout.
func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}
