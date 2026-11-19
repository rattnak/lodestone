package raft

import (
	"bytes"
	"fmt"
	"testing"
)

func noViolations(t *testing.T, c *Cluster) {
	t.Helper()
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("safety violations:\n%s", v)
	}
}

func TestLeaderElection(t *testing.T) {
	c := NewCluster(1, 3)
	leader := c.WaitLeader(200)
	if leader == "" {
		t.Fatal("no leader elected")
	}
	// Stability: with no faults, the leader should hold.
	c.RunTicks(50)
	if got := c.Leader(); got != leader {
		t.Fatalf("leader changed from %s to %s without faults", leader, got)
	}
	noViolations(t, c)
}

func TestReelectionAfterLeaderCrash(t *testing.T) {
	c := NewCluster(2, 3)
	first := c.WaitLeader(200)
	if first == "" {
		t.Fatal("no initial leader")
	}
	c.Crash(first)
	second := c.WaitLeader(300)
	if second == "" || second == first {
		t.Fatalf("no new leader after crash (got %q)", second)
	}
	noViolations(t, c)
}

func TestReplicationConvergence(t *testing.T) {
	c := NewCluster(3, 3)
	if c.WaitLeader(200) == "" {
		t.Fatal("no leader")
	}
	for i := 0; i < 20; i++ {
		if !c.Propose(fmt.Appendf(nil, "op-%d", i)) {
			t.Fatalf("propose %d rejected", i)
		}
		c.RunTicks(3)
	}
	c.RunTicks(50)
	for _, id := range c.LiveIDs() {
		got := c.Applied(id)
		if len(got) != 20 {
			t.Fatalf("%s applied %d entries, want 20", id, len(got))
		}
		for i, e := range got {
			if want := fmt.Sprintf("op-%d", i); string(e.Data) != want {
				t.Fatalf("%s entry %d = %q, want %q", id, i, e.Data, want)
			}
		}
	}
	noViolations(t, c)
}

func TestFollowerCatchUpAfterPartition(t *testing.T) {
	c := NewCluster(4, 3)
	leader := c.WaitLeader(200)
	var follower string
	for _, id := range c.LiveIDs() {
		if id != leader {
			follower = id
			break
		}
	}
	rest := []string{}
	for _, id := range c.LiveIDs() {
		if id != follower {
			rest = append(rest, id)
		}
	}
	c.Partition(rest, []string{follower})
	for i := 0; i < 10; i++ {
		if !c.Propose(fmt.Appendf(nil, "while-partitioned-%d", i)) {
			t.Fatalf("propose %d rejected", i)
		}
		c.RunTicks(2)
	}
	c.RunTicks(20)
	if n := len(c.Applied(follower)); n != 0 {
		t.Fatalf("partitioned follower applied %d entries", n)
	}
	c.Heal()
	c.RunTicks(100)
	if n := len(c.Applied(follower)); n != 10 {
		t.Fatalf("after heal, follower applied %d entries, want 10", n)
	}
	noViolations(t, c)
}

func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := NewCluster(5, 3)
	old := c.WaitLeader(200)
	// Isolate the leader; it keeps believing it leads for a while.
	var others []string
	for _, id := range c.LiveIDs() {
		if id != old {
			others = append(others, id)
		}
	}
	c.Partition([]string{old}, others)
	oldCommit := c.Node(old).Status().CommitIndex
	c.Node(old).Propose([]byte("doomed"))
	c.RunTicks(50)
	if got := c.Node(old).Status().CommitIndex; got != oldCommit {
		t.Fatalf("minority leader advanced commit from %d to %d", oldCommit, got)
	}
	// The majority side elects fresh and commits new data.
	newLeader := c.WaitLeader(300)
	if newLeader == old || newLeader == "" {
		t.Fatalf("majority side has no new leader (got %q)", newLeader)
	}
	c.Propose([]byte("survivor"))
	c.RunTicks(30)
	c.Heal()
	c.RunTicks(100)
	// The doomed entry must be gone everywhere; survivor must be present.
	for _, id := range c.LiveIDs() {
		for _, e := range c.Applied(id) {
			if string(e.Data) == "doomed" {
				t.Fatalf("%s applied an uncommitted minority entry", id)
			}
		}
		found := false
		for _, e := range c.Applied(id) {
			if string(e.Data) == "survivor" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s missing the committed entry; applied: %v", id, c.Applied(id))
		}
	}
	noViolations(t, c)
}

// TestFigure8CommitRule pins §5.4.2 directly: a leader must not count
// replicas to commit an entry from an older term; only a current-term entry
// commits (and carries older ones with it).
func TestFigure8CommitRule(t *testing.T) {
	store := NewMemoryStorage()
	n, err := NewNode(Config{
		ID: "L", Peers: []string{"a", "b", "c", "d"},
		Store: store, Trans: discard{}, Apply: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Hand-build the Figure 8 state: term-2 entry at index 1 replicated on
	// a majority, but the leader is now in term 4.
	n.term = 4
	n.state = Leader
	n.log = []Entry{{Index: 1, Term: 2, Data: []byte("old")}}
	n.matchIndex = map[string]uint64{"a": 1, "b": 1, "c": 0, "d": 0}
	n.maybeCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed old-term entry by counting replicas (commit=%d)", n.commitIndex)
	}
	// A current-term entry on a majority commits, and the old one with it.
	n.log = append(n.log, Entry{Index: 2, Term: 4, Data: []byte("new")})
	n.matchIndex = map[string]uint64{"a": 2, "b": 2, "c": 0, "d": 0}
	n.maybeCommit()
	if n.commitIndex != 2 {
		t.Fatalf("current-term entry on majority did not commit (commit=%d)", n.commitIndex)
	}
}

type discard struct{}

func (discard) Send(Message) {}

func TestRestartPersistence(t *testing.T) {
	c := NewCluster(5, 3)
	if c.WaitLeader(200) == "" {
		t.Fatal("no leader")
	}
	for i := 0; i < 10; i++ {
		c.Propose(fmt.Appendf(nil, "persist-%d", i))
		c.RunTicks(3)
	}
	c.RunTicks(30)
	for _, id := range append([]string(nil), c.LiveIDs()...) {
		c.Crash(id)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		c.Restart(id)
	}
	if c.WaitLeader(300) == "" {
		t.Fatal("no leader after full restart")
	}
	c.RunTicks(100)
	for _, id := range c.LiveIDs() {
		if n := len(c.Applied(id)); n != 10 {
			t.Fatalf("%s re-applied %d entries after restart, want 10", id, n)
		}
	}
	c.Propose([]byte("after-restart"))
	c.RunTicks(50)
	for _, id := range c.LiveIDs() {
		got := c.Applied(id)
		if len(got) != 11 || string(got[10].Data) != "after-restart" {
			t.Fatalf("%s did not accept post-restart proposal: %d entries", id, len(got))
		}
	}
	noViolations(t, c)
}

func TestSnapshotCatchesUpLaggingFollower(t *testing.T) {
	c := NewCluster(6, 3)
	leader := c.WaitLeader(200)
	var follower string
	for _, id := range c.LiveIDs() {
		if id != leader {
			follower = id
			break
		}
	}
	var rest []string
	for _, id := range c.LiveIDs() {
		if id != follower {
			rest = append(rest, id)
		}
	}
	c.Partition(rest, []string{follower})
	for i := 0; i < 30; i++ {
		c.Propose(fmt.Appendf(nil, "snap-%d", i))
		c.RunTicks(2)
	}
	c.RunTicks(30)
	// Compact every connected node: the entries the follower needs are now
	// gone from the logs, forcing an InstallSnapshot on heal.
	for _, id := range rest {
		if err := c.SnapshotNode(id); err != nil {
			t.Fatal(err)
		}
	}
	if st := c.Node(leader).Status(); st.SnapIndex == 0 {
		t.Fatal("leader did not compact")
	}
	c.Heal()
	c.RunTicks(150)
	got := c.Applied(follower)
	if len(got) != 30 {
		t.Fatalf("follower applied %d entries after snapshot install, want 30", len(got))
	}
	for i, e := range got {
		if want := fmt.Sprintf("snap-%d", i); string(e.Data) != want {
			t.Fatalf("entry %d = %q, want %q", i, e.Data, want)
		}
	}
	noViolations(t, c)
}

func TestReadIndexConfirmation(t *testing.T) {
	c := NewCluster(7, 3)
	leader := c.WaitLeader(200)
	c.Propose([]byte("x=1"))
	c.RunTicks(20)

	// A healthy leader can start a read, and after a heartbeat round a quorum
	// confirms it.
	barrier, _, ok := c.Node(leader).ReadBarrier()
	if !ok {
		t.Fatal("healthy leader cannot start a read")
	}
	c.RunTicks(5) // let the heartbeat and acks flow
	if !c.Node(leader).ReadConfirmed(barrier) {
		t.Fatal("healthy leader never got a quorum read confirmation")
	}
	// A follower cannot start a read at all.
	for _, id := range c.LiveIDs() {
		if id != leader {
			if _, _, ok := c.Node(id).ReadBarrier(); ok {
				t.Fatalf("follower %s started a read — that's the stale-read bug", id)
			}
		}
	}
	// Isolate the leader: it must never get confirmation (no quorum contact),
	// and once CheckQuorum fires it steps down and can't even start a read.
	var others []string
	for _, id := range c.LiveIDs() {
		if id != leader {
			others = append(others, id)
		}
	}
	barrier2, _, started := c.Node(leader).ReadBarrier()
	c.Partition([]string{leader}, others)
	c.RunTicks(30)
	if started && c.Node(leader).ReadConfirmed(barrier2) {
		t.Fatal("isolated leader got a read confirmation — stale read possible")
	}
	if _, _, ok := c.Node(leader).ReadBarrier(); ok {
		t.Fatal("isolated leader still starts reads after stepping down")
	}
	noViolations(t, c)
}

// TestRandomizedChaos is the main safety net: seeded random faults, then
// convergence and the cluster-wide divergence checks.
func TestRandomizedChaos(t *testing.T) {
	for seed := int64(100); seed < 108; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := NewCluster(seed, 3)
			c.SetMaxDelay(3)
			rng := c.rng
			proposed := 0
			for round := 0; round < 60; round++ {
				switch rng.Intn(10) {
				case 0:
					live := c.LiveIDs()
					if len(live) == 3 { // keep a quorum possible
						c.Crash(live[rng.Intn(len(live))])
					}
				case 1:
					for _, id := range []string{"n1", "n2", "n3"} {
						if c.Node(id) == nil {
							c.Restart(id)
							break
						}
					}
				case 2:
					side := []string{fmt.Sprintf("n%d", 1+rng.Intn(3))}
					var rest []string
					for _, id := range []string{"n1", "n2", "n3"} {
						if id != side[0] {
							rest = append(rest, id)
						}
					}
					c.Partition(side, rest)
				case 3:
					c.Heal()
				case 4:
					c.SetDropRate(rng.Float64() * 0.3)
				default:
					if c.Propose(fmt.Appendf(nil, "chaos-%d", proposed)) {
						proposed++
					}
				}
				c.RunTicks(5 + rng.Intn(10))
			}
			// Recovery: heal everything and let the cluster converge.
			c.Heal()
			c.SetDropRate(0)
			for _, id := range []string{"n1", "n2", "n3"} {
				if c.Node(id) == nil {
					c.Restart(id)
				}
			}
			if c.WaitLeader(500) == "" {
				t.Fatal("no leader after recovery")
			}
			c.RunTicks(300)
			noViolations(t, c)
			// All nodes must agree exactly after convergence.
			base := c.Applied("n1")
			for _, id := range []string{"n2", "n3"} {
				other := c.Applied(id)
				if len(other) != len(base) {
					t.Fatalf("%s applied %d entries, n1 applied %d", id, len(other), len(base))
				}
				for i := range base {
					if base[i].Term != other[i].Term || !bytes.Equal(base[i].Data, other[i].Data) {
						t.Fatalf("divergence at %d between n1 and %s", i, id)
					}
				}
			}
			t.Logf("seed %d: %d proposals, %d committed entries survived chaos", seed, proposed, len(base))
		})
	}
}

func TestWALStorageRoundtrip(t *testing.T) {
	dir := t.TempDir()
	ws, err := OpenWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	ws.SetState(3, "n2")
	ws.Append([]Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 2, Data: []byte("b")}, {Index: 3, Term: 3, Data: []byte("c")}})
	// Suffix truncation via re-append at an existing index.
	ws.Append([]Entry{{Index: 2, Term: 3, Data: []byte("b2")}})
	ws.Close()

	ws, err = OpenWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs, _ := ws.Load()
	if hs.Term != 3 || hs.VotedFor != "n2" {
		t.Fatalf("state = %d/%q", hs.Term, hs.VotedFor)
	}
	if len(hs.Log) != 2 || string(hs.Log[1].Data) != "b2" || hs.Log[1].Term != 3 {
		t.Fatalf("log after truncating append = %+v", hs.Log)
	}

	// Snapshot compacts the WAL file itself and survives reopen.
	if err := ws.SetSnapshot([]byte("snap-state"), 2, 3); err != nil {
		t.Fatal(err)
	}
	ws.Close()
	ws, err = OpenWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	hs, _ = ws.Load()
	if string(hs.SnapData) != "snap-state" || hs.SnapIndex != 2 || hs.SnapTerm != 3 {
		t.Fatalf("snapshot after reopen = %q %d %d", hs.SnapData, hs.SnapIndex, hs.SnapTerm)
	}
	if len(hs.Log) != 0 {
		t.Fatalf("log not compacted: %+v", hs.Log)
	}
	if hs.Term != 3 || hs.VotedFor != "n2" {
		t.Fatalf("state lost in compaction: %d/%q", hs.Term, hs.VotedFor)
	}
}

// A node whose log is behind must not win an election (§5.4.1) — otherwise
// committed entries die with the old leader's term.
func TestElectionRestriction(t *testing.T) {
	c := NewCluster(8, 3)
	leader := c.WaitLeader(200)
	var lagger string
	for _, id := range c.LiveIDs() {
		if id != leader {
			lagger = id
			break
		}
	}
	var rest []string
	for _, id := range c.LiveIDs() {
		if id != lagger {
			rest = append(rest, id)
		}
	}
	c.Partition(rest, []string{lagger})
	for i := 0; i < 5; i++ {
		c.Propose(fmt.Appendf(nil, "committed-%d", i))
		c.RunTicks(3)
	}
	c.RunTicks(20)
	c.Heal()
	// The lagger has had election timeouts firing the whole partition; on
	// heal its vote requests must be refused and the data must survive.
	c.RunTicks(200)
	for _, id := range c.LiveIDs() {
		if n := len(c.Applied(id)); n != 5 {
			t.Fatalf("%s has %d entries after lagger rejoin, want 5", id, n)
		}
	}
	noViolations(t, c)
}
