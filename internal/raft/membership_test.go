package raft

import (
	"fmt"
	"slices"
	"testing"
)

func proposeN(t *testing.T, c *Cluster, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !c.Propose(fmt.Appendf(nil, "%s-%d", prefix, i)) {
			t.Fatalf("propose %s-%d rejected", prefix, i)
		}
		c.RunTicks(3)
	}
	c.RunTicks(30)
}

func TestAddNode(t *testing.T) {
	c := NewCluster(31, 3)
	leader := c.WaitLeader(200)
	proposeN(t, c, "before", 5)

	c.AddNode("n4")
	if _, ok := c.Node(leader).ProposeConfChange(ConfChange{Members: addTo(c.Node(leader).Members(), "n4")}); !ok {
		t.Fatal("conf change rejected")
	}
	c.RunTicks(100)

	if !slices.Contains(c.Node(leader).Peers(), "n4") {
		t.Fatalf("leader peers = %v, missing n4", c.Node(leader).Peers())
	}
	// The new node must have replicated history, and new proposals must
	// reach it.
	proposeN(t, c, "after", 5)
	got := c.Applied("n4")
	if len(got) != 10 {
		t.Fatalf("n4 applied %d entries, want 10", len(got))
	}
	// Four members: quorum is 3, so one crash must not block commits.
	c.Crash("n1")
	if c.WaitLeader(300) == "" {
		t.Fatal("no leader with 3 of 4 up")
	}
	proposeN(t, c, "degraded", 3)
	noViolations(t, c)
}

func TestAddNodeCatchesUpViaSnapshot(t *testing.T) {
	c := NewCluster(32, 3)
	c.WaitLeader(200)
	proposeN(t, c, "old", 10)
	// Compact everyone: history is only available as a snapshot now.
	for _, id := range c.LiveIDs() {
		if err := c.SnapshotNode(id); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.Leader()
	c.AddNode("n4")
	if _, ok := c.Node(leader).ProposeConfChange(ConfChange{Members: addTo(c.Node(leader).Members(), "n4")}); !ok {
		t.Fatal("conf change rejected")
	}
	c.RunTicks(150)
	got := c.Applied("n4")
	if len(got) != 10 {
		t.Fatalf("n4 applied %d entries via snapshot, want 10", len(got))
	}
	if st := c.Node("n4").Status(); st.SnapIndex == 0 {
		t.Fatal("n4 never installed a snapshot — catch-up used a log that should be compacted")
	}
	noViolations(t, c)
}

func TestRemoveNode(t *testing.T) {
	c := NewCluster(33, 3)
	leader := c.WaitLeader(200)
	proposeN(t, c, "x", 3)
	var victim string
	for _, id := range c.LiveIDs() {
		if id != leader {
			victim = id
			break
		}
	}
	if _, ok := c.Node(leader).ProposeConfChange(ConfChange{Members: without(c.Node(leader).Members(), victim)}); !ok {
		t.Fatal("conf change rejected")
	}
	c.RunTicks(60)
	if slices.Contains(c.Node(leader).Peers(), victim) {
		t.Fatalf("leader peers still contain %s", victim)
	}
	// The hosting layer stops removed nodes; model that.
	c.Crash(victim)
	// Two voting members remain; quorum is 2, both alive, commits proceed.
	if c.WaitLeader(300) == "" {
		t.Fatal("no leader after removal")
	}
	proposeN(t, c, "post-remove", 3)
	noViolations(t, c)
}

func TestOneConfChangeAtATime(t *testing.T) {
	c := NewCluster(34, 3)
	leader := c.WaitLeader(200)
	// Stall commits by isolating the leader, then try two changes.
	var others []string
	for _, id := range c.LiveIDs() {
		if id != leader {
			others = append(others, id)
		}
	}
	c.Partition([]string{leader}, others)
	c.AddNode("n4")
	c.AddNode("n5")
	if _, ok := c.Node(leader).ProposeConfChange(ConfChange{Members: addTo(c.Node(leader).Members(), "n4")}); !ok {
		t.Fatal("first conf change rejected")
	}
	if _, ok := c.Node(leader).ProposeConfChange(ConfChange{Members: addTo(c.Node(leader).Members(), "n5")}); ok {
		t.Fatal("second conf change accepted while first uncommitted — quorums can stop overlapping")
	}
}

func TestWALStoragePersistsTypeAndPeers(t *testing.T) {
	dir := t.TempDir()
	ws, err := OpenWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	cc, _ := EncodeConfChange(ConfChange{Members: []string{"n1", "n2", "n9"}})
	ws.Append([]Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("cmd")},
		{Index: 2, Term: 1, Type: EntryConfChange, Data: cc},
	})
	ws.SetConfig([]string{"n2", "n9"}, true)
	ws.Close()

	ws, err = OpenWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	hs, _ := ws.Load()
	if len(hs.Log) != 2 || hs.Log[0].Type != EntryNormal || hs.Log[1].Type != EntryConfChange {
		t.Fatalf("entry types lost: %+v", hs.Log)
	}
	got, err := DecodeConfChange(hs.Log[1].Data)
	if err != nil || !slices.Equal(got.Members, []string{"n1", "n2", "n9"}) {
		t.Fatalf("conf change payload = %+v, %v", got, err)
	}
	if !slices.Equal(hs.Peers, []string{"n2", "n9"}) || !hs.Voter || !hs.Configured {
		t.Fatalf("config = peers %v voter %v configured %v", hs.Peers, hs.Voter, hs.Configured)
	}
}

func addTo(set []string, x string) []string {
	return append(append([]string(nil), set...), x)
}
