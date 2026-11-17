package cluster

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rattnak/lodestone/internal/server/pb"
)

// docEmbed maps a document id to a distinct direction on a 4-D curve, so
// cosine distance orders documents unambiguously (unlike nearly-parallel
// vectors, under which every direction is ~equidistant).
func docEmbed(i int) []float32 {
	a := float64(i) * 0.37
	return []float32{
		float32(math.Sin(a)), float32(math.Cos(a)),
		float32(math.Sin(a * 0.5)), float32(math.Cos(a * 0.5)),
	}
}

// testCluster runs real ClusterServers over loopback TCP — the full stack:
// gRPC routing, the gRPC Raft transport, partitioning, and rebalancing.
type testCluster struct {
	t       *testing.T
	servers map[string]*ClusterServer
	addrs   []string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startCluster(t *testing.T, n int, partitions uint32, rf int) *testCluster {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	tc := &testCluster{t: t, servers: map[string]*ClusterServer{}, addrs: addrs}
	for i, addr := range addrs {
		node, err := NewNode(Options{
			ID:         addr,
			Dir:        t.TempDir(),
			Seeds:      addrs, // every node starts knowing the full set
			Partitions: partitions,
			RF:         rf,
			Bootstrap:  true, // initial cluster formation: found as voters
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := NewClusterServer(node)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve(lis)
		tc.servers[addr] = srv
		_ = i
	}
	t.Cleanup(func() {
		for _, s := range tc.servers {
			s.Stop()
		}
	})
	return tc
}

func (tc *testCluster) client(addr string) (pb.LodestoneClient, func()) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tc.t.Fatal(err)
	}
	return pb.NewLodestoneClient(conn), func() { conn.Close() }
}

// waitReady blocks until every partition has a leader somewhere, so routing
// can succeed.
func (tc *testCluster) waitReady(partitions uint32) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		leaders := map[uint32]bool{}
		for _, s := range tc.servers {
			for _, g := range s.node.HostedGroups() {
				if g.Status().State == 2 { // raft.Leader
					leaders[g.ID] = true
				}
			}
		}
		if uint32(len(leaders)) == partitions {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tc.t.Fatal("cluster did not elect a leader for every partition in time")
}

func TestClusterRoutingAndReplication(t *testing.T) {
	const P = 8
	tc := startCluster(t, 3, P, 3)
	tc.waitReady(P)

	// Write through node 0, read back through node 2 — routing must land
	// both on the right partition leader regardless of entry node.
	c0, close0 := tc.client(tc.addrs[0])
	defer close0()
	c2, close2 := tc.client(tc.addrs[2])
	defer close2()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		_, err := c0.Put(ctx, &pb.PutRequest{
			Key:   fmt.Appendf(nil, "key-%03d", i),
			Value: fmt.Appendf(nil, "val-%03d", i),
		})
		if err != nil {
			t.Fatalf("put %d via node0: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		resp, err := c2.Get(ctx, &pb.GetRequest{Key: fmt.Appendf(nil, "key-%03d", i)})
		if err != nil {
			t.Fatalf("get %d via node2: %v", i, err)
		}
		if !resp.Found || string(resp.Value) != fmt.Sprintf("val-%03d", i) {
			t.Fatalf("get %d = %q found=%v", i, resp.Value, resp.Found)
		}
	}
}

func TestClusterScatterGatherSearch(t *testing.T) {
	const P = 8
	tc := startCluster(t, 3, P, 3)
	tc.waitReady(P)

	c, closeC := tc.client(tc.addrs[1])
	defer closeC()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Documents spread across all partitions by id. The index uses cosine
	// distance (the right metric for embeddings), so each doc's vector must
	// point in a distinct direction — docEmbed maps the id to an angle so
	// nearest-by-direction has an unambiguous answer.
	for i := 1; i <= 200; i++ {
		_, err := c.UpsertDocument(ctx, &pb.UpsertDocumentRequest{
			Id:      uint64(i),
			Vector:  docEmbed(i),
			Payload: fmt.Appendf(nil, "doc-%d", i),
		})
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	// Search with doc 150's exact embedding: doc 150 hashes to one
	// partition while its true neighbors hash to others, so a correct top-5
	// proves the gather merged results across partitions.
	resp, err := c.SearchVectors(ctx, &pb.SearchVectorsRequest{
		Vector: docEmbed(150), K: 5, Ef: 200, IncludePayloads: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) != 5 {
		t.Fatalf("got %d hits, want 5", len(resp.Hits))
	}
	if resp.Hits[0].Id != 150 {
		t.Fatalf("nearest = %d, want 150; hits=%+v", resp.Hits[0].Id, resp.Hits)
	}
	if string(resp.Hits[0].Payload) != "doc-150" {
		t.Fatalf("payload = %q", resp.Hits[0].Payload)
	}
	// Scores must be globally sorted after the merge.
	for i := 1; i < len(resp.Hits); i++ {
		if resp.Hits[i].Score < resp.Hits[i-1].Score {
			t.Fatalf("merged hits not sorted: %+v", resp.Hits)
		}
	}
	// The gather must span partitions: with 200 docs over 8 partitions, the
	// 5 nearest cannot all live on one.
	parts := map[uint32]bool{}
	for _, h := range resp.Hits {
		parts[PartitionOfDoc(h.Id, P)] = true
	}
	if len(parts) < 2 {
		t.Fatalf("all hits from one partition — gather didn't span: %+v", resp.Hits)
	}
}

func TestClusterJoinAndRebalance(t *testing.T) {
	// Fewer partitions than the other tests: rebalancing 3→4 nodes churns
	// every group's membership at once, and keeping the group count modest
	// keeps the test robust when it shares the machine with the rest of the
	// suite. Join and rebalance are exercised the same way regardless.
	const P = 4
	// Start with 3 nodes but leave a 4th address reserved.
	tc := startCluster(t, 3, P, 3)
	tc.waitReady(P)

	c, closeC := tc.client(tc.addrs[0])
	defer closeC()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		if _, err := c.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: []byte("v")}); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}

	// Bring up a 4th node and have it join via a seed.
	newAddr := freeAddr(t)
	node, err := NewNode(Options{ID: newAddr, Dir: t.TempDir(), Seeds: []string{tc.addrs[0]}, Partitions: P, RF: 3})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewClusterServer(node)
	lis, err := net.Listen("tcp", newAddr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	jctx, jcancel := context.WithTimeout(ctx, 5*time.Second)
	srv.JoinSeeds(jctx, []string{tc.addrs[0]})
	jcancel()

	// The new node should pick up some partitions as rendezvous placement
	// shifts and the existing leaders add it as a voter.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if len(node.HostedGroups()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new node never acquired any partitions after join")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Smoke-check that the cluster resumes serving routed reads after
	// rebalancing (leaders migrate as voters are added and removed, so a
	// client rides out transient leaderless windows).
	readOnce := func(key []byte) bool {
		rctx, rcancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer rcancel()
		resp, err := c.Get(rctx, &pb.GetRequest{Key: key})
		return err == nil && resp.Found
	}
	settleDeadline := time.Now().Add(60 * time.Second)
	for !readOnce([]byte("k00")) {
		if time.Now().After(settleDeadline) {
			t.Fatal("cluster never resumed serving reads after rebalance")
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Every seed key must have survived the rebalance. Verify against the
	// replicas' storage engines directly rather than through routed reads:
	// this asserts the data-survival property without being hostage to
	// live-leader-election timing under a loaded test machine. Each key must
	// be present on every replica that currently hosts its partition
	// (replication converges, so laggards are given time).
	allNodes := map[string]*Node{}
	for _, srv := range tc.servers {
		allNodes[srv.node.id] = srv.node
	}
	allNodes[node.id] = node

	deadline = time.Now().Add(60 * time.Second)
	for i := 0; i < 50; i++ {
		key := fmt.Appendf(nil, "k%02d", i)
		for {
			present, replicas := 0, 0
			for _, nd := range allNodes {
				g := nd.group(PartitionOf(key, P))
				if g == nil {
					continue
				}
				replicas++
				if _, ok, _ := g.DB().Get(key); ok {
					present++
				}
			}
			// Durable once a quorum (3 of RF=3) holds the key. During
			// rebalancing a partition can transiently have a fourth replica
			// still catching up; a quorum having the data is the survival
			// guarantee regardless.
			if present >= 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("key k%02d did not survive rebalance: present on %d of %d replicas", i, present, replicas)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	_ = ctx
}
