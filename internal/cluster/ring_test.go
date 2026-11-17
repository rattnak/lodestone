package cluster

import (
	"fmt"
	"slices"
	"testing"
)

func TestReplicasDeterministicAndBalanced(t *testing.T) {
	nodes := []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
	counts := map[string]int{}
	const P = 64
	for p := uint32(0); p < P; p++ {
		r1 := Replicas(p, nodes, 3)
		r2 := Replicas(p, slices.Clone(nodes), 3)
		if !slices.Equal(r1, r2) {
			t.Fatalf("partition %d: nondeterministic placement %v vs %v", p, r1, r2)
		}
		if len(r1) != 3 {
			t.Fatalf("partition %d: %d replicas", p, len(r1))
		}
		seen := map[string]bool{}
		for _, n := range r1 {
			if seen[n] {
				t.Fatalf("partition %d: duplicate replica %s", p, n)
			}
			seen[n] = true
			counts[n]++
		}
	}
	// 64 partitions x 3 replicas over 5 nodes: expect ~38 each; flag gross
	// imbalance only.
	for n, c := range counts {
		if c < 15 || c > 60 {
			t.Fatalf("node %s owns %d of %d placements — badly imbalanced", n, c, 3*P)
		}
	}
}

func TestReplicasMinimalMovementOnJoin(t *testing.T) {
	before := []string{"a:1", "b:1", "c:1", "d:1"}
	after := append(slices.Clone(before), "e:1")
	const P = 256
	moved := 0
	for p := uint32(0); p < P; p++ {
		b := Replicas(p, before, 3)
		a := Replicas(p, after, 3)
		for _, n := range b {
			if !slices.Contains(a, n) {
				moved++
			}
		}
	}
	// Rendezvous: each placement moves to the new node with probability
	// ~1/5, so ~20% of 768 placements; far below a naive rehash.
	if moved > 768/3 {
		t.Fatalf("%d of 768 placements moved on join — not minimal movement", moved)
	}
	if moved == 0 {
		t.Fatal("no placements moved — the new node got nothing")
	}
}

func TestReplicasFewNodes(t *testing.T) {
	if got := Replicas(1, []string{"only:1"}, 3); len(got) != 1 {
		t.Fatalf("1 node, rf 3 = %v", got)
	}
	if got := Replicas(1, nil, 3); got != nil {
		t.Fatalf("no nodes = %v", got)
	}
}

func TestPartitionOfSpread(t *testing.T) {
	counts := make([]int, 8)
	for i := 0; i < 10000; i++ {
		counts[PartitionOf(fmt.Appendf(nil, "key-%d", i), 8)]++
	}
	for p, c := range counts {
		if c < 800 || c > 1800 {
			t.Fatalf("partition %d got %d of 10000 keys", p, c)
		}
	}
}
