package raft

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
)

// TestExtendedChaos sweeps a configurable seed range with the same fault mix
// as TestRandomizedChaos but longer runs. Skipped unless LODESTONE_CHAOS_SEEDS
// is set; the scheduled torture CI job runs it with hundreds of seeds.
//
//	LODESTONE_CHAOS_SEEDS=200 go test ./internal/raft/ -run TestExtendedChaos -v
func TestExtendedChaos(t *testing.T) {
	nStr := os.Getenv("LODESTONE_CHAOS_SEEDS")
	if nStr == "" {
		t.Skip("set LODESTONE_CHAOS_SEEDS to run the extended sweep")
	}
	n, err := strconv.Atoi(nStr)
	if err != nil {
		t.Fatal(err)
	}
	for seed := int64(1000); seed < 1000+int64(n); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runChaos(t, seed)
		})
	}
}

func runChaos(t *testing.T, seed int64) {
	c := NewCluster(seed, 3)
	c.SetMaxDelay(4)
	rng := c.rng
	proposed := 0
	for round := 0; round < 120; round++ {
		switch rng.Intn(12) {
		case 0:
			live := c.LiveIDs()
			if len(live) == 3 {
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
			c.SetDropRate(rng.Float64() * 0.4)
		case 5:
			// Compact a random live node mid-flight.
			live := c.LiveIDs()
			if len(live) > 0 {
				c.SnapshotNode(live[rng.Intn(len(live))])
			}
		default:
			if c.Propose(fmt.Appendf(nil, "chaos-%d", proposed)) {
				proposed++
			}
		}
		c.RunTicks(3 + rng.Intn(12))
	}
	c.Heal()
	c.SetDropRate(0)
	for _, id := range []string{"n1", "n2", "n3"} {
		if c.Node(id) == nil {
			c.Restart(id)
		}
	}
	if c.WaitLeader(600) == "" {
		t.Fatal("no leader after recovery")
	}
	c.RunTicks(400)
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("seed %d violations:\n%s", seed, v)
	}
	base := c.Applied("n1")
	for _, id := range []string{"n2", "n3"} {
		other := c.Applied(id)
		if len(other) != len(base) {
			t.Fatalf("seed %d: %s applied %d, n1 applied %d", seed, id, len(other), len(base))
		}
		for i := range base {
			if base[i].Term != other[i].Term || !bytes.Equal(base[i].Data, other[i].Data) {
				t.Fatalf("seed %d: divergence at %d", seed, i)
			}
		}
	}
}
