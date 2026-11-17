package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rattnak/lodestone/internal/index"
)

// This file holds the client-facing operations. Any node can be a
// coordinator: it maps the key to a partition, and if it hosts that
// partition's leader it serves locally, otherwise it returns ErrNotLeader
// with a hint so the caller (or the gRPC layer) can retry on the right node.
// Searches fan out across all partitions (scatter) and merge the per-
// partition top-k (gather).

const opTimeout = 5 * time.Second

// Put routes a key/value write to its partition leader.
func (n *Node) Put(ctx context.Context, key, value []byte) error {
	p := PartitionOf(key, n.partitions)
	return n.proposeLocal(ctx, p, Command{Op: OpPut, Key: key, Value: value})
}

// Delete routes a key deletion to its partition leader.
func (n *Node) Delete(ctx context.Context, key []byte) error {
	p := PartitionOf(key, n.partitions)
	return n.proposeLocal(ctx, p, Command{Op: OpDelete, Key: key})
}

// UpsertDocument routes a document write to its partition (keyed by doc id).
func (n *Node) UpsertDocument(ctx context.Context, id uint64, vector []float32, payload []byte) error {
	p := PartitionOfDoc(id, n.partitions)
	return n.proposeLocal(ctx, p, Command{Op: OpUpsertDoc, DocID: id, Vector: vector, Payload: payload})
}

// DeleteDocument routes a document deletion to its partition.
func (n *Node) DeleteDocument(ctx context.Context, id uint64) error {
	p := PartitionOfDoc(id, n.partitions)
	return n.proposeLocal(ctx, p, Command{Op: OpDeleteDoc, DocID: id})
}

// Get routes a linearizable read to its partition leader.
func (n *Node) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	p := PartitionOf(key, n.partitions)
	g := n.group(p)
	if g == nil {
		return nil, false, ErrNotLeader{LeaderID: n.leaderHint(p)}
	}
	return g.Get(ctx, key)
}

func (n *Node) proposeLocal(ctx context.Context, p uint32, cmd Command) error {
	g := n.group(p)
	if g == nil {
		return ErrNotLeader{LeaderID: n.leaderHint(p)}
	}
	return g.Propose(ctx, cmd)
}

// LocalGroup returns the hosted group for partition p for the gRPC layer to
// serve a partition-pinned request, or nil with a leader hint.
func (n *Node) LocalGroup(p uint32) (*Group, string) {
	g := n.group(p)
	if g == nil {
		return nil, n.leaderHint(p)
	}
	return g, ""
}

// PartitionSearcher searches one partition, wherever its leader lives, and
// returns that partition's local top-k. The cluster server supplies an
// implementation that serves locally when this node leads the partition and
// forwards to the leader otherwise, retrying across leader churn. It must
// return an error (not empty results) when the partition cannot be reached,
// so the gather can refuse to return a silently-incomplete answer.
type PartitionSearcher func(ctx context.Context, partition uint32, vector []float32, k, ef int) ([]index.Result, error)

// Search runs a cluster-wide vector search: scatter to every partition in
// parallel, then gather and merge the global top-k. A vector search that
// skips a partition can miss the true nearest neighbors, so any partition
// error fails the whole query rather than returning a partial result.
func (n *Node) Search(ctx context.Context, vector []float32, k, ef int, search PartitionSearcher) ([]index.Result, error) {
	type partRes struct {
		hits []index.Result
		err  error
	}
	results := make([]partRes, n.partitions)
	var wg sync.WaitGroup
	for p := uint32(0); p < n.partitions; p++ {
		wg.Add(1)
		go func(p uint32) {
			defer wg.Done()
			hits, err := search(ctx, p, vector, k, ef)
			results[p] = partRes{hits, err}
		}(p)
	}
	wg.Wait()

	var merged []index.Result
	for p, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("partition %d: %w", p, r.err)
		}
		merged = append(merged, r.hits...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Score < merged[j].Score })
	if len(merged) > k {
		merged = merged[:k]
	}
	return merged, nil
}
