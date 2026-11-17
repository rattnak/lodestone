package cluster

import (
	"fmt"
	"slices"
)

// Partitioning scheme: a fixed number of partitions (chosen at cluster
// creation, never changed) and a dynamic node set. Keys hash to partitions;
// partitions map to replica sets by rendezvous (highest-random-weight)
// hashing, which moves only ~1/n of placements when a node joins or leaves —
// the property consistent hashing exists for, without maintaining a ring
// structure.
//
// Fixed partitions is the Cassandra/DynamoDB-style answer to "how do you
// rebalance without rehashing everything": data never changes partition;
// partitions change owners.

// DefaultPartitions is deliberately modest; each partition is a full Raft
// group with its own engine.
const DefaultPartitions = 8

// PartitionOf maps a KV key to a partition.
func PartitionOf(key []byte, partitions uint32) uint32 {
	return uint32(fnv64(key) % uint64(partitions))
}

// PartitionOfDoc maps a document id to a partition.
func PartitionOfDoc(id uint64, partitions uint32) uint32 {
	var b [8]byte
	for i := range b {
		b[i] = byte(id >> (8 * i))
	}
	return uint32(fnv64(b[:]) % uint64(partitions))
}

// Replicas returns the nodes responsible for a partition: the rf highest
// rendezvous scores. Deterministic given the node set, so every node
// computes identical placements without coordination.
func Replicas(partition uint32, nodes []string, rf int) []string {
	if len(nodes) == 0 {
		return nil
	}
	type scored struct {
		node  string
		score uint64
	}
	ss := make([]scored, 0, len(nodes))
	for _, n := range nodes {
		ss = append(ss, scored{n, rendezvousScore(partition, n)})
	}
	slices.SortFunc(ss, func(a, b scored) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		default: // break score ties stably by name
			return cmpString(a.node, b.node)
		}
	})
	if rf > len(ss) {
		rf = len(ss)
	}
	out := make([]string, rf)
	for i := range out {
		out[i] = ss[i].node
	}
	slices.Sort(out)
	return out
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func rendezvousScore(partition uint32, node string) uint64 {
	return fnv64(fmt.Appendf(nil, "%d/%s", partition, node))
}

func fnv64(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
