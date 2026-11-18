// Package client is the typed Go client for a lodestone server, used by the
// CLI, the ingestion workers, and tests.
package client

import (
	"context"
	"iter"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rattnak/lodestone/internal/server/pb"
)

type Client struct {
	conn  *grpc.ClientConn
	rpc   pb.LodestoneClient
	admin pb.ClusterAdminClient
}

// Dial connects to a lodestone server at addr (host:port).
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return FromConn(conn), nil
}

// FromConn wraps an existing connection (tests use this with bufconn).
func FromConn(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, rpc: pb.NewLodestoneClient(conn), admin: pb.NewClusterAdminClient(conn)}
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.rpc.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	return err
}

func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	resp, err := c.rpc.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.rpc.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

func (c *Client) UpsertDocument(ctx context.Context, id uint64, vector []float32, payload []byte) error {
	_, err := c.rpc.UpsertDocument(ctx, &pb.UpsertDocumentRequest{Id: id, Vector: vector, Payload: payload})
	return err
}

func (c *Client) DeleteDocument(ctx context.Context, id uint64) error {
	_, err := c.rpc.DeleteDocument(ctx, &pb.DeleteDocumentRequest{Id: id})
	return err
}

// Hit is one similarity-search result; Score is distance (lower is closer).
type Hit struct {
	ID      uint64
	Score   float32
	Payload []byte
}

// SearchVectors returns the k nearest documents. ef tunes the recall/latency
// trade-off; 0 lets the server choose.
func (c *Client) SearchVectors(ctx context.Context, vector []float32, k, ef int, includePayloads bool) ([]Hit, error) {
	resp, err := c.rpc.SearchVectors(ctx, &pb.SearchVectorsRequest{
		Vector:          vector,
		K:               uint32(k),
		Ef:              uint32(ef),
		IncludePayloads: includePayloads,
	})
	if err != nil {
		return nil, err
	}
	hits := make([]Hit, len(resp.Hits))
	for i, h := range resp.Hits {
		hits[i] = Hit{ID: h.Id, Score: h.Score, Payload: h.Payload}
	}
	return hits, nil
}

// GroupInfo describes one partition's Raft group, from a cluster node.
type GroupInfo struct {
	ID          uint32
	State       string
	Leader      string
	Term        uint64
	LastApplied uint64
	Members     []string
	Vectors     uint64
}

// Topology is a cluster node's view of the cluster: members, partition count,
// and the groups this node hosts.
type Topology struct {
	Nodes      []string
	Partitions uint32
	Groups     []GroupInfo
}

// Describe returns the cluster topology from a cluster node. Single-node
// servers don't register ClusterAdmin, so this returns an error there — the
// caller treats that as "standalone node, no topology".
func (c *Client) Describe(ctx context.Context) (*Topology, error) {
	resp, err := c.admin.Describe(ctx, &pb.DescribeRequest{})
	if err != nil {
		return nil, err
	}
	t := &Topology{Nodes: resp.Nodes, Partitions: resp.Partitions}
	for _, g := range resp.Groups {
		t.Groups = append(t.Groups, GroupInfo{
			ID: g.Id, State: g.State, Leader: g.Leader, Term: g.Term,
			LastApplied: g.LastApplied, Members: g.Members, Vectors: g.Vectors,
		})
	}
	return t, nil
}

// Telemetry is one snapshot of server state.
type Telemetry struct {
	UnixMs         int64
	MemtableBytes  uint64
	SSTableCount   uint32
	IndexedVectors uint64
	PutsTotal      uint64
	GetsTotal      uint64
	SearchesTotal  uint64
}

// WatchTelemetry yields snapshots every intervalMs until ctx is cancelled or
// the stream fails; a stream error surfaces via errCheck after iteration.
func (c *Client) WatchTelemetry(ctx context.Context, intervalMs int) (iter.Seq[Telemetry], func() error, error) {
	stream, err := c.rpc.WatchTelemetry(ctx, &pb.WatchTelemetryRequest{IntervalMs: uint32(intervalMs)})
	if err != nil {
		return nil, nil, err
	}
	var streamErr error
	seq := func(yield func(Telemetry) bool) {
		for {
			snap, err := stream.Recv()
			if err != nil {
				if ctx.Err() == nil {
					streamErr = err
				}
				return
			}
			t := Telemetry{
				UnixMs:         snap.UnixMs,
				MemtableBytes:  snap.MemtableBytes,
				SSTableCount:   snap.SstableCount,
				IndexedVectors: snap.IndexedVectors,
				PutsTotal:      snap.PutsTotal,
				GetsTotal:      snap.GetsTotal,
				SearchesTotal:  snap.SearchesTotal,
			}
			if !yield(t) {
				return
			}
		}
	}
	return seq, func() error { return streamErr }, nil
}
