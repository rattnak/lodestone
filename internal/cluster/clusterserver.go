package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/rattnak/lodestone/internal/index"
	"github.com/rattnak/lodestone/internal/raft"
	"github.com/rattnak/lodestone/internal/server/pb"
)

// forwardedKey marks a request that has already been routed once. A node
// receiving a forwarded request serves it locally or fails — it never
// forwards again, so cross-node routing is always a single hop and can't
// loop between replicas that briefly disagree about the leader.
const forwardedKey = "lode-forwarded"

func isForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedKey)) > 0
}

func markForwarded(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, forwardedKey, "1")
}

// ClusterServer exposes a Node over gRPC: the client Lodestone service (with
// cross-node routing), the internal RaftTransport, and ClusterAdmin. One
// node is addressable at one host:port for all three.
type ClusterServer struct {
	pb.UnimplementedLodestoneServer
	pb.UnimplementedRaftTransportServer
	pb.UnimplementedClusterAdminServer

	node *Node
	gs   *grpc.Server

	mu     sync.Mutex
	admin  map[string]pb.ClusterAdminClient
	rawAdm map[string]*grpc.ClientConn
}

func NewClusterServer(node *Node) *ClusterServer {
	return &ClusterServer{
		node:   node,
		admin:  map[string]pb.ClusterAdminClient{},
		rawAdm: map[string]*grpc.ClientConn{},
	}
}

// Serve registers all services on a gRPC server bound to lis and starts the
// node's background loops.
func (s *ClusterServer) Serve(lis net.Listener) error {
	s.gs = grpc.NewServer()
	pb.RegisterLodestoneServer(s.gs, s)
	pb.RegisterRaftTransportServer(s.gs, s)
	pb.RegisterClusterAdminServer(s.gs, s)
	s.node.Run()
	return s.gs.Serve(lis)
}

func (s *ClusterServer) Stop() error {
	if s.gs != nil {
		s.gs.GracefulStop()
	}
	s.mu.Lock()
	for _, c := range s.rawAdm {
		c.Close()
	}
	s.mu.Unlock()
	return s.node.Close()
}

// JoinSeeds contacts a seed node to learn the current member list and
// register self, so a starting node discovers the cluster. Best-effort: on
// failure the node runs with whatever seeds it was given.
func (s *ClusterServer) JoinSeeds(ctx context.Context, seeds []string) {
	for _, seed := range seeds {
		if seed == s.node.id {
			continue
		}
		client := s.adminClient(seed)
		if client == nil {
			continue
		}
		resp, err := client.Join(ctx, &pb.JoinRequest{Addr: s.node.id})
		if err != nil {
			continue
		}
		for _, addr := range resp.Nodes {
			s.node.AddMember(addr)
		}
		return
	}
}

// --- RaftTransport service ---

func (s *ClusterServer) Send(ctx context.Context, env *pb.RaftEnvelope) (*pb.RaftAck, error) {
	var m raft.Message
	if err := gob.NewDecoder(bytes.NewReader(env.Message)).Decode(&m); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.node.deliver(env.Group, m)
	return &pb.RaftAck{}, nil
}

// --- ClusterAdmin service ---

func (s *ClusterServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	nodes := s.node.AddMember(req.Addr)
	// Gossip the newcomer to everyone we know so the member set converges
	// without a central registry.
	for _, addr := range nodes {
		if addr == s.node.id || addr == req.Addr {
			continue
		}
		if c := s.adminClient(addr); c != nil {
			go func(c pb.ClusterAdminClient) {
				cctx, cancel := withTimeout(context.Background(), opTimeout)
				defer cancel()
				c.Join(cctx, &pb.JoinRequest{Addr: req.Addr})
			}(c)
		}
	}
	return &pb.JoinResponse{Nodes: nodes, Partitions: s.node.Partitions()}, nil
}

func (s *ClusterServer) Describe(ctx context.Context, _ *pb.DescribeRequest) (*pb.DescribeResponse, error) {
	resp := &pb.DescribeResponse{
		Nodes:      s.node.Members(),
		Partitions: s.node.Partitions(),
	}
	for _, g := range s.node.HostedGroups() {
		st := g.Status()
		members := append([]string{st.ID}, g.Raft().Peers()...)
		resp.Groups = append(resp.Groups, &pb.GroupInfo{
			Id:          g.ID,
			State:       st.State.String(),
			Leader:      st.LeaderID,
			Term:        st.Term,
			LastApplied: st.LastApplied,
			Members:     members,
			Vectors:     uint64(g.Index().Len()),
		})
	}
	return resp, nil
}

func (s *ClusterServer) SearchPartition(ctx context.Context, req *pb.SearchPartitionRequest) (*pb.SearchVectorsResponse, error) {
	g, hint := s.node.LocalGroup(req.Group)
	if g == nil {
		return nil, notLeader(hint)
	}
	res, err := g.Search(ctx, req.Vector, int(req.K), int(req.Ef))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.SearchVectorsResponse{Hits: s.toHits(g, res, req.IncludePayloads)}, nil
}

// --- Lodestone service (client-facing, with routing) ---

func (s *ClusterServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Key[0] == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys beginning with 0x00 are reserved")
	}
	err := s.routeKey(ctx, req.Key, func(fctx context.Context, c pb.LodestoneClient) error {
		_, e := c.Put(fctx, req)
		return e
	}, func() error {
		return s.node.Put(ctx, req.Key, req.Value)
	})
	return &pb.PutResponse{}, err
}

func (s *ClusterServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	var resp *pb.GetResponse
	err := s.routeKey(ctx, req.Key, func(fctx context.Context, c pb.LodestoneClient) error {
		r, e := c.Get(fctx, req)
		resp = r
		return e
	}, func() error {
		v, found, e := s.node.Get(ctx, req.Key)
		if e == nil {
			resp = &pb.GetResponse{Value: v, Found: found}
		}
		return e
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ClusterServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.routeKey(ctx, req.Key, func(fctx context.Context, c pb.LodestoneClient) error {
		_, e := c.Delete(fctx, req)
		return e
	}, func() error {
		return s.node.Delete(ctx, req.Key)
	})
	return &pb.DeleteResponse{}, err
}

func (s *ClusterServer) UpsertDocument(ctx context.Context, req *pb.UpsertDocumentRequest) (*pb.UpsertDocumentResponse, error) {
	if len(req.Vector) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty vector")
	}
	err := s.routeDoc(ctx, req.Id, func(fctx context.Context, c pb.LodestoneClient) error {
		_, e := c.UpsertDocument(fctx, req)
		return e
	}, func() error {
		return s.node.UpsertDocument(ctx, req.Id, req.Vector, req.Payload)
	})
	return &pb.UpsertDocumentResponse{}, err
}

func (s *ClusterServer) DeleteDocument(ctx context.Context, req *pb.DeleteDocumentRequest) (*pb.DeleteDocumentResponse, error) {
	err := s.routeDoc(ctx, req.Id, func(fctx context.Context, c pb.LodestoneClient) error {
		_, e := c.DeleteDocument(fctx, req)
		return e
	}, func() error {
		return s.node.DeleteDocument(ctx, req.Id)
	})
	return &pb.DeleteDocumentResponse{}, err
}

func (s *ClusterServer) SearchVectors(ctx context.Context, req *pb.SearchVectorsRequest) (*pb.SearchVectorsResponse, error) {
	if req.K == 0 {
		return nil, status.Error(codes.InvalidArgument, "k must be positive")
	}
	// Scatter-gather: this node coordinates, serving partitions it leads in
	// process and forwarding the rest to their leaders via SearchPartition.
	res, err := s.node.Search(ctx, req.Vector, int(req.K), int(req.Ef), s.searchPartition)
	if err != nil {
		return nil, toStatus(err)
	}
	// Fill payloads from whichever node hosts each hit's partition.
	resp := &pb.SearchVectorsResponse{Hits: make([]*pb.SearchHit, len(res))}
	for i, r := range res {
		hit := &pb.SearchHit{Id: r.ID, Score: r.Score}
		if req.IncludePayloads {
			if p := s.node.group(PartitionOfDoc(r.ID, s.node.partitions)); p != nil {
				if payload, ok := p.Payload(r.ID); ok {
					hit.Payload = payload
				}
			}
		}
		resp.Hits[i] = hit
	}
	return resp, nil
}

func (s *ClusterServer) WatchTelemetry(req *pb.WatchTelemetryRequest, stream grpc.ServerStreamingServer[pb.TelemetrySnapshot]) error {
	interval := time.Duration(req.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		var tables uint32
		var vectors, mem uint64
		for _, g := range s.node.HostedGroups() {
			tables += uint32(g.DB().TableCount())
			mem += uint64(g.DB().MemtableSize())
			vectors += uint64(g.Index().Len())
		}
		snap := &pb.TelemetrySnapshot{
			UnixMs:         time.Now().UnixMilli(),
			MemtableBytes:  mem,
			SstableCount:   tables,
			IndexedVectors: vectors,
		}
		if err := stream.Send(snap); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-tick.C:
		}
	}
}

// searchPartition searches one partition for the scatter-gather, retrying
// across leader churn until the deadline. It serves locally when this node
// leads the partition and forwards to the believed leader otherwise. It
// returns an error only when no leader can be reached in time — never empty
// results masking an unreached partition.
func (s *ClusterServer) searchPartition(ctx context.Context, p uint32, vector []float32, k, ef int) ([]index.Result, error) {
	deadline := time.Now().Add(opTimeout)
	for attempt := 0; ; attempt++ {
		g := s.node.group(p)
		if g != nil && g.Status().State == raft.Leader {
			hits, err := g.Search(ctx, vector, k, ef)
			if err == nil {
				return hits, nil
			}
			if !isNotLeader(err) {
				return nil, err
			}
			// Lost leadership mid-read; fall through to forward/retry.
		} else {
			for _, target := range s.node.routeTargets(p) {
				if target == "" || target == s.node.id {
					continue
				}
				c := s.adminClient(target)
				if c == nil {
					continue
				}
				resp, err := c.SearchPartition(ctx, &pb.SearchPartitionRequest{
					Group: p, Vector: vector, K: uint32(k), Ef: uint32(ef),
				})
				if err == nil {
					out := make([]index.Result, len(resp.Hits))
					for i, h := range resp.Hits {
						out[i] = index.Result{ID: h.Id, Score: h.Score}
					}
					return out, nil
				}
				if status.Code(err) != codes.FailedPrecondition {
					return nil, err
				}
				// Not the leader (stale target); try the next replica.
			}
		}
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.FailedPrecondition, "no reachable leader for partition %d", p)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
}

// --- routing helpers ---

// routeKey runs local if this node leads the key's partition, else forwards
// to the hinted leader. A short retry loop absorbs leader churn: the hint can
// be stale right after an election.
func (s *ClusterServer) routeKey(ctx context.Context, key []byte, forward func(context.Context, pb.LodestoneClient) error, local func() error) error {
	p := PartitionOf(key, s.node.partitions)
	return s.route(ctx, p, forward, local)
}

func (s *ClusterServer) routeDoc(ctx context.Context, id uint64, forward func(context.Context, pb.LodestoneClient) error, local func() error) error {
	p := PartitionOfDoc(id, s.node.partitions)
	return s.route(ctx, p, forward, local)
}

func (s *ClusterServer) route(ctx context.Context, p uint32, forward func(context.Context, pb.LodestoneClient) error, local func() error) error {
	// A request that was already forwarded to us is served here or not at
	// all: never forward twice (that is what would loop).
	if isForwarded(ctx) {
		if g := s.node.group(p); g != nil && g.Status().State == raft.Leader {
			return local()
		}
		return notLeader(s.node.leaderHint(p))
	}
	deadline := time.Now().Add(opTimeout)
	for attempt := 0; ; attempt++ {
		g := s.node.group(p)
		if g != nil && g.Status().State == raft.Leader {
			err := local()
			if err == nil || !isNotLeader(err) {
				return err
			}
		} else {
			// Try the believed leader, then every replica that could host
			// the partition — necessary when this node doesn't host it (a
			// client may reach any node, and rebalancing moves partitions).
			fctx := markForwarded(ctx)
			for _, target := range s.node.routeTargets(p) {
				if target == "" || target == s.node.id {
					continue
				}
				if c := s.lodestoneClient(target); c != nil {
					err := forward(fctx, c)
					if err == nil || status.Code(err) != codes.FailedPrecondition {
						return err
					}
				}
			}
		}
		if time.Now().After(deadline) {
			return status.Error(codes.FailedPrecondition, "no reachable leader for partition")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(20<<min(attempt, 5)) * time.Millisecond
	if d > 500*time.Millisecond {
		d = 500 * time.Millisecond
	}
	return d
}

// --- hit payloads and connection caches ---

func (s *ClusterServer) toHits(g *Group, res []index.Result, payloads bool) []*pb.SearchHit {
	hits := make([]*pb.SearchHit, len(res))
	for i, r := range res {
		h := &pb.SearchHit{Id: r.ID, Score: r.Score}
		if payloads {
			if p, ok := g.Payload(r.ID); ok {
				h.Payload = p
			}
		}
		hits[i] = h
	}
	return hits
}

func (s *ClusterServer) adminClient(addr string) pb.ClusterAdminClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.admin[addr]; ok {
		return c
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	s.admin[addr] = pb.NewClusterAdminClient(conn)
	s.rawAdm[addr] = conn
	return s.admin[addr]
}

func (s *ClusterServer) lodestoneClient(addr string) pb.LodestoneClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reuse the admin connection's transport for the client service.
	if conn, ok := s.rawAdm[addr]; ok {
		return pb.NewLodestoneClient(conn)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	s.rawAdm[addr] = conn
	s.admin[addr] = pb.NewClusterAdminClient(conn)
	return pb.NewLodestoneClient(conn)
}

func notLeader(hint string) error {
	return status.Error(codes.FailedPrecondition, "not leader (hint: "+hint+")")
}

func toStatus(err error) error {
	var nl ErrNotLeader
	if errors.As(err, &nl) {
		return notLeader(nl.LeaderID)
	}
	if errors.Is(err, index.ErrDimension) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func isNotLeader(err error) bool {
	var nl ErrNotLeader
	return errors.As(err, &nl)
}
