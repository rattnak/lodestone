package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rattnak/lodestone/internal/raft"
	"github.com/rattnak/lodestone/internal/server/pb"
)

// grpcTransport routes raft messages to peer nodes over gRPC. Message.To is a
// node address (host:port); connections are dialed lazily and cached. Sends
// are fire-and-forget on their own goroutines: a slow or dead peer must never
// stall the Raft state machine that queued the message.
type grpcTransport struct {
	mu      sync.Mutex
	conns   map[string]pb.RaftTransportClient
	rawConn map[string]*grpc.ClientConn
	closed  bool
}

func newGRPCTransport() *grpcTransport {
	return &grpcTransport{
		conns:   map[string]pb.RaftTransportClient{},
		rawConn: map[string]*grpc.ClientConn{},
	}
}

// SendGroup satisfies GroupTransport; Group wraps it per-group internally.
func (t *grpcTransport) SendGroup(gid uint32, m raft.Message) { t.send(gid, m) }

func (t *grpcTransport) send(gid uint32, m raft.Message) {
	client := t.clientFor(m.To)
	if client == nil {
		return
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Errors are ignored by design; Raft retransmits on its own clock.
		_, _ = client.Send(ctx, &pb.RaftEnvelope{Group: gid, Message: buf.Bytes()})
	}()
}

func (t *grpcTransport) clientFor(addr string) pb.RaftTransportClient {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	if c, ok := t.conns[addr]; ok {
		return c
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	c := pb.NewRaftTransportClient(conn)
	t.conns[addr] = c
	t.rawConn[addr] = conn
	return c
}

func (t *grpcTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, c := range t.rawConn {
		c.Close()
	}
}
