package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/rattnak/lodestone/internal/client"
	"github.com/rattnak/lodestone/internal/server/pb"
)

// startServer runs a Server over an in-memory transport and returns a
// connected client and a stop function. Stop drains the server and closes
// the engine so the dataDir can be reopened by a second startServer call.
func startServer(t *testing.T, dataDir string) (*client.Client, func()) {
	t.Helper()
	srv, err := New(dataDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	c := client.FromConn(conn)
	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		c.Close()
		srv.Stop()
	}
	t.Cleanup(stop)
	return c, stop
}

func TestKVOverGRPC(t *testing.T) {
	c, _ := startServer(t, t.TempDir())
	ctx := context.Background()

	if err := c.Put(ctx, []byte("greeting"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	v, found, err := c.Get(ctx, []byte("greeting"))
	if err != nil || !found || string(v) != "hello" {
		t.Fatalf("Get = %q %v %v", v, found, err)
	}
	if err := c.Delete(ctx, []byte("greeting")); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.Get(ctx, []byte("greeting")); found {
		t.Fatal("deleted key still found")
	}
	// Reserved and invalid keys are rejected with InvalidArgument.
	for _, bad := range [][]byte{nil, {0x00, 'x'}} {
		err := c.Put(ctx, bad, []byte("v"))
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Put(%v) error = %v, want InvalidArgument", bad, err)
		}
	}
}

func docVec(i int) []float32 {
	return []float32{float32(i), float32(i) * 2, 1}
}

func TestDocumentsAndSearch(t *testing.T) {
	c, _ := startServer(t, t.TempDir())
	ctx := context.Background()

	for i := 1; i <= 50; i++ {
		payload := fmt.Appendf(nil, "doc number %d", i)
		if err := c.UpsertDocument(ctx, uint64(i), docVec(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := c.SearchVectors(ctx, docVec(25), 3, 200, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 3 || hits[0].ID != 25 {
		t.Fatalf("search near doc 25 = %+v", hits)
	}
	if string(hits[0].Payload) != "doc number 25" {
		t.Fatalf("payload = %q", hits[0].Payload)
	}

	// Upsert replaces: move doc 25 far away, search again.
	if err := c.UpsertDocument(ctx, 25, []float32{-1000, -1000, 1}, []byte("moved")); err != nil {
		t.Fatal(err)
	}
	hits, _ = c.SearchVectors(ctx, docVec(25), 1, 200, false)
	if len(hits) != 1 || hits[0].ID == 25 {
		t.Fatalf("moved doc still nearest: %+v", hits)
	}

	// Delete excludes from search.
	if err := c.DeleteDocument(ctx, 30); err != nil {
		t.Fatal(err)
	}
	hits, _ = c.SearchVectors(ctx, docVec(30), 5, 200, false)
	for _, h := range hits {
		if h.ID == 30 {
			t.Fatal("deleted doc returned from search")
		}
	}

	// Dimension mismatch is the client's fault, not a server crash.
	if _, err := c.SearchVectors(ctx, []float32{1, 2}, 1, 0, false); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("dimension mismatch error = %v, want InvalidArgument", err)
	}
}

func TestRestartRebuildsIndexFromEngine(t *testing.T) {
	dir := t.TempDir()
	c, stop := startServer(t, dir)
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		if err := c.UpsertDocument(ctx, uint64(i), docVec(i), fmt.Appendf(nil, "doc %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.DeleteDocument(ctx, 10)
	stop()

	// A fresh server on the same directory must serve identical results:
	// the index is derived state, rebuilt from the engine.
	c2, _ := startServer(t, dir)
	hits, err := c2.SearchVectors(ctx, docVec(15), 3, 200, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 3 || hits[0].ID != 15 || string(hits[0].Payload) != "doc 15" {
		t.Fatalf("after restart, search = %+v", hits)
	}
	hits, _ = c2.SearchVectors(ctx, docVec(10), 20, 200, false)
	for _, h := range hits {
		if h.ID == 10 {
			t.Fatal("deleted doc resurrected by restart rebuild")
		}
	}
}

func TestTelemetryStream(t *testing.T) {
	c, _ := startServer(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.Put(ctx, []byte("k"), []byte("v"))
	c.UpsertDocument(ctx, 1, []float32{1, 2, 3}, nil)

	seq, errCheck, err := c.WatchTelemetry(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var got client.Telemetry
	n := 0
	for snap := range seq {
		got = snap
		if n++; n >= 2 {
			break
		}
	}
	if err := errCheck(); err != nil {
		t.Fatal(err)
	}
	if got.PutsTotal < 2 || got.IndexedVectors != 1 || got.UnixMs == 0 {
		t.Fatalf("telemetry = %+v", got)
	}
}

// TestSchemaEvolution pins the DDIA ch 4 property the protocol relies on: a
// message carrying fields this schema has never heard of must decode cleanly
// and keep the known fields intact — that is what lets old servers accept
// requests from newer clients.
func TestSchemaEvolution(t *testing.T) {
	msg := &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}
	wire, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	// A "future client" adds field 99 (bytes) and field 100 (varint).
	wire = protowire.AppendTag(wire, 99, protowire.BytesType)
	wire = protowire.AppendBytes(wire, []byte("from the future"))
	wire = protowire.AppendTag(wire, 100, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 42)

	var decoded pb.PutRequest
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("old schema rejected a newer message: %v", err)
	}
	if string(decoded.Key) != "k" || string(decoded.Value) != "v" {
		t.Fatalf("known fields corrupted by unknown ones: %+v", &decoded)
	}
	// Unknown fields are preserved on reserialization, so an old proxy
	// does not strip data it does not understand.
	rewire, err := proto.Marshal(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewire) < len(wire) {
		t.Fatalf("unknown fields dropped: %d bytes -> %d", len(wire), len(rewire))
	}
}
