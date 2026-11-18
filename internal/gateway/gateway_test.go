package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rattnak/lodestone/internal/client"
	"github.com/rattnak/lodestone/internal/server"
)

// liveServer starts a real single-node server and returns a connected client.
func liveServer(t *testing.T) *client.Client {
	t.Helper()
	srv, err := server.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); srv.Stop() })
	return client.FromConn(conn)
}

func TestGatewayServesDashboard(t *testing.T) {
	gw := New(liveServer(t), nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("index status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("index content-type %q", ct)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if !strings.Contains(buf.String(), "Lodestone") || !strings.Contains(buf.String(), "/api/telemetry") {
		t.Fatal("dashboard HTML missing expected markers")
	}
}

func TestGatewaySearchOverVectors(t *testing.T) {
	cl := liveServer(t)
	ctx := context.Background()
	// Seed documents directly, then search by raw vector through the gateway.
	for i := 1; i <= 20; i++ {
		vec := []float32{float32(i), float32(i) * 2, 1}
		if err := cl.UpsertDocument(ctx, uint64(i), vec, []byte(`{"title":"doc `+itoa(i)+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	gw := New(cl, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	body, _ := json.Marshal(searchRequest{Vector: []float32{10, 20, 1}, K: 3})
	resp, err := http.Post(srv.URL+"/api/search", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("search status %d", resp.StatusCode)
	}
	var out struct {
		Hits []searchHit `json:"hits"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Hits) != 3 || out.Hits[0].ID != 10 {
		t.Fatalf("search hits = %+v, want nearest id 10", out.Hits)
	}
	if !strings.Contains(out.Hits[0].Payload, "doc 10") {
		t.Fatalf("payload = %q", out.Hits[0].Payload)
	}
}

func TestGatewayTextSearchNeedsEmbedder(t *testing.T) {
	gw := New(liveServer(t), nil) // no embedder
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()
	body, _ := json.Marshal(searchRequest{Query: "hello", K: 3})
	resp, err := http.Post(srv.URL+"/api/search", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("text search without embedder status %d, want 400", resp.StatusCode)
	}
}

func TestGatewayTelemetryStream(t *testing.T) {
	cl := liveServer(t)
	cl.Put(context.Background(), []byte("k"), []byte("v"))
	gw := New(cl, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/telemetry", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("telemetry content-type %q", ct)
	}
	// Read the first SSE frame and confirm it's valid telemetry JSON.
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			var snap map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snap); err != nil {
				t.Fatalf("bad telemetry frame: %v", err)
			}
			if _, ok := snap["puts"]; !ok {
				t.Fatalf("telemetry frame missing fields: %v", snap)
			}
			return
		}
	}
	t.Fatal("no telemetry frame received")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
