// Package gateway serves the Lodestone dashboard and bridges the cluster's
// gRPC API to the browser. Browsers can't speak gRPC or reach a raw gRPC
// stream, so the gateway exposes telemetry as Server-Sent Events and search
// as plain JSON over HTTP, and serves the single-page dashboard from the same
// origin so the strict page sandbox can still reach these endpoints.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rattnak/lodestone/internal/client"
	"github.com/rattnak/lodestone/internal/embed"
)

// Gateway is an HTTP server in front of a lodestone cluster.
type Gateway struct {
	cl    *client.Client
	embed embed.Embedder // optional; enables text search
	mux   *http.ServeMux
}

// New builds a gateway over an existing cluster client. embedder may be nil,
// in which case the search endpoint accepts only raw vectors.
func New(cl *client.Client, embedder embed.Embedder) *Gateway {
	g := &Gateway{cl: cl, embed: embedder, mux: http.NewServeMux()}
	g.mux.HandleFunc("/", g.handleIndex)
	g.mux.HandleFunc("/api/telemetry", g.handleTelemetry)
	g.mux.HandleFunc("/api/search", g.handleSearch)
	g.mux.HandleFunc("/api/topology", g.handleTopology)
	g.mux.HandleFunc("/api/health", g.handleHealth)
	return g
}

func (g *Gateway) Handler() http.Handler { return g.mux }

// Serve runs the HTTP server until ctx is cancelled.
func (g *Gateway) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: g.mux}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (g *Gateway) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleTopology returns the cluster's partition/Raft layout for the ring and
// Raft panels. A standalone (non-cluster) node has no ClusterAdmin service, so
// this reports {"standalone": true} rather than an error.
func (g *Gateway) handleTopology(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	topo, err := g.cl.Describe(ctx)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"standalone": true})
		return
	}
	groups := make([]map[string]any, len(topo.Groups))
	for i, gr := range topo.Groups {
		groups[i] = map[string]any{
			"id": gr.ID, "state": gr.State, "leader": gr.Leader,
			"term": gr.Term, "lastApplied": gr.LastApplied,
			"members": gr.Members, "vectors": gr.Vectors,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": topo.Nodes, "partitions": topo.Partitions, "groups": groups,
	})
}

// handleTelemetry streams telemetry snapshots as Server-Sent Events. It opens
// a gRPC WatchTelemetry stream and relays each snapshot as a JSON SSE frame
// until the client disconnects.
func (g *Gateway) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	seq, errCheck, err := g.cl.WatchTelemetry(r.Context(), 1000)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for snap := range seq {
		b, _ := json.Marshal(map[string]any{
			"unixMs":         snap.UnixMs,
			"memtableBytes":  snap.MemtableBytes,
			"sstableCount":   snap.SSTableCount,
			"indexedVectors": snap.IndexedVectors,
			"puts":           snap.PutsTotal,
			"gets":           snap.GetsTotal,
			"searches":       snap.SearchesTotal,
		})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
		flusher.Flush()
	}
	_ = errCheck()
}

type searchRequest struct {
	Query  string    `json:"query"`
	Vector []float32 `json:"vector"`
	K      int       `json:"k"`
}

type searchHit struct {
	ID      uint64  `json:"id"`
	Score   float32 `json:"score"`
	Payload string  `json:"payload"`
}

// handleSearch runs a vector (or, with an embedder, a text) search and returns
// the hits as JSON — the data behind the dashboard's RAG panel.
func (g *Gateway) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.K <= 0 {
		req.K = 5
	}
	vec := req.Vector
	if len(vec) == 0 {
		if req.Query == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide a query or a vector"})
			return
		}
		if g.embed == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "text search needs an embedder; send a raw vector instead"})
			return
		}
		vecs, err := g.embed.Embed(r.Context(), []string{req.Query})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "embedding failed: " + err.Error()})
			return
		}
		vec = vecs[0]
	}
	hits, err := g.cl.SearchVectors(r.Context(), vec, req.K, 0, true)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	out := make([]searchHit, len(hits))
	for i, h := range hits {
		out[i] = searchHit{ID: h.ID, Score: h.Score, Payload: string(h.Payload)}
	}
	writeJSON(w, http.StatusOK, map[string]any{"hits": out})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
