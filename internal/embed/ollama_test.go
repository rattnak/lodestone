package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func fakeOllama(t *testing.T, dim int, fail *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embed" {
			http.NotFound(w, r)
			return
		}
		if fail != nil && fail.Add(-1) >= 0 {
			http.Error(w, `{"error":"model busy"}`, http.StatusInternalServerError)
			return
		}
		var req embedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := embedResponse{Embeddings: make([][]float32, len(req.Input))}
		for i, text := range req.Input {
			v := make([]float32, dim)
			for j := range v {
				v[j] = float32(len(text)+i) / float32(j+1)
			}
			resp.Embeddings[i] = v
		}
		json.NewEncoder(w).Encode(resp)
	}))
}

func TestEmbedBatchingAndOrder(t *testing.T) {
	srv := fakeOllama(t, 8, nil)
	defer srv.Close()
	c := NewClient(srv.URL, "test-model")

	// 150 texts forces three batches (cap is 64); order must be preserved.
	texts := make([]string, 150)
	for i := range texts {
		texts[i] = string(make([]byte, i+1)) // length i+1 → distinct vectors
	}
	vecs, err := c.Embed(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 150 {
		t.Fatalf("got %d vectors, want 150", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 8 {
			t.Fatalf("vector %d has dim %d", i, len(v))
		}
		// First component encodes len(text)+indexInBatch, so adjacent
		// outputs must differ — a shuffled batch would collide.
		if i > 0 && v[0] == vecs[i-1][0] {
			t.Fatalf("vectors %d and %d identical: batching lost order", i-1, i)
		}
	}
}

func TestEmbedRetriesTransientErrors(t *testing.T) {
	var fail atomic.Int32
	fail.Store(2) // two 500s, then success
	srv := fakeOllama(t, 4, &fail)
	defer srv.Close()
	c := NewClient(srv.URL, "test-model")
	vecs, err := c.Embed(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatalf("should have recovered after retries: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) != 4 {
		t.Fatalf("unexpected result %v", vecs)
	}
}

func TestEmbedGivesUpAfterMaxRetries(t *testing.T) {
	var fail atomic.Int32
	fail.Store(100)
	srv := fakeOllama(t, 4, &fail)
	defer srv.Close()
	c := NewClient(srv.URL, "test-model")
	if _, err := c.Embed(context.Background(), []string{"hello"}); err == nil {
		t.Fatal("persistent 500s did not surface as an error")
	}
}

func TestEmbedRejectsDimensionDrift(t *testing.T) {
	dims := []int{8, 8, 4} // third request shrinks: must be rejected
	i := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req embedRequest
		json.NewDecoder(r.Body).Decode(&req)
		d := dims[min(i, len(dims)-1)]
		i++
		resp := embedResponse{Embeddings: make([][]float32, len(req.Input))}
		for j := range resp.Embeddings {
			resp.Embeddings[j] = make([]float32, d)
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "test-model")
	ctx := context.Background()
	if _, err := c.Embed(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Embed(ctx, []string{"b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Embed(ctx, []string{"c"}); err == nil {
		t.Fatal("dimension change across requests accepted")
	}
}

func TestEmbedEmptyInput(t *testing.T) {
	c := NewClient("http://127.0.0.1:1", "test") // unreachable: must not be called
	vecs, err := c.Embed(context.Background(), nil)
	if err != nil || vecs != nil {
		t.Fatalf("empty input: %v %v", vecs, err)
	}
}
