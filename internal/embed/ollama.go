// Package embed turns text into vectors by calling a local Ollama server's
// embedding API. Keeping the model behind one small interface means the
// embedder can later be swapped for a hosted API without touching callers.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Embedder is what the rest of lodestone depends on.
type Embedder interface {
	// Embed returns one vector per input text, in order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

const (
	DefaultBaseURL = "http://localhost:11434"
	DefaultModel   = "nomic-embed-text"

	// maxBatch caps texts per request; larger inputs are split.
	maxBatch   = 64
	maxRetries = 3
)

// Client calls Ollama's /api/embed endpoint.
type Client struct {
	baseURL string
	model   string
	hc      *http.Client
	// dim is learned from the first response and enforced afterwards, so a
	// model swap mid-run cannot silently mix vector spaces.
	dim int
}

func NewClient(baseURL, model string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if model == "" {
		model = DefaultModel
	}
	return &Client{
		baseURL: baseURL,
		model:   model,
		hc:      &http.Client{Timeout: 60 * time.Second},
	}
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	Error      string      `json:"error"`
}

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += maxBatch {
		batch := texts[start:min(start+maxBatch, len(texts))]
		vecs, err := c.embedBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (c *Client) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(embedRequest{Model: c.model, Input: texts})
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		vecs, retryable, err := c.doRequest(ctx, body, len(texts))
		if err == nil {
			return vecs, nil
		}
		lastErr = err
		if !retryable {
			break
		}
	}
	return nil, fmt.Errorf("embed: %w", lastErr)
}

func (c *Client) doRequest(ctx context.Context, body []byte, want int) (vecs [][]float32, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, true, err // network errors are worth retrying
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return nil, true, err
	}
	if resp.StatusCode != http.StatusOK {
		var er embedResponse
		msg := string(data)
		if json.Unmarshal(data, &er) == nil && er.Error != "" {
			msg = er.Error
		}
		return nil, resp.StatusCode >= 500, fmt.Errorf("ollama %d: %s", resp.StatusCode, msg)
	}
	var er embedResponse
	if err := json.Unmarshal(data, &er); err != nil {
		return nil, false, err
	}
	if len(er.Embeddings) != want {
		return nil, false, fmt.Errorf("ollama returned %d embeddings for %d inputs", len(er.Embeddings), want)
	}
	for _, v := range er.Embeddings {
		if c.dim == 0 {
			c.dim = len(v)
		}
		if len(v) == 0 || len(v) != c.dim {
			return nil, false, fmt.Errorf("ollama returned %d-dim vector, expected %d", len(v), c.dim)
		}
	}
	return er.Embeddings, false, nil
}
