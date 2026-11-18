package ingest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rattnak/lodestone/internal/lodelog"
)

// fakeEmbedder returns a deterministic vector per text (length-based) so tests
// can assert without a real model.
type fakeEmbedder struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = []float32{float32(len(t)), 1, 0}
	}
	return out, nil
}

// fakeWriter records upserts and can be inspected for idempotency.
type fakeWriter struct {
	mu    sync.Mutex
	docs  map[uint64][]byte
	count int // total upsert calls, including re-writes
}

func newFakeWriter() *fakeWriter { return &fakeWriter{docs: map[uint64][]byte{}} }

func (w *fakeWriter) UpsertDocument(_ context.Context, id uint64, _ []float32, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.docs[id] = payload
	w.count++
	return nil
}

func (w *fakeWriter) unique() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.docs)
}

// drainEmbedder runs the embedder until its output count has been stable for
// a short window (caught up), then stops it.
func drainEmbedder(t *testing.T, e *Embedder, w *fakeWriter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()
	last, stable, deadline := -1, 0, time.Now().Add(5*time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(40 * time.Millisecond)
		n := w.unique()
		if n == last {
			if stable++; stable >= 3 {
				break
			}
		} else {
			last, stable = n, 0
		}
	}
	cancel()
	<-done
}

func TestEmbedderIndexesEvents(t *testing.T) {
	log, _ := lodelog.Open(t.TempDir(), 2)
	defer log.Close()
	pub := NewPublisher(log)
	for i := 0; i < 20; i++ {
		pub.Publish(Event{Source: "test", ID: fmt.Sprint(i), Title: fmt.Sprintf("title %d", i), Text: "body"})
	}
	log.Sync()

	w := newFakeWriter()
	e, err := NewEmbedder(Config{Log: log, Embedder: &fakeEmbedder{}, Writer: w, Batch: 5})
	if err != nil {
		t.Fatal(err)
	}
	drainEmbedder(t, e, w)

	if w.unique() != 20 {
		t.Fatalf("indexed %d unique docs, want 20", w.unique())
	}
}

func TestEmbedderDedupesRedelivery(t *testing.T) {
	// Publish the same event id multiple times (as a flaky source might);
	// the stable DocID means they collapse to one document.
	log, _ := lodelog.Open(t.TempDir(), 1)
	defer log.Close()
	pub := NewPublisher(log)
	for i := 0; i < 5; i++ {
		pub.Publish(Event{Source: "s", ID: "dup", Title: "same", Text: "same"})
	}
	pub.Publish(Event{Source: "s", ID: "other", Title: "x", Text: "y"})
	log.Sync()

	w := newFakeWriter()
	e, _ := NewEmbedder(Config{Log: log, Embedder: &fakeEmbedder{}, Writer: w, Batch: 10})
	drainEmbedder(t, e, w)
	if w.unique() != 2 {
		t.Fatalf("unique docs %d, want 2 (dup collapsed)", w.unique())
	}
}

func TestEmbedderResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	log, _ := lodelog.Open(dir, 1)
	pub := NewPublisher(log)
	for i := 0; i < 10; i++ {
		pub.Publish(Event{Source: "s", ID: fmt.Sprint(i), Title: "t", Text: "b"})
	}
	log.Sync()

	w1 := newFakeWriter()
	e1, _ := NewEmbedder(Config{Log: log, Embedder: &fakeEmbedder{}, Writer: w1, Batch: 4})
	drainEmbedder(t, e1, w1)
	if w1.unique() != 10 {
		t.Fatalf("first run indexed %d, want 10", w1.unique())
	}
	log.Close()

	// New process, same group: committed offsets mean no re-processing.
	log2, _ := lodelog.Open(dir, 1)
	defer log2.Close()
	for i := 10; i < 15; i++ {
		pub2 := NewPublisher(log2)
		pub2.Publish(Event{Source: "s", ID: fmt.Sprint(i), Title: "t", Text: "b"})
	}
	log2.Sync()
	w2 := newFakeWriter()
	e2, _ := NewEmbedder(Config{Log: log2, Embedder: &fakeEmbedder{}, Writer: w2, Batch: 4})
	drainEmbedder(t, e2, w2)
	// Only the 5 new events (10..14) should be processed on resume.
	if w2.unique() != 5 {
		t.Fatalf("resumed run indexed %d, want 5 (offsets resumed)", w2.unique())
	}
}

func TestReindexFromScratch(t *testing.T) {
	log, _ := lodelog.Open(t.TempDir(), 2)
	defer log.Close()
	pub := NewPublisher(log)
	for i := 0; i < 30; i++ {
		pub.Publish(Event{Source: "s", ID: fmt.Sprint(i), Title: "t", Text: "b"})
	}
	log.Sync()

	w := newFakeWriter()
	e, _ := NewEmbedder(Config{Log: log, Embedder: &fakeEmbedder{}, Writer: w, Batch: 8})
	drainEmbedder(t, e, w)
	if w.unique() != 30 {
		t.Fatalf("live run indexed %d, want 30", w.unique())
	}

	// A from-scratch rebuild re-processes every event (derived data, ch 10).
	w2 := newFakeWriter()
	e2, _ := NewEmbedder(Config{Log: log, Embedder: &fakeEmbedder{}, Writer: w2, Batch: 8})
	n, err := e2.ReindexFromScratch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 30 || w2.unique() != 30 {
		t.Fatalf("reindex processed %d / %d unique, want 30/30", n, w2.unique())
	}
}

func TestWikipediaSSEParsing(t *testing.T) {
	// A fake SSE endpoint serving two recent-change frames; only the valid
	// enwiki article edit should produce an event.
	body := "event: message\n" +
		`data: {"type":"edit","wiki":"enwiki","namespace":0,"bot":false,"title":"Go (programming language)","comment":"copyedit","meta":{"id":"abc-1","uri":"https://en.wikipedia.org/wiki/Go"}}` + "\n\n" +
		`data: {"type":"edit","wiki":"dewiki","namespace":0,"title":"Skip me"}` + "\n\n" +
		`data: {"type":"log","wiki":"enwiki","namespace":2,"title":"Not an article"}` + "\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(body))
	}))
	defer srv.Close()

	var got []Event
	var mu sync.Mutex
	src := &WikipediaSource{URL: srv.URL}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	src.Run(ctx, func(e Event) error {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
		return nil
	})

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("no events parsed from SSE")
	}
	if got[0].Title != "Go (programming language)" || got[0].ID != "abc-1" {
		t.Fatalf("parsed event = %+v", got[0])
	}
}

func TestHackerNewsPolling(t *testing.T) {
	// Fake HN API: maxitem returns 3; items 1-3 are a story, a comment, and
	// a dead story. Only the live story should be emitted.
	mux := http.NewServeMux()
	mux.HandleFunc("/maxitem.json", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("3"))
	})
	items := map[string]string{
		"/item/1.json": `{"type":"story","title":"Show HN: Lodestone","url":"http://x","by":"a"}`,
		"/item/2.json": `{"type":"comment","text":"nice"}`,
		"/item/3.json": `{"type":"story","title":"Dead","dead":true}`,
	}
	for path, body := range items {
		b := body
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(b)) })
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var got []Event
	var mu sync.Mutex
	src := &HackerNewsSource{BaseURL: srv.URL, Interval: 50 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	src.Run(ctx, func(e Event) error {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
		return nil
	})

	mu.Lock()
	defer mu.Unlock()
	var stories int
	for _, e := range got {
		if e.Title == "Show HN: Lodestone" {
			stories++
		}
	}
	if stories == 0 {
		t.Fatalf("expected the live story, got %+v", got)
	}
}

func TestEventDocIDStableAndDistinct(t *testing.T) {
	a := Event{Source: "wikipedia", ID: "123"}
	if a.DocID() != a.DocID() {
		t.Fatal("DocID not stable")
	}
	// Same id, different source must not collide.
	b := Event{Source: "hackernews", ID: "123"}
	if a.DocID() == b.DocID() {
		t.Fatal("cross-source DocID collision")
	}
}
