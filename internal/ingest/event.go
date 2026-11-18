// Package ingest connects live event sources to the cluster: source
// connectors publish events into lodelog, and the embedder worker consumes
// them, turns their text into vectors, and upserts documents into Lodestone.
package ingest

import (
	"encoding/json"
	"hash/fnv"
)

// Event is a normalized ingestion record. Sources map their payloads into
// this shape before publishing to the log, so the embedder is source-
// agnostic. ID is a stable, source-unique string: the embedder derives the
// document id from it and dedupes on it, which is what makes redelivery
// (at-least-once) safe.
type Event struct {
	Source string `json:"source"` // "wikipedia", "hackernews", ...
	ID     string `json:"id"`     // stable unique id within the source
	Title  string `json:"title"`
	Text   string `json:"text"` // the text that gets embedded
	URL    string `json:"url,omitempty"`
}

func (e Event) encode() ([]byte, error) { return json.Marshal(e) }

func decodeEvent(b []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(b, &e)
	return e, err
}

// DocID maps an event's source+id to a stable 64-bit document id. Collisions
// across sources are avoided by hashing the source prefix too. The same event
// always maps to the same id, so a redelivered event upserts in place rather
// than creating a duplicate.
func (e Event) DocID() uint64 {
	h := fnv.New64a()
	h.Write([]byte(e.Source))
	h.Write([]byte{0})
	h.Write([]byte(e.ID))
	return h.Sum64()
}

// embedText is what the embedder sends to the model: title and body joined,
// so short-title events still carry signal.
func (e Event) embedText() string {
	if e.Text == "" {
		return e.Title
	}
	if e.Title == "" {
		return e.Text
	}
	return e.Title + "\n\n" + e.Text
}

// Partitioner decides which log partition an event lands in. Keying by event
// id spreads load while keeping a given event always on one partition (so its
// redeliveries stay ordered relative to each other).
func partitionFor(id string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(n))
}
