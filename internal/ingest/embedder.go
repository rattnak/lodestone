package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/rattnak/lodestone/internal/embed"
	"github.com/rattnak/lodestone/internal/lodelog"
)

// Writer is the sink the embedder upserts documents into — satisfied by the
// cluster node and by the single-node client. Upserts must be idempotent in
// the document id: redelivering an event (at-least-once) re-writes the same
// document rather than duplicating it.
type Writer interface {
	UpsertDocument(ctx context.Context, id uint64, vector []float32, payload []byte) error
}

// Embedder consumes events from the log, embeds their text, and upserts the
// results as documents. It commits log offsets only after a batch is durably
// written, so a crash replays at-least-once; the stable DocID makes that
// replay a harmless re-upsert.
type Embedder struct {
	log      *lodelog.Log
	consumer *lodelog.Consumer
	embed    embed.Embedder
	writer   Writer
	batch    int
	log2     *slog.Logger

	// dedupe suppresses re-embedding an event whose id we have already
	// processed in this run — a cheap guard against the common redelivery
	// case, on top of the idempotent upsert that handles the rest.
	seen map[string]struct{}
}

// Config assembles an Embedder.
type Config struct {
	Log      *lodelog.Log
	Embedder embed.Embedder
	Writer   Writer
	Group    string // consumer group; defaults to "embedder"
	Batch    int    // events embedded per request; defaults to 32
	Logger   *slog.Logger
}

func NewEmbedder(cfg Config) (*Embedder, error) {
	group := cfg.Group
	if group == "" {
		group = "embedder"
	}
	c, err := lodelog.NewConsumer(cfg.Log, group)
	if err != nil {
		return nil, err
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 32
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Embedder{
		log:      cfg.Log,
		consumer: c,
		embed:    cfg.Embedder,
		writer:   cfg.Writer,
		batch:    batch,
		log2:     logger,
		seen:     map[string]struct{}{},
	}, nil
}

// Run drains every partition in a loop until ctx is cancelled, sleeping
// briefly when the log is fully caught up. Returns the number of documents
// indexed over the run.
func (e *Embedder) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		progressed := false
		for part := 0; part < e.log.Partitions(); part++ {
			n, err := e.drainPartition(ctx, part)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				e.log2.Warn("embedder partition error", "partition", part, "err", err)
				continue
			}
			if n > 0 {
				progressed = true
			}
		}
		if !progressed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return ctx.Err()
}

// drainPartition processes one batch from a partition and returns how many
// events it handled (0 means caught up).
func (e *Embedder) drainPartition(ctx context.Context, part int) (int, error) {
	msgs, err := e.consumer.Poll(part, e.batch)
	if err != nil || len(msgs) == 0 {
		return 0, err
	}
	if err := e.process(ctx, msgs, e.seen); err != nil {
		return 0, err
	}
	// Commit only after the batch is durably upserted: at-least-once.
	return len(msgs), e.consumer.Commit(part, msgs[len(msgs)-1].Offset+1)
}

// process embeds and upserts a batch. seen suppresses re-embedding ids
// already handled in this run (nil disables it, as a from-scratch rebuild
// wants). The upsert is idempotent either way, so dedup is an optimization,
// not a correctness requirement.
func (e *Embedder) process(ctx context.Context, msgs []lodelog.Message, seen map[string]struct{}) error {
	events := make([]Event, 0, len(msgs))
	texts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ev, err := decodeEvent(m.Value)
		if err != nil {
			continue // skip undecodable records rather than stalling the stream
		}
		if seen != nil {
			if _, dup := seen[ev.Source+"/"+ev.ID]; dup {
				continue
			}
		}
		events = append(events, ev)
		texts = append(texts, ev.embedText())
	}
	if len(events) == 0 {
		return nil
	}
	vecs, err := e.embed.Embed(ctx, texts)
	if err != nil {
		return err
	}
	for i, ev := range events {
		if err := e.writer.UpsertDocument(ctx, ev.DocID(), vecs[i], m2payload(ev)); err != nil {
			return err
		}
		if seen != nil {
			seen[ev.Source+"/"+ev.ID] = struct{}{}
		}
	}
	return nil
}

// payload stored with the document: enough to render a search hit.
func m2payload(e Event) []byte {
	b, _ := e.encode()
	return b
}

// ReindexFromScratch replays every partition from offset 0, re-embedding and
// re-upserting all events — the derived-data rebuild (DDIA ch 10): the log is
// the source of truth and the indexed documents are a view that can be rebuilt
// at any time. Uses a distinct consumer group so it does not disturb the live
// embedder's offsets.
func (e *Embedder) ReindexFromScratch(ctx context.Context) (int, error) {
	batchC, err := lodelog.NewConsumer(e.log, "reindex")
	if err != nil {
		return 0, err
	}
	total := 0
	fresh := map[string]struct{}{} // dedupe within this rebuild only
	for part := 0; part < e.log.Partitions(); part++ {
		if err := batchC.Seek(part, 0); err != nil {
			return total, err
		}
		for {
			msgs, err := batchC.Poll(part, e.batch)
			if err != nil {
				return total, err
			}
			if len(msgs) == 0 {
				break
			}
			if err := e.process(ctx, msgs, fresh); err != nil {
				return total, err
			}
			total += len(msgs)
			if err := batchC.Commit(part, msgs[len(msgs)-1].Offset+1); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}
