# Phase 6 — Streaming ingestion

*November 2026 · DDIA chapters 11 (streams) and 10 (derived data)*

Phases 1–5 built a distributed vector database you write to directly. Phase 6
makes it *live*: real-world events flow in continuously, get embedded, and
become semantically searchable within seconds.

## The pipeline

```
 Wikipedia SSE ─┐
                ├─▶ lodelog (partitioned log) ─▶ embedder ─▶ cluster upsert
 Hacker News ──┘        at-least-once            (Ollama)     (idempotent)
```

- **lodelog** (`internal/lodelog`) — a minimal Kafka: a topic split into
  partitions, each an ordered sequence of CRC-framed records with a dense
  monotonic offset, fsync durability, and torn-tail recovery — the same
  durability design as the storage engine's WAL. Consumers track per-partition
  offsets that survive restarts.
- **Source connectors** (`internal/ingest/source.go`) — Wikipedia's
  recent-changes feed over Server-Sent Events (a push stream, with reconnect)
  and Hacker News via API polling (a pull source tracking the max item id).
  Both normalize into a single `Event` shape so the embedder is
  source-agnostic.
- **Embedder worker** (`internal/ingest/embedder.go`) — consumes the log,
  embeds event text in batches via Ollama, and upserts documents into the
  cluster.

## The two ideas worth keeping

**At-least-once, made safe by idempotency.** The embedder commits a
partition's offset only *after* a batch is durably upserted. A crash between
upsert and commit therefore replays the last batch — at-least-once delivery,
the honest guarantee for a log consumer. What makes replay harmless is that a
document's id is a stable hash of its source and event id, so a redelivered
event re-writes the same document instead of creating a duplicate.
At-least-once delivery plus idempotent writes equals effectively-exactly-once
state, without distributed transactions — the pattern DDIA ch 11 builds toward.

**The log is the source of truth; the index is a view.** `ReindexFromScratch`
replays every partition from offset 0 and rebuilds all the indexed documents.
This is DDIA ch 10's derived-data idea in miniature: the durable log is
authoritative, and the searchable documents are a materialized view that can
be thrown away and recomputed — to change the embedding model, to recover from
index corruption, or just to reprocess history.

## Running it

```sh
ollama pull nomic-embed-text
lodestone serve  -data ./node1 -addr :7437 &
lodestone ingest -data ./ingest -server localhost:7437 -sources wikipedia,hackernews
```

Within seconds, live Wikipedia edits and HN stories are vectors you can search.

## Note

Everything here is tested against fakes — a fake SSE endpoint, a fake HN API,
a fake embedder and writer — so the suite runs offline and deterministically.
The real connectors hit public endpoints; the `ingest` command wires them to a
live Ollama and cluster.
