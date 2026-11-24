# Lodestone

> A distributed vector database built from scratch in Go — a log-structured
> storage engine, an HNSW vector index, Raft replication, consistent-hash
> partitioning, a streaming ingestion pipeline, and a live web console. No
> database, consensus, or search libraries: every layer is implemented from
> first principles.

Lodestone was built to learn the ideas in *Designing Data-Intensive
Applications* by implementing them, and to be a system you can actually run:
live events stream in, get embedded into vectors, and become semantically
searchable within seconds across a replicated, partitioned cluster.

## Architecture

```
 Wikipedia / Hacker News ─▶ lodelog ─▶ embedder ─▶ ┌─────────────────────────────┐
   (live event streams)   (mini-Kafka)  (Ollama)   │   lodestone cluster          │
                                                    │   • LSM storage engine       │  ch 3
                                                    │   • HNSW vector index        │  ch 3
                                                    │   • Raft replication         │  ch 5,9
                                                    │   • consistent-hash sharding │  ch 6
                                                    └───────┬──────────────┬───────┘
                                                      gRPC  │              │ SSE / HTTP
                                                   ┌────────▼───┐   ┌──────▼─────────┐
                                                   │  clients   │   │  web console   │
                                                   └────────────┘   └────────────────┘
```

## What's inside

| Layer | What it does | Package | DDIA |
|---|---|---|---|
| **Storage engine** | LSM tree: write-ahead log, skiplist memtable, SSTables, bloom filters, compaction | `internal/storage` | ch 3 |
| **Vector index** | HNSW approximate nearest-neighbor search, persisted, with metadata filters | `internal/index` | ch 3 |
| **Server** | gRPC + protobuf API, typed client, derived-index-from-log | `internal/server` | ch 4 |
| **Consensus** | Raft from scratch: elections, replication, snapshots, membership, pre-vote, check-quorum reads | `internal/raft` | ch 5, 9 |
| **Partitioning** | Rendezvous hashing, request routing, live rebalancing, scatter-gather search | `internal/cluster` | ch 6 |
| **Streaming** | `lodelog` append-only broker, Wikipedia/HN connectors, idempotent embedder | `internal/lodelog`, `internal/ingest` | ch 10, 11 |
| **Console** | Live web dashboard bridging gRPC telemetry to the browser | `internal/gateway` | — |

## Correctness

Distributed systems fail in ways happy-path tests never see, so the testing is
the point:

- **Deterministic chaos simulator** (`internal/raft`) — drives whole Raft
  clusters through seeded message loss, delays, partitions, and crash/restart.
  Every failure replays exactly from its seed. The nightly [torture job](.github/workflows/torture.yml)
  runs 5000 seeds.
- **Linearizability checking** (`internal/cluster`) — a real replicated
  key-value register under concurrent load and leader-killing faults, verified
  with [porcupine](https://github.com/anishathalye/porcupine), with a negative
  control proving the check isn't vacuous.
- **Reference-model fuzzing** throughout — thousands of random operations
  mirrored against a plain Go map, with random flushes, compactions, and
  reopens, asserting agreement.

Chaos sweeps caught several real consensus bugs before they could hide; the
devlogs tell those stories.

## Run it

Requires Go 1.26+. For live ingestion and text search, also
[Ollama](https://ollama.com) with `ollama pull nomic-embed-text`.

```sh
# a 3-node cluster
go run ./cmd/lodestone cluster -addr 127.0.0.1:7601 -data ./n1 -bootstrap
go run ./cmd/lodestone cluster -addr 127.0.0.1:7602 -data ./n2 -seeds 127.0.0.1:7601
go run ./cmd/lodestone cluster -addr 127.0.0.1:7603 -data ./n3 -seeds 127.0.0.1:7601

# the live console  →  http://localhost:8437
go run ./cmd/lodestone dashboard -server 127.0.0.1:7601

# stream live data in
go run ./cmd/lodestone ingest -data ./ingest -server 127.0.0.1:7601 \
  -sources wikipedia,hackernews
```

```sh
go build ./...
go test -race ./...                              # full suite
go test ./internal/storage/ -bench . -run ^$     # benchmarks (see docs/BENCHMARKS.md)
```

## Docs

- [`docs/devlog/`](docs/devlog/) — one write-up per phase: the design, what
  broke, and what the book said.
- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — numbers and profiling.
- [`learn/`](learn/) — a rebuild-it-yourself track: specs and tests to
  reconstruct each component from scratch.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — how changes land: one branch, one
  reviewed pull request, never a direct push to `main`.

## License

[MIT](LICENSE)
