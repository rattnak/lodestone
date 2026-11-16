# Lodestone

> A distributed vector database built from scratch in Go — LSM storage engine, HNSW vector index, Raft replication, consistent-hash partitioning — fed by a real-time streaming ingestion pipeline and queried by a RAG application.

**Status: early development.** Lodestone is being built in public, phase by phase. Progress is tracked in the [issue tracker](../../issues) and documented in [`docs/devlog/`](docs/devlog/).

## Why

Lodestone is a deep dive into the ideas in *Designing Data-Intensive Applications*: instead of reading about storage engines, replication, and stream processing, every major component is implemented from first principles — then wired into a system you can actually use: live events stream in, get embedded into vectors, and become semantically searchable within seconds.

## Architecture

```
 live event streams                    ┌──────────────────────────────┐
 (Wikipedia edits, HN)   ─────────▶   │  lodelog (mini log broker)   │
                                      │  append-only, partitioned    │
                                      └──────────┬───────────────────┘
                                                 │ consumers
                                      ┌──────────▼───────────────────┐
                                      │  embedder worker             │
                                      │  text → vector               │
                                      └──────────┬───────────────────┘
                                                 │ gRPC writes
                   ┌─────────────────────────────▼────────────────────┐
                   │   lodestone cluster (3+ nodes)                   │
                   │   • LSM storage engine (WAL, memtable, SSTables) │
                   │   • HNSW vector index + metadata filters         │
                   │   • Raft replication / leader election           │
                   │   • consistent-hash partitioning + rebalancing   │
                   └───────────┬───────────────────────┬──────────────┘
                               │ query API             │ telemetry
                   ┌───────────▼──────────┐  ┌─────────▼──────────────┐
                   │  RAG demo app        │  │  live cluster dashboard │
                   └──────────────────────┘  └────────────────────────┘
```

## Roadmap

| Phase | Component | Highlights |
|-------|-----------|------------|
| 1 | Storage engine | WAL, memtable, SSTables, compaction, bloom filters |
| 2 | Vector index | HNSW from the paper, metadata filtering |
| 3 | Server | gRPC + protobuf API, client library |
| 4 | Replication | Raft from scratch: elections, log replication, snapshots |
| 5 | Partitioning | Consistent hashing, rebalancing, scatter-gather search |
| 6 | Streaming | lodelog broker, live source connectors, embedder pipeline |
| 7 | Dashboard + RAG | Live cluster visualization, chat over ingested data |
| 8 | Chaos | Fault injection, linearizability checking |
| 9 | Ship | Benchmarks, demo, write-ups |

## Repository layout

```
cmd/lodestone/        entry point
internal/storage/     LSM storage engine
internal/index/       HNSW vector index
internal/server/      gRPC server + protobuf definitions
internal/raft/        Raft consensus
internal/cluster/     partitioning, routing, rebalancing
lodelog/              log broker, stream sources, embedder worker
dashboard/            React dashboard + RAG chat UI
chaos/                fault-injection and consistency tests
docs/devlog/          one write-up per phase
```

## Development

Requires Go 1.26+.

```sh
go build ./...
go test -race ./...
```

## License

[MIT](LICENSE)
