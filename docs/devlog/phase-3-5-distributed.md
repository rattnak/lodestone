# Phases 3–5 — From single node to a distributed cluster

*November 2026 · DDIA chapters 4 (encoding), 5 & 9 (replication, consensus), 6 (partitioning)*

Phases 1–2 built an embedded vector database. Phases 3–5 turn it into a
distributed one: a gRPC API, Raft replication, and partitioning with live
rebalancing.

## Phase 3 — Server and wire protocol (ch 4)

A protobuf/gRPC service (`proto/lodestone/v1`) exposes KV ops, document
upsert/search, and a streaming telemetry RPC. The engine is the single source
of truth and the vector index is derived state: on startup the index is
rebuilt from a full engine scan, so an index/engine inconsistency heals on
restart. A wire-level test pins the DDIA ch 4 property the protocol leans on —
a message carrying unknown fields decodes cleanly and preserves them.

## Phase 4 — Raft from scratch (ch 5, 9)

A tick-driven Raft node (no goroutines, no clocks) with leader election, log
replication, the Figure 8 commit rule, snapshots, and single-server
membership changes. The node is driven by a deterministic simulator that is
also a safety checker: seeded message loss, delays, partitions, and
crash/restart, with divergence and dual-leader detection. Every failure
replays from its seed.

Chaos sweeps over thousands of seeds found three real bugs before they could
hide: a new leader that could never advance commit without a term no-op;
stale rejections dragging `nextIndex` below a follower's acknowledged match;
and appends overlapping a compacted prefix being rejected forever.

## Phase 5 — Partitioning and rebalancing (ch 6)

Data is split into a fixed number of partitions by key hash; each partition is
its own Raft group, placed on nodes by rendezvous hashing (deterministic,
~1/n movement on membership change). Any node routes a request to the owning
partition's leader; vector search scatters to every partition and gathers a
merged top-k. Nodes join by contacting a seed, and each partition's leader
migrates its Raft membership toward the computed replica set.

Rebalancing is where the distributed-systems reality bit hardest. Making a
3→4 node join converge correctly forced a chain of fixes, each a known
technique learned the hard way:

- **Membership must live in the log.** Applying configuration changes on
  commit (rather than on append, as the paper specifies) let a removed leader
  step down while followers still held the old configuration — a deadlock.
  Encoding the *full* member set in each change lets a late-joining node learn
  the whole configuration from the one entry that adds it.
- **Pre-vote** stops a node that can't win (behind, or partitioned) from
  disrupting a healthy leader by bumping terms — without it, co-located groups
  livelocked during churn.
- **Check-quorum leader reads** replaced a passive lease that lapsed under
  load: a busy leader's heartbeat acks arrive unevenly, so lease reads
  flapped. A leader now serves reads once it has committed in its term and
  steps down if it loses quorum contact.
- **Learner and demoted roles.** A joining node is a non-voter until added, so
  it never campaigns against an established group; a removed node keeps
  receiving entries until it learns of its own removal, so it stops instead of
  disrupting forever.
- **Routing fallback.** A node must be able to reach a partition it does not
  host — once rebalancing moves a partition off the node a client is talking
  to, routing falls back to the full member set.

## What's next

Phase 6 feeds the cluster from live event streams; Phase 7 puts a dashboard
and a RAG app on top; Phase 8 is dedicated chaos and linearizability checking
of the whole system, not just Raft in isolation.
