# Phase 2 — Vector index

*November 2026 · DDIA chapter 3 (indexes), HNSW paper (Malkov & Yashunin 2016)*

Phase 1 built a key-value engine; this phase gives lodestone its reason to
exist — answering "find the documents that *mean* this" via approximate
nearest-neighbor search over embedding vectors.

## HNSW in one paragraph

Every vector becomes a node linked to near neighbors. Layer 0 holds all
nodes; each higher layer keeps an exponentially thinner subset (a node
reaches level L with probability ~e^-L), forming a highway network. A search
enters at the sparse top, greedily descends toward the query, then widens to
an `ef`-sized candidate beam on the bottom layer. Cost grows logarithmically
with collection size; the price is approximation, measured as recall.

## What got built (`internal/index`, `internal/embed`)

- **Construction and search** (`hnsw.go`): the paper's INSERT and
  SEARCH-LAYER with bidirectional links pruned to 2M on layer 0. Deletes mark
  nodes as waypoints — they still route traffic but never appear in results,
  the same compromise hnswlib and FAISS make, because removing edges would
  tear routes other nodes depend on.
- **Snapshot persistence** (`persistence.go`): atomic save/load of the exact
  graph. Loads produce byte-identical search results — asserted in tests —
  and accept further inserts. The distance function can't be serialized, so
  the loader requires the caller to resupply the metric.
- **Filtered search**: `SearchFilter` post-filters by predicate; with a
  selective filter you raise `ef` proportionally so enough allowed candidates
  survive the beam.
- **Ollama client** (`embed/ollama.go`): batched, order-preserving, retrying,
  and dimension-pinned — if the model changes mid-run and starts emitting
  different-sized vectors, that's an error, not a silently corrupted space.
- **CLI demo**: `lodestone search -file docs.txt -query "..."` embeds each
  line, indexes it, and answers semantically. End-to-end proof the pieces fit.

## Numbers

Random vectors, Apple M1 Pro:

| Metric | Value |
|---|---|
| recall@10 vs brute force (2k vecs, ef=64) | 0.98 (L2), 0.997 (cosine) |
| search @ 20k vecs, 64 dims | 322 µs |
| brute force, same set | 1.2 ms |
| insert | 582 µs |

The 3.7× search advantage at 20k vectors understates the asymptotics: brute
force is O(n), HNSW is ~O(log n), so the gap widens with every order of
magnitude. Worth revisiting in the profiling phase: the visited-set is a map
allocation per query, the standard first optimization target.

## Decisions worth remembering

- Vectors are **not copied** on insert (they're write-once, read-many and
  large); the API contract forbids callers mutating them afterwards.
- The index is snapshot-persisted rather than stored entry-by-entry in the
  LSM engine: backlinks mutate many nodes per insert, which would amplify
  writes badly. Snapshot + WAL-replay-of-the-tail is the planned recovery
  path once index and engine are wired together at the server layer.
