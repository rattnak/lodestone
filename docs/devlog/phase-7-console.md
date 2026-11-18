# Phase 7 — The live console

*November 2026*

The database had no face. Phase 7 gives it one: a web console that shows the
cluster breathing — throughput, storage internals, the partition ring, and
per-group Raft state — and runs real semantic searches.

## The bridge

A browser can't speak gRPC, and it can't reach a raw gRPC stream. So the node
runs an HTTP **gateway** (`internal/gateway`) that serves the single-page
dashboard from its own origin and translates:

- **`/api/telemetry`** — relays the `WatchTelemetry` gRPC stream to the browser
  as Server-Sent Events. Each snapshot (memtable bytes, SSTable count, indexed
  vectors, operation counters) becomes a JSON frame.
- **`/api/topology`** — the partition/Raft layout from `ClusterAdmin.Describe`.
  A standalone node reports `{"standalone": true}` instead of failing.
- **`/api/search`** — a vector or (with Ollama) text query against the live
  index, returning cited document chunks.

Serving the page same-origin is what lets the strict browser sandbox reach
these endpoints at all.

## The design

The console leans into Lodestone's namesake — a lodestone is a magnetic
navigating stone — so the hash ring reads as a compass/instrument, with a warm
magnetite-bronze brand accent against a cool slate control-room ground and cyan
telemetry lines. Monospace carries the data (it reads as "instrument"); a sans
carries the prose. It's theme-aware in both directions and respects reduced
motion.

The panels follow information-design rules rather than document typography:
the summary (vectors, throughput) sits in the header; state is encoded in form
as well as number — a leader is a colored node on the ring and a green pill in
the Raft table; throughput sparklines get an area fill, a faint baseline, and
an emphasized endpoint.

## Running it

```sh
# a 3-node cluster
lodestone cluster -addr 127.0.0.1:7601 -data ./n1 -bootstrap
lodestone cluster -addr 127.0.0.1:7602 -data ./n2 -seeds 127.0.0.1:7601
lodestone cluster -addr 127.0.0.1:7603 -data ./n3 -seeds 127.0.0.1:7601

# the console
lodestone dashboard -server 127.0.0.1:7601   # → http://localhost:8437
```

## A deliberate omission

The design mockup lets you click to kill a node and watch re-election. The
live console does not wire that up: operating on a real node from an
unauthenticated web page is the wrong default. Node lifecycle stays on the CLI;
authenticated admin controls are a future addition.
