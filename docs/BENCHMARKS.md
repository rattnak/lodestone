# Benchmarks

Measured on an Apple M1 Pro. Numbers are indicative, not a competitive claim —
the point is to show the design trade-offs are real and measurable. Reproduce
with the commands under each table.

## Storage engine — LSM vs B-tree

`go test ./internal/storage/ -bench 'Put|Get' -benchtime 1s -run ^$`

100-byte values, compared against [bbolt](https://github.com/etcd-io/bbolt) (a
B-tree store):

| Operation | Lodestone (LSM) | bbolt (B-tree) | Why |
|---|---|---|---|
| Put, no fsync | **3.6 µs** | 42.6 µs | LSM writes are buffered log appends; the B-tree updates pages in place |
| Put, fsync each | 5.9 ms | 11.4 ms | both dominated by the disk; fsync is the floor |
| Get (hit, 100k keys) | 4.2 µs | **1.5 µs** | the B-tree searches one tree; the LSM may consult several tables |
| Get (miss) | **0.33 µs** | — | the bloom filter skips the disk entirely |

The textbook LSM trade-off: **writes win big, point reads lose a little, misses
are nearly free.** The gap widens with data size as the LSM keeps appending
sequentially.

## Vector index — HNSW vs brute force

`go test ./internal/index/ -bench 'Search|Add' -benchtime 1s -run ^$`

20,000 vectors, 64 dimensions:

| Operation | Time | Note |
|---|---|---|
| HNSW search (k=10, ef=64) | **316 µs** | approximate; recall ≥ 0.98 vs brute force |
| Brute-force search | 1.55 ms | exact, O(n) |
| HNSW insert | 448 µs | graph construction cost |

The ~5× search speedup understates it: brute force is O(n) and HNSW is
~O(log n), so the gap grows by an order of magnitude for every 10× more
vectors. Recall is measured in `TestRecallAgainstBruteForce` (0.98 L2 / 0.997
cosine at ef=64), unchanged by the tuning below.

**Allocation tuning.** Replacing the per-query `map[int32]bool` visited set
with a pooled, generation-stamped buffer (the standard HNSW trick — clearing
is a single generation bump, not a reallocation) cut the work measurably:

| | before | after |
|---|---|---|
| search | 384 µs · 52.8 KB/op | **316 µs · 13.0 KB/op** |
| insert | 642 µs · 108 KB/op | **448 µs · 65 KB/op** |

Insert gains most because it searches the graph repeatedly during
construction. Profile the remaining allocations with the commands below — the
candidate heaps are the next target.

## Log broker — lodelog

`go test ./internal/lodelog/ -bench . -benchtime 1s -run ^$`

200-byte events:

| Operation | Throughput | Note |
|---|---|---|
| Append (buffered) | **266 MB/s** (0.75 µs/op) | the ingestion hot path |
| Append + fsync each | 0.03 MB/s (6.3 ms/op) | durability floor, as expected |
| Read (batch of 100) | 33 µs | offset-indexed, scans one interval |

## Profiling

Every package's benchmarks accept the standard pprof flags. To find where time
or allocations go:

```sh
# CPU profile of HNSW search
go test ./internal/index/ -bench BenchmarkHNSWSearch -benchtime 5s \
  -run ^$ -cpuprofile cpu.out
go tool pprof -http=:0 cpu.out        # opens a flame graph

# Allocation profile of the storage write path
go test ./internal/storage/ -bench BenchmarkLodestonePut/sync=false \
  -benchtime 5s -run ^$ -memprofile mem.out
go tool pprof -alloc_objects -http=:0 mem.out
```

Known hot spots worth a look (noted for future tuning): the HNSW candidate
heaps still allocate their backing arrays per search (pooling them is the next
step after the visited-set win above), and the storage read path builds a
fresh bufio reader per SSTable lookup.
