# Phase 1 — Storage engine

*November 2026 · DDIA chapter 3*

Lodestone's foundation is a log-structured merge tree, built piece by piece in
`internal/storage`: write-ahead log → memtable → SSTables → bloom filters →
compaction, tied together by an engine facade.

## Design

**WAL** (`wal.go`). Every write is appended to a log before anything else, and
acknowledged only after fsync. Records are framed with a length and CRC32, so
a crash mid-write leaves a detectably-torn tail that gets truncated on reopen.
The property test simulates `kill -9` at 200 random byte offsets; recovery
must always yield a clean prefix of the synced records.

**Memtable** (`memtable.go`). A skiplist keeps recent writes sorted in memory.
Deletions are stored as tombstones because a delete must *shadow* older copies
of the key that live in SSTables — removing the memtable entry would resurrect
them. Sorted order is the point: a full memtable streams straight out as an
already-sorted file.

**SSTables** (`sstable.go`). Immutable sorted files with a sparse in-memory
index (every 16th key): a lookup binary-searches the index and scans at most
one interval on disk. Files are written to a temp name, fsynced, then renamed,
so a half-written table can never be mistaken for a real one.

**Bloom filters** (`bloom.go`). The LSM read problem is that a miss would
search every table. A 10-bits-per-key filter (double hashing, k=7, ~1% false
positives) is persisted in each table and consulted before any disk read.

**Compaction** (`compaction.go`). Every flush adds a table; compaction k-way
merges them back into one, keeping only the newest version per key. Tombstones
are deliberately *kept*: a crash between the rename and old-file cleanup can
leave old tables visible on recovery, and a dropped tombstone would let a
deleted key resurrect from one. Dropping them safely needs a manifest of live
tables — noted for later.

**Engine** (`engine.go`). The ordering subtleties live here. On flush, the WAL
may only reset *after* the new SSTable is durably renamed in; a crash between
the two replays writes the table already holds, which is harmless. Reads check
memtable, then tables newest-first — that ordering is what makes overwrites
and deletes work at all.

## Numbers

Apple M1 Pro, 100-byte values, vs [bbolt](https://github.com/etcd-io/bbolt)
(B-tree) as the comparison point:

| Operation | lodestone | bbolt | |
|---|---|---|---|
| Put, no fsync | **3.1 µs/op** | 28.0 µs/op | LSM: writes are log appends |
| Put, fsync each | 7.2 ms/op | 12.9 ms/op | both dominated by fsync |
| Get (hit, 100k keys) | 2.4 µs/op | **0.9 µs/op** | B-tree: one tree to search |
| Get (miss) | **0.23 µs/op** | — | bloom filter skips the disk |

Exactly the trade-off the book describes: the LSM wins writes ~9×, the B-tree
wins reads ~2.6×, and bloom filters make misses nearly free.

## Decisions worth remembering

- Tombstones survive compaction (see above) until a manifest exists. The
  cost is permanent markers for deleted keys; the alternative is silent
  resurrection after a badly-timed crash.
- Flushes and compactions run synchronously inside the write path for now.
  Background compaction is deferred until the telemetry work makes its
  behavior observable.
- `Close` does not flush the memtable — the WAL already has that data, and
  replay-on-open is the code path that most needs exercising.
- Testing leans on reference-model fuzzing: thousands of random operations
  mirrored into a plain map, with random flushes, compactions, and reopens,
  asserting full agreement at checkpoints. Nearly every guarantee above is
  pinned by one of these tests rather than by example-based cases alone.
