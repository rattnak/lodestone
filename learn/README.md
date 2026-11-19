# Lodestone — Learn by Rebuilding

This is your workspace. The real system lives in `internal/`; **here you
rebuild it yourself**, component by component, and compare against the
reference afterward.

## The rule

> **Don't open the reference implementation in `internal/` until you've made
> the tests pass (or you're genuinely stuck and have used all the hints).**

Reading the answer feels like learning but isn't. The whole value is in
reconstructing the reasoning yourself. The tests are your target; the spec is
your guide; the hints are there when you're stuck.

This is a separate Go module (`learn/go.mod`), so nothing here affects the
main build or CI. Work freely.

## How each exercise goes

1. Read `SPEC.md` — the contract you must satisfy.
2. Open the stub (e.g. `01-wal/wal.go`) — the API is given; the bodies are
   yours.
3. Run the tests and watch them fail:
   ```sh
   cd learn/01-wal
   go test -race -v ./...
   ```
4. Implement until they pass. Use the hints in `SPEC.md` only when stuck.
5. Tell me "done with the WAL" — I'll diff your version against the reference,
   point out what differs and why it matters, and ask you a few questions to
   check your understanding before we move on.

## Order (matches DDIA)

- **01-wal** — write-ahead log (durability) · *you are here*
- 02-memtable — skiplist memtable
- 03-sstable — sorted string tables + bloom filter
- 04-engine — tie them into an LSM tree
- …then Phase 2 (HNSW), Phase 4 (Raft), etc.

One component at a time. Depth over speed.
