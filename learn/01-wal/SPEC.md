# Exercise 01 — The Write-Ahead Log

**DDIA chapter 3.** This is the foundation everything else sits on. Build it
well and the rest of the storage engine is downhill.

## The idea

The simplest durable database is an append-only file: to store something, you
append it; to recover after a crash, you replay the file. A **write-ahead log**
is exactly that — every mutation is appended to the log *before* it's
considered saved. The storage engine will keep its live data in memory (the
memtable, next exercise) and use this log only to survive crashes: on restart,
replay the log to rebuild what was in memory.

The entire reason the WAL exists is one hard guarantee, so internalize it:

> **A write may be reported as durable only after it is physically on disk.**
> In between — buffered in memory or in the OS page cache — it can vanish in a
> crash, and that must be fine.

## What you must build

Implement every method in `wal.go`. The public API (already stubbed) is:

```go
type Op byte
const ( OpPut Op = iota + 1; OpDelete )

type Record struct {
    Op    Op
    Key   []byte
    Value []byte   // ignored for OpDelete
}

func OpenWAL(path string) (*WAL, error)      // open or create the log at path
func (w *WAL) Append(rec Record) error       // buffer a record (not yet durable)
func (w *WAL) Sync() error                    // flush + fsync; NOW it's durable
func (w *WAL) Replay(fn func(Record) error) error  // call fn for each record, in order
func (w *WAL) Size() int64                    // byte offset of the end of the last record
func (w *WAL) Close() error
```

You design the struct's fields and the on-disk byte format. The tests check
*behavior*, not your layout — so the format is genuinely your decision.

## The contract (what the tests enforce)

1. **Round-trip.** Append records, Sync, Replay → you get the same records
   back, in append order, byte-for-byte (keys and values, including empty
   ones).
2. **Durability across reopen.** After `Sync` returns, `Close` and `OpenWAL`
   the same path → Replay returns everything that was synced. Appends continue
   from where you left off.
3. **Torn-tail recovery — the hard one.** If the process crashes *during* an
   append (so the file ends with a partial or garbage record), reopening must
   recover every intact record before the damage and silently discard the junk
   — and appends afterward must produce a clean log again.
4. **Crash-prefix property.** For *any* truncation of a synced log at *any*
   byte offset, reopening recovers exactly a prefix of the records: never a
   partial record, never a gap, never an error. (A crash can cut the file
   anywhere.)

`Size()` returns the end offset of the last valid record — the tests use it to
know where record boundaries fall.

## The design question to sit with

How does `OpenWAL` tell a *complete* record from a *half-written* one at the end
of the file? You wrote the bytes; the reader has to decide "is this whole?"
with no external bookkeeping. Think about what you'd need to store alongside
each record to answer that. (This is the crux. Don't read the hints until
you've thought about it.)

## Running the tests

```sh
cd learn/01-wal
go test -race -v ./...
```

They'll panic until you implement the methods. Green means you've matched the
reference's guarantees.

---

## Hints (open only when stuck)

<details>
<summary>Hint 1 — framing a record</summary>

Store each record as: a fixed-size header followed by the payload. Put the
payload's **length** in the header so the reader knows how many bytes to read.
Now a truncated record is detectable: the header promises N payload bytes but
the file ends early.
</details>

<details>
<summary>Hint 2 — detecting corruption, not just truncation</summary>

Length alone catches a short tail, but not a header whose length field itself
got half-written (it could claim a plausible-but-wrong size). Add a
**checksum** (e.g. `hash/crc32`) of the payload to the header. On read,
recompute and compare — mismatch means "torn," stop there. This is why real
WALs (and your reference) are length+CRC framed.
</details>

<details>
<summary>Hint 3 — the write path vs the durability boundary</summary>

`Append` should write into a `bufio.Writer` (buffered — fast, not yet durable).
`Sync` flushes the buffer to the file and then calls `file.Sync()` (fsync).
Only after fsync returns is the data really on disk. That split is the whole
point of the Append/Sync separation.
</details>

<details>
<summary>Hint 4 — reopen and truncation</summary>

On `OpenWAL`, scan from the start, reading records until you hit EOF or a
torn/corrupt record. Track the byte offset of the last *good* record. Then
`file.Truncate(thatOffset)` to chop any garbage, and seek there so new appends
land cleanly. Keep that offset as your `size`.
</details>

<details>
<summary>Hint 5 — record wire format that works</summary>

One layout the reference uses (yours can differ):

```
crc32   uint32   // CRC of the payload bytes
length  uint32   // payload length
payload [length]byte:
    op     byte
    keyLen uint32
    key    [keyLen]byte
    value  [rest]byte
```

Read header (8 bytes) → read `length` payload bytes → check CRC → decode.
A `ReadFull` that returns `io.ErrUnexpectedEOF`, or a CRC mismatch, means
"torn tail": stop, truncate, done.
</details>
