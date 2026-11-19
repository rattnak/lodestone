# Phase 8 — Chaos & hardening

*November 2026 · DDIA chapters 8–9 (the trouble with distributed systems, consistency)*

By Phase 7 the system worked. Phase 8 is about *proving* it works under the
conditions distributed systems actually fail in — and measuring what it costs.

## Linearizability under faults

The headline result. `TestLinearizableUnderFaults` runs a real three-node
replicated key-value register, points several concurrent clients at it, and —
while they hammer it — a fault injector repeatedly isolates or pauses whichever
node is currently the leader, forcing election after election. Every
operation's invocation time, completion time, and result is recorded, and the
whole history is handed to [porcupine](https://github.com/anishathalye/porcupine),
the linearizability checker that has found real bugs in real databases.

Linearizability is the strong guarantee: there must exist *some single
sequential order* of all the operations that is consistent with what every
client observed, respecting real-time ordering. If a read ever returned a
stale value — if the leader lease or the commit rules had a hole — porcupine
would find the contradiction. Across ~500 operations per run, spanning forced
leader changes, the history is always linearizable.

The subtlety that makes this honest: a write whose client gave up (timeout
during a leader change) might still have committed. The harness handles it by
retrying the *same* value until it definitively succeeds and timestamping the
operation from its first attempt — so the recorded window covers any secret
commit, and the history stays faithful.

## The bug it found

The test earned its keep immediately: it flagged a stale-read bug on the first
serious run. The leader reads had been using a lease based on the logical
clock, and a lease is unsafe under a process pause — a paused leader's clock
freezes, so on resume it can still think its lease is valid and serve a stale
read before it learns a new leader was elected. The fix is the ReadIndex
protocol with *active* confirmation: a read takes a barrier and then waits for
a quorum to acknowledge the leader *after* it, so a resumed ex-leader fails
confirmation and refuses the read. (This is exactly why etcd defaults to
ReadIndex over lease reads.) Getting there also meant fixing the harness: an
indeterminate write must be modeled as a *pending* operation porcupine can
place anywhere or nowhere, not as a completed one — otherwise the checker
reports false violations of its own.

## Making sure the check isn't vacuous

A linearizability test that always passes could mean "the system is correct" or
"the checker never says no." `TestLinearizabilityCheckerCatchesViolation`
pins it to the former: it feeds porcupine a hand-built history with a stale
read (two non-overlapping writes A then B, then a read returning A) and asserts
the checker *rejects* it. A correctness test you haven't tried to fool is not
a correctness test.

## The torture job

Chaos is too slow for every push, so `.github/workflows/torture.yml` runs
nightly: the 5000-seed deterministic Raft sweep plus ten race-detected
linearizability runs. A failing seed prints and reproduces locally — the
deterministic simulator means a nightly failure is never a heisenbug.

## What it costs

`docs/BENCHMARKS.md` collects the numbers with the profiling commands. The
summary: the LSM engine writes ~12× faster than a B-tree and loses a little on
point reads; HNSW search is ~3.4× faster than brute force at 20k vectors and
pulls further ahead with scale; the log broker appends at ~266 MB/s buffered.
The profiling section names the first allocations worth pooling — honest about
where the easy wins still are.

## The lesson

The through-line of Phases 4, 5, and 8: in distributed systems, the tests are
the hard part, not the happy path. Anyone can write a Raft that works when
nothing fails. The value — and the thing that separates a toy from a
foundation — is the deterministic chaos simulator and the linearizability
checker that keep it honest when everything fails at once.
