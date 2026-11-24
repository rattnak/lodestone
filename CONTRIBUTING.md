# Contributing to Lodestone

Lodestone is built phase by phase (see [`docs/devlog/`](docs/devlog/) and the
[issue milestones](../../milestones)). This document describes how changes land.

## One change, one branch, one draft PR

`main` is only ever updated by merging a reviewed pull request — nothing is
pushed to it directly. Every change, however small, goes through this loop:

1. **Branch** off `main`, one branch per logical change:
   `feat/…`, `perf/…`, `fix/…`, `docs/…`, `test/…`.
2. **Build it** following the phase plan, with tests alongside the code.
3. **Validate** locally:
   ```sh
   go build ./...
   go vet ./...
   go test -race ./internal/...          # -race for concurrent code
   go test ./internal/<pkg> -bench . -run ^$   # for performance changes
   ```
4. **Open a draft PR** (`gh pr create --draft`) filled out per the
   [template](.github/pull_request_template.md): summary, changes, the exact
   validation you ran, and review notes.
5. **Review and merge** happen on the PR. The author of a change does not merge
   their own work unreviewed; the maintainer reviews the draft, requests
   changes if needed, marks it ready, and merges.

Keeping each PR scoped to a single concern is what makes review fast and the
history legible.

## Testing expectations

- Unit tests live next to the code they cover.
- Anything concurrent (the Raft node, the cluster, the log broker) must pass
  under `-race`.
- Consensus and distributed behavior is covered by the deterministic chaos
  simulator (`internal/raft`) and the linearizability check
  (`internal/cluster`); a change there should keep both green. The long sweep
  runs nightly via [`torture.yml`](.github/workflows/torture.yml) and can be
  run locally:
  ```sh
  LODESTONE_CHAOS_SEEDS=2000 go test -run TestExtendedChaos ./internal/raft/
  go test -race -count=5 -run TestLinearizable ./internal/cluster/
  ```
- Performance changes include before/after numbers in the PR and, when
  meaningful, in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Commit messages

Describe the behavior change and the reasoning, not the mechanics of the diff.
Reference the issue or phase. Keep the subject line imperative and under ~72
characters.

## Learning track

[`learn/`](learn/) is a separate module for rebuilding components from scratch;
it is not part of the main build or CI and follows its own README.
