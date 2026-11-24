# Development

How Lodestone is built. This is a personal, source-available project — not an
open-source one — so this is a record of my own workflow and gates, not a
guide for outside contributors.

## One change, one branch, one reviewed PR

`main` is only ever updated by merging a pull request I have reviewed; nothing
is pushed to it directly (branch protection enforces this). Every change goes
through the same loop:

1. **Branch** off `main`, one branch per logical change:
   `feat/…`, `perf/…`, `fix/…`, `docs/…`, `test/…`, `chore/…`.
2. **Build it** following the phase plan (see [`docs/devlog/`](docs/devlog/)),
   with tests alongside the code.
3. **Validate** locally:
   ```sh
   go build ./...
   go vet ./...
   gofmt -l .                              # must print nothing
   go test -race ./internal/...            # -race for concurrent code
   go test ./internal/<pkg> -bench . -run ^$   # for performance changes
   ```
4. **Open a draft PR** filled out per the
   [template](.github/pull_request_template.md): summary, changes, the exact
   validation run, and review notes.
5. **Review, then merge.** I read the diff, confirm CI is green, mark it ready,
   and merge it myself. Draft PRs are not merged until reviewed.

Trivial fixups to a PR already under review (a typo, a formatting fix, a review
nit) go straight to that PR's branch rather than a new PR, so review stays in
one place. Anything standalone gets its own PR.

Keeping each PR scoped to a single concern is what keeps review fast and the
history legible.

## Testing gates

- Unit tests live next to the code they cover.
- Anything concurrent (the Raft node, the cluster, the log broker) must pass
  under `-race`.
- Consensus and distributed behavior is held by the deterministic chaos
  simulator (`internal/raft`) and the linearizability check
  (`internal/cluster`); a change there must keep both green. The long sweep
  runs nightly via [`torture.yml`](.github/workflows/torture.yml) and locally:
  ```sh
  LODESTONE_CHAOS_SEEDS=2000 go test -run TestExtendedChaos ./internal/raft/
  go test -race -count=5 -run TestLinearizable ./internal/cluster/
  ```
- Performance changes carry before/after numbers in the PR and, when
  meaningful, in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Commit messages

Describe the behavior change and the reasoning, not the mechanics of the diff.
Reference the phase or issue. Imperative subject under ~72 characters.

## Learning track

[`learn/`](learn/) is a separate module for rebuilding components from scratch;
it is not part of the main build or CI and follows its own README.
