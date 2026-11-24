<!--
Keep this order; fill every section. Open as a DRAFT until reviewed.
See DEVELOPMENT.md for the workflow.
-->

## Summary

_What this change does and why — a sentence or two. Link the phase/issue._

## Changes

- _file or area_ — _what changed_

## Validation

_Exact commands run and their results. At minimum the tests covering the
changed code; `-race` for anything concurrent; benchmarks for perf changes._

```
$ go test -race ./internal/...
ok  ...
```

## Review notes / risks

_What to look at closely: trade-offs, follow-ups, things deliberately left
out. "None" is valid._

## Checklist

- [ ] `go build ./...`, `go vet ./...`, and `gofmt -l .` clean
- [ ] Tests cover the change and pass (with `-race` where relevant)
- [ ] Devlog / BENCHMARKS updated if the change warrants it
- [ ] Scoped to one logical change
