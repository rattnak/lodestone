<!--
Keep this order. Fill every section; delete none. Open as a DRAFT until reviewed.
-->

## Summary

_What this change does and why — one or two sentences. Link the phase/issue._

## Changes

- _file or area_ — _what changed_

## Validation

_Exact commands run and their results. At minimum, the tests covering the
changed code; `-race` for anything concurrent; benchmarks for perf changes._

```
$ go test -race ./internal/...
ok  ...
```

## Review notes / risks

_Anything a reviewer should look at closely: trade-offs, follow-ups, things
deliberately left out. "None" is a valid answer._

## Checklist

- [ ] `go build ./...` and `go vet ./...` clean
- [ ] Tests cover the change and pass (with `-race` where relevant)
- [ ] Devlog / BENCHMARKS updated if the change warrants it
- [ ] Scoped to one logical change
