# Compatibility promise — `llm-agent-flow` v0.1.x

Effective from **v0.1.0** (tagged 2026-05-21).

## What v0.1 freezes

Within the `v0.1.x` series the **exported API of every importable
package** is **additive-only**. Specifically:

- **No exported symbol is removed** (no removed funcs, types,
  methods, constants, vars).
- **No exported symbol is renamed.**
- **No exported symbol is re-signed**:
  - func / method parameter and return types are stable;
  - struct field names and types are stable;
  - interface method sets are stable.

The committed baseline lives at [`api/v0.1.snapshot.txt`](../api/v0.1.snapshot.txt).
The gate `internal/apisnapshot` regenerates the surface at every
`go test ./...` and fails any drift against that baseline.

## What v0.1 does NOT cover

- **JSON IR additions.** New optional fields may appear in the flow
  JSON shape (`Edge.Condition` joined v0.0.4, `flow.tools` manifests
  joined v0.0.3). Older flows continue to load. Removal of an
  existing field would still need a major bump.
- **HTTP endpoints.** New endpoints may appear; their request/response
  shapes are stable in v0.1 if listed in the README under
  "Implemented". Renames or response-shape changes require a major.
- **`internal/` packages.** Anything under `internal/` is unstable
  and not importable from outside the module.
- **`cmd/flow` and `cmd/flowd` command-line flags.** New flags may
  appear; the meaning of existing flags is stable.
- **Behavior under unspecified inputs.** "Unspecified" means a
  combination not explicitly documented (e.g. concurrent CRUD races
  beyond what the SQLite store serializes); fixes can change
  behavior without a major.

## Breaking changes go to `/v2`

Any change that breaks the rules above ships in a new module path
`github.com/costa92/llm-agent-flow/v2`. v0.1.x continues to receive
security and bug fixes during the v2 deprecation window.

## Updating the snapshot baseline

Deliberate **additive** changes (new exported symbol, new field on
a new type, new interface method on a new interface, etc.) require
regenerating the baseline as part of the same commit:

```
go test ./internal/apisnapshot/ -run TestAPISnapshot -update
git add api/v0.1.snapshot.txt
git commit ...
```

The gate then accepts the new surface and rejects any future drift.

## Why a snapshot gate

The compatibility promise is only as strong as the smallest
contributor's discipline. The snapshot gate makes the promise
**executable**: a refactor that drops a method or renames a
parameter fails CI before review. Reviewers see the diff in
`api/v0.1.snapshot.txt` alongside the source diff — making it
trivial to spot accidental breakage.

The gate is pure stdlib (`go/parser` + `go/printer`) — no module
dependency, no separate tool to install, runs in every `go test`.
