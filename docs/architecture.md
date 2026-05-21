# Architecture — `llm-agent-flow`

This document describes how the pieces of `llm-agent-flow` fit
together. For the stability contract see [`compatibility.md`](compatibility.md);
for first-time use see [`tutorial.md`](tutorial.md); for production
deployment see [`operations.md`](operations.md).

## At a glance

```
                           ┌─────────────────────────────────────┐
                           │           Flow JSON file            │
                           │  { id, nodes, edges, in, out, ... } │
                           └──────────────┬──────────────────────┘
                                          │
                                          ▼
              ┌──────────────────────────────────────────────────┐
              │                  flow.Compile                    │
              │  IR → Validate → NodeRegistry.Build → topo sort  │
              │             → CEL-precompile edges               │
              └──────────────┬───────────────────────────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │ flow.Engine  │── satisfies ──▶ flow.Runner
                      └──────┬───────┘                       ▲
                             │                               │
            ┌────────────────┴──┐                            │
            ▼                   ▼                            │
        Engine.Run         Engine.RunStream                  │
            │                   │                            │
            └─────────┬─────────┘                            │
                      ▼                                      │
            map[string]string outputs                        │
            <-chan flow.FlowEvent                            │
                                                             │
              ┌──────────────────────────────────────────────┘
              │                          decorator pattern (K3)
              ▼
       otelflow.Wrap(Runner) Runner   ← in llm-agent-otel sibling repo
              │
              ▼
        Same Runner contract, OTel spans on every Run / per-node lifecycle
```

The library is split into clear layers, each with its own
dependency posture:

| Layer | Package | Imports | Purpose |
|---|---|---|---|
| IR | `flow` | stdlib + llm-agent | Flow / Node / Edge / Port / FlowEvent types; Engine; Runner interface |
| Conditions | `flow/cond/cel` | + `google/cel-go` | CEL-backed ConditionEvaluator |
| Tools | `flow/tools` | stdlib + flow | Manifest + http/exec built-in kinds |
| Persistence | `flow/store` | stdlib + flow | Store interface (CRUD + run lifecycle + events) |
| Persistence impl | `flow/store/sqlite` | + `modernc.org/sqlite` | SQLite-backed Store with bulk-insert capability |
| HTTP service | `cmd/flowd/server` | flow + flow/store | Auth + CRUD + sync/stream/replay endpoints |
| Binaries | `cmd/flow`, `cmd/flowd` | all of the above | CLI + long-running HTTP server |
| OTel | `llm-agent-otel/otelflow` (sister repo) | flow + go.opentelemetry.io/otel | Decorator that wraps any `flow.Runner` |

The **`flow` core package itself is stdlib-only at the source level**
outside its back-edge to `github.com/costa92/llm-agent`. CEL,
SQLite, and OTel deps each enter only when their dedicated
sub-package is imported.

## Core types — `flow` package

The IR is small and JSON-serializable. See `flow/ir.go`:

```go
type Flow struct {
    ID          string
    Name        string
    Description string
    Nodes       []Node
    Edges       []Edge
    Inputs      []NamedPortRef   // caller-supplied
    Outputs     []NamedPortRef   // returned to caller
}

type Node struct { ID string; Type string; Config json.RawMessage }
type Edge struct { Source PortRef; Target PortRef; Condition string }
type PortRef struct { Node string; Port string }
type NamedPortRef struct { Name string; PortRef }
```

The **Runner interface** (the stability seam decorators target) is
exactly two methods:

```go
type Runner interface {
    Run(ctx, inputs map[string]string) (map[string]string, error)
    RunStream(ctx, inputs map[string]string) (<-chan FlowEvent, error)
}
```

`*Engine` satisfies it; a compile-time assertion (`var _ Runner = (*Engine)(nil)`)
keeps the contract honest.

## Execution model

1. **Compile** validates the IR, resolves every Node Type through
   the `NodeRegistry` into a runtime `NodeKind`, optionally
   pre-compiles each `Edge.Condition` through the configured
   `ConditionEvaluator`, and computes a **topological layer order**.

2. **Layered execution** runs each layer's active nodes in
   parallel via [`pkg/fanout`](https://github.com/costa92/llm-agent/blob/main/pkg/fanout/fanout.go).
   Sibling errors trigger fail-fast cancel. Layers themselves are
   sequential — a layer's nodes start only after all nodes of the
   previous layer have finished.

3. **Node activation** is the conditional-edge subtlety: a node
   runs iff (no incoming edges) OR (named in `Flow.Inputs`) OR (at
   least one incoming edge fired). An edge "fires" iff its
   `Condition` evaluates to true (an empty condition always fires).
   Skipped nodes emit a `NodeSkipped` event; their outgoing edges
   don't fire; downstream nodes whose only inputs were skipped
   become skipped too. Outputs declared on skipped nodes are
   silently omitted from the final outputs map.

4. **FlowEvent stream** is a typed union following the K1 idiom
   from the wider ecosystem:

   ```
   FlowStarted → (NodeStarted|NodeSkipped|NodeFinished)* → FlowDone|FlowErr
   ```

   Sibling events within a layer may interleave; per-node ordering
   and the FlowStarted-first / FlowDone-or-FlowErr-last invariants
   hold.

## Persistence — `flow/store`

Two concerns bundled into one interface:

- **Flows** — CRUD; last-write-wins; stored as raw JSON bytes
  alongside `id / name / created_at / updated_at`.
- **Runs** — lifecycle (StartRun → FinishRun) + per-event history
  (AppendRunEvent / ListRunEvents).

The bundled `flow/store/sqlite` implementation is pure-Go via
[`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) — no CGO.
Schema is idempotent on `Open`. Run history is **NOT** cascaded on
flow delete; historical audit survives.

A **batched insert** capability is exposed beyond the `Store`
interface — `(*sqlite.Store).AppendRunEvents(ctx, runID, items)`.
Servers can type-assert to detect support:

```go
if batcher, ok := store.(interface {
    AppendRunEvents(ctx context.Context, runID string,
                    items []flowstore.RunEventBatchItem) error
}); ok {
    _ = batcher.AppendRunEvents(...)
}
```

The bulk method is *not* on the `Store` interface to honor the v0.1
freeze — downstream custom `Store` implementations stay
source-compatible without adding the method.

## HTTP service — `cmd/flowd`

`flowd` is the long-running variant of `cmd/flow`. It binds:

```
GET    /healthz
POST   /flows                 GET /flows               GET    /flows/{id}
PUT    /flows/{id}            DELETE /flows/{id}
POST   /flows/{id}/run        POST /flows/{id}/run/stream
GET    /flows/{id}/runs
GET    /runs/{id}             GET /runs/{id}/events    POST   /runs/{id}/replay
```

Plus, when `--flow <file>` boots a seed flow, the v0.0.4-compat
aliases:

```
POST /run    POST /run/stream
```

Authentication is **off** by default. Setting `--token <secret>` (or
`FLOWD_TOKEN`) wires `server.BearerTokenAuthenticator` in front of
every endpoint except `/healthz`. The `server.Authenticator`
interface lets callers plug in JWT / OAuth / mTLS / etc. without
forking the package.

Engine compilation is **cached** keyed by flow id. The cache is
LRU-bounded when `Config.EngineCacheSize > 0`; PUT / DELETE evict
explicitly. Compile errors surface at `POST /flows` / `PUT /flows/{id}`
time as 400, not at the next run.

## Telemetry — `otelflow` (sister repo)

Per Keystone K3 of the wider ecosystem, OTel **never hooks** into
the engine — it composes via `flow/cond/cel` style decorators:

```go
import (
    "github.com/costa92/llm-agent-flow/flow"
    "github.com/costa92/llm-agent-otel/otelflow"
)

eng, _ := flow.LoadCompile(r, reg, deps, flow.WithConditionEvaluator(...))
runner := otelflow.Wrap(eng, otelflow.Config{TracerProvider: tp})
out, _ := runner.Run(ctx, inputs) // emits flow.run + per-node spans
```

Span layout:

- One root `flow.run <id>` per `Run`; or `flow.run.stream <id>` per `RunStream`.
- One child `flow.node <id>` per `NodeStarted`/`NodeFinished` pair.
- A zero-duration `flow.node <id>` with `flow.node.skipped=true` for
  every `NodeSkipped` event.

The wrapper composes — `otelflow.Wrap(otelflow.Wrap(...))` is legal,
ctx propagates normally, no surprises.

## Why the snapshot gate

`internal/apisnapshot` is a pure-stdlib generator that walks the
module's own source, renders a sorted text snapshot of every
exported declaration, and diffs against the committed baseline at
`api/v0.1.snapshot.txt`. It runs as part of `go test ./...`.

The gate makes the v0.1 promise **executable** — a refactor that
drops a method, renames a parameter, or re-signs an interface
fails CI before review. See [compatibility.md](compatibility.md)
for the rules and the regeneration procedure.

## Where things live

```
llm-agent-flow/
├── api/                          ← committed v0.1 baseline
│   └── v0.1.snapshot.txt
├── cmd/
│   ├── flow/                     ← CLI: `flow run <file.json>`
│   └── flowd/                    ← HTTP service
│       └── server/               ← split for httptest reuse
├── docs/
│   ├── architecture.md           ← this file
│   ├── compatibility.md          ← v0.1 stability promise
│   ├── operations.md             ← flowd deployment
│   └── tutorial.md               ← first-time user
├── examples/
│   ├── echo_chain/               ← canonical linear demo
│   ├── http_tool/                ← --tools http kind demo
│   └── router/                   ← CEL conditional routing demo
├── flow/                         ← library core
│   ├── cond/cel/                 ← CEL evaluator (pulls cel-go)
│   ├── store/                    ← Store interface
│   │   └── sqlite/               ← SQLite impl (pulls modernc-sqlite)
│   └── tools/                    ← Tool manifest (http + exec)
└── internal/
    └── apisnapshot/              ← v0.1 gate (pure stdlib)
```
