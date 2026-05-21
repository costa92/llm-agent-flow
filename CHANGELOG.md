# Changelog

All notable changes to `github.com/costa92/llm-agent-flow` are documented here.

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — note: v0.0.x is provisional. -->

## [v0.0.7] - 2026-05-21

Phase 7 prep — introduces the seam `llm-agent-otel/otelflow` plugs
into.

### Added

- **`flow.Runner` interface.** Exactly `Run(ctx, inputs)` +
  `RunStream(ctx, inputs)`. `*flow.Engine` satisfies it; the package
  carries a compile-time assertion so wrapper packages can rely on
  it without explicit type assertions.
- **`(*Engine).FlowID() string`** and **`FlowName() string`** —
  read-only getters useful as span attributes / log fields in
  wrappers. The underlying fields remain unexported.

Pure additive; no existing API touched. No new dependency.

## [v0.0.6] - 2026-05-21

Phase 6 — Per-event run-history persistence.

v0.0.5 only stored start/finish for each run. This release captures
every FlowEvent — flow_started, node_started, node_finished,
node_skipped, flow_done, flow_err — with timestamps and payloads, so
the run history reflects what actually happened inside the engine,
not just the final outcome.

### Added

- **`flow/store.RunEvent` type** + `RunEventKind` enum
  (`flow_started`, `node_started`, `node_finished`, `node_skipped`,
  `flow_done`, `flow_err`). String values match the SSE event names
  so a replay client can reuse the existing decoder.
- **Store interface extensions:** `AppendRunEvent` +
  `ListRunEvents`. Append is safe to call on finished runs and on
  unknown runs (the latter returns `ErrNotFound`).
- **SQLite implementation:** new `run_events` table with
  `(run_id, seq)` index. `seq` is assigned server-side as
  `max(existing) + 1` per run for a single-statement race-free
  monotonic ordering. Schema is idempotent — existing v0.0.5
  databases pick up the new table on next `Open`.
- **flowd unifies sync and stream paths through `engine.RunStream`.**
  Whether the client wants a JSON response or SSE, every event is
  persisted to the store BEFORE being forwarded to the client. A
  consumer that drops mid-stream still leaves a complete audit trail.
- **`GET /runs/{id}/events`** — ordered event log for a run with
  full payloads. Unknown ids return an empty list (idempotent for
  replay-style clients).

### Tests

- `flow/store/sqlite/events_test.go` (7 cases): append + list
  round-trip with payload JSON decode, empty case, unknown-run
  returns ErrNotFound, nil-payload stored as empty, limit honored,
  events survive flow delete, empty run_id rejected.
- `cmd/flowd/server/server_events_test.go` (6 cases): sync run
  persists every event, stream run persists AND streams, GET
  /runs/{id}/events endpoint, failed-run includes flow_err with
  error payload, router includes node_skipped, missing-run GET
  returns empty list.

13 new test cases; 93 tests overall — all green.

Live smoke confirmed: a sync echo_chain run produces 6 ordered
events in the store (`flow_started` → `node_started/finished × 2`
→ `flow_done`) with per-node input/output payloads and
microsecond-precision timestamps.

No public API removal. The `Store` interface gains two methods; any
v0.0.5 implementation must add them — the bundled SQLite store
handles them automatically.

## [v0.0.5] - 2026-05-21

Phase 5 — Run history (SQLite) + flow CRUD endpoints. The most
architectural phase of the v0.0.x band: flowd grows from a
single-flow server into a fully store-backed REST service.

### Added

- **`flow/store` package.** Pluggable `Store` interface for flow
  CRUD + run lifecycle. Types: `FlowMeta`, `FlowRecord`, `RunMeta`,
  `RunRecord`, `RunStatus`. Sentinel errors `ErrNotFound`,
  `ErrAlreadyExists`.
- **`flow/store/sqlite` sub-package.** Pure-Go SQLite-backed Store
  using `modernc.org/sqlite` (no CGO required). Schema migrates
  on-open; supports `":memory:"` for tests. The core `flow` library
  remains stdlib-only at the source level — the dep enters only when
  this sub-package is imported.
- **flowd HTTP REST surface.**
  - `POST /flows`, `GET /flows`, `GET /flows/{id}`, `PUT /flows/{id}`,
    `DELETE /flows/{id}` — flow CRUD with compile-probe validation
    on create/update (bad flow JSON fails fast with 400 instead of
    surfacing at run time).
  - `POST /flows/{id}/run` (sync) — returns `X-Run-ID` header and
    run_id in body. Result persisted as `done` or `failed`.
  - `POST /flows/{id}/run/stream` (SSE) — same `X-Run-ID`; final
    outcome captured on stream close.
  - `GET /flows/{id}/runs` — run history for a flow (ordered
    descending by `started_at`).
  - `GET /runs/{id}` — single run record with full inputs / outputs
    / error.
- **Engine cache.** `server.Server` holds a `sync.Map` of compiled
  Engines keyed by flow id; PUT and DELETE evict the cache.
- **`cmd/flowd --db <path>` flag.** Defaults to `:memory:` so the
  binary still boots out-of-box. On-disk DSNs persist run history
  across restarts.
- **`cmd/flowd --flow` becomes optional.** When supplied, the file
  is seeded into the store at boot AND enables the legacy `/run` /
  `/run/stream` aliases for v0.0.4 backward compat (routed to that
  flow's id).
- **`server.New(cfg) (*Server, error)`** — new constructor for the
  v0.0.5 surface. `server.NewMux(engine, logger)` is retained for
  the single-engine no-persistence pattern.

### Tests

- `flow/store/sqlite/store_test.go` (11 cases): flow CRUD round-trip,
  duplicate-create rejection, PUT replace semantics, ListFlows
  ordering, run lifecycle, failed-run path, ListRuns ordering,
  ErrNotFound surfacing, flow delete preserving historical runs.
- `cmd/flowd/server/server_crud_test.go` (15 cases): create happy /
  conflict / bad-compile, list, get, get-missing, PUT with cache
  invalidation, delete, run records run with X-Run-ID, list runs,
  get run record, failed-run is persisted as `failed`, legacy
  `/run` routing, no-legacy-route returns 404.

### Live smoke confirmed end-to-end:
- POST a flow → list → run twice → list runs → get one run record
  → DELETE → subsequent run returns 404.

### Dependencies

- Adds `modernc.org/sqlite v1.50.1` (pure-Go) at the module level.
  The `flow` package and its existing sub-packages import none of it.

No public API removal vs v0.0.4. The `Edge.Condition` and `Engine`
contracts are unchanged. The HTTP service grew new endpoints; the
old `/run`, `/run/stream`, `/healthz` continue to work when `--flow`
seeds a flow at boot.

## [v0.0.4] - 2026-05-21

Phase 4 — CEL conditional edges + node activation.

### Added

- **`Edge.Condition` IR field.** Optional CEL expression evaluated at
  edge-firing time. Empty (the default) preserves the v0.0.3 DAG
  semantics — every edge always fires.
- **`flow.ConditionEvaluator` + `flow.Condition` interfaces.** Pluggable
  guard-expression engine. Compile errors surface at Engine.Compile
  time so flow load fails fast.
- **`flow.WithConditionEvaluator(e)` engine option.** Required when
  any edge has a non-empty Condition. Without it, Compile rejects.
- **`flow/cond/cel` sub-package.** CEL-backed evaluator
  (`google/cel-go`). The CORE flow library remains stdlib-only;
  applications opt in by importing this sub-package explicitly.
- **Node activation semantics.** A node is active iff it has no
  incoming edges, OR it is named by Flow.Inputs, OR at least one
  incoming edge fired. Inactive nodes don't run and don't emit
  outgoing-edge fires.
- **`NodeSkipped` FlowEvent kind.** Emitted once per skipped node so
  tracing layers can distinguish "skipped" from "still pending".
- **Flow.Outputs from skipped nodes are silently omitted** from the
  returned outputs map — router-style flows can declare every
  branch's outputs and only the firing branch contributes.
- **`cmd/flow` and `cmd/flowd` auto-wire `flow/cond/cel`** so all
  conditions in user flows just work without extra flags. Both
  default fallback tool catalogs now include the router demo so
  `flow run examples/router/flow.json` runs out-of-box.
- **`examples/router/`.** Two-branch CEL-routed flow + integration
  test covering both branches.

### Tests

- `flow/engine_cond_test.go` (7 cases): router both-paths, skip
  propagation, evaluator-required error, compile-time syntax error,
  runtime evaluate error, backward-compat regression.
- `flow/cond/cel/cel_test.go` (9 cases): equality, `startsWith`,
  `matches` regex, `size` + boolean logic, syntax error, non-bool
  return rejected, unknown variable rejected, engine integration.
- `examples/router/example_test.go` (2 cases): greet/other branches.

### Dependencies

- Adds `github.com/google/cel-go v0.28.1` (+ ~6 transitive: antlr,
  protobuf, etc.) at the MODULE level. The `flow` package itself
  imports none of them; tree-shaking applies to downstream users.

No public API removal vs v0.0.3. The Edge JSON shape is strictly
additive — pre-v0.0.4 flow files load and run unchanged.

## [v0.0.3] - 2026-05-21

Phase 3 — tool manifest. Unblocks practical use: any flow can be run
against any tools without forking the binary.

### Added

- **`flow/tools` package.** JSON manifest format that lists each tool
  by `name` and `kind`, plus `LoadManifest` / `LoadAndBuild`. The
  manifest decouples a flow's IR (which references tools by name)
  from the underlying tool implementations.
- **Built-in `http` kind.** POSTs the JSON args to a URL; expects
  `{"output":"..."}` or falls back to raw body. Headers + timeout
  configurable per entry.
- **Built-in `exec` kind.** Spawns a command, writes the JSON args
  on stdin, captures stdout. Timeout and exit-code error handling
  included; stdout / stderr capped at 1 MiB / 16 KiB respectively.
- **`tools.KindRegistry`.** Pluggable — downstream code calls
  `RegisterKind(name, factory)` to add custom kinds without forking.
- **`--tools <manifest.json>` flag** on both `cmd/flow` and
  `cmd/flowd`. Backward-compatible: when unset the bundled
  `echo_chain` demo tools are registered.
- **`examples/http_tool/`** — flow + manifest example + integration
  test driving the full pipeline through an httptest backend.

### Tests

- `flow/tools/manifest_test.go` — load + build happy path, duplicate-
  name, unknown-kind, missing-field, custom-kind registration.
- `flow/tools/http_test.go` — `{"output"}` JSON decode, raw-body
  fallback, non-2xx → error, header passthrough.
- `flow/tools/exec_test.go` — stdout capture (round-trip via `cat`),
  non-zero exit error, timeout enforcement.
- `examples/http_tool/example_test.go` — end-to-end flow execution
  through HTTP-backed manifest tools.

### Tests run green

- Live smoke: `flow run examples/echo_chain/flow.json --tools <manifest> --input in=hello`
  with exec-backed Python tools returns `OLLEH`.

## [v0.0.2] - 2026-05-21

Phase 2 — parallel execution + HTTP service.

### Added

- **Per-layer parallel execution.** Engine now schedules each
  topological layer's nodes concurrently via
  `github.com/costa92/llm-agent/pkg/fanout`. Within a layer, sibling
  nodes execute in parallel; layers themselves remain sequential.
  Fail-fast: the first node error cancels in-flight peers via
  `fanout.WithFailFast`.
- **`WithMaxNodeConcurrency(n)` engine option.** Caps the
  per-layer goroutine count. `n <= 0` (the default) is unlimited;
  `n == 1` restores the pre-v0.0.2 sequential behavior.
- **`cmd/flowd` HTTP service.** Boots a single compiled flow once and
  exposes:
  - `GET /healthz` — liveness, returns `ok`.
  - `POST /run` — synchronous JSON; body `{"inputs":{...}}` returns
    `{"outputs":{...}}`. 400 on missing inputs / bad JSON, 500 on
    engine error.
  - `POST /run/stream` — Server-Sent Events; one frame per FlowEvent
    (`flow_started`, `node_started`, `node_finished`, `flow_done`,
    `flow_err`).
- **FlowEvent ordering contract documented.** Sibling events within
  a single layer may interleave, but per-node ordering
  (`NodeStarted` before `NodeFinished`), the FlowStarted-first /
  FlowDone-last invariants, and cross-layer ordering all hold.

### Tests

- New `engine_parallel_test.go`: wallclock parity (siblings overlap),
  `WithMaxNodeConcurrency(1)` forces serial, fail-fast peer cancel
  within 200 ms, stream events keyed by node ID.
- New `cmd/flowd/server/server_test.go`: `/healthz`, `/run` happy
  path, `/run` missing-input → 400, `/run` bad JSON → 400, method-
  not-allowed → 405, `/run/stream` SSE end-to-end.

## [v0.0.1] - 2026-05-21

Initial walking skeleton — see git log / GitHub release notes.
