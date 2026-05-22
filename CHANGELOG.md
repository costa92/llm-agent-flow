# Changelog

All notable changes to `github.com/costa92/llm-agent-flow` are documented here.

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — additive-only stability from v0.1.0. -->

## [v0.1.3] - 2026-05-22

Phase — P1-18: `FlowEvent.Metadata` + `MetadataAware` optional
capability (umbrella roadmap O4).

### Added (additive only — v0.1 promise honored)

- **`FlowEvent.Metadata map[string]string`** — new optional field on
  `flow.FlowEvent`. Populated on `NodeFinished` events emitted by
  nodes that implement the new `MetadataAware` capability; `nil` on
  every other event. Intended for per-node side-channel signals such
  as HTTP status, exec exit code, or LLM token usage. Error-path
  metadata is preserved so failed runs still surface debugging
  signal (e.g. `http_status=500`).
- **`flow.MetadataAware` interface** — optional sibling to
  `NodeKind`. Implementations add a single
  `RunWithMetadata(ctx, in) (out, metadata, err)` method; the engine
  detects the capability via type assertion on every invocation.
  Existing `NodeKind` implementations remain unchanged and continue
  to run unmodified. `MetadataAware` will remain optional through
  the v0.1.x band — it will not be promoted into the required
  `NodeKind` shape before v0.2.

### Changed (internal — no API removal)

- **Engine `RunStream` / `Run`** now type-asserts each node against
  `MetadataAware` before invoking `NodeKind.Run`. Metadata returned
  by capable nodes is cloned (mirroring the existing `Output` clone)
  and placed on the emitted `NodeFinished` event.
- **`cmd/flowd` SSE / replay payload** gains a `"metadata"` key on
  events whose `FlowEvent.Metadata` is non-empty. The key is
  omitted entirely for nil and empty maps, so existing SSE / replay
  consumers see byte-identical payloads for legacy flows.

### Tests

- `flow/event_metadata_test.go` (1 case) — `FlowEvent.Metadata`
  field shape.
- `flow/engine_metadata_test.go` (3 cases) — engine propagates
  metadata for `MetadataAware` nodes; leaves `Metadata=nil` for
  plain `NodeKind` nodes; preserves metadata on the error path.
- `cmd/flowd/server/server_metadata_test.go` (4 cases) —
  `streamPayload` includes metadata when populated, omits it on
  nil and empty maps, and the run-history replay round-trips the
  metadata JSON through SQLite persistence.

### Snapshot baseline

`api/v0.1.snapshot.txt` regenerated for the additive shape
(`field Metadata map[string]string` on `FlowEvent` +
`type MetadataAware interface`).

### Follow-ups (deferred — scope kept tight)

- A sample `MetadataAware` implementation on the bundled `toolNode`
  (e.g. surfacing HTTP status / exec exit code) is deferred to its
  own PR (D3) so this PR stays additive-only and stdlib-only.
- The umbrella `docs/source-design-llm-agent-flow.zh-CN.md`
  status-table update for O4 lives in the umbrella repo and is
  deferred to a follow-up cross-repo PR (D5).

## [v0.1.2] - 2026-05-22

Phase — P1-17: SQLite write-throughput hardening (umbrella roadmap).

### Changed (internal — no API change)

- **`(*sqlite.Store)` now enables `PRAGMA journal_mode=WAL` and
  `synchronous=NORMAL` on every on-disk DSN** (`:memory:` and
  `mode=memory` URI variants are detected and left at defaults).
  WAL gives concurrent readers, NORMAL trades a tiny crash-window
  for a large fsync reduction. PRAGMA failures during `Open` are
  surfaced as errors so misconfigured environments fail fast.

### Operational note

On-disk SQLite databases now produce two sidecar files:

- `<db>-wal` — the write-ahead log
- `<db>-shm` — shared-memory index

Both must be included in backup / snapshot / volume-mount strategies
or the database can be left in an inconsistent state. `cmd/flowd`
logs a one-line reminder on startup when a non-memory DSN is used.

### Performance (measured 2026-05-22, 5 iter × 3 count, median)

| Workload                         | Before WAL (v0.1.1) | After WAL (v0.1.2) | Speedup |
|----------------------------------|---------------------|--------------------|---------|
| `AppendRunEvents` batch-of-600   | ~26 ms/op           | ~1.5 ms/op         | **~17×** |
| `AppendRunEvent` × 600 single    | ~14,800 ms/op       | ~42 ms/op          | **~350×** |

The umbrella roadmap P1-17 target was 5–16×; WAL alone exceeds
it. A subsequent multi-VALUES `INSERT` change was scoped and
deferred — see PR description for the YAGNI rationale.

### Tests

- `flow/store/sqlite/wal_test.go` (3 cases) — on-disk enables WAL +
  NORMAL; `:memory:` does not; PRAGMA failure surfaces as `Open`
  error.
- `flow/store/sqlite/events_batch_test.go` (+2 cases) — large-batch
  one-statement contract baseline, chunk-boundary correctness.
- `flow/store/sqlite/events_bench_test.go` — benchmarks above.

## [v0.1.1] - 2026-05-21

Phase 11 — Performance: engine cache LRU + sync-run event batching.

### Added (additive only — v0.1 promise honored)

- **`server.Config.EngineCacheSize`** — bounds the compiled-Engine
  cache via LRU eviction. Default `0` disables bounding (v0.1.0
  behavior preserved). PUT/DELETE handlers still evict immediately.
- **`flow/store.RunEventBatchItem`** — input shape for bulk-insert
  capable stores. The `Store` interface itself is unchanged in
  v0.1.x; bulk insertion is exposed as an optional capability via
  type assertion (`store.(interface { AppendRunEvents(...) error })`).
- **`(*sqlite.Store).AppendRunEvents(ctx, runID, items)`** —
  single-transaction multi-INSERT. `seq` continues monotonically from
  the run's current max; the whole batch shares one `ts`.

### Changed (internal — no API change)

- **Sync `/flows/{id}/run` runs now batch their event persistence.**
  Events collected during the engine loop are flushed in one
  transaction at the end of the run. Stream runs (`/run/stream`)
  unchanged — they still persist per-event before forwarding to
  preserve the v0.0.6 "events outlive a dropped client" guarantee.
- **Engine cache is now LRU-bounded** when `EngineCacheSize > 0`.
  Internal swap from `sync.Map` to a small `container/list`-backed
  cache; behavior at default (cap=0) is byte-identical to v0.1.0.

### Tests

- `cmd/flowd/server/lru_test.go` (5 cases) — cap=0 unbounded,
  LRU eviction order, overwrite, delete idempotent, delete-at-cap.
- `flow/store/sqlite/events_batch_test.go` (4 cases) — happy path,
  empty batch no-op, unknown run → ErrNotFound, seq continues after
  single-event Append.

### Snapshot baseline

`api/v0.1.snapshot.txt` regenerated for the additive type
(`flow/store.RunEventBatchItem`).

## [v0.1.0] - 2026-05-21

**Phase 10 — SemVer freeze.** The v0.1.x exported API is now
guaranteed additive-only. v0.0.x was an exploration band; v0.1.0
freezes the surface and lays down the diff machinery that keeps it
frozen.

### Added

- `docs/compatibility.md` — written stability promise. Within
  v0.1.x: no exported symbol removed, renamed, or re-signed; flow
  JSON IR additive-only; breaking changes go to `/v2`.
- `internal/apisnapshot/` — exported-API snapshot gate. Pure stdlib
  (`go/parser` + `go/printer`); runs on every `go test`; fails any
  drift against `api/v0.1.snapshot.txt`. Deliberate additive
  changes regenerate the baseline with `-update`.
- `api/v0.1.snapshot.txt` — the committed baseline.

### Changed

- Status: v0.0.x walking-skeleton band → v0.1.x stable.

No new product features in this tag; the freeze IS the deliverable.
All v0.0.9 endpoints / library functions are preserved verbatim.

## [v0.0.9] - 2026-05-21

Phase 9 — Replay endpoint.

### Added

- `POST /runs/{id}/replay` — re-streams a run's persisted events as
  a fresh SSE session. No new engine run; the events come straight
  out of `run_events`. `X-Replay: true` + `X-Run-ID` identify the
  replay. Stored payload JSON is forwarded verbatim so clients
  decode replay frames identically to live frames.

Unknown run id → 404. Wrong method → 405. Empty event log → 200
with zero SSE frames (idempotent).

4 new test cases.

## [v0.0.8] - 2026-05-21

Phase 8 — Bearer-token auth + pluggable Authenticator.

### Added

- `server.Authenticator` interface — pluggable extension point.
  Returning `server.ErrUnauthorized` → 401 + `WWW-Authenticate`
  Bearer challenge; any other error → 403.
- `server.BearerTokenAuthenticator{Token: ...}` — static-token
  implementation with constant-time comparison.
- `/healthz` bypass — always allowed so external monitors work
  without a token.
- `cmd/flowd --token <secret>` (or `FLOWD_TOKEN` env var) — when
  set, every endpoint except `/healthz` requires
  `Authorization: Bearer <secret>`.

8 new test cases; no public API removal.

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
