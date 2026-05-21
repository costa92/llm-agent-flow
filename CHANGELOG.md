# Changelog

All notable changes to `github.com/costa92/llm-agent-flow` are documented here.

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — note: v0.0.x is provisional. -->

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
