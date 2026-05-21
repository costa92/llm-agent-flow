# Changelog

All notable changes to `github.com/costa92/llm-agent-flow` are documented here.

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — note: v0.0.x is provisional. -->

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
