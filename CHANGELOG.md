# Changelog

All notable changes to `github.com/costa92/llm-agent-flow` are documented here.

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — note: v0.0.x is provisional. -->

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
