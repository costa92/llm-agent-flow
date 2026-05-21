# llm-agent-flow

Serializable flow IR + DAG executor for the
[`llm-agent`](https://github.com/costa92/llm-agent) ecosystem.

## Docs

- **[Tutorial](docs/tutorial.md)** — your first flow, custom tools,
  conditional routing, `cmd/flowd` REST API.
- **[Architecture](docs/architecture.md)** — how Engine / Store /
  otelflow / flowd compose; Runner interface; execution model.
- **[Operations](docs/operations.md)** — deploying `flowd`: auth,
  SQLite, OTel, perf, backup, upgrade path.
- **[Compatibility promise](docs/compatibility.md)** — what the v0.1
  freeze covers; how the snapshot gate works.

A flow is a directed acyclic graph of nodes connected by typed edges,
authored as JSON, validated at load time, and executed by a topological
engine. Each node wraps an existing `llm-agent` primitive
(`agents.Tool`, `agents.Agent`) — `flow` does not invent a parallel
component model; it makes the existing ones composable as files.

## Position in the ecosystem

```
llm-agent-flow ──depends on──▶  llm-agent
```

The `flow` library package is stdlib-only outside the back-edge to
`github.com/costa92/llm-agent`. Subcommands under `cmd/` may pull
additional deps in future phases (HTTP server, run store) — the
library never will.

This is a **sister repo to `llm-agent-rag`, `-providers`, `-otel`,
`-customer-support`**. It does not absorb their surfaces. It complements
the in-process `orchestrate.StateGraph[S]` and the in-flight
`orchestrate.Supervisor` (Phase 37) — `flow` is the *file format* and
*DAG engine*, `StateGraph` is the *in-process state machine*; they
compose (a flow Node may run a Supervisor; a Supervisor Worker may
invoke a flow).

## Status

**v0.0.x — walking skeleton.** Library API and JSON schema are
provisional and may change between v0.0.x tags. SemVer stability
begins at v0.1.0.

Implemented (v0.0.6):

- Flow / Node / Edge / Port Go types with JSON round-trip
- `Load(r io.Reader) (Flow, error)`
- `Validate(Flow) error` — cycle detection, dangling-edge / port
  reference check, duplicate node ID check
- `NodeRegistry` — pluggable node-type registration
- `ToolNode` adapter — wraps any `agents.Tool` as a one-input /
  one-output node
- `Engine` — topological DAG executor with **per-layer parallelism**
  via `github.com/costa92/llm-agent/pkg/fanout`; fail-fast cancel on
  the first node error; `WithMaxNodeConcurrency(n)` opt-in cap
- `FlowEvent` typed union — `FlowStarted | NodeStarted | NodeFinished
  | FlowDone | FlowErr` mirroring the K1 streaming idiom; sibling
  events within a layer may interleave but per-node ordering and the
  FlowStarted-first / FlowDone-last invariants hold
- `cmd/flow run <file.json>` CLI
- `cmd/flowd` HTTP service — `GET /healthz`, `POST /run` (sync JSON),
  `POST /run/stream` (SSE)
- **`flow/tools` tool-manifest format** — describe tools as JSON;
  load through `tools.LoadAndBuild`. Two built-in kinds:
  - `http` — POST `{"input":...}` to a URL; decode `{"output":...}` or
    fall back to raw body. Headers + timeout configurable.
  - `exec` — run a command with the JSON args on stdin; capture stdout
    as the tool output. Timeout and exit-code error handling included.
- **`--tools <manifest.json>` flag** on both `cmd/flow` and
  `cmd/flowd`. Without it, the bundled `echo_chain` demo tools are
  registered so the binary still runs against `examples/echo_chain`
  out-of-box.
- **`tools.KindRegistry.RegisterKind(...)`** lets downstream code add
  custom kinds without forking the library.
- **CEL conditional edges.** `Edge.Condition` is an optional CEL
  expression evaluated against `value` (source port output). When
  set, the edge fires only if the expression returns true; downstream
  nodes whose incoming edges all skip are themselves skipped
  (NodeSkipped event). `flow/cond/cel` (separate sub-package) holds
  the cel-go dependency — the core `flow` library stays stdlib-only.
- **Activation semantics.** A node runs iff it has no incoming edges,
  is named by `Flow.Inputs`, or at least one incoming edge fires.
  Skipped-branch outputs are silently omitted from the result map so
  router flows can declare both branches' outputs.
- **SQLite-backed persistence + run history.** `flow/store` exposes a
  pluggable `Store` interface; `flow/store/sqlite` is a pure-Go
  implementation backed by `modernc.org/sqlite` (no CGO). Flows are
  CRUD-managed; runs are recorded with status / inputs / outputs /
  error / timestamps.
- **flowd REST API.** Full surface:
  - `POST /flows`, `GET /flows`, `GET /flows/{id}`, `PUT /flows/{id}`,
    `DELETE /flows/{id}` — flow CRUD; PUT/DELETE invalidates the
    compiled-engine cache.
  - `POST /flows/{id}/run` — sync; returns `X-Run-ID` header + run id
    in body.
  - `POST /flows/{id}/run/stream` — SSE; same X-Run-ID, final outcome
    persisted on stream close.
  - `GET /flows/{id}/runs` — list runs for a flow.
  - `GET /runs/{id}` — full run record (inputs / outputs / error).
  - `GET /runs/{id}/events` — full ordered FlowEvent history for a
    run (every flow_started / node_started / node_finished /
    node_skipped / flow_done / flow_err frame, with payloads).
  - `POST /run`, `POST /run/stream` — legacy aliases against the seed
    flow set by `--flow`. v0.0.4 clients keep working.
- **Every FlowEvent is persisted.** Both sync and stream runs drive
  the engine through `RunStream` internally; every event lands in
  `run_events` before being forwarded (in stream mode) to the SSE
  client. A client that drops mid-stream still leaves a complete
  audit trail in the store.

Deferred to next phases:

- `otelflow.Wrap(Engine) Engine` decorator (in `llm-agent-otel`)
- AuthN / authZ on the HTTP API (no auth at v0.0.x)
- Replay endpoint (`POST /runs/{id}/replay` that re-streams the
  persisted events to a new client)

## Quick start

CLI (one-shot):

```bash
go install github.com/costa92/llm-agent-flow/cmd/flow@latest
flow run examples/echo_chain/flow.json --input in=hello
# {"out": "OLLEH"}

flow run examples/echo_chain/flow.json --input in=hello --stream
# one JSON line per FlowEvent
```

HTTP service (long-running, with persistence):

```bash
go install github.com/costa92/llm-agent-flow/cmd/flowd@latest

# In-memory (ephemeral) mode — fastest start, useful for demos:
flowd --addr :7861 --flow examples/echo_chain/flow.json

# Or with on-disk SQLite for run history that survives restarts:
flowd --addr :7861 --db /var/lib/flowd/flow.db
```

CRUD against `/flows`:

```bash
# Create a flow.
curl -X POST http://localhost:7861/flows \
     -H 'Content-Type: application/json' \
     --data-binary "{\"flow\":$(cat examples/echo_chain/flow.json)}"
# 201 with the FlowRecord

# List flows.
curl http://localhost:7861/flows
# {"flows":[{"id":"echo_chain", ...}]}

# Run + capture the run id from the X-Run-ID header.
curl -i -X POST http://localhost:7861/flows/echo_chain/run \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# HTTP/1.1 200 OK
# X-Run-ID: 4351cce92d54ba5d
# {"outputs":{"out":"OLLEH"},"run_id":"4351cce92d54ba5d"}

# List the run history for a flow.
curl http://localhost:7861/flows/echo_chain/runs
# {"runs":[{"id":"...","status":"done", ...}]}

# Fetch a single run with full inputs/outputs/error.
curl http://localhost:7861/runs/4351cce92d54ba5d
# {"id":"4351cce92d54ba5d","status":"done","inputs":{"in":"hello"},
#  "outputs":{"out":"OLLEH"},...}

# Fetch the per-event audit log for a run.
curl http://localhost:7861/runs/4351cce92d54ba5d/events
# {"events":[
#   {"seq":1,"kind":"flow_started",  "payload":{"flow":"echo_chain"}, "ts":"..."},
#   {"seq":2,"kind":"node_started",  "node_id":"upper",   ...},
#   {"seq":3,"kind":"node_finished", "node_id":"upper",   "payload":{"output":{...}}, ...},
#   ...
#   {"seq":6,"kind":"flow_done",     "payload":{"outputs":{"out":"OLLEH"}}, "ts":"..."}
# ]}

# SSE-stream a run.
curl -X POST http://localhost:7861/flows/echo_chain/run/stream \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# event: flow_started \n data: {...} \n\n ... (final flow_done persisted)

# Legacy clients still work when --flow is supplied:
curl http://localhost:7861/healthz                  # ok
curl -X POST http://localhost:7861/run -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hi"}}'                    # 200 OK
```

Or from this repo:

```bash
go run ./cmd/flow  run examples/echo_chain/flow.json --input in=hello
go run ./cmd/flowd --flow examples/echo_chain/flow.json
```

With a custom tool manifest (any flow + any tools, no code changes):

```bash
# upper.json: HTTP-backed tools
cat > /tmp/tools.json <<'EOF'
{"tools":[
  {"name":"upper",  "kind":"http","url":"http://localhost:8080/upper"},
  {"name":"reverse","kind":"http","url":"http://localhost:8080/reverse"}
]}
EOF
flow run examples/echo_chain/flow.json --tools /tmp/tools.json --input in=hello

# Or use exec-backed tools running on the host:
cat > /tmp/tools.json <<'EOF'
{"tools":[
  {"name":"upper",  "kind":"exec",
   "command":["python3","-c","import sys,json; print(json.load(sys.stdin)['input'].upper())"]},
  {"name":"reverse","kind":"exec",
   "command":["python3","-c","import sys,json; print(json.load(sys.stdin)['input'][::-1])"]}
]}
EOF
flow run examples/echo_chain/flow.json --tools /tmp/tools.json --input in=hello
# {"out": "OLLEH"}
```

See `examples/http_tool/` for a fully-tested end-to-end recipe.

Conditional routing (CEL edges):

```bash
flow run examples/router/flow.json --input in="hello world"
# {"greeting":"Hello! Nice to see you."}

flow run examples/router/flow.json --input in="what time is it"
# {"other":"Sorry — I do not know how to handle that yet."}
```

The router flow has two outgoing edges from `classify` with CEL
guards `value == "greet"` and `value != "greet"`. Only the matching
branch fires; the other is skipped (and emits a `node_skipped`
event in `--stream` mode).

## JSON flow shape (v0)

```json
{
  "id": "echo_chain",
  "name": "echo chain",
  "nodes": [
    { "id": "upper",   "type": "tool", "config": { "tool": "upper" } },
    { "id": "reverse", "type": "tool", "config": { "tool": "reverse" } }
  ],
  "edges": [
    { "source": { "node": "upper",   "port": "output" },
      "target": { "node": "reverse", "port": "input"  } }
  ],
  "inputs":  [{ "node": "upper",   "port": "input"  }],
  "outputs": [{ "node": "reverse", "port": "output" }]
}
```

Each `nodes[].type` resolves through `NodeRegistry`. The bundled
`tool` type looks up an `agents.Tool` by name in a tool registry the
caller provides at engine construction time.

## License

MIT — see [LICENSE](LICENSE).
