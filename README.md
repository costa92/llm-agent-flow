# llm-agent-flow

Serializable flow IR + DAG executor for the
[`llm-agent`](https://github.com/costa92/llm-agent) ecosystem.

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

Implemented (v0.0.2):

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
- **`cmd/flowd`** HTTP service — boots one compiled flow, exposes
  `GET /healthz`, `POST /run` (sync JSON), `POST /run/stream` (SSE)

Deferred to next phases:

- conditional edges (CEL expressions)
- run-history store (sqlite) + flow CRUD endpoints
- `otelflow.Wrap(Engine) Engine` decorator (in `llm-agent-otel`)

## Quick start

CLI (one-shot):

```bash
go install github.com/costa92/llm-agent-flow/cmd/flow@latest
flow run examples/echo_chain/flow.json --input in=hello
# {"out": "OLLEH"}

flow run examples/echo_chain/flow.json --input in=hello --stream
# one JSON line per FlowEvent
```

HTTP service (long-running):

```bash
go install github.com/costa92/llm-agent-flow/cmd/flowd@latest
flowd --addr :7861 --flow examples/echo_chain/flow.json

# in another shell:
curl http://localhost:7861/healthz
# ok

curl -X POST http://localhost:7861/run \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# {"outputs":{"out":"OLLEH"}}

curl -X POST http://localhost:7861/run/stream \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# SSE stream of FlowEvents
```

Or from this repo:

```bash
go run ./cmd/flow  run examples/echo_chain/flow.json --input in=hello
go run ./cmd/flowd --flow examples/echo_chain/flow.json
```

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
