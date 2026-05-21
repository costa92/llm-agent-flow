# Tutorial — your first flow

A walk through `llm-agent-flow` from "run the demo" to "deploy a
custom flow with HTTP-backed tools and a conditional branch."

For the wire-level details see [`architecture.md`](architecture.md);
for production deployment see [`operations.md`](operations.md).

## 1. Install + run the bundled demo

```bash
go install github.com/costa92/llm-agent-flow/cmd/flow@latest
flow run examples/echo_chain/flow.json --input in=hello
# {"out": "OLLEH"}
```

`examples/echo_chain/flow.json` is the canonical chain:

```
upper (tool) ──output──▶ reverse (tool)
   ▲                              │
   │ in                           │ out
```

`upper` uppercases the input ("hello" → "HELLO"); `reverse`
reverses ("HELLO" → "OLLEH"). The bundled `cmd/flow` ships demo
tools so it boots out-of-box.

Stream mode shows every event:

```bash
flow run examples/echo_chain/flow.json --input in=hi --stream
# {"kind":"flow_started",  ...}
# {"kind":"node_started",  "node":"upper",   "input":{"input":"hi"}}
# {"kind":"node_finished", "node":"upper",   "output":{"output":"HI"}}
# {"kind":"node_started",  "node":"reverse", "input":{"input":"HI"}}
# {"kind":"node_finished", "node":"reverse", "output":{"output":"IH"}}
# {"kind":"flow_done",     "outputs":{"out":"IH"}}
```

## 2. Author your own flow

A flow is JSON. The minimum:

```json
{
  "id": "my_flow",
  "nodes": [
    { "id": "step1", "type": "tool", "config": { "tool": "upper" } }
  ],
  "edges": [],
  "inputs":  [{ "name": "in",  "node": "step1", "port": "input"  }],
  "outputs": [{ "name": "out", "node": "step1", "port": "output" }]
}
```

Rules:

- **`id` is the canonical handle.** It must be non-empty.
- **`nodes[].type`** resolves through the `NodeRegistry`. The
  bundled `tool` type looks up the tool by name in the runtime's
  `Deps.Tools` catalog.
- **Edges** wire `source.node.port` → `target.node.port`. Source =
  Target rejects at validate time. Cycles too.
- **Inputs** name the entry ports — the caller supplies them by
  name at run time.
- **Outputs** name the exit ports — they're returned by name in the
  result map.

Validate locally before shipping a flow:

```bash
# Trying to run an obviously broken flow surfaces the validate
# error before the engine starts:
echo '{"id":"x","nodes":[],"edges":[]}' > /tmp/empty.json
flow run /tmp/empty.json
# flow: flow: validate: no nodes
```

## 3. Plug in your own tools — `--tools <manifest.json>`

The bundled CLI only knows the demo tools. For real flows, provide
a **tool manifest**:

```bash
cat > /tmp/tools.json <<'EOF'
{
  "tools": [
    {
      "name": "translate",
      "kind": "http",
      "url":  "https://api.example.com/translate",
      "headers": { "Authorization": "Bearer SECRET" }
    },
    {
      "name": "wc",
      "kind": "exec",
      "command": ["wc", "-w"]
    }
  ]
}
EOF
flow run my_flow.json --tools /tmp/tools.json --input in="hello world"
```

Two built-in kinds:

- **`http`** — POSTs the JSON args to `url`; expects a JSON response
  `{"output":"..."}` or falls back to the raw body as a string.
  Configurable `method`, `headers`, `timeout_ms` (default 30 s).
- **`exec`** — runs `command[0]` with `command[1:]` as argv, sends
  the JSON args on stdin, captures stdout as the tool output.
  Configurable `timeout_ms` (default 30 s); stdout/stderr capped at
  1 MiB / 16 KiB.

Custom kinds plug in via `tools.KindRegistry.RegisterKind` if you
embed the library yourself (the bundled CLI only ships the
built-ins).

### Real demo with exec tools

```bash
cat > /tmp/py_tools.json <<'EOF'
{
  "tools": [
    {
      "name": "upper",
      "kind": "exec",
      "command": ["python3", "-c",
        "import sys,json; print(json.load(sys.stdin)['input'].upper())"]
    },
    {
      "name": "reverse",
      "kind": "exec",
      "command": ["python3", "-c",
        "import sys,json; print(json.load(sys.stdin)['input'][::-1])"]
    }
  ]
}
EOF
flow run examples/echo_chain/flow.json --tools /tmp/py_tools.json --input in=hello
# {"out": "OLLEH"}
```

The flow itself is unchanged — only the tools behind the names
switched out for Python subprocesses. That's the manifest pattern
in one line: **flows reference tools by name; tools live anywhere**.

## 4. Conditional routing — CEL edges

`Edge.Condition` is a CEL expression evaluated against the source
port's outbound value (exposed as `value`). The edge fires only if
the expression returns true. Downstream nodes whose incoming edges
all skip become skipped themselves.

Example: `examples/router/flow.json`

```json
{
  "id": "router",
  "nodes": [
    { "id": "classify",   "type": "tool", "config": { "tool": "classify" } },
    { "id": "greet_path", "type": "tool", "config": { "tool": "make_greeting" } },
    { "id": "other_path", "type": "tool", "config": { "tool": "say_other" } }
  ],
  "edges": [
    { "source": {"node":"classify","port":"output"},
      "target": {"node":"greet_path","port":"input"},
      "condition": "value == \"greet\"" },
    { "source": {"node":"classify","port":"output"},
      "target": {"node":"other_path","port":"input"},
      "condition": "value != \"greet\"" }
  ],
  "inputs":  [{ "name": "in",       "node": "classify",   "port": "input"  }],
  "outputs": [
    { "name": "greeting", "node": "greet_path", "port": "output" },
    { "name": "other",    "node": "other_path", "port": "output" }
  ]
}
```

Run it:

```bash
flow run examples/router/flow.json --input in="hello world"
# {"greeting":"Hello! Nice to see you."}

flow run examples/router/flow.json --input in="what time is it"
# {"other":"Sorry — I do not know how to handle that yet."}
```

Note that both branches' outputs are declared but **only the active
branch's output appears in the result map** — the other key is
silently omitted. This is what makes router-style flows usable.

CEL grammar supports the standard library — `value.startsWith(...)`,
`value.matches("^[A-Z]+$")`, `size(value) > 0`, `&&`, `||`, etc. See
[cel-go's spec](https://github.com/google/cel-go/blob/master/cel/decls.go).
At v0.1 the only variable exposed is `value`; future versions may
widen the environment.

## 5. Long-running service — `cmd/flowd`

For real deployments, run the HTTP service:

```bash
go install github.com/costa92/llm-agent-flow/cmd/flowd@latest

# In-memory mode (ephemeral runs — great for demos):
flowd --addr :7861 --flow examples/echo_chain/flow.json

# Or with persistent run history + flow CRUD:
flowd --addr :7861 --db /var/lib/flowd/flow.db
```

Manage flows via REST:

```bash
# Create
curl -X POST http://localhost:7861/flows \
     -H 'Content-Type: application/json' \
     --data-binary "{\"flow\":$(cat my_flow.json)}"

# Run + capture run id
curl -i -X POST http://localhost:7861/flows/my_flow/run \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hi"}}'
# X-Run-ID: 4351cce92d54ba5d
# {"outputs":{"out":"IH"},"run_id":"4351cce92d54ba5d"}

# Inspect the event history
curl http://localhost:7861/runs/4351cce92d54ba5d/events

# Replay it back as an SSE stream
curl -X POST http://localhost:7861/runs/4351cce92d54ba5d/replay
```

For auth, OTel tracing, scaling considerations — see
[`operations.md`](operations.md).

## 6. Library use — embedding `llm-agent-flow` in your own Go service

```go
import (
    "github.com/costa92/llm-agent-flow/flow"
    cond "github.com/costa92/llm-agent-flow/flow/cond/cel"
)

// 1. Register node types
reg := flow.NewNodeRegistry()
_ = flow.RegisterToolNode(reg)

// 2. Build a tool catalog. Tools satisfy the narrow flow.Tool
//    interface — usually you adapt your existing llm-agent agents.Tool
//    via flow.FromAgentTool(t) / flow.FromAgentTools(ts).
tools := flow.ToolMap{
    "translate": myTranslateTool,
    "wc":         myWcTool,
}

// 3. Optional CEL evaluator for conditional flows
ev, _ := cond.NewEvaluator()

// 4. Compile
eng, err := flow.LoadCompile(flowJSONReader, reg, flow.Deps{Tools: tools},
    flow.WithConditionEvaluator(ev),
    flow.WithMaxNodeConcurrency(8),
)
if err != nil { /* compile error — bad flow */ }

// 5. Run
out, err := eng.Run(ctx, map[string]string{"in": "hello"})

// Or stream events
ch, _ := eng.RunStream(ctx, inputs)
for ev := range ch {
    // ev.Kind / ev.NodeID / ev.Input / ev.Output / ev.Outputs / ev.Err
}
```

For OTel-traced flows, wrap with `otelflow.Wrap` from the
`llm-agent-otel` sister repo — see [`architecture.md`](architecture.md#telemetry--otelflow-sister-repo).

## 7. Next steps

- [`compatibility.md`](compatibility.md) — what the v0.1.x freeze covers.
- [`architecture.md`](architecture.md) — internal design.
- [`operations.md`](operations.md) — deploying `flowd` in production.
