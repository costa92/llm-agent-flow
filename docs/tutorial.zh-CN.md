[English](./tutorial.md) | [简体中文](./tutorial.zh-CN.md)

# 教程 —— 你的第一个流程

从“运行演示”到“用 HTTP 支撑的工具和条件分支部署一个自定义流程”，
带你走一遍 `llm-agent-flow`。

关于线级细节参见 [`architecture.md`](architecture.md)；关于生产
部署参见 [`operations.md`](operations.md)。

## 1. 安装 + 运行随附的演示

```bash
go install github.com/costa92/llm-agent-flow/cmd/flow@latest
flow run examples/echo_chain/flow.json --input in=hello
# {"out": "OLLEH"}
```

`examples/echo_chain/flow.json` 是规范的链：

```
upper (tool) ──output──▶ reverse (tool)
   ▲                              │
   │ in                           │ out
```

`upper` 把输入大写化（"hello" → "HELLO"）；`reverse` 反转
（"HELLO" → "OLLEH"）。随附的 `cmd/flow` 自带演示工具，因此开箱
即可启动。

流式模式展示每个事件：

```bash
flow run examples/echo_chain/flow.json --input in=hi --stream
# {"kind":"flow_started",  ...}
# {"kind":"node_started",  "node":"upper",   "input":{"input":"hi"}}
# {"kind":"node_finished", "node":"upper",   "output":{"output":"HI"}}
# {"kind":"node_started",  "node":"reverse", "input":{"input":"HI"}}
# {"kind":"node_finished", "node":"reverse", "output":{"output":"IH"}}
# {"kind":"flow_done",     "outputs":{"out":"IH"}}
```

## 2. 编写你自己的流程

流程是 JSON。最小形态：

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

规则：

- **`id` 是规范的句柄。** 它必须非空。
- **`nodes[].type`** 通过 `NodeRegistry` 解析。随附的 `tool` 类型
  在运行时的 `Deps.Tools` 目录中按名称查找工具。
- **Edges** 把 `source.node.port` → `target.node.port` 连线。
  Source = Target 在校验时被拒绝。环也是。
- **Inputs** 命名入口端口 —— 调用方在运行时按名称提供它们。
- **Outputs** 命名出口端口 —— 它们在结果映射中按名称返回。

在发布流程前在本地校验：

```bash
# Trying to run an obviously broken flow surfaces the validate
# error before the engine starts:
echo '{"id":"x","nodes":[],"edges":[]}' > /tmp/empty.json
flow run /tmp/empty.json
# flow: flow: validate: no nodes
```

## 3. 接入你自己的工具 —— `--tools <manifest.json>`

随附的 CLI 只知道演示工具。对于真实流程，提供一个 **工具清单**：

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

两个内置 kind：

- **`http`** —— 把 JSON 参数 POST 到 `url`；期望一个 JSON 响应
  `{"output":"..."}` 或回退到原始 body 作为字符串。可配置
  `method`、`headers`、`timeout_ms`（默认 30 s）。
- **`exec`** —— 以 `command[1:]` 作为 argv 运行 `command[0]`，在
  stdin 上发送 JSON 参数，将 stdout 捕获为工具输出。可配置
  `timeout_ms`（默认 30 s）；stdout/stderr 上限为 1 MiB / 16 KiB。

如果你自己嵌入库，自定义 kind 通过
`tools.KindRegistry.RegisterKind` 接入（随附的 CLI 只携带内置 kind）。

### 用 exec 工具的真实演示

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

流程本身不变 —— 只是名称背后的工具被换成了 Python 子进程。
这就是清单范式的一句话总结：**流程按名称引用工具；工具可以
住在任何地方**。

## 4. 条件路由 —— CEL 边

`Edge.Condition` 是一个针对源端口出站值（暴露为 `value`）求值的
CEL 表达式。仅当表达式返回 true 时该边才触发。所有入边都被跳过的
下游节点自身也变为被跳过。

示例：`examples/router/flow.json`

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

运行它：

```bash
flow run examples/router/flow.json --input in="hello world"
# {"greeting":"Hello! Nice to see you."}

flow run examples/router/flow.json --input in="what time is it"
# {"other":"Sorry — I do not know how to handle that yet."}
```

注意两个分支的输出都被声明，但 **只有激活分支的输出出现在结果
映射中** —— 另一个键被静默省略。这正是路由型流程可用的原因。

CEL 语法支持标准库 —— `value.startsWith(...)`、
`value.matches("^[A-Z]+$")`、`size(value) > 0`、`&&`、`||` 等。参见
[cel-go's spec](https://github.com/google/cel-go/blob/master/cel/decls.go)。
在 v0.1，暴露的唯一变量是 `value`；未来版本可能拓宽该环境。

## 5. 长期运行的服务 —— `cmd/flowd`

对于真实部署，运行 HTTP 服务：

```bash
go install github.com/costa92/llm-agent-flow/cmd/flowd@latest

# In-memory mode (ephemeral runs — great for demos):
flowd --addr :7861 --flow examples/echo_chain/flow.json

# Or with persistent run history + flow CRUD:
flowd --addr :7861 --db /var/lib/flowd/flow.db
```

通过 REST 管理流程：

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

关于鉴权、OTel 追踪、扩展考量 —— 参见
[`operations.md`](operations.md)。

## 6. 库用法 —— 在你自己的 Go 服务中嵌入 `llm-agent-flow`

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

对于 OTel 追踪的流程，用 `llm-agent-otel` 兄弟仓的
`otelflow.Wrap` 包装 —— 参见
[`architecture.md`](architecture.md#telemetry--otelflow-sister-repo)。

## 7. 后续步骤

- [`compatibility.md`](compatibility.md) —— v0.1.x 冻结覆盖什么。
- [`architecture.md`](architecture.md) —— 内部设计。
- [`operations.md`](operations.md) —— 在生产中部署 `flowd`。
