[English](./README.md) | [简体中文](./README.zh-CN.md)

# llm-agent-flow

面向 [`llm-agent`](https://github.com/costa92/llm-agent) 生态的可序列化
流程 IR + DAG 执行器。

## 文档

- **[教程](docs/tutorial.zh-CN.md)** —— 你的第一个流程、自定义工具、
  条件路由、`cmd/flowd` REST API。
- **[架构](docs/architecture.zh-CN.md)** —— Engine / Store /
  otelflow / flowd 如何组合；Runner 接口；执行模型。
- **[运维](docs/operations.zh-CN.md)** —— 部署 `flowd`：鉴权、
  SQLite、OTel、性能、备份、升级路径。
- **[兼容性承诺](docs/compatibility.zh-CN.md)** —— v0.1
  冻结覆盖了哪些内容；快照门禁如何工作。

流程是由类型化的边连接的节点构成的有向无环图，
以 JSON 编写，在加载时校验，并由拓扑引擎执行。每个节点包装一个已有的
`llm-agent` 原语（`agents.Tool`、`agents.Agent`）—— `flow` 并不发明
一套平行的组件模型；它让既有的组件能以文件形式可组合。

## 在生态中的定位

```
llm-agent-flow ──depends on──▶  llm-agent
```

`flow` 库包除了对 `github.com/costa92/llm-agent` 的反向边以外，
是仅标准库的。`cmd/` 下的子命令在未来阶段可能引入额外依赖（HTTP
服务器、运行存储）—— 但库本身永远不会。

这是 **`llm-agent-rag`、`-providers`、`-otel`、`-customer-support`
的兄弟仓**。它不吸收它们的接口面，而是与进程内的
`orchestrate.StateGraph[S]` 以及在研发中的
`orchestrate.Supervisor`（Phase 37）互补 —— `flow` 是*文件格式*和
*DAG 引擎*，`StateGraph` 是*进程内状态机*；它们可以组合（一个流程
Node 可以运行一个 Supervisor；一个 Supervisor Worker 可以调用一个流程）。

## 状态

**v0.0.x —— 走通骨架。** 库 API 和 JSON schema 是临时的，
可能在 v0.0.x 各 tag 之间变更。SemVer 稳定性从 v0.1.0 开始。

已实现（v0.0.6）：

- Flow / Node / Edge / Port Go 类型，支持 JSON 往返
- `Load(r io.Reader) (Flow, error)`
- `Validate(Flow) error` —— 环检测、悬空边 / 端口引用检查、
  重复节点 ID 检查
- `NodeRegistry` —— 可插拔的节点类型注册
- `ToolNode` 适配器 —— 将任意 `agents.Tool` 包装为单输入 /
  单输出的节点
- `Engine` —— 拓扑 DAG 执行器，通过
  `github.com/costa92/llm-agent/pkg/fanout` 实现 **逐层并行**；
  在首个节点出错时 fail-fast 取消；`WithMaxNodeConcurrency(n)` 可选上限
- `FlowEvent` 类型化联合 —— `FlowStarted | NodeStarted | NodeFinished
  | FlowDone | FlowErr`，沿用 K1 流式范式；同一层内的兄弟事件可能
  交错，但每个节点的顺序以及 FlowStarted 在先 / FlowDone 在后的不变式成立
- `cmd/flow run <file.json>` CLI
- `cmd/flowd` HTTP 服务 —— `GET /healthz`、`POST /run`（同步 JSON）、
  `POST /run/stream`（SSE）
- **`flow/tools` 工具清单格式** —— 以 JSON 描述工具；
  通过 `tools.LoadAndBuild` 加载。两个内置 kind：
  - `http` —— 向某个 URL POST `{"input":...}`；解码 `{"output":...}` 或
    回退到原始 body。Headers + 超时可配置。
  - `exec` —— 以 stdin 上的 JSON 参数运行一个命令；将 stdout 捕获
    为工具输出。包含超时与退出码错误处理。
- **`cmd/flow` 和 `cmd/flowd` 上的 `--tools <manifest.json>` 标志**。
  不带它时，会注册随附的 `echo_chain` 演示工具，使二进制开箱即可
  对 `examples/echo_chain` 运行。
- **`tools.KindRegistry.RegisterKind(...)`** 让下游代码无需 fork
  库即可添加自定义 kind。
- **CEL 条件边。** `Edge.Condition` 是一个可选的 CEL 表达式，
  针对 `value`（源端口输出）求值。设置后，仅当表达式返回 true 时
  该边才触发；所有入边都被跳过的下游节点自身也会被跳过
  （NodeSkipped 事件）。`flow/cond/cel`（独立子包）持有 cel-go
  依赖 —— 核心 `flow` 库保持仅标准库。
- **激活语义。** 一个节点在它没有入边、被 `Flow.Inputs` 命名、
  或至少有一条入边触发时运行。被跳过分支的输出会从结果映射中
  静默省略，因此路由型流程可以同时声明两个分支的输出。
- **SQLite 支撑的持久化 + 运行历史。** `flow/store` 暴露一个
  可插拔的 `Store` 接口；`flow/store/sqlite` 是基于
  `modernc.org/sqlite` 的纯 Go 实现（无 CGO）。流程以 CRUD 管理；
  运行记录带有状态 / 输入 / 输出 / 错误 / 时间戳。
- **flowd REST API。** 完整接口面：
  - `POST /flows`、`GET /flows`、`GET /flows/{id}`、`PUT /flows/{id}`、
    `DELETE /flows/{id}` —— 流程 CRUD；PUT/DELETE 会使已编译的
    引擎缓存失效。
  - `POST /flows/{id}/run` —— 同步；返回 `X-Run-ID` 响应头 + body
    中的 run id。
  - `POST /flows/{id}/run/stream` —— SSE；同样的 X-Run-ID，最终结果
    在流关闭时持久化。
  - `GET /flows/{id}/runs` —— 列出某个流程的运行。
  - `GET /runs/{id}` —— 完整运行记录（输入 / 输出 / 错误）。
  - `GET /runs/{id}/events` —— 某次运行的完整有序 FlowEvent 历史
    （每一个 flow_started / node_started / node_finished /
    node_skipped / flow_done / flow_err 帧，含载荷）。
  - `POST /run`、`POST /run/stream` —— 针对由 `--flow` 设置的种子
    流程的旧版别名。v0.0.4 客户端继续可用。
- **每个 FlowEvent 都被持久化。** 同步与流式运行在内部都通过
  `RunStream` 驱动引擎；每个事件先落入 `run_events`，然后（在流式
  模式下）转发给 SSE 客户端。一个在流中途断开的客户端，仍会在存储
  中留下完整的审计轨迹。

延后到后续阶段：

- `otelflow.Wrap(Engine) Engine` 装饰器（在 `llm-agent-otel` 中）
- HTTP API 上的 AuthN / authZ（v0.0.x 无鉴权）
- 重放端点（`POST /runs/{id}/replay`，把持久化事件重新流式
  推送给新客户端）

## 快速开始

CLI（一次性）：

```bash
go install github.com/costa92/llm-agent-flow/cmd/flow@latest
flow run examples/echo_chain/flow.json --input in=hello
# {"out": "OLLEH"}

flow run examples/echo_chain/flow.json --input in=hello --stream
# 每个 FlowEvent 一行 JSON
```

HTTP 服务（长期运行，带持久化）：

```bash
go install github.com/costa92/llm-agent-flow/cmd/flowd@latest

# 内存（临时）模式 —— 启动最快，适合演示：
flowd --addr :7861 --flow examples/echo_chain/flow.json

# 或使用磁盘上的 SQLite，让运行历史在重启后存活：
flowd --addr :7861 --db /var/lib/flowd/flow.db
```

对 `/flows` 进行 CRUD：

```bash
# 创建一个流程。
curl -X POST http://localhost:7861/flows \
     -H 'Content-Type: application/json' \
     --data-binary "{\"flow\":$(cat examples/echo_chain/flow.json)}"
# 201，返回 FlowRecord

# 列出流程。
curl http://localhost:7861/flows
# {"flows":[{"id":"echo_chain", ...}]}

# 运行 + 从 X-Run-ID 响应头捕获 run id。
curl -i -X POST http://localhost:7861/flows/echo_chain/run \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# HTTP/1.1 200 OK
# X-Run-ID: 4351cce92d54ba5d
# {"outputs":{"out":"OLLEH"},"run_id":"4351cce92d54ba5d"}

# 列出某个流程的运行历史。
curl http://localhost:7861/flows/echo_chain/runs
# {"runs":[{"id":"...","status":"done", ...}]}

# 获取单次运行，含完整的 inputs/outputs/error。
curl http://localhost:7861/runs/4351cce92d54ba5d
# {"id":"4351cce92d54ba5d","status":"done","inputs":{"in":"hello"},
#  "outputs":{"out":"OLLEH"},...}

# 获取某次运行的逐事件审计日志。
curl http://localhost:7861/runs/4351cce92d54ba5d/events
# {"events":[
#   {"seq":1,"kind":"flow_started",  "payload":{"flow":"echo_chain"}, "ts":"..."},
#   {"seq":2,"kind":"node_started",  "node_id":"upper",   ...},
#   {"seq":3,"kind":"node_finished", "node_id":"upper",   "payload":{"output":{...}}, ...},
#   ...
#   {"seq":6,"kind":"flow_done",     "payload":{"outputs":{"out":"OLLEH"}}, "ts":"..."}
# ]}

# 以 SSE 流式运行。
curl -X POST http://localhost:7861/flows/echo_chain/run/stream \
     -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hello"}}'
# event: flow_started \n data: {...} \n\n ...（最终的 flow_done 已持久化）

# 提供 --flow 时旧版客户端仍可用：
curl http://localhost:7861/healthz                  # ok
curl -X POST http://localhost:7861/run -H 'Content-Type: application/json' \
     -d '{"inputs":{"in":"hi"}}'                    # 200 OK
```

或者从本仓库运行：

```bash
go run ./cmd/flow  run examples/echo_chain/flow.json --input in=hello
go run ./cmd/flowd --flow examples/echo_chain/flow.json
```

使用自定义工具清单（任意流程 + 任意工具，无需改代码）：

```bash
# upper.json：HTTP 支撑的工具
cat > /tmp/tools.json <<'EOF'
{"tools":[
  {"name":"upper",  "kind":"http","url":"http://localhost:8080/upper"},
  {"name":"reverse","kind":"http","url":"http://localhost:8080/reverse"}
]}
EOF
flow run examples/echo_chain/flow.json --tools /tmp/tools.json --input in=hello

# 或使用运行在宿主机上的 exec 支撑工具：
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

完整的端到端、经测试的范例参见 `examples/http_tool/`。

条件路由（CEL 边）：

```bash
flow run examples/router/flow.json --input in="hello world"
# {"greeting":"Hello! Nice to see you."}

flow run examples/router/flow.json --input in="what time is it"
# {"other":"Sorry — I do not know how to handle that yet."}
```

router 流程从 `classify` 引出两条出边，CEL 守卫分别为
`value == "greet"` 和 `value != "greet"`。仅匹配的分支触发；
另一个被跳过（在 `--stream` 模式下会发出 `node_skipped` 事件）。

## JSON 流程形状（v0）

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

每个 `nodes[].type` 通过 `NodeRegistry` 解析。随附的 `tool` 类型
会在调用方于引擎构造时提供的工具注册表中按名称查找一个
`agents.Tool`。

## PR 自动化

本仓库现期望由 `.github/workflows/pr-governance.yml` 强制执行一套
简单策略：

- 由 `costa92` 创建的 PR 应自动通过治理，并在必需检查通过后启用自动合并。
- 同仓库的 owner 分支应在 PR 确认合并后由该工作流显式删除。
- 由其他任何人创建的 PR 应请求 `costa92` 评审，并保持阻塞，
  直到 `costa92` 批准当前 PR 头部。

该策略设计为与要求 `go` 和 `governance` 状态检查的分支保护配合使用，
而非 GitHub 内置的必需批准门禁。

仓库级的 `deleteBranchOnMerge` 设置仍保持启用作为安全网，但当前
主要的、经测试的路径已在 `pr-governance.yml` 内部：启用自动合并、
等待 PR 可见地合并，然后用 GitHub API 删除同仓库头部 ref。独立的
下游清理工作流在推广期间经过测试，现已不再是文档记录的主要机制。

完整的多仓治理设计，包括 `llm-agent`、`llm-agent-rag`、
`llm-agent-flow`、`llm-agent-providers`、`llm-agent-otel` 与
`llm-agent-customer-support` 之间的关系，位于核心仓库的文档中：

- [`PR-GOVERNANCE-OVERVIEW.md`](https://github.com/costa92/llm-agent/blob/main/docs/PR-GOVERNANCE-OVERVIEW.zh-CN.md)
- [`PR-GOVERNANCE-PROJECTS.md`](https://github.com/costa92/llm-agent/blob/main/docs/PR-GOVERNANCE-PROJECTS.zh-CN.md)
- [`PR-GOVERNANCE-RULES.md`](https://github.com/costa92/llm-agent/blob/main/docs/PR-GOVERNANCE-RULES.zh-CN.md)
- [`PR-GOVERNANCE-OPERATIONS.md`](https://github.com/costa92/llm-agent/blob/main/docs/PR-GOVERNANCE-OPERATIONS.zh-CN.md)

## 许可证

MIT —— 参见 [LICENSE](LICENSE)。
