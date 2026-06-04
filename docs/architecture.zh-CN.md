[English](./architecture.md) | [简体中文](./architecture.zh-CN.md)

# 架构 —— `llm-agent-flow`

本文档描述 `llm-agent-flow` 的各部分如何组合在一起。关于稳定性契约
参见 [`compatibility.md`](compatibility.md)；关于首次使用参见
[`tutorial.md`](tutorial.md)；关于生产部署参见
[`operations.md`](operations.md)。

## 一览

```
                           ┌─────────────────────────────────────┐
                           │           Flow JSON file            │
                           │  { id, nodes, edges, in, out, ... } │
                           └──────────────┬──────────────────────┘
                                          │
                                          ▼
              ┌──────────────────────────────────────────────────┐
              │                  flow.Compile                    │
              │  IR → Validate → NodeRegistry.Build → topo sort  │
              │             → CEL-precompile edges               │
              └──────────────┬───────────────────────────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │ flow.Engine  │── satisfies ──▶ flow.Runner
                      └──────┬───────┘                       ▲
                             │                               │
            ┌────────────────┴──┐                            │
            ▼                   ▼                            │
        Engine.Run         Engine.RunStream                  │
            │                   │                            │
            └─────────┬─────────┘                            │
                      ▼                                      │
            map[string]string outputs                        │
            <-chan flow.FlowEvent                            │
                                                             │
              ┌──────────────────────────────────────────────┘
              │                          decorator pattern (K3)
              ▼
       otelflow.Wrap(Runner) Runner   ← in llm-agent-otel sibling repo
              │
              ▼
        Same Runner contract, OTel spans on every Run / per-node lifecycle
```

库被拆分为清晰的层，每层有自己的依赖姿态：

| Layer | Package | Imports | Purpose |
|---|---|---|---|
| IR | `flow` | stdlib + llm-agent | Flow / Node / Edge / Port / FlowEvent 类型；Engine；Runner 接口 |
| Conditions | `flow/cond/cel` | + `google/cel-go` | CEL 支撑的 ConditionEvaluator |
| Tools | `flow/tools` | stdlib + flow | 清单 + http/exec 内置 kind |
| Persistence | `flow/store` | stdlib + flow | Store 接口（CRUD + 运行生命周期 + 事件） |
| Persistence impl | `flow/store/sqlite` | + `modernc.org/sqlite` | 带批量插入能力的 SQLite 支撑 Store |
| HTTP service | `cmd/flowd/server` | flow + flow/store | 鉴权 + CRUD + 同步/流式/重放端点 |
| Binaries | `cmd/flow`, `cmd/flowd` | all of the above | CLI + 长期运行的 HTTP 服务器 |
| OTel | `llm-agent-otel/otelflow` (sister repo) | flow + go.opentelemetry.io/otel | 包装任意 `flow.Runner` 的装饰器 |

**`flow` 核心包本身在源码层面是仅标准库的**，除了它对
`github.com/costa92/llm-agent` 的反向边。CEL、SQLite 和 OTel 依赖
各自仅在其专用子包被导入时才进入。

## 核心类型 —— `flow` 包

IR 很小且可 JSON 序列化。参见 `flow/ir.go`：

```go
type Flow struct {
    ID          string
    Name        string
    Description string
    Nodes       []Node
    Edges       []Edge
    Inputs      []NamedPortRef   // caller-supplied
    Outputs     []NamedPortRef   // returned to caller
}

type Node struct { ID string; Type string; Config json.RawMessage }
type Edge struct { Source PortRef; Target PortRef; Condition string }
type PortRef struct { Node string; Port string }
type NamedPortRef struct { Name string; PortRef }
```

**Runner 接口**（装饰器所针对的稳定性接缝）恰好是两个方法：

```go
type Runner interface {
    Run(ctx, inputs map[string]string) (map[string]string, error)
    RunStream(ctx, inputs map[string]string) (<-chan FlowEvent, error)
}
```

`*Engine` 满足它；一个编译期断言（`var _ Runner = (*Engine)(nil)`）
使契约保持诚实。

## 执行模型

1. **Compile** 校验 IR，通过 `NodeRegistry` 将每个 Node Type 解析为
   运行时的 `NodeKind`，可选地通过配置的 `ConditionEvaluator`
   预编译每个 `Edge.Condition`，并计算一个 **拓扑层序**。

2. **分层执行** 通过 [`pkg/fanout`](https://github.com/costa92/llm-agent/blob/main/pkg/fanout/fanout.go)
   并行运行每层的激活节点。兄弟错误触发 fail-fast 取消。层本身
   是顺序的 —— 一层的节点只有在上一层所有节点都完成后才启动。

3. **节点激活** 是条件边的微妙之处：一个节点在（没有入边）或
   （在 `Flow.Inputs` 中被命名）或（至少有一条入边触发）时运行。
   一条边在其 `Condition` 求值为 true 时“触发”（空条件总是触发）。
   被跳过的节点发出一个 `NodeSkipped` 事件；它们的出边不触发；
   仅有的输入都被跳过的下游节点也变为被跳过。声明在被跳过节点上的
   输出会从最终输出映射中静默省略。

4. **FlowEvent 流** 是一个遵循更广生态 K1 范式的类型化联合：

   ```
   FlowStarted → (NodeStarted|NodeSkipped|NodeFinished)* → FlowDone|FlowErr
   ```

   一层内的兄弟事件可能交错；每个节点的顺序以及 FlowStarted 在先 /
   FlowDone-或-FlowErr 在后的不变式成立。

## 持久化 —— `flow/store`

两个关注点被打包进一个接口：

- **Flows** —— CRUD；后写胜出；与 `id / name / created_at /
  updated_at` 一起作为原始 JSON 字节存储。
- **Runs** —— 生命周期（StartRun → FinishRun）+ 逐事件历史
  （AppendRunEvent / ListRunEvents）。

随附的 `flow/store/sqlite` 实现通过
[`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) 是纯 Go 的
—— 无 CGO。Schema 在 `Open` 时幂等。运行历史在流程删除时**不**级联；
历史审计存活。

在 `Store` 接口之外暴露了一个 **批量插入** 能力 ——
`(*sqlite.Store).AppendRunEvents(ctx, runID, items)`。服务器可以
类型断言以检测是否支持：

```go
if batcher, ok := store.(interface {
    AppendRunEvents(ctx context.Context, runID string,
                    items []flowstore.RunEventBatchItem) error
}); ok {
    _ = batcher.AppendRunEvents(...)
}
```

批量方法 *不* 在 `Store` 接口上，以遵守 v0.1 冻结 —— 下游自定义
`Store` 实现无需添加该方法即可保持源码兼容。

## HTTP 服务 —— `cmd/flowd`

`flowd` 是 `cmd/flow` 的长期运行变体。它绑定：

```
GET    /healthz
POST   /flows                 GET /flows               GET    /flows/{id}
PUT    /flows/{id}            DELETE /flows/{id}
POST   /flows/{id}/run        POST /flows/{id}/run/stream
GET    /flows/{id}/runs
GET    /runs/{id}             GET /runs/{id}/events    POST   /runs/{id}/replay
```

此外，当 `--flow <file>` 启动一个种子流程时，提供 v0.0.4 兼容别名：

```
POST /run    POST /run/stream
```

鉴权默认 **关闭**。设置 `--token <secret>`（或 `FLOWD_TOKEN`）会在
除 `/healthz` 外的每个端点之前接入 `server.BearerTokenAuthenticator`。
`server.Authenticator` 接口让调用方无需 fork 包即可插入
JWT / OAuth / mTLS 等。

引擎编译以流程 id 为键 **被缓存**。当 `Config.EngineCacheSize > 0`
时缓存受 LRU 限界；PUT / DELETE 显式驱逐。编译错误在
`POST /flows` / `PUT /flows/{id}` 时以 400 上浮，而非在下次运行时。

## 遥测 —— `otelflow`（兄弟仓）

按更广生态的 Keystone K3，OTel **从不钩入**引擎 —— 它通过
`flow/cond/cel` 风格的装饰器组合：

```go
import (
    "github.com/costa92/llm-agent-flow/flow"
    "github.com/costa92/llm-agent-otel/otelflow"
)

eng, _ := flow.LoadCompile(r, reg, deps, flow.WithConditionEvaluator(...))
runner := otelflow.Wrap(eng, otelflow.Config{TracerProvider: tp})
out, _ := runner.Run(ctx, inputs) // emits flow.run + per-node spans
```

Span 布局：

- 每个 `Run` 一个根 `flow.run <id>`；或每个 `RunStream` 一个
  `flow.run.stream <id>`。
- 每对 `NodeStarted`/`NodeFinished` 一个子 `flow.node <id>`。
- 每个 `NodeSkipped` 事件对应一个零时长的 `flow.node <id>`，带
  `flow.node.skipped=true`。

包装器可组合 —— `otelflow.Wrap(otelflow.Wrap(...))` 是合法的，
ctx 正常传播，无意外。

## 为什么要快照门禁

`internal/apisnapshot` 是一个纯标准库的生成器，它遍历 module 自身的
源码，渲染出每个导出声明的排序文本快照，并对 `api/v0.1.snapshot.txt`
处提交的基线做 diff。它作为 `go test ./...` 的一部分运行。

该门禁使 v0.1 承诺 **可执行** —— 一次丢掉方法、重命名参数或重新
签名接口的重构会在评审前让 CI 失败。关于规则与重新生成流程参见
[compatibility.md](compatibility.md)。

## 各部分位置

```
llm-agent-flow/
├── api/                          ← committed v0.1 baseline
│   └── v0.1.snapshot.txt
├── cmd/
│   ├── flow/                     ← CLI: `flow run <file.json>`
│   └── flowd/                    ← HTTP service
│       └── server/               ← split for httptest reuse
├── docs/
│   ├── architecture.md           ← this file
│   ├── compatibility.md          ← v0.1 stability promise
│   ├── operations.md             ← flowd deployment
│   └── tutorial.md               ← first-time user
├── examples/
│   ├── echo_chain/               ← canonical linear demo
│   ├── http_tool/                ← --tools http kind demo
│   └── router/                   ← CEL conditional routing demo
├── flow/                         ← library core
│   ├── cond/cel/                 ← CEL evaluator (pulls cel-go)
│   ├── store/                    ← Store interface
│   │   └── sqlite/               ← SQLite impl (pulls modernc-sqlite)
│   └── tools/                    ← Tool manifest (http + exec)
└── internal/
    └── apisnapshot/              ← v0.1 gate (pure stdlib)
```
