# Changelog

`github.com/costa92/llm-agent-flow` 的所有重要变更都记录于此。

<!-- Keep a Changelog: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ — additive-only stability from v0.1.0. -->

## [v0.1.4] - 2026-05-23

阶段 —— D3（收尾 P1-18 延后工作）：生产环境的 `toolNode`
现在会上浮工具侧的元数据。

### Added（仅增量 —— 遵守 v0.1 承诺）

- **`flow.MetadataAwareTool`** —— `flow.Tool` 的可选兄弟能力。
  实现它的工具可以在字符串输出之外发布键 / 值元数据
  （HTTP 状态、exec 退出码、响应大小、请求耗时、token 用量）。
  `toolNode` 在运行时对其进行类型断言；普通的 `flow.Tool`
  实现继续无变化地运行并产生 nil 元数据。
- **`toolNode` 现在实现了 `MetadataAware`。** 其 `RunWithMetadata`
  会把它包装的任何 `MetadataAwareTool` 的元数据转发到引擎的
  `FlowEvent.Metadata` 通道 —— 包括在错误路径上（D1 契约：
  失败时保留元数据）。
- **内置工具实现 `MetadataAwareTool`：**
  - `http` kind 发出 `http_status`、`bytes`、`duration_ms` ——
    包括在 5xx 错误路径上。
  - `exec` kind 在干净退出时发出 `exit_code`、`duration_ms`；
    在 context 截止取消时发出 `signal: "timeout"` + `duration_ms`。

### Tests

- `flow/tool_node_test.go`（3 个用例）—— toolNode 从感知元数据的
  工具传播元数据、对普通工具返回 nil、在错误路径上保留元数据。
- `flow/engine_metadata_test.go`（+1 个用例）—— 引擎通过生产环境
  的 "tool" 节点类型端到端上浮工具元数据。
- `flow/tools/http_test.go`（+2 个用例）—— httpTool 在 2xx 和 5xx
  （D1）上发出 status / bytes / duration 元数据。
- `flow/tools/exec_test.go`（+3 个用例）—— execTool 在成功与失败时
  发出 exit_code；在截止时发出 signal=timeout。
- `cmd/flowd/server/server_metadata_test.go`（+1 个用例）—— 重放
  使用生产环境的 `toolNode` + `httpTool` 组合，将 http_status
  经引擎 → 存储 → 重放往返。

### Notes

- 无上游变更 —— `agents.Tool`（位于 `llm-agent`）未被触碰。
  `flow.Tool` 保持流程本地，如 `flow/node.go:66-69` 中所记录。
- `agentToolAdapter`（位于 `flow/adapter_llmagent.go`）有意保持
  对元数据无感知。不在 v0.1.x 范围内。

## [v0.1.3] - 2026-05-22

阶段 —— P1-18：`FlowEvent.Metadata` + `MetadataAware` 可选能力
（伞形路线图 O4）。

### Added（仅增量 —— 遵守 v0.1 承诺）

- **`FlowEvent.Metadata map[string]string`** —— `flow.FlowEvent`
  上新增的可选字段。在实现新 `MetadataAware` 能力的节点发出的
  `NodeFinished` 事件上被填充；其他每个事件上为 `nil`。意在用于
  逐节点的旁路信号，如 HTTP 状态、exec 退出码或 LLM token 用量。
  错误路径的元数据被保留，因此失败的运行仍能上浮调试信号
  （例如 `http_status=500`）。
- **`flow.MetadataAware` 接口** —— `NodeKind` 的可选兄弟。
  实现添加一个 `RunWithMetadata(ctx, in) (out, metadata, err)`
  方法；引擎在每次调用时通过类型断言检测该能力。既有的
  `NodeKind` 实现保持不变并继续无修改地运行。`MetadataAware`
  将在整个 v0.1.x 带保持可选 —— 在 v0.2 之前不会被提升进必需的
  `NodeKind` 形状。

### Changed（内部 —— 无 API 移除）

- **引擎 `RunStream` / `Run`** 现在会在调用 `NodeKind.Run` 之前
  对每个节点进行 `MetadataAware` 类型断言。具备能力的节点返回的
  元数据会被克隆（镜像既有的 `Output` 克隆）并放在发出的
  `NodeFinished` 事件上。
- **`cmd/flowd` SSE / 重放载荷** 在 `FlowEvent.Metadata` 非空的
  事件上新增一个 `"metadata"` 键。对于 nil 和空 map，该键被完全
  省略，因此既有的 SSE / 重放消费者对旧版流程看到字节级一致的载荷。

### Tests

- `flow/event_metadata_test.go`（1 个用例）—— `FlowEvent.Metadata`
  字段形状。
- `flow/engine_metadata_test.go`（3 个用例）—— 引擎为
  `MetadataAware` 节点传播元数据；对普通 `NodeKind` 节点保留
  `Metadata=nil`；在错误路径上保留元数据。
- `cmd/flowd/server/server_metadata_test.go`（4 个用例）——
  `streamPayload` 在填充时包含元数据，在 nil 和空 map 上省略它，
  并且运行历史重放会把元数据 JSON 经 SQLite 持久化往返。

### Snapshot baseline

`api/v0.1.snapshot.txt` 针对增量形状重新生成
（`FlowEvent` 上的 `field Metadata map[string]string` +
`type MetadataAware interface`）。

### Follow-ups（延后 —— 保持范围紧凑）

- 在随附的 `toolNode` 上提供一个示例 `MetadataAware` 实现
  （例如上浮 HTTP 状态 / exec 退出码）被延后到它自己的 PR（D3），
  以使本 PR 保持仅增量且仅标准库。
- 伞形仓的 `docs/source-design-llm-agent-flow.zh-CN.md` 关于 O4 的
  状态表更新位于伞形仓库，被延后到后续的跨仓 PR（D5）。

## [v0.1.2] - 2026-05-22

阶段 —— P1-17：SQLite 写吞吐加固（伞形路线图）。

### Changed（内部 —— 无 API 变更）

- **`(*sqlite.Store)` 现在在每个磁盘 DSN 上启用
  `PRAGMA journal_mode=WAL` 和 `synchronous=NORMAL`**
  （`:memory:` 和 `mode=memory` 的 URI 变体会被检测并保持默认）。
  WAL 提供并发读者，NORMAL 以微小的崩溃窗口换取大幅的 fsync 减少。
  `Open` 期间的 PRAGMA 失败会以错误上浮，使配置错误的环境快速失败。

### Operational note

磁盘上的 SQLite 数据库现在会产生两个 sidecar 文件：

- `<db>-wal` —— 预写日志（write-ahead log）
- `<db>-shm` —— 共享内存索引

两者都必须纳入备份 / 快照 / 卷挂载策略，否则数据库可能处于
不一致状态。当使用非内存 DSN 时，`cmd/flowd` 在启动时会记录
一行提醒。

### Performance（测量于 2026-05-22，5 iter × 3 count，中位数）

| Workload                         | Before WAL (v0.1.1) | After WAL (v0.1.2) | Speedup |
|----------------------------------|---------------------|--------------------|---------|
| `AppendRunEvents` batch-of-600   | ~26 ms/op           | ~1.5 ms/op         | **~17×** |
| `AppendRunEvent` × 600 single    | ~14,800 ms/op       | ~42 ms/op          | **~350×** |

伞形路线图 P1-17 的目标是 5–16×；仅 WAL 就超过了它。后续的
multi-VALUES `INSERT` 变更被界定并延后 —— 关于 YAGNI 理由参见
PR 描述。

### Tests

- `flow/store/sqlite/wal_test.go`（3 个用例）—— 磁盘启用 WAL +
  NORMAL；`:memory:` 不启用；PRAGMA 失败以 `Open` 错误上浮。
- `flow/store/sqlite/events_batch_test.go`（+2 个用例）—— 大批量
  单语句契约基线、chunk 边界正确性。
- `flow/store/sqlite/events_bench_test.go` —— 上述基准测试。

## [v0.1.1] - 2026-05-21

Phase 11 —— 性能：引擎缓存 LRU + 同步运行事件批处理。

### Added（仅增量 —— 遵守 v0.1 承诺）

- **`server.Config.EngineCacheSize`** —— 通过 LRU 驱逐限制已编译
  引擎缓存。默认 `0` 禁用限界（保留 v0.1.0 行为）。PUT/DELETE
  处理器仍会立即驱逐。
- **`flow/store.RunEventBatchItem`** —— 具备批量插入能力的存储的
  输入形状。`Store` 接口本身在 v0.1.x 中保持不变；批量插入通过
  类型断言（`store.(interface { AppendRunEvents(...) error })`）
  作为可选能力暴露。
- **`(*sqlite.Store).AppendRunEvents(ctx, runID, items)`** ——
  单事务多 INSERT。`seq` 从该运行当前的最大值继续单调递增；整批
  共享一个 `ts`。

### Changed（内部 —— 无 API 变更）

- **同步 `/flows/{id}/run` 运行现在会批量持久化其事件。** 引擎
  循环期间收集的事件在运行结束时以一个事务刷新。流式运行
  （`/run/stream`）不变 —— 它们仍逐事件持久化后再转发，以保留
  v0.0.6 的“事件比断开的客户端存活更久”保证。
- **引擎缓存现在在 `EngineCacheSize > 0` 时受 LRU 限界。** 内部从
  `sync.Map` 切换为基于小型 `container/list` 的缓存；在默认
  （cap=0）下的行为与 v0.1.0 字节级一致。

### Tests

- `cmd/flowd/server/lru_test.go`（5 个用例）—— cap=0 不限界、
  LRU 驱逐顺序、覆写、删除幂等、满容量时删除。
- `flow/store/sqlite/events_batch_test.go`（4 个用例）—— 正常路径、
  空批量空操作、未知运行 → ErrNotFound、在单事件 Append 之后 seq
  继续。

### Snapshot baseline

`api/v0.1.snapshot.txt` 针对增量类型
（`flow/store.RunEventBatchItem`）重新生成。

## [v0.1.0] - 2026-05-21

**Phase 10 —— SemVer 冻结。** v0.1.x 导出的 API 现在保证仅增量。
v0.0.x 是探索带；v0.1.0 冻结接口面，并铺设保持其冻结的 diff 机制。

### Added

- `docs/compatibility.md` —— 书面的稳定性承诺。在 v0.1.x 内：
  不移除、不重命名、不重新签名任何导出符号；流程 JSON IR 仅增量；
  破坏性变更进入 `/v2`。
- `internal/apisnapshot/` —— 导出 API 的快照门禁。纯标准库
  （`go/parser` + `go/printer`）；在每次 `go test` 时运行；对
  `api/v0.1.snapshot.txt` 的任何漂移都会失败。有意的增量变更用
  `-update` 重新生成基线。
- `api/v0.1.snapshot.txt` —— 提交的基线。

### Changed

- 状态：v0.0.x 走通骨架带 → v0.1.x 稳定。

本 tag 无新产品特性；冻结本身就是交付物。所有 v0.0.9 端点 / 库
函数都被逐字保留。

## [v0.0.9] - 2026-05-21

Phase 9 —— 重放端点。

### Added

- `POST /runs/{id}/replay` —— 把某次运行的持久化事件作为一个新的
  SSE 会话重新流式推送。没有新的引擎运行；事件直接来自
  `run_events`。`X-Replay: true` + `X-Run-ID` 标识此次重放。
  存储的载荷 JSON 被逐字转发，因此客户端解码重放帧的方式与实时
  帧完全相同。

未知 run id → 404。错误方法 → 405。空事件日志 → 200，零个 SSE 帧
（幂等）。

4 个新测试用例。

## [v0.0.8] - 2026-05-21

Phase 8 —— Bearer-token 鉴权 + 可插拔的 Authenticator。

### Added

- `server.Authenticator` 接口 —— 可插拔的扩展点。返回
  `server.ErrUnauthorized` → 401 + `WWW-Authenticate` Bearer 质询；
  任何其他错误 → 403。
- `server.BearerTokenAuthenticator{Token: ...}` —— 使用常量时间
  比较的静态 token 实现。
- `/healthz` 旁路 —— 始终允许，使外部监控无需 token 即可工作。
- `cmd/flowd --token <secret>`（或 `FLOWD_TOKEN` 环境变量）——
  设置后，除 `/healthz` 外的每个端点都要求
  `Authorization: Bearer <secret>`。

8 个新测试用例；无公共 API 移除。

## [v0.0.7] - 2026-05-21

Phase 7 准备 —— 引入 `llm-agent-otel/otelflow` 插入的接缝。

### Added

- **`flow.Runner` 接口。** 恰好是 `Run(ctx, inputs)` +
  `RunStream(ctx, inputs)`。`*flow.Engine` 满足它；该包携带一个
  编译期断言，使包装器包无需显式类型断言即可依赖它。
- **`(*Engine).FlowID() string`** 与 **`FlowName() string`** ——
  只读的 getter，在包装器中作为 span 属性 / 日志字段很有用。
  底层字段保持未导出。

纯增量；未触碰任何既有 API。无新依赖。

## [v0.0.6] - 2026-05-21

Phase 6 —— 逐事件的运行历史持久化。

v0.0.5 只为每次运行存储 start/finish。本次发布捕获每个 FlowEvent
—— flow_started、node_started、node_finished、node_skipped、
flow_done、flow_err —— 带时间戳和载荷，使运行历史反映引擎内部
实际发生的事，而不只是最终结果。

### Added

- **`flow/store.RunEvent` 类型** + `RunEventKind` 枚举
  （`flow_started`、`node_started`、`node_finished`、`node_skipped`、
  `flow_done`、`flow_err`）。字符串值与 SSE 事件名匹配，因此重放
  客户端可以复用既有的解码器。
- **Store 接口扩展：** `AppendRunEvent` + `ListRunEvents`。Append
  在已完成的运行和未知运行上调用都是安全的（后者返回
  `ErrNotFound`）。
- **SQLite 实现：** 新的 `run_events` 表，带 `(run_id, seq)` 索引。
  `seq` 在服务端按每次运行 `max(existing) + 1` 分配，实现单语句、
  无竞态的单调排序。Schema 是幂等的 —— 既有的 v0.0.5 数据库在下次
  `Open` 时拾起新表。
- **flowd 将同步与流式路径通过 `engine.RunStream` 统一。** 无论
  客户端想要 JSON 响应还是 SSE，每个事件都在转发给客户端**之前**
  持久化到存储。一个在流中途断开的消费者仍会留下完整的审计轨迹。
- **`GET /runs/{id}/events`** —— 某次运行的有序事件日志，含完整
  载荷。未知 id 返回空列表（对重放型客户端幂等）。

### Tests

- `flow/store/sqlite/events_test.go`（7 个用例）：带载荷 JSON 解码
  的 append + list 往返、空情形、未知运行返回 ErrNotFound、
  nil-payload 存为空、limit 被遵守、事件在流程删除后存活、空 run_id
  被拒绝。
- `cmd/flowd/server/server_events_test.go`（6 个用例）：同步运行
  持久化每个事件、流式运行持久化并流式推送、GET /runs/{id}/events
  端点、失败运行包含带错误载荷的 flow_err、router 包含
  node_skipped、缺失运行的 GET 返回空列表。

13 个新测试用例；总计 93 个测试 —— 全绿。

实测冒烟确认：一次同步 echo_chain 运行在存储中产生 6 个有序事件
（`flow_started` → `node_started/finished × 2` → `flow_done`），
带逐节点的输入/输出载荷和微秒级精度的时间戳。

无公共 API 移除。`Store` 接口新增两个方法；任何 v0.0.5 实现都必须
添加它们 —— 随附的 SQLite 存储会自动处理。

## [v0.0.5] - 2026-05-21

Phase 5 —— 运行历史（SQLite）+ 流程 CRUD 端点。v0.0.x 带中最具
架构性的阶段：flowd 从单流程服务器成长为完全由存储支撑的 REST 服务。

### Added

- **`flow/store` 包。** 用于流程 CRUD + 运行生命周期的可插拔
  `Store` 接口。类型：`FlowMeta`、`FlowRecord`、`RunMeta`、
  `RunRecord`、`RunStatus`。哨兵错误 `ErrNotFound`、
  `ErrAlreadyExists`。
- **`flow/store/sqlite` 子包。** 使用 `modernc.org/sqlite` 的纯 Go
  SQLite 支撑 Store（无需 CGO）。Schema 在打开时迁移；支持
  `":memory:"` 用于测试。核心 `flow` 库在源码层面保持仅标准库 ——
  依赖仅在导入此子包时进入。
- **flowd HTTP REST 接口面。**
  - `POST /flows`、`GET /flows`、`GET /flows/{id}`、`PUT /flows/{id}`、
    `DELETE /flows/{id}` —— 在创建/更新时带编译探针校验的流程 CRUD
    （坏的流程 JSON 以 400 快速失败，而非在运行时才上浮）。
  - `POST /flows/{id}/run`（同步）—— 返回 `X-Run-ID` 响应头和 body
    中的 run_id。结果持久化为 `done` 或 `failed`。
  - `POST /flows/{id}/run/stream`（SSE）—— 同样的 `X-Run-ID`；最终
    结果在流关闭时捕获。
  - `GET /flows/{id}/runs` —— 某个流程的运行历史（按
    `started_at` 降序排列）。
  - `GET /runs/{id}` —— 单次运行记录，含完整的 inputs / outputs /
    error。
- **引擎缓存。** `server.Server` 持有一个以流程 id 为键的已编译
  引擎 `sync.Map`；PUT 和 DELETE 驱逐缓存。
- **`cmd/flowd --db <path>` 标志。** 默认 `:memory:`，使二进制仍
  能开箱启动。磁盘 DSN 使运行历史跨重启持久化。
- **`cmd/flowd --flow` 变为可选。** 提供时，文件在启动时被种入
  存储，并启用面向 v0.0.4 向后兼容的旧版 `/run` / `/run/stream`
  别名（路由到该流程的 id）。
- **`server.New(cfg) (*Server, error)`** —— v0.0.5 接口面的新构造
  函数。`server.NewMux(engine, logger)` 被保留用于单引擎、无持久化
  的模式。

### Tests

- `flow/store/sqlite/store_test.go`（11 个用例）：流程 CRUD 往返、
  重复创建拒绝、PUT 替换语义、ListFlows 排序、运行生命周期、失败
  运行路径、ListRuns 排序、ErrNotFound 上浮、流程删除保留历史运行。
- `cmd/flowd/server/server_crud_test.go`（15 个用例）：创建正常 /
  冲突 / 坏编译、列出、获取、获取缺失、带缓存失效的 PUT、删除、
  run 记录带 X-Run-ID 的运行、列出运行、获取运行记录、失败运行被
  持久化为 `failed`、旧版 `/run` 路由、无旧版路由返回 404。

### Live smoke confirmed end-to-end:
- POST 一个流程 → 列出 → 运行两次 → 列出运行 → 获取一条运行记录
  → DELETE → 后续运行返回 404。

### Dependencies

- 在 module 层面添加 `modernc.org/sqlite v1.50.1`（纯 Go）。
  `flow` 包及其既有子包都不导入它。

相较 v0.0.4 无公共 API 移除。`Edge.Condition` 和 `Engine` 契约不变。
HTTP 服务新增了端点；当 `--flow` 在启动时种入一个流程时，旧的
`/run`、`/run/stream`、`/healthz` 继续可用。

## [v0.0.4] - 2026-05-21

Phase 4 —— CEL 条件边 + 节点激活。

### Added

- **`Edge.Condition` IR 字段。** 在边触发时求值的可选 CEL 表达式。
  空（默认）保留 v0.0.3 的 DAG 语义 —— 每条边总是触发。
- **`flow.ConditionEvaluator` + `flow.Condition` 接口。** 可插拔的
  守卫表达式引擎。编译错误在 Engine.Compile 时上浮，因此流程加载
  快速失败。
- **`flow.WithConditionEvaluator(e)` 引擎选项。** 当任何边有非空
  Condition 时必需。没有它，Compile 拒绝。
- **`flow/cond/cel` 子包。** CEL 支撑的求值器
  （`google/cel-go`）。核心 flow 库保持仅标准库；应用通过显式
  导入此子包来选择开启。
- **节点激活语义。** 一个节点在它没有入边、或被 Flow.Inputs 命名、
  或至少有一条入边触发时为激活。未激活的节点不运行，也不发出出边
  触发。
- **`NodeSkipped` FlowEvent kind。** 每个被跳过的节点发出一次，
  使追踪层能区分“被跳过”与“仍待处理”。
- **来自被跳过节点的 Flow.Outputs 被静默省略** 于返回的输出映射 ——
  路由型流程可以声明每个分支的输出，而只有触发的分支贡献。
- **`cmd/flow` 和 `cmd/flowd` 自动接入 `flow/cond/cel`**，使用户
  流程中的所有条件无需额外标志即可工作。两者默认的回退工具目录
  现在都包含 router 演示，因此 `flow run examples/router/flow.json`
  开箱即可运行。
- **`examples/router/`。** 两分支 CEL 路由流程 + 覆盖两个分支的
  集成测试。

### Tests

- `flow/engine_cond_test.go`（7 个用例）：router 两条路径、跳过
  传播、需要求值器的错误、编译期语法错误、运行时求值错误、
  向后兼容回归。
- `flow/cond/cel/cel_test.go`（9 个用例）：等值、`startsWith`、
  `matches` 正则、`size` + 布尔逻辑、语法错误、拒绝非布尔返回、
  拒绝未知变量、引擎集成。
- `examples/router/example_test.go`（2 个用例）：greet/other 分支。

### Dependencies

- 在 MODULE 层面添加 `github.com/google/cel-go v0.28.1`（+ 约 6 个
  传递依赖：antlr、protobuf 等）。`flow` 包本身不导入它们；
  tree-shaking 对下游用户适用。

相较 v0.0.3 无公共 API 移除。Edge 的 JSON 形状严格增量 ——
v0.0.4 之前的流程文件无变化地加载并运行。

## [v0.0.3] - 2026-05-21

Phase 3 —— 工具清单。解除实际使用的阻塞：任何流程都能针对任何
工具运行，而无需 fork 二进制。

### Added

- **`flow/tools` 包。** 按 `name` 和 `kind` 列出每个工具的 JSON
  清单格式，加上 `LoadManifest` / `LoadAndBuild`。该清单将流程的
  IR（按名称引用工具）与底层工具实现解耦。
- **内置 `http` kind。** 把 JSON 参数 POST 到一个 URL；期望
  `{"output":"..."}` 或回退到原始 body。Headers + 超时可逐条配置。
- **内置 `exec` kind。** 派生一个命令，在 stdin 上写入 JSON 参数，
  捕获 stdout。包含超时与退出码错误处理；stdout / stderr 分别
  上限为 1 MiB / 16 KiB。
- **`tools.KindRegistry`。** 可插拔 —— 下游代码调用
  `RegisterKind(name, factory)` 来添加自定义 kind 而无需 fork。
- **`cmd/flow` 和 `cmd/flowd` 上的 `--tools <manifest.json>` 标志**。
  向后兼容：未设置时注册随附的 `echo_chain` 演示工具。
- **`examples/http_tool/`** —— 流程 + 清单示例 + 通过 httptest
  后端驱动完整流水线的集成测试。

### Tests

- `flow/tools/manifest_test.go` —— 加载 + 构建正常路径、重复
  名称、未知 kind、缺失字段、自定义 kind 注册。
- `flow/tools/http_test.go` —— `{"output"}` JSON 解码、原始 body
  回退、非 2xx → 错误、header 透传。
- `flow/tools/exec_test.go` —— stdout 捕获（通过 `cat` 往返）、
  非零退出错误、超时强制。
- `examples/http_tool/example_test.go` —— 通过 HTTP 支撑的清单
  工具的端到端流程执行。

### Tests run green

- 实测冒烟：`flow run examples/echo_chain/flow.json --tools <manifest> --input in=hello`
  搭配 exec 支撑的 Python 工具返回 `OLLEH`。

## [v0.0.2] - 2026-05-21

Phase 2 —— 并行执行 + HTTP 服务。

### Added

- **逐层并行执行。** 引擎现在通过
  `github.com/costa92/llm-agent/pkg/fanout` 并发调度每个拓扑层的
  节点。在一层内，兄弟节点并行执行；层本身保持顺序。Fail-fast：
  首个节点错误通过 `fanout.WithFailFast` 取消在途的同伴。
- **`WithMaxNodeConcurrency(n)` 引擎选项。** 限制逐层的 goroutine
  数。`n <= 0`（默认）不限；`n == 1` 恢复 v0.0.2 之前的顺序行为。
- **`cmd/flowd` HTTP 服务。** 一次性启动一个已编译流程并暴露：
  - `GET /healthz` —— 存活性，返回 `ok`。
  - `POST /run` —— 同步 JSON；body `{"inputs":{...}}` 返回
    `{"outputs":{...}}`。缺失输入 / 坏 JSON 时 400，引擎错误时 500。
  - `POST /run/stream` —— Server-Sent Events；每个 FlowEvent 一帧
    （`flow_started`、`node_started`、`node_finished`、`flow_done`、
    `flow_err`）。
- **FlowEvent 排序契约已记录。** 单层内的兄弟事件可能交错，但每个
  节点的顺序（`NodeStarted` 在 `NodeFinished` 之前）、FlowStarted
  在先 / FlowDone 在后的不变式以及跨层排序都成立。

### Tests

- 新 `engine_parallel_test.go`：墙钟一致性（兄弟重叠）、
  `WithMaxNodeConcurrency(1)` 强制串行、200 ms 内 fail-fast 同伴
  取消、按节点 ID 键控的流式事件。
- 新 `cmd/flowd/server/server_test.go`：`/healthz`、`/run` 正常
  路径、`/run` 缺失输入 → 400、`/run` 坏 JSON → 400、方法不允许
  → 405、`/run/stream` SSE 端到端。

## [v0.0.1] - 2026-05-21

初始走通骨架 —— 参见 git log / GitHub release notes。
