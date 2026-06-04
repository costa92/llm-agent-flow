[English](./operations.md) | [简体中文](./operations.zh-CN.md)

# 运维 —— 部署 `cmd/flowd`

如何在生产中运行 `flowd`。关于首次使用参见
[`tutorial.md`](tutorial.zh-CN.md)；关于 v0.1.x 冻结覆盖什么参见
[`compatibility.md`](compatibility.zh-CN.md)。

## 标志 + 环境变量

```
flowd \
  --addr               :7861                    HTTP listen address.
  --db                 /var/lib/flowd/flow.db   SQLite DSN (":memory:" for ephemeral).
  --flow               <file.json>              Optional boot-time seed + legacy /run aliases.
  --tools              <manifest.json>          Tool catalog (http/exec kinds).
  --token              <secret>                 Or FLOWD_TOKEN env var.
  --read-timeout       5s                       HTTP server read timeout.
  --write-timeout      0                        HTTP write timeout (0 disables — required for SSE).
  --max-node-concurrency 0                      Per-layer goroutine cap (0 = unlimited).
```

鉴权、数据库路径和 `--write-timeout 0` 是真实部署最关心的三项设置。

## 持久化布局

`--db` 接受 `modernc.org/sqlite` 能理解的任何 DSN。实用值：

| DSN | Use case |
|---|---|
| `:memory:` | 演示、测试、完全临时的运行（历史随进程消亡） |
| `/var/lib/flowd/flow.db` | 单进程磁盘上；在重启后存活 |
| `file:/var/lib/flowd/flow.db?cache=shared&_pragma=journal_mode(WAL)` | 用于并发读者的 WAL 模式 |

Schema 在 `Open` 时创建且幂等。备份就是对文件的普通
`sqlite3 .backup` 调用。

**重要：** SQLite 在设计上是单写者。`flowd` 不跨进程分片 ——
每个数据库文件运行一个实例。对于多副本 HA，在存储层共享 DB
（NFS / EBS）并以 `flock` / 租约把关，或者构建一个由 Postgres 支撑的
自定义 `flow/store.Store` 实现，通过 `server.Config.Store` 喂给它。

### 备份 + 恢复

```bash
# Live backup (safe while flowd is running, WAL mode).
sqlite3 /var/lib/flowd/flow.db ".backup /backup/flow-$(date +%F).db"

# Restore: stop flowd, copy file in place, start.
systemctl stop flowd
cp /backup/flow-2026-05-21.db /var/lib/flowd/flow.db
systemctl start flowd
```

`run_events` 随工作负载线性增长。对于繁忙的服务，考虑每夜剪枝：

```sql
DELETE FROM run_events
 WHERE run_id IN (
   SELECT id FROM runs WHERE started_at < ?  -- old runs cutoff
 );
DELETE FROM runs WHERE started_at < ?;
VACUUM;
```

v0.1.x 中没有内置的保留标志 —— 这是一个明确的非目标；运维团队有
站点特定的策略。Schema 记录在
[`flow/store/sqlite/open.go`](../flow/store/sqlite/open.go)。

## 鉴权

默认禁用。用 `--token`（或 `FLOWD_TOKEN`）启用：

```bash
flowd --addr :7861 --db /var/lib/flowd/flow.db \
      --token "$(cat /etc/flowd/token)"
```

设置后，除 `/healthz` 外的每个端点都要求：

```
Authorization: Bearer <secret>
```

缺失 header → 401 + `WWW-Authenticate: Bearer realm="flowd"`。
错误 token → 403。坏的 scheme → 401。

常量时间比较；`server.BearerTokenAuthenticator` 中随附的实现适用于
静态密钥的场景。对于更丰富的鉴权（JWT、mTLS、OAuth introspection、
逐用户审计），接入一个自定义 `server.Authenticator`：

```go
type myAuth struct { /* ... */ }
func (a *myAuth) Authenticate(r *http.Request) error {
    // ... return server.ErrUnauthorized for 401, any other err for 403
    return nil
}

srv, _ := server.New(server.Config{
    // ... usual fields ...
    Authenticator: &myAuth{},
})
```

`/healthz` 旁路是硬编码的 —— k8s 存活探针和负载均衡器不需要凭证。

**在 v0.1 没有限流也没有审计日志。** 两者在此带都是明确的非目标；
通过上游 API 网关（nginx、envoy、kong 等）来分层实现它们。

## OpenTelemetry

`cmd/flowd` 本身不配置 OTel 流水线。要获得追踪，运行时需在 SDK
层面配置一个 TracerProvider：

```go
// In a custom main wired around server.New(cfg):
import (
    "go.opentelemetry.io/otel"
    "github.com/costa92/llm-agent-otel" // root pkg sets up SDK exporters
)
// ... wire TracerProvider via otel.SetTracerProvider(tp) BEFORE
//     server.New(cfg). The flowd server itself emits no spans;
//     spans come from otelflow.Wrap() called by user code.
```

实践中推荐的范式是：**把 `cmd/flowd/server` 嵌入你自己的二进制**，
它同时也做 OTel 设置并在传入引擎之前用 `otelflow.Wrap` 包装它。
没有那个包装器，flowd 运行时不带遥测 —— 引擎缓存存储普通的
`*flow.Engine`，而不是被追踪的 runner。

未来的 flowd 发布可能反转这一点：在 Config 中接受一个 `flow.Runner`
工厂，使为某个流程 id 生成的任何引擎都能被自动包装。在那之前，
普通的 `flowd` 不追踪任何东西。

如果你只想要 **otel-exporter 信封** 日志（错误、警告、请求），
监听 stdout —— 嵌入的 `log.Default()` 通过 `cfg.Logger` 发出逐请求
的错误行。

## 性能调优

### `--max-node-concurrency`

限制单个拓扑层可以并行派生的 goroutine 数量。默认 `0` = 不限
（每个节点一个 goroutine）。当单个节点昂贵（LLM 调用）时，较低的值
会激进地节流；当节点廉价（字符串操作、本地子进程）时，0 / 不限是
正确的默认。

### `EngineCacheSize`

已编译引擎缓存以流程 id 为键。通过 `server.Config.EngineCacheSize`
对其限界，以防止在有许多不同流程的部署中无限增长：

```go
srv, _ := server.New(server.Config{
    // ...
    EngineCacheSize: 256, // capacity-bounded LRU
})
```

非正值（默认）禁用限界 —— 缓存无限增长。无论上限如何，PUT / DELETE
处理器始终显式驱逐。

### 事件 INSERT 批处理（对同步运行自动）

`POST /flows/{id}/run`（同步模式）在内存中缓冲事件，并在
`FinishRun` 时以 **单个事务** 刷新它们 —— 对于有 N 个事件的流程，
大致比逐事件 INSERT 快 N 倍。

`POST /flows/{id}/run/stream` 仍在转发每一帧 **之前** 持久化事件，
因此中途断开的客户端会留下完整的审计轨迹。运维者以小的延迟成本
换取流式运行的持久性。

### 何时横向扩展

单个 `flowd` 进程在现代硬件上能处理数百个并发运行。瓶颈往往按此
顺序浮现：

1. **工具延迟**（HTTP/exec）—— 主导墙钟时间。在工具上游缓存或
   批处理。使用 `--max-node-concurrency` 来限制对限流工具服务的扇出。
2. **SQLite 写入** —— 写吞吐在本地磁盘上约 2-5 K 事务/秒到达平台。
   启用 WAL 模式并通过 DSN 增大 page cache 以获得更多。
3. **引擎缓存未命中** —— 纯 CPU。提高 `EngineCacheSize`。

关于横向扩展，参见上文的“持久化布局”：SQLite 是单写者，因此实用
路径是每个 DB 一个 flowd 加上游路由。

## 优雅关闭

`flowd` 捕获 `SIGINT` + `SIGTERM`，并以 5 秒截止运行
`http.Server.Shutdown`。在途的运行完成；拒绝新连接。SQLite 句柄
在延迟的 `store.Close()` 上关闭。

对于负载均衡器后的零停机部署：在 LB 处排空，等待 `/healthz` 停止
被路由到旧副本，然后 SIGTERM。默认 5 s 截止假定运行很短；对于
长期运行的流程，通过 patch 源码来提高它，或者用 `runit`/`s6`
风格的能处理更深截止的监督者来管理此二进制。

## 可观测性快速检查

```bash
# Liveness — does the process respond?
curl -fsSL http://localhost:7861/healthz
# ok

# Did flows persist across restart?
curl -fsSL http://localhost:7861/flows | jq '.flows | length'

# How busy was the last hour?  (no auth)
sqlite3 flow.db "
  SELECT status, COUNT(*) FROM runs
   WHERE started_at > (strftime('%s', 'now') - 3600) * 1000000
   GROUP BY status
"
```

## 升级路径

`v0.1.x` 是仅增量的。v0.1 线内的版本提升是即插即用的 —— 无标志
变更、无 DB 迁移、无需客户端重新编译。

`v0.2.0`（发布时）**将** 被允许破坏 IR / API 形状；清晰的迁移说明
会出现在 [CHANGELOG](../CHANGELOG.zh-CN.md) 中，并且 `/v2` module 路径
会为破坏性部分替代 `/v0.1`。

## v0.1 的非目标

记录于此，以免运维者期待它们：

- 无限流（使用上游网关）。
- 无超出逐运行事件的审计日志（使用 OTel + 日志投递）。
- 无多租户（每个租户一个 DB）。
- 无集群（单写者 SQLite —— 在存储层联合）。
- 无对在途运行的重启重放（关闭时的 FinishRun 是突兀的；处于
  `running` 状态的运行在崩溃后仍保持那样，必须手动清理）。
- 无流程调度 / cron 触发器（在 API 之上构建）。
