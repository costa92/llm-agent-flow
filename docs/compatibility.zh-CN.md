[English](./compatibility.md) | [简体中文](./compatibility.zh-CN.md)

# 兼容性承诺 —— `llm-agent-flow` v0.1.x

自 **v0.1.0** 起生效（tagged 2026-05-21）。

## v0.1 冻结了什么

在 `v0.1.x` 系列内，**每个可导入包的导出 API** 是 **仅增量** 的。
具体而言：

- **不移除任何导出符号**（不移除 funcs、types、methods、constants、
  vars）。
- **不重命名任何导出符号。**
- **不重新签名任何导出符号**：
  - func / method 的参数和返回类型稳定；
  - struct 字段名和类型稳定；
  - interface 方法集稳定。

提交的基线位于 [`api/v0.1.snapshot.txt`](../api/v0.1.snapshot.txt)。
门禁 `internal/apisnapshot` 在每次 `go test ./...` 时重新生成接口面，
并对该基线的任何漂移失败。

## v0.1 不覆盖什么

- **JSON IR 新增。** 流程 JSON 形状中可能出现新的可选字段
  （`Edge.Condition` 在 v0.0.4 加入，`flow.tools` 清单在 v0.0.3
  加入）。旧的流程继续加载。移除一个既有字段仍需要 major 提升。
- **HTTP 端点。** 可能出现新端点；如果它们在 README 的
  “Implemented”下列出，其请求/响应形状在 v0.1 中稳定。重命名或
  响应形状变更需要 major。
- **`internal/` 包。** `internal/` 下的任何内容都不稳定，且无法
  从 module 外部导入。
- **`cmd/flow` 和 `cmd/flowd` 的命令行标志。** 可能出现新标志；
  既有标志的含义稳定。
- **未指定输入下的行为。** “未指定”指未被显式记录的组合
  （例如超出 SQLite 存储所序列化范围的并发 CRUD 竞态）；修复可以
  在不进行 major 的情况下改变行为。

## 破坏性变更进入 `/v2`

任何违反上述规则的变更都在新的 module 路径
`github.com/costa92/llm-agent-flow/v2` 中发布。v0.1.x 在 v2 弃用窗口
期间继续接收安全与缺陷修复。

## 更新快照基线

有意的 **增量** 变更（新的导出符号、新类型上的新字段、新接口上的新
接口方法等）要求在同一次提交中重新生成基线：

```
go test ./internal/apisnapshot/ -run TestAPISnapshot -update
git add api/v0.1.snapshot.txt
git commit ...
```

门禁随后接受新的接口面，并拒绝任何未来漂移。

## 为什么要快照门禁

兼容性承诺的强度只取决于最弱贡献者的纪律。快照门禁使该承诺
**可执行**：一次丢掉方法或重命名参数的重构会在评审前让 CI 失败。
评审者在源码 diff 旁看到 `api/v0.1.snapshot.txt` 中的 diff ——
使发现意外破坏变得轻而易举。

门禁是纯标准库的（`go/parser` + `go/printer`）—— 无 module 依赖、
无需安装单独工具、在每次 `go test` 中运行。
