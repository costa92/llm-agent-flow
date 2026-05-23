// Package flow is the serializable flow IR + DAG executor for the
// llm-agent-flow module.
//
// A flow is a directed acyclic graph of nodes connected by typed
// edges, authored as JSON, validated at load time, and executed by a
// topological engine.
//
// Three layers:
//
//   - IR — Flow / Node / Edge / Port / PortRef Go types with JSON
//     round-trip. Load(r) parses; Marshal round-trips.
//   - Validate — cycle detection (Kahn topo + cycle finder), dangling
//     edges, duplicate node IDs, missing input/output declarations.
//   - Engine — topological executor. Each layer runs sequentially
//     (per-layer parallelism deferred). Emits a typed FlowEvent
//     stream mirroring K1 streaming idioms.
//
// Nodes are pluggable through NodeRegistry. The bundled ToolNode
// adapter wraps any github.com/costa92/llm-agent/agents.Tool as a
// one-input / one-output node, so an entire flow can be assembled
// from already-existing Tools without writing a single Node type.
//
// MetadataAware (in node.go) is an additive optional sibling capability:
// a NodeKind that also implements RunWithMetadata can publish key/value
// metadata (HTTP status, exec exit code, token usage, etc.) alongside
// its outputs. The Engine detects the capability via type assertion, so
// existing NodeKind implementations remain unchanged. NodeFinished
// events carry the metadata in FlowEvent.Metadata when present.
//
// MetadataAwareTool (also in node.go) is the equivalent capability at
// the Tool layer. Tools implementing ExecuteWithMetadata surface
// per-call metadata up through the bundled toolNode, which itself
// implements MetadataAware. The built-in http and exec tools (under
// flow/tools) implement MetadataAwareTool — http_status / bytes /
// duration_ms for http calls, exit_code / duration_ms / signal for
// exec invocations. Plain flow.Tool implementations continue to work
// unchanged and produce nil metadata.
package flow
