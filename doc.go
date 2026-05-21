// Package llmflow is the documentation anchor for the llm-agent-flow
// module — a serializable flow IR + DAG executor for the llm-agent
// ecosystem.
//
// The root package exports no symbols. Callers import the sub-packages
// directly:
//
//   - flow — Flow / Node / Edge / Port types, JSON load + validate,
//     NodeRegistry, Engine (DAG executor), FlowEvent typed union.
//
// And the sub-command:
//
//   - cmd/flow — CLI entry point: flow run <file.json>.
//
// A flow is a directed acyclic graph of nodes connected by typed
// edges, authored as JSON, validated at load time, and executed by a
// topological engine. Each node wraps an existing llm-agent primitive
// (agents.Tool, agents.Agent); flow does not invent a parallel
// component model.
//
// Position in the ecosystem dep graph: llm-agent-flow → llm-agent.
// The flow library itself stays stdlib-only outside that back-edge.
package llmflow
