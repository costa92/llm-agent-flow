package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// NodeKind is the runtime contract a node satisfies. Each Node IR
// entry resolves through a NodeRegistry into one of these.
//
// Inputs / Outputs declare the port shape statically so the Engine
// can wire edges and verify each non-input port is supplied exactly
// once at run time.
//
// Run executes the node against the resolved inputs map (port name ->
// value) and returns the outputs map (port name -> value). Values are
// strings at v0 — typed values are reserved for a later port-type
// expansion.
type NodeKind interface {
	Inputs() []Port
	Outputs() []Port
	Run(ctx context.Context, in map[string]string) (map[string]string, error)
}

// MetadataAware is an OPTIONAL capability sibling to NodeKind. Nodes
// that implement it can publish key/value metadata alongside their
// outputs (e.g. HTTP status code, exec exit code, LLM token usage).
//
// The engine type-asserts every node against MetadataAware on each
// invocation; nodes that don't implement it run via NodeKind.Run as
// before and emit a FlowEvent with Metadata == nil. This means
// MetadataAware can be added to any existing NodeKind implementation
// without breaking compilation or behavior.
//
// MetadataAware will remain optional throughout v0.1.x; it will NOT
// be promoted to a required NodeKind method before v0.2.
//
// Implementations MUST also implement NodeKind.Run (typically by
// delegating: out, _, err := n.RunWithMetadata(ctx, in); return out, err).
type MetadataAware interface {
	NodeKind
	RunWithMetadata(ctx context.Context, in map[string]string) (map[string]string, map[string]string, error)
}

// NodeFactory constructs a NodeKind from the raw JSON Config blob
// attached to a Node IR entry, given the engine's dependency context.
type NodeFactory func(cfg json.RawMessage, deps Deps) (NodeKind, error)

// Deps is the bag of dependencies a NodeFactory may pull from at
// construction time. v0 ships only Tools — Agents and ChatModels will
// follow in their own factories without breaking this shape.
type Deps struct {
	Tools ToolLookup
}

// ToolLookup resolves a tool by name. Implementations are typically
// backed by github.com/costa92/llm-agent/agents.Registry or a plain
// map[string]agents.Tool.
type ToolLookup interface {
	Lookup(name string) (Tool, bool)
}

// Tool is the narrow subset of github.com/costa92/llm-agent/agents.Tool
// flow needs. Declared locally so flow's library API is not coupled to
// any specific Tool constructor — callers wrap their preferred Tool
// implementation via adapters.ToolFromAgent or a hand-written shim.
type Tool interface {
	Name() string
	Execute(ctx context.Context, args json.RawMessage) (string, error)
}

// MetadataAwareTool is an OPTIONAL capability sibling to Tool. Tools
// implementing it can publish key/value metadata alongside their
// string output — e.g. HTTP response status, exec exit code,
// response body size, request duration, token usage.
//
// toolNode type-asserts this at run time. Tools that do not implement
// it run via the plain Tool.Execute path and produce nil metadata
// (matching pre-v0.2 behavior).
//
// MetadataAwareTool will remain optional throughout v0.1.x; it will
// NOT be promoted to a required Tool method before v0.2.
//
// Implementations MUST also implement Tool.Execute (typically by
// delegating: out, _, err := t.ExecuteWithMetadata(ctx, args);
// return out, err). Metadata is preserved on the error path (D1).
type MetadataAwareTool interface {
	Tool
	ExecuteWithMetadata(ctx context.Context, args json.RawMessage) (string, map[string]string, error)
}

// NodeRegistry is the central mapping of node-type strings to factories.
// Concurrency-safe.
type NodeRegistry struct {
	mu        sync.RWMutex
	factories map[string]NodeFactory
}

// NewNodeRegistry returns an empty registry.
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{factories: make(map[string]NodeFactory)}
}

// Register associates a node-type string with a factory. Duplicate
// registration of the same type returns an error.
func (r *NodeRegistry) Register(typ string, fac NodeFactory) error {
	if typ == "" {
		return errors.New("flow: register: empty type")
	}
	if fac == nil {
		return errors.New("flow: register: nil factory")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.factories[typ]; dup {
		return fmt.Errorf("flow: register: type %q already registered", typ)
	}
	r.factories[typ] = fac
	return nil
}

// Build resolves a Node IR entry through the registry into a runtime
// NodeKind. Unknown types and factory errors are surfaced verbatim.
func (r *NodeRegistry) Build(n Node, deps Deps) (NodeKind, error) {
	r.mu.RLock()
	fac, ok := r.factories[n.Type]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("flow: build node %q: unknown type %q", n.ID, n.Type)
	}
	kind, err := fac(n.Config, deps)
	if err != nil {
		return nil, fmt.Errorf("flow: build node %q: %w", n.ID, err)
	}
	return kind, nil
}
