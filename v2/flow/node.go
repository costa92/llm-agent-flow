package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// NodeKind is the runtime contract a v2 node satisfies. Each Flow.Node
// IR entry resolves through a NodeRegistry into one of these. (Named
// NodeKind rather than Node so the IR struct Node and the runtime
// interface don't collide — same split as v0.1.)
//
// Inputs / Outputs declare the port shape statically so the Engine can
// thread values between nodes at run time.
//
// Run executes the node against the resolved inputs map (port name ->
// value) and returns the outputs map (port name -> value). Unlike
// v0.1, the carrier is any: a value may be a structured Go value, not
// only a string.
type NodeKind interface {
	Inputs() []Port
	Outputs() []Port
	Run(ctx context.Context, in map[string]any) (map[string]any, error)
}

// NodeFactory constructs a NodeKind from the raw JSON Config blob
// attached to a Flow.Node IR entry, given the engine's dependency
// context.
type NodeFactory func(cfg json.RawMessage, deps Deps) (NodeKind, error)

// Deps is the bag of dependencies a NodeFactory may pull from at
// construction time. Kept minimal for task 1 — component-node deps
// (ChatModel/Tool/Template) land with those nodes in later tasks.
type Deps struct{}

// NodeRegistry is the central mapping of node-type strings to
// factories. Concurrency-safe.
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

// Build resolves a Flow.Node IR entry through the registry into a
// runtime NodeKind. Unknown types and factory errors are surfaced
// verbatim.
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
