package graph

import (
	"context"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// AddBranch adds a key-routing node to g. The node takes a single input
// value of type T on its "in" port, calls key(ctx, value) to pick a route,
// and forwards the (unchanged) value out the matching route-key output
// port. Only the selected port receives a value; the others produce
// nothing, so the engine's "no incoming edge fired -> target skipped"
// semantics prune the unchosen branches — no new engine machinery needed.
//
// Each entry in routes maps a key string to the node the value flows to
// when key() returns that string. Every route target's input type must
// accept T (checked via the standard assignability rules). AddBranch also
// registers the branch -> target edges, lowered to one IR edge per route
// from branchNode.<key> to target.in.
//
// The branch NodeRef's declared input and output types are both T (the
// value is passed through unchanged), so it composes with Entry and the
// type checker like any single-T node.
func AddBranch[GI, GO, T any](g *Graph[GI, GO], id string, key func(context.Context, T) (string, error), routes map[string]NodeRef) (NodeRef, error) {
	if key == nil {
		return NodeRef{}, g.latch(fmt.Errorf("graph: branch %q: nil key func", id))
	}
	if len(routes) == 0 {
		return NodeRef{}, g.latch(fmt.Errorf("graph: branch %q: no routes", id))
	}
	tT := reflect.TypeFor[T]()

	// Validate each route target accepts T before registering anything.
	for k, target := range routes {
		if k == "" {
			return NodeRef{}, g.latch(fmt.Errorf("graph: branch %q: empty route key", id))
		}
		if ok, _ := assignable(tT, target.inT); !ok {
			return NodeRef{}, g.latch(fmt.Errorf("graph: branch %q route %q -> %q: %s not assignable to %s",
				id, k, target.id, tT, target.inT))
		}
	}

	// Stable order for the published output ports (Go map order is
	// non-deterministic; the adapter reuses this slice).
	keys := make([]string, 0, len(routes))
	for k := range routes {
		keys = append(keys, k)
	}

	newKind := func() flow.NodeKind {
		return &branchAdapter[T]{id: id, key: key, keys: keys, inT: tT}
	}
	// Branch in/out are both T: the routed value is the input passed
	// through unchanged.
	ref, err := g.addNode(id, tT, tT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "branch node holds a Go key function (in-process only)")

	// Register one edge per route: branch.<key> -> target.in.
	for k, target := range routes {
		g.edges = append(g.edges, graphEdge{from: id, to: target.id, fromPort: k, toPort: portIn})
	}
	return ref, nil
}

// branchAdapter is the flow.NodeKind backing a Branch node. It publishes
// one input port ("in") and one output port per route key. Run asserts the
// boxed input to T, evaluates key, and emits the value on exactly the
// selected key's port — leaving the rest unproduced so their edges don't
// fire.
type branchAdapter[T any] struct {
	id   string
	key  func(context.Context, T) (string, error)
	keys []string // route-key output port names
	inT  reflect.Type
}

func (a *branchAdapter[T]) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: a.inT}}
}

func (a *branchAdapter[T]) Outputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inT})
	}
	return ports
}

func (a *branchAdapter[T]) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: branch %q: missing input port %q", a.id, portIn)
	}
	typed, ok := raw.(T)
	if !ok {
		return nil, fmt.Errorf("graph: branch %q: input is %T, not assignable to %s", a.id, raw, a.inT)
	}
	k, err := a.key(ctx, typed)
	if err != nil {
		return nil, fmt.Errorf("graph: branch %q: key: %w", a.id, err)
	}
	// The chosen key must be a declared route port.
	found := false
	for _, candidate := range a.keys {
		if candidate == k {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("graph: branch %q: key returned %q, not a declared route", a.id, k)
	}
	// Emit only the selected port; the rest stay unproduced so their
	// edges don't fire and their targets are skipped.
	return map[string]any{k: typed}, nil
}
