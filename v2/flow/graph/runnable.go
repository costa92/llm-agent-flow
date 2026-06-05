package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// graphInputKey / graphOutputKey are the external Flow.Inputs/Outputs
// names used to thread the boxed I in and the boxed O out of the engine.
const (
	graphInputKey  = "__graph_in"
	graphOutputKey = "__graph_out"
)

// Runnable is a compiled typed graph: a thin typed shell over a task-1
// flow.Engine. Invoke boxes the I input into the engine, runs it, and
// unboxes the declared output back to O.
type Runnable[I, O any] struct {
	engine *flow.Engine
	inT    reflect.Type
	outT   reflect.Type
}

// Compile re-validates the graph (including any latched build error),
// lowers it to a task-1 flow.Flow + in-process NodeRegistry, and returns
// a typed Runnable. Engine options pass through to flow.Compile.
func (g *Graph[I, O]) Compile(_ context.Context, opts ...flow.EngineOption) (*Runnable[I, O], error) {
	if g.latchedErr != nil {
		return nil, fmt.Errorf("graph: compile: %w", g.latchedErr)
	}
	if g.entry == nil {
		return nil, fmt.Errorf("graph: compile: no entry node (call Entry)")
	}
	if g.exit == nil {
		return nil, fmt.Errorf("graph: compile: no exit node (call Exit)")
	}

	// Full reflect re-check of edges + entry/exit, in case the caller
	// never invoked the builder methods' returns. (latchedErr already
	// covers the methods that were called; this re-derives independently
	// so the invariant holds regardless.)
	for _, e := range g.edges {
		from, to := g.byID[e.from], g.byID[e.to]
		if ok, _ := assignable(from.outT, to.inT); !ok {
			return nil, fmt.Errorf("graph: compile: edge %q.%s -> %q.%s: %s not assignable to %s",
				e.from, e.fromPort, e.to, e.toPort, from.outT, to.inT)
		}
	}
	if ok, _ := assignable(g.inT, g.entry.inT); !ok {
		return nil, fmt.Errorf("graph: compile: entry %q: graph input %s not assignable to %s",
			g.entry.id, g.inT, g.entry.inT)
	}
	if ok, _ := assignable(g.exit.outT, g.outT); !ok {
		return nil, fmt.Errorf("graph: compile: exit %q: node output %s not assignable to graph output %s",
			g.exit.id, g.exit.outT, g.outT)
	}

	// Lower: assign each node a synthetic Type, register a factory that
	// returns its prebuilt adapter, and build the Flow IR.
	reg := flow.NewNodeRegistry()
	irNodes := make([]flow.Node, 0, len(g.nodes))
	for i, gn := range g.nodes {
		typ := fmt.Sprintf("g%d", i)
		kind := gn.newKind() // captured below; factory ignores Config
		if err := reg.Register(typ, func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
			return kind, nil
		}); err != nil {
			return nil, fmt.Errorf("graph: compile: register node %q: %w", gn.ref.id, err)
		}
		irNodes = append(irNodes, flow.Node{ID: gn.ref.id, Type: typ})
	}

	irEdges := make([]flow.Edge, 0, len(g.edges))
	for _, e := range g.edges {
		irEdges = append(irEdges, flow.Edge{
			Source: flow.PortRef{Node: e.from, Port: e.fromPort},
			Target: flow.PortRef{Node: e.to, Port: e.toPort},
		})
	}

	f := flow.Flow{
		ID:    "graph",
		Nodes: irNodes,
		Edges: irEdges,
		Inputs: []flow.NamedPortRef{{
			Name:    graphInputKey,
			PortRef: flow.PortRef{Node: g.entry.id, Port: portIn},
		}},
		Outputs: []flow.NamedPortRef{{
			Name:    graphOutputKey,
			PortRef: flow.PortRef{Node: g.exit.id, Port: portOut},
		}},
	}

	engine, err := flow.Compile(f, reg, flow.Deps{}, opts...)
	if err != nil {
		return nil, fmt.Errorf("graph: compile: lower: %w", err)
	}
	return &Runnable[I, O]{engine: engine, inT: g.inT, outT: g.outT}, nil
}

// Invoke runs the compiled graph: box in into the engine input port, run,
// and unbox the single declared output back to O.
func (r *Runnable[I, O]) Invoke(ctx context.Context, in I) (O, error) {
	var zero O
	out, err := r.engine.Run(ctx, map[string]any{graphInputKey: in})
	if err != nil {
		return zero, err
	}
	raw, ok := out[graphOutputKey]
	if !ok {
		return zero, fmt.Errorf("graph: invoke: engine produced no output %q", graphOutputKey)
	}
	typed, ok := raw.(O)
	if !ok {
		return zero, fmt.Errorf("graph: invoke: output is %T, not assignable to %s", raw, r.outT)
	}
	return typed, nil
}

// Stream runs the compiled graph and returns the engine's lifecycle event
// channel, boxing in into the engine input port. The channel is closed
// after the terminal event. The graph output is delivered via the FlowDone
// event's Outputs map (keyed by graphOutputKey) rather than unboxed to O —
// callers wanting the typed result should use Invoke.
func (r *Runnable[I, O]) Stream(ctx context.Context, in I) (<-chan flow.Event, error) {
	return r.engine.RunStream(ctx, map[string]any{graphInputKey: in})
}
