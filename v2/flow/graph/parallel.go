package graph

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// AddParallel adds a fan-out node that forwards its single input value of
// type In to EVERY branch (unlike Branch, which selects one). It publishes
// one output port per branch key, all carrying In, and Run emits In on ALL
// of them so every branch's in-edge fires. Registers id.<key> -> branch.in
// edges; each branch's input type must accept In.
//
// The returned NodeRef has inT = outT = In (a pass-through fan-out), so
// callers wire entry/upstream -> this ref via AddEdge on portIn. Because it
// publishes route-key output ports (fromPort != portOut) the graph becomes
// non-linear and Stream degrades to box(Invoke). It is in-process only (a Go
// adapter), like Branch.
func AddParallel[GI, GO, In any](g *Graph[GI, GO], id string, branches map[string]NodeRef) (NodeRef, error) {
	if len(branches) == 0 {
		return NodeRef{}, g.latch(fmt.Errorf("graph: parallel %q: no branches", id))
	}
	inT := reflect.TypeFor[In]()

	// Validate each branch accepts In before registering anything.
	for k, target := range branches {
		if k == "" {
			return NodeRef{}, g.latch(fmt.Errorf("graph: parallel %q: empty branch key", id))
		}
		if ok, _ := assignable(inT, target.inT); !ok {
			return NodeRef{}, g.latch(fmt.Errorf("graph: parallel %q branch %q -> %q: %s not assignable to %s",
				id, k, target.id, inT, target.inT))
		}
	}

	// Stable order for the published output ports (Go map order is
	// non-deterministic; the adapter reuses this slice).
	keys := make([]string, 0, len(branches))
	for k := range branches {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newKind := func() flow.NodeKind {
		return &parallelAdapter[In]{id: id, keys: keys, inT: inT}
	}
	ref, err := g.addNode(id, inT, inT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "parallel fan-out node (in-process only)")

	// Register one edge per branch: parallel.<key> -> branch.in.
	for k, target := range branches {
		g.edges = append(g.edges, graphEdge{from: id, to: target.id, fromPort: k, toPort: portIn})
	}
	return ref, nil
}

// parallelAdapter is the flow.NodeKind backing a Parallel fan-out node. It
// publishes one input port ("in") and one output port per branch key. Run
// asserts the boxed input to In and emits the SAME value on EVERY key port
// so all branch edges fire.
type parallelAdapter[In any] struct {
	id   string
	keys []string // branch-key output port names (stable sorted)
	inT  reflect.Type
}

func (a *parallelAdapter[In]) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: a.inT}}
}

func (a *parallelAdapter[In]) Outputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inT})
	}
	return ports
}

func (a *parallelAdapter[In]) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: parallel %q: missing input port %q", a.id, portIn)
	}
	typed, ok := raw.(In)
	if !ok {
		return nil, fmt.Errorf("graph: parallel %q: input is %T, not assignable to %s", a.id, raw, a.inT)
	}
	out := make(map[string]any, len(a.keys))
	for _, k := range a.keys {
		out[k] = typed
	}
	return out, nil
}

// AddCombine adds a fan-in node with one input port per source key. When
// scheduled (after its predecessors complete), Run gathers the present
// inputs into a map[string]any (a source that was skipped/conditional is
// absent) and calls combine to produce Out on its single output port.
//
// The returned NodeRef declares inPorts = {key: source.outT}, outT = Out;
// inT is left zero (unused for multi-input nodes — wire upstreams in via
// AddEdgeTo on a named port). It is in-process only (a Go adapter).
func AddCombine[GI, GO, Out any](g *Graph[GI, GO], id string, sources map[string]NodeRef, combine func(ctx context.Context, in map[string]any) (Out, error)) (NodeRef, error) {
	if combine == nil {
		return NodeRef{}, g.latch(fmt.Errorf("graph: combine %q: nil combine func", id))
	}
	if len(sources) == 0 {
		return NodeRef{}, g.latch(fmt.Errorf("graph: combine %q: no sources", id))
	}

	inPorts := make(map[string]reflect.Type, len(sources))
	keys := make([]string, 0, len(sources))
	for k, src := range sources {
		if k == "" {
			return NodeRef{}, g.latch(fmt.Errorf("graph: combine %q: empty source key", id))
		}
		inPorts[k] = src.outT
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outT := reflect.TypeFor[Out]()
	newKind := func() flow.NodeKind {
		return &combineAdapter[Out]{id: id, keys: keys, inPorts: inPorts, combine: combine, outT: outT}
	}
	// inT left zero: a multi-input node resolves port types via inPorts.
	ref, err := g.addNode(id, nil, outT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	ref.inPorts = inPorts
	g.byID[id] = ref
	g.markInProcess(id, "combine fan-in node (in-process only)")

	// Register one edge per source: source.out -> combine.<key>.
	for k, src := range sources {
		g.edges = append(g.edges, graphEdge{from: src.id, to: id, fromPort: portOut, toPort: k})
	}
	return ref, nil
}

// combineAdapter is the flow.NodeKind backing a Combine fan-in node. It
// publishes one input port per source key and a single "out" port. Run
// gathers ONLY the input ports actually present (a skipped source is absent)
// into a map[string]any and calls combine.
type combineAdapter[Out any] struct {
	id      string
	keys    []string // source-key input port names (stable sorted)
	inPorts map[string]reflect.Type
	combine func(ctx context.Context, in map[string]any) (Out, error)
	outT    reflect.Type
}

func (a *combineAdapter[Out]) Inputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inPorts[k]})
	}
	return ports
}

func (a *combineAdapter[Out]) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: a.outT}}
}

func (a *combineAdapter[Out]) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	// Gather only the present source ports — a skipped/conditional source
	// is absent from in, and the combine func sees only the keys that fired.
	gathered := make(map[string]any, len(a.keys))
	for _, k := range a.keys {
		if v, ok := in[k]; ok {
			gathered[k] = v
		}
	}
	out, err := a.combine(ctx, gathered)
	if err != nil {
		return nil, fmt.Errorf("graph: combine %q: %w", a.id, err)
	}
	return map[string]any{portOut: out}, nil
}
