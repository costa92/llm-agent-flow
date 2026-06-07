package graph

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// AddCopy adds a streaming fan-out node: its single input (a value or a live
// stream of T) is delivered IN FULL to every one of `targets`. It is the
// streaming sibling of AddParallel — on the VALUE path (Invoke) it forwards
// the SAME value of T to every target (copyAdapter.Run == parallelAdapter.Run);
// on the STREAM path each target receives an independent StreamReader[T] via
// the Phase-2 Copy tee (wired by runStreamGraph). Registers id.<key> ->
// target.in edges; each target's input type must accept T.
//
// The returned NodeRef has inT = outT = T (a pass-through fan-out). Because it
// publishes route-key output ports (fromPort != portOut) the graph is
// non-linear; a graph with NO streamable shape degrades to box(Invoke). It is
// in-process only (a Go adapter that holds/produces streams + Go wiring →
// non-serializable → non-checkpointable), like AddParallel.
func AddCopy[GI, GO, T any](g *Graph[GI, GO], id string, targets map[string]NodeRef) (NodeRef, error) {
	if len(targets) == 0 {
		return NodeRef{}, g.latch(fmt.Errorf("graph: copy %q: no targets", id))
	}
	inT := reflect.TypeFor[T]()

	// Validate each target accepts T before registering anything.
	for k, target := range targets {
		if k == "" {
			return NodeRef{}, g.latch(fmt.Errorf("graph: copy %q: empty target key", id))
		}
		if ok, _ := assignable(inT, target.inT); !ok {
			return NodeRef{}, g.latch(fmt.Errorf("graph: copy %q target %q -> %q: %s not assignable to %s",
				id, k, target.id, inT, target.inT))
		}
	}

	// Stable order for the published output ports (Go map order is
	// non-deterministic; the adapter reuses this slice).
	keys := make([]string, 0, len(targets))
	for k := range targets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newKind := func() flow.NodeKind {
		return &copyAdapter[T]{id: id, keys: keys, inT: inT}
	}
	ref, err := g.addNode(id, inT, inT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "copy fan-out node (in-process only)")

	// Register one edge per target: copy.<key> -> target.in.
	for k, target := range targets {
		g.edges = append(g.edges, graphEdge{from: id, to: target.id, fromPort: k, toPort: portIn})
	}
	return ref, nil
}

// copyAdapter is the flow.NodeKind backing a Copy fan-out node. It publishes
// one input port ("in") and one output port per target key. Run asserts the
// boxed input to T and emits the SAME value on EVERY key port so all target
// edges fire — identical to parallelAdapter.Run. This is the value-path
// (Invoke) semantics; the stream path is driven by runStreamGraph, which
// recognizes this kind via copyKind.
type copyAdapter[T any] struct {
	id   string
	keys []string // target-key output port names (stable sorted)
	inT  reflect.Type
}

func (a *copyAdapter[T]) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: a.inT}}
}

func (a *copyAdapter[T]) Outputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inT})
	}
	return ports
}

func (a *copyAdapter[T]) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: copy %q: missing input port %q", a.id, portIn)
	}
	typed, ok := raw.(T)
	if !ok {
		return nil, fmt.Errorf("graph: copy %q: input is %T, not assignable to %s", a.id, raw, a.inT)
	}
	out := make(map[string]any, len(a.keys))
	for _, k := range a.keys {
		out[k] = typed
	}
	return out, nil
}

// copyKind is the capability a Copy node exposes to the stream planner /
// executor: it reports its declared target-key output ports and the element
// type T it carries, so buildStreamGraphPlan can recognize a copy fan-out
// (distinct from a value node or a branch) and runStreamGraph can re-stamp the
// erased copyReader carriers with T. *copyAdapter[T] satisfies it.
type copyKind interface {
	flow.NodeKind
	copyKeys() []string
	copyElemT() reflect.Type
}

func (a *copyAdapter[T]) copyKeys() []string      { return a.keys }
func (a *copyAdapter[T]) copyElemT() reflect.Type { return a.inT }
