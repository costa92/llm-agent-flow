// Package graph is the typed, program-only front-end for v2 flows. A
// Graph[I, O] is a statically-typed DAG of Go nodes (Lambda closures,
// Passthrough identities, …) that compiles down — through the task-1
// engine's exported API only — into a runnable Invoke(I) -> O.
//
// Lower strategy: a typed node cannot live in the JSON IR (it holds a Go
// closure). Instead Compile builds an in-process flow.NodeRegistry whose
// factories return typed adapters, assembles a flow.Flow IR that
// references them by a synthetic Type string, and calls flow.Compile.
// The engine never learns it is driving typed nodes — see runnable.go.
package graph

import (
	"context"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// portIn and portOut are the single in/out port names every typed node
// in this task publishes. Multi-port nodes land in a later task.
const (
	portIn  = "in"
	portOut = "out"
)

// NodeRef is an opaque handle to a node added to a Graph. It carries the
// node's id and its declared Go input/output types so AddEdge/Entry/Exit
// can run reflect-based assignability checks at build time.
type NodeRef struct {
	id   string
	inT  reflect.Type
	outT reflect.Type
}

// graphNode is the internal record of one typed node: its id plus a
// factory that produces the runtime flow.NodeKind adapter at Compile.
type graphNode struct {
	ref     NodeRef
	newKind func() flow.NodeKind
}

// graphEdge records a build-time-accepted connection between two nodes.
type graphEdge struct {
	from string
	to   string
}

// Graph is a typed DAG builder parameterised on its overall input I and
// output O types. Construction methods accumulate nodes/edges and latch
// the first type error; Compile re-checks everything and lowers to the
// task-1 engine.
type Graph[I, O any] struct {
	inT  reflect.Type
	outT reflect.Type

	nodes []graphNode
	byID  map[string]NodeRef
	edges []graphEdge

	entry *NodeRef
	exit  *NodeRef

	// latchedErr holds the first build-time type/usage error so Compile
	// can report it even when AddEdge's return value was ignored.
	latchedErr error
}

// NewGraph returns an empty typed graph with input type I and output
// type O.
func NewGraph[I, O any]() *Graph[I, O] {
	return &Graph[I, O]{
		inT:  reflect.TypeFor[I](),
		outT: reflect.TypeFor[O](),
		byID: make(map[string]NodeRef),
	}
}

// latch records err as the graph's first build error if none is set yet,
// and returns err for the caller to also return.
func (g *Graph[I, O]) latch(err error) error {
	if err != nil && g.latchedErr == nil {
		g.latchedErr = err
	}
	return err
}

// addNode registers a node record + ref, guarding duplicate ids.
func (g *Graph[I, O]) addNode(id string, inT, outT reflect.Type, newKind func() flow.NodeKind) (NodeRef, error) {
	if id == "" {
		return NodeRef{}, g.latch(fmt.Errorf("graph: add node: empty id"))
	}
	if _, dup := g.byID[id]; dup {
		return NodeRef{}, g.latch(fmt.Errorf("graph: add node %q: duplicate id", id))
	}
	ref := NodeRef{id: id, inT: inT, outT: outT}
	g.nodes = append(g.nodes, graphNode{ref: ref, newKind: newKind})
	g.byID[id] = ref
	return ref, nil
}

// AddLambdaNode adds a program-only closure node to g. The adapter's Run
// pulls in["in"], asserts it to In (the rule-3 runtime check for any
// sources happens here), calls fn, and emits {"out": Out}.
//
// GI/GO are the graph's I/O params (so the *Graph type matches); In/Out
// are this node's own port types.
func AddLambdaNode[GI, GO, In, Out any](g *Graph[GI, GO], id string, fn func(context.Context, In) (Out, error)) (NodeRef, error) {
	inT := reflect.TypeFor[In]()
	outT := reflect.TypeFor[Out]()
	newKind := func() flow.NodeKind {
		return &lambdaAdapter[In, Out]{id: id, fn: fn, inT: inT, outT: outT}
	}
	return g.addNode(id, inT, outT, newKind)
}

// AddPassthroughNode adds an identity node carrying a value of type T
// from its in port to its out port unchanged.
func AddPassthroughNode[GI, GO, T any](g *Graph[GI, GO], id string) (NodeRef, error) {
	return AddLambdaNode[GI, GO, T, T](g, id, func(_ context.Context, v T) (T, error) { return v, nil })
}

// lambdaAdapter is the flow.NodeKind that wraps a typed closure. It
// publishes a single in/out port and bridges the any carrier to In/Out.
type lambdaAdapter[In, Out any] struct {
	id   string
	fn   func(context.Context, In) (Out, error)
	inT  reflect.Type
	outT reflect.Type
}

func (a *lambdaAdapter[In, Out]) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: a.inT}}
}

func (a *lambdaAdapter[In, Out]) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: a.outT}}
}

func (a *lambdaAdapter[In, Out]) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: node %q: missing input port %q", a.id, portIn)
	}
	// rule-3 runtime check: the static edge may have been an `any` source
	// deferred to here. Assert the boxed value to the node's declared In.
	typed, ok := raw.(In)
	if !ok {
		return nil, fmt.Errorf("graph: node %q: input is %T, not assignable to %s", a.id, raw, a.inT)
	}
	out, err := a.fn(ctx, typed)
	if err != nil {
		return nil, err
	}
	return map[string]any{portOut: out}, nil
}
