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
	"encoding/json"
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
//
// inPorts is non-nil ONLY for multi-input nodes (fan-in combiners): it maps
// each declared input-port name to its Go type. Single-port nodes leave
// inPorts nil and continue to use inT with the implicit portIn name.
type NodeRef struct {
	id      string
	inT     reflect.Type
	outT    reflect.Type
	inPorts map[string]reflect.Type
}

// graphNode is the internal record of one typed node: its id plus a
// factory that produces the runtime flow.NodeKind adapter at Compile.
type graphNode struct {
	ref     NodeRef
	newKind func() flow.NodeKind
}

// graphEdge records a build-time-accepted connection between two nodes.
// fromPort/toPort name the source/target ports; both default to the
// single-port portOut/portIn. Multi-output nodes (Branch) set fromPort to
// a route-key port name.
type graphEdge struct {
	from     string
	to       string
	fromPort string
	toPort   string
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

	// lowering records, per node id, the serializable IR descriptor — set
	// iff the node has a serializable form (no Go closure / injected dep).
	// A node is serializable iff lowering[id] != nil.
	lowering map[string]*nodeLowering
	// reasons records, per node id, the human reason a node is NOT
	// serializable. Exactly one of lowering[id] / reasons[id] is set per
	// node-adding function.
	reasons map[string]string

	// latchedErr holds the first build-time type/usage error so Compile
	// can report it even when AddEdge's return value was ignored.
	latchedErr error
}

// nodeLowering is the serializable IR descriptor of a node: the globally
// registered node-type string and its JSON config (nil if none needed).
type nodeLowering struct {
	typ    string
	config json.RawMessage
}

// NewGraph returns an empty typed graph with input type I and output
// type O.
func NewGraph[I, O any]() *Graph[I, O] {
	return &Graph[I, O]{
		inT:      reflect.TypeFor[I](),
		outT:     reflect.TypeFor[O](),
		byID:     make(map[string]NodeRef),
		lowering: make(map[string]*nodeLowering),
		reasons:  make(map[string]string),
	}
}

// markSerializable records that node id has a serializable IR form of the
// given type and config. A node so marked lowers into the JSON IR.
func (g *Graph[I, O]) markSerializable(id, typ string, config json.RawMessage) {
	g.lowering[id] = &nodeLowering{typ: typ, config: config}
}

// markInProcess records that node id is in-process only (no serializable
// form), with a human reason. It overwrites any prior reason and clears any
// lowering entry so the node is unambiguously non-serializable.
func (g *Graph[I, O]) markInProcess(id, reason string) {
	g.reasons[id] = reason
	delete(g.lowering, id)
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
	ref, err := g.addNode(id, inT, outT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "lambda node holds a Go closure (in-process only)")
	return ref, nil
}

// AddPassthroughNode adds an identity node carrying a value of type T from
// its in port to its out port unchanged. Unlike Lambda it is serializable
// (builtin.passthrough): it holds no Go closure, so it round-trips through
// JSON and recompiles against NewBuiltinRegistry.
func AddPassthroughNode[GI, GO, T any](g *Graph[GI, GO], id string) (NodeRef, error) {
	t := reflect.TypeFor[T]()
	newKind := func() flow.NodeKind { return &passthroughAdapter{id: id, t: t} }
	ref, err := g.addNode(id, t, t, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markSerializable(id, builtinPassthrough, nil)
	return ref, nil
}

// passthroughAdapter is the flow.NodeKind for an identity node: it forwards
// the value on its in port to its out port unchanged. It is serializable
// (builtin.passthrough) because it holds no Go closure — the runtime form
// rebuilt from JSON operates on any and forwards regardless of element type.
type passthroughAdapter struct {
	id string
	t  reflect.Type // declared port Go type; nil when rebuilt from JSON
}

func (a *passthroughAdapter) Inputs() []flow.Port  { return []flow.Port{{Name: portIn, GoType: a.t}} }
func (a *passthroughAdapter) Outputs() []flow.Port { return []flow.Port{{Name: portOut, GoType: a.t}} }

func (a *passthroughAdapter) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: passthrough %q: missing input port %q", a.id, portIn)
	}
	return map[string]any{portOut: raw}, nil
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
