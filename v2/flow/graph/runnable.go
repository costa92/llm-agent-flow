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
//
// pipeline is non-nil only when the graph is a single linear chain (one
// entry, one exit, every node ≤1 in / ≤1 out edge, no Branch / condition
// edges). When set, Stream runs a dedicated data-level streaming executor
// over it; otherwise Stream degrades to box(Invoke).
type Runnable[I, O any] struct {
	engine   *flow.Engine
	inT      reflect.Type
	outT     reflect.Type
	pipeline *linearPipeline
}

// linearPipeline is the ordered node chain for a linear graph plus the
// per-node adapter instances. The data-stream executor walks pipeline in
// order from the boxed I input to the O output.
type linearPipeline struct {
	nodes []pipelineNode
}

// pipelineNode is one node in a linear pipeline: its runtime adapter, its
// declared In/Out types, and whether it can emit a data stream directly.
type pipelineNode struct {
	kind   flow.NodeKind
	stream streamNode // non-nil iff kind implements streamNode
	inT    reflect.Type
	outT   reflect.Type
}

// StreamCapable reports whether Stream runs as a true data-level stream
// (linear chain) rather than degrading to box(Invoke).
func (r *Runnable[I, O]) StreamCapable() bool { return r.pipeline != nil }

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

	// Detect the linear-chain shape and, if linear, build the data-stream
	// pipeline — validating every stream-edge concatenator NOW so a
	// missing folder is a Compile error, not a runtime surprise.
	pipeline, err := g.buildLinearPipeline()
	if err != nil {
		return nil, err
	}
	return &Runnable[I, O]{engine: engine, inT: g.inT, outT: g.outT, pipeline: pipeline}, nil
}

// buildLinearPipeline analyses the graph for the linear-chain shape and,
// when it matches, returns an ordered pipeline of adapter instances. It
// returns (nil, nil) for non-linear graphs (Stream then degrades to
// box(Invoke)). It returns an error only when the graph IS linear but a
// required stream-edge concatenator is missing.
//
// Linear ⇔ single entry, single exit, every node has ≤1 incoming and ≤1
// outgoing edge, and no node fans out to multiple ports (Branch). Under
// those constraints the unique entry→exit walk is the pipeline order.
func (g *Graph[I, O]) buildLinearPipeline() (*linearPipeline, error) {
	// Count in/out edges per node and capture the single successor.
	indeg := make(map[string]int, len(g.nodes))
	outdeg := make(map[string]int, len(g.nodes))
	next := make(map[string]string, len(g.nodes))
	for _, e := range g.edges {
		// A Branch lowers to multiple edges from distinct fromPorts; any
		// non-portOut source port means a fan-out node → not linear.
		if e.fromPort != portOut {
			return nil, nil
		}
		outdeg[e.from]++
		indeg[e.to]++
		next[e.from] = e.to
	}
	for _, n := range g.nodes {
		if indeg[n.ref.id] > 1 || outdeg[n.ref.id] > 1 {
			return nil, nil
		}
	}

	// Walk from entry following the unique successor; require exactly all
	// nodes visited and end at exit. Any deviation → not linear.
	order := make([]string, 0, len(g.nodes))
	seen := make(map[string]bool, len(g.nodes))
	cur := g.entry.id
	for {
		if seen[cur] {
			return nil, nil // cycle: not a linear chain
		}
		seen[cur] = true
		order = append(order, cur)
		succ, ok := next[cur]
		if !ok {
			break // no outgoing edge: end of chain
		}
		cur = succ
	}
	if len(order) != len(g.nodes) || order[len(order)-1] != g.exit.id {
		return nil, nil // disconnected / multiple components
	}

	// Build the ordered pipeline; instantiate each adapter once.
	byID := make(map[string]graphNode, len(g.nodes))
	for _, n := range g.nodes {
		byID[n.ref.id] = n
	}
	pnodes := make([]pipelineNode, 0, len(order))
	for _, id := range order {
		gn := byID[id]
		kind := gn.newKind()
		sn, _ := kind.(streamNode)
		pnodes = append(pnodes, pipelineNode{
			kind:   kind,
			stream: sn,
			inT:    gn.ref.inT,
			outT:   gn.ref.outT,
		})
	}

	// Compile-time concatenator check: for each adjacent pair where the
	// upstream may emit a stream (its node is a streamNode) and the
	// downstream needs a value, the downstream In type must have a
	// registered folder. We can't know statically whether a folder will
	// actually be exercised, but a streamNode followed by anything that
	// is NOT itself a stream sink WILL fold at the downstream In type, so
	// validate it now.
	for i := 0; i+1 < len(pnodes); i++ {
		up, down := pnodes[i], pnodes[i+1]
		if up.stream == nil {
			continue // upstream never emits a stream; value path only
		}
		// Downstream consumes a value at down.inT (no node both emits and
		// re-consumes a raw stream in task 5), so a folder is required.
		if _, ok := lookupConcat(down.inT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> %q: no Concatenator for %s",
				order[i], order[i+1], down.inT)
		}
	}
	// Tail-edge check: if the last node emits a stream it must fold to the
	// graph output O at run time, so O needs a registered concatenator.
	if last := pnodes[len(pnodes)-1]; last.stream != nil {
		if _, ok := lookupConcat(g.outT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> output: no Concatenator for %s",
				order[len(order)-1], g.outT)
		}
	}

	return &linearPipeline{nodes: pnodes}, nil
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

// Events runs the compiled graph and returns the engine's LIFECYCLE event
// channel, boxing in into the engine input port. The channel is closed
// after the terminal event. The graph output is delivered via the
// FlowDone event's Outputs map (keyed by graphOutputKey) rather than
// unboxed to O — callers wanting the typed result should use Invoke. This
// is the engine-driven observability stream, distinct from the data-level
// Stream below.
func (r *Runnable[I, O]) Events(ctx context.Context, in I) (<-chan flow.Event, error) {
	return r.engine.RunStream(ctx, map[string]any{graphInputKey: in})
}

// Stream runs the compiled graph as a DATA stream, returning a
// StreamReader of the graph output O. Consumers MUST Close the reader
// (idempotent) to release the underlying llm stream and avoid leaks.
//
//   - Non-linear graph (pipeline == nil): degrade gracefully — run Invoke
//     to completion and box the single result into a one-element reader.
//     No error; the result is identical to Invoke, just delivered as a
//     stream.
//   - Linear chain: runLinearStream walks the pipeline so a streaming
//     ChatModel node streams end-to-end; the tail is retyped (stream) or
//     boxed (value) to StreamReader[O].
func (r *Runnable[I, O]) Stream(ctx context.Context, in I) (StreamReader[O], error) {
	if r.pipeline == nil {
		out, err := r.Invoke(ctx, in)
		if err != nil {
			return nil, err
		}
		return box(out), nil
	}
	return r.runLinearStream(ctx, in)
}

// runLinearStream is the dedicated data-stream execution path for a linear
// chain. It threads a streamCarrier from node to node WITHOUT touching the
// engine: each node either streamRun's (chatModelAdapter) or, when the
// upstream is a stream, has that stream folded to a value (drain + close)
// before the node's Run is invoked.
//
// Checkpoint note: while a streamCarrier has isStream == true the data is
// a live single-pass iterator and is NOT checkpoint-eligible. Only the
// seams where isStream == false hold a materialised, snapshottable value.
// Task 5 implements no checkpointing — this is documented intent only.
func (r *Runnable[I, O]) runLinearStream(ctx context.Context, in I) (StreamReader[O], error) {
	carrier := streamCarrier{value: in} // entry input is always a value
	for _, n := range r.pipeline.nodes {
		if n.stream != nil {
			// Stream node: it needs a materialised input. If upstream is a
			// stream, fold it to this node's In type first.
			if carrier.isStream {
				v, err := concatToValue(carrier, n.inT)
				if err != nil {
					return nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := n.stream.streamRun(ctx, carrier)
			if err != nil {
				return nil, err
			}
			carrier = out
			continue
		}
		// Value node: fold any upstream stream down to its In type, then
		// run the ordinary value path.
		if carrier.isStream {
			v, err := concatToValue(carrier, n.inT)
			if err != nil {
				return nil, err
			}
			carrier = streamCarrier{value: v}
		}
		out, err := n.kind.Run(ctx, map[string]any{portIn: carrier.value})
		if err != nil {
			return nil, err
		}
		carrier = streamCarrier{value: out[portOut]}
	}

	// Materialise the tail into StreamReader[O].
	if carrier.isStream {
		// The tail node emitted a stream. Its element type (carrier.elemT,
		// e.g. llm.StreamEvent) is generally NOT the graph output O (e.g.
		// llm.Response = the node's DECLARED out type, which Exit checked
		// against O). So fold the stream to O via O's concatenator,
		// lazily, so Close-before-Next still releases the upstream. The
		// folder was validated to exist at Compile time.
		fold, ok := lookupConcat(r.outT)
		if !ok {
			carrier.stream.Close()
			return nil, fmt.Errorf("graph: stream: no Concatenator for tail output %s", r.outT)
		}
		return foldTail[O](carrier.stream, fold), nil
	}
	typed, ok := carrier.value.(O)
	if !ok {
		return nil, fmt.Errorf("graph: stream: tail output is %T, not assignable to %s", carrier.value, r.outT)
	}
	return box(typed), nil
}
