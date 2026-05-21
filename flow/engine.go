package flow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/costa92/llm-agent/pkg/fanout"
)

// Engine compiles a parsed Flow against a NodeRegistry + Deps once,
// then can Run it many times. Compilation runs Validate, resolves
// every node through the registry, and computes a topological layer
// order.
//
// Compilation is one-shot — Engine instances are immutable post-Compile.
// Concurrency-safe Run / RunStream.
type Engine struct {
	flow   Flow
	deps   Deps
	nodes  map[string]NodeKind // resolved node runtime
	layers [][]string          // topological layers (node IDs)
	preds  map[string][]Edge   // incoming edges per node

	// maxNodeConcurrency caps the number of nodes that may execute
	// concurrently within a single layer. 0 means "unlimited — one
	// goroutine per node" (the default). The cap applies per layer
	// independently; layers themselves remain sequential.
	maxNodeConcurrency int
}

// EngineOption configures Compile.
type EngineOption func(*Engine)

// WithMaxNodeConcurrency caps the number of goroutines spawned inside
// a single topological layer. n <= 0 means unlimited. Layers remain
// sequential — a layer's nodes are scheduled only after the previous
// layer has fully completed.
func WithMaxNodeConcurrency(n int) EngineOption {
	return func(e *Engine) { e.maxNodeConcurrency = n }
}

// Compile validates the flow, resolves every Node type through the
// registry, then computes the topological layer ordering. Returns a
// runnable Engine on success.
func Compile(f Flow, reg *NodeRegistry, deps Deps, opts ...EngineOption) (*Engine, error) {
	if reg == nil {
		return nil, errors.New("flow: compile: nil registry")
	}
	if err := Validate(f); err != nil {
		return nil, err
	}
	nodes := make(map[string]NodeKind, len(f.Nodes))
	for _, n := range f.Nodes {
		kind, err := reg.Build(n, deps)
		if err != nil {
			return nil, err
		}
		nodes[n.ID] = kind
	}
	preds := make(map[string][]Edge, len(f.Nodes))
	indeg := make(map[string]int, len(f.Nodes))
	out := make(map[string][]string, len(f.Nodes))
	for _, n := range f.Nodes {
		indeg[n.ID] = 0
	}
	for _, e := range f.Edges {
		preds[e.Target.Node] = append(preds[e.Target.Node], e)
		out[e.Source.Node] = append(out[e.Source.Node], e.Target.Node)
		indeg[e.Target.Node]++
	}
	var layers [][]string
	queue := make([]string, 0)
	for _, n := range f.Nodes {
		if indeg[n.ID] == 0 {
			queue = append(queue, n.ID)
		}
	}
	visited := 0
	for len(queue) > 0 {
		layer := append([]string(nil), queue...)
		layers = append(layers, layer)
		next := queue[:0]
		for _, u := range layer {
			visited++
			for _, v := range out[u] {
				indeg[v]--
				if indeg[v] == 0 {
					next = append(next, v)
				}
			}
		}
		queue = make([]string, len(next))
		copy(queue, next)
	}
	if visited != len(f.Nodes) {
		// Validate already caught cycles; this branch protects against
		// IR drift introduced post-Validate.
		return nil, errors.New("flow: compile: residual cycle after validate")
	}
	eng := &Engine{
		flow:   f,
		deps:   deps,
		nodes:  nodes,
		layers: layers,
		preds:  preds,
	}
	for _, opt := range opts {
		opt(eng)
	}
	return eng, nil
}

// Run executes the flow synchronously with the caller-supplied inputs
// (keyed by Flow.Inputs[].Name) and returns the declared outputs
// (keyed by Flow.Outputs[].Name).
func (e *Engine) Run(ctx context.Context, inputs map[string]string) (map[string]string, error) {
	out, err := e.run(ctx, inputs, nil)
	return out, err
}

// RunStream executes the flow asynchronously and emits FlowEvents
// describing each node's lifecycle. The returned channel is closed
// after the terminal event (FlowDone or FlowErr). Callers MUST drain
// the channel to avoid blocking the engine; cancel ctx to abort.
//
// Within a layer, sibling node events (NodeStarted / NodeFinished)
// MAY interleave in arrival order. The contract is:
//
//   - FlowStarted is always the first event,
//   - FlowDone / FlowErr is always the last event (channel close immediately follows),
//   - For any given node, NodeStarted precedes NodeFinished,
//   - Across nodes in different layers, all events of an earlier layer
//     precede all events of a later layer.
func (e *Engine) RunStream(ctx context.Context, inputs map[string]string) (<-chan FlowEvent, error) {
	ch := make(chan FlowEvent, 16)
	go func() {
		defer close(ch)
		_, _ = e.run(ctx, inputs, ch)
	}()
	return ch, nil
}

// run is the shared core. ch may be nil for the sync path.
func (e *Engine) run(ctx context.Context, inputs map[string]string, ch chan<- FlowEvent) (map[string]string, error) {
	// emit serializes channel sends from multiple node goroutines so
	// consumers see one event at a time even though sibling nodes run
	// in parallel. The mutex also covers writes to portValues so a
	// node's outputs are visible to the next layer without further
	// synchronization.
	var emitMu sync.Mutex
	emit := func(ev FlowEvent) {
		if ch == nil {
			return
		}
		emitMu.Lock()
		defer emitMu.Unlock()
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	}

	emit(FlowEvent{Kind: FlowStarted, FlowID: e.flow.ID})

	// portValues[nodeID][portName] = produced value. Populated by both
	// declared Flow.Inputs and by upstream node outputs. Reads inside
	// a layer are safe because the previous layer has fully completed
	// before the current layer's goroutines start; writes for a node
	// happen on that node's own goroutine and are read-after-write
	// only on the NEXT layer.
	portValues := make(map[string]map[string]string, len(e.flow.Nodes))
	var pvMu sync.Mutex
	setPort := func(nodeID, port, value string) {
		pvMu.Lock()
		defer pvMu.Unlock()
		if portValues[nodeID] == nil {
			portValues[nodeID] = make(map[string]string)
		}
		portValues[nodeID][port] = value
	}
	getPorts := func(nodeID string) map[string]string {
		pvMu.Lock()
		defer pvMu.Unlock()
		// return a copy so callers can mutate without locking
		src := portValues[nodeID]
		if src == nil {
			return map[string]string{}
		}
		out := make(map[string]string, len(src))
		for k, v := range src {
			out[k] = v
		}
		return out
	}

	for _, ref := range e.flow.Inputs {
		v, ok := inputs[ref.Name]
		if !ok {
			err := fmt.Errorf("flow: run: missing required input %q", ref.Name)
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}
		setPort(ref.Node, ref.Port, v)
	}

	for _, layer := range e.layers {
		if err := ctx.Err(); err != nil {
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}

		// Build one fanout Task per node in this layer. Tasks return
		// the node's outputs map; errors are captured in the per-task
		// Result.Err and joined below. WithFailFast cancels peers as
		// soon as one task errors so long-running siblings exit early.
		tasks := make([]fanout.Task[map[string]string], 0, len(layer))
		layerIDs := make([]string, 0, len(layer))
		for _, nodeID := range layer {
			nodeID := nodeID
			node := e.nodes[nodeID]

			in := getPorts(nodeID)
			for _, edge := range e.preds[nodeID] {
				upstream := getPorts(edge.Source.Node)
				v, ok := upstream[edge.Source.Port]
				if !ok {
					err := fmt.Errorf("flow: run: node %q awaits port %q.%q but it was not emitted", nodeID, edge.Source.Node, edge.Source.Port)
					emit(FlowEvent{Kind: FlowErr, Err: err})
					return nil, err
				}
				in[edge.Target.Port] = v
			}
			// Persist resolved inputs so getPorts(nodeID) on the next
			// pass (e.g. an output port lookup by Flow.Outputs) finds
			// the same values.
			for k, v := range in {
				setPort(nodeID, k, v)
			}

			layerIDs = append(layerIDs, nodeID)
			tasks = append(tasks, func(taskCtx context.Context) (map[string]string, error) {
				emit(FlowEvent{Kind: NodeStarted, NodeID: nodeID, Input: cloneStrMap(in)})
				out, err := node.Run(taskCtx, in)
				if err != nil {
					wrapped := fmt.Errorf("flow: run: node %q: %w", nodeID, err)
					emit(FlowEvent{Kind: NodeFinished, NodeID: nodeID, Err: wrapped})
					return nil, wrapped
				}
				for k, v := range out {
					setPort(nodeID, k, v)
				}
				emit(FlowEvent{Kind: NodeFinished, NodeID: nodeID, Output: cloneStrMap(out)})
				return out, nil
			})
		}

		results, runErr := fanout.Run(ctx, e.maxNodeConcurrency, tasks, fanout.WithFailFast())
		if runErr != nil {
			emit(FlowEvent{Kind: FlowErr, Err: runErr})
			return nil, runErr
		}
		// If any task in this layer failed, surface the first error.
		// Sibling failures (peers also flagged by fail-fast cancel)
		// produce their own NodeFinished{Err} events already; the
		// terminal FlowErr is the first non-nil cause in input order.
		for i, r := range results {
			if r.Err != nil {
				// Skip ctx-canceled siblings caused by failfast; surface
				// only the original cause when possible.
				if errors.Is(r.Err, context.Canceled) && ctx.Err() == nil {
					continue
				}
				_ = layerIDs[i] // index documented for clarity
				emit(FlowEvent{Kind: FlowErr, Err: r.Err})
				return nil, r.Err
			}
		}
	}

	// Collect declared outputs.
	outputs := make(map[string]string, len(e.flow.Outputs))
	for _, ref := range e.flow.Outputs {
		ports := getPorts(ref.Node)
		v, ok := ports[ref.Port]
		if !ok {
			err := fmt.Errorf("flow: run: output %q awaits %q.%q but it was not emitted", ref.Name, ref.Node, ref.Port)
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}
		key := ref.Name
		if key == "" {
			key = ref.Node + "." + ref.Port
		}
		outputs[key] = v
	}

	emit(FlowEvent{Kind: FlowDone, Outputs: cloneStrMap(outputs)})
	return outputs, nil
}

func cloneStrMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// LoadCompile is a convenience for the common load-then-compile path.
// Equivalent to Load + Compile.
func LoadCompile(r io.Reader, reg *NodeRegistry, deps Deps, opts ...EngineOption) (*Engine, error) {
	f, err := Load(r)
	if err != nil {
		return nil, err
	}
	return Compile(f, reg, deps, opts...)
}
