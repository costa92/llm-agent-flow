package flow

import (
	"context"
	"errors"
	"fmt"
	"io"
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
}

// Compile validates the flow, resolves every Node type through the
// registry, then computes the topological layer ordering. Returns a
// runnable Engine on success.
func Compile(f Flow, reg *NodeRegistry, deps Deps) (*Engine, error) {
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
	return &Engine{
		flow:   f,
		deps:   deps,
		nodes:  nodes,
		layers: layers,
		preds:  preds,
	}, nil
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
func (e *Engine) RunStream(ctx context.Context, inputs map[string]string) (<-chan FlowEvent, error) {
	ch := make(chan FlowEvent, 8)
	go func() {
		defer close(ch)
		_, _ = e.run(ctx, inputs, ch)
	}()
	return ch, nil
}

// run is the shared core. ch may be nil for the sync path.
func (e *Engine) run(ctx context.Context, inputs map[string]string, ch chan<- FlowEvent) (map[string]string, error) {
	emit := func(ev FlowEvent) {
		if ch == nil {
			return
		}
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	}

	emit(FlowEvent{Kind: FlowStarted, FlowID: e.flow.ID})

	// portValues[nodeID][portName] = produced value. Populated by both
	// declared Flow.Inputs and by upstream node outputs.
	portValues := make(map[string]map[string]string, len(e.flow.Nodes))
	for _, ref := range e.flow.Inputs {
		v, ok := inputs[ref.Name]
		if !ok {
			err := fmt.Errorf("flow: run: missing required input %q", ref.Name)
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}
		if portValues[ref.Node] == nil {
			portValues[ref.Node] = make(map[string]string)
		}
		portValues[ref.Node][ref.Port] = v
	}

	for _, layer := range e.layers {
		for _, nodeID := range layer {
			if err := ctx.Err(); err != nil {
				emit(FlowEvent{Kind: FlowErr, Err: err})
				return nil, err
			}

			// Wire predecessor edges into the node's input map.
			in := portValues[nodeID]
			if in == nil {
				in = map[string]string{}
			}
			for _, edge := range e.preds[nodeID] {
				upstream := portValues[edge.Source.Node]
				if upstream == nil {
					err := fmt.Errorf("flow: run: node %q awaits %q but no value produced", nodeID, edge.Source.Node)
					emit(FlowEvent{Kind: FlowErr, Err: err})
					return nil, err
				}
				v, ok := upstream[edge.Source.Port]
				if !ok {
					err := fmt.Errorf("flow: run: node %q awaits port %q.%q but it was not emitted", nodeID, edge.Source.Node, edge.Source.Port)
					emit(FlowEvent{Kind: FlowErr, Err: err})
					return nil, err
				}
				in[edge.Target.Port] = v
			}
			portValues[nodeID] = in

			emit(FlowEvent{Kind: NodeStarted, NodeID: nodeID, Input: cloneStrMap(in)})

			out, err := e.nodes[nodeID].Run(ctx, in)
			if err != nil {
				wrapped := fmt.Errorf("flow: run: node %q: %w", nodeID, err)
				emit(FlowEvent{Kind: NodeFinished, NodeID: nodeID, Err: wrapped})
				emit(FlowEvent{Kind: FlowErr, Err: wrapped})
				return nil, wrapped
			}

			// Merge node outputs back into the port-value map so
			// downstream edges can find them on the next layer.
			if portValues[nodeID] == nil {
				portValues[nodeID] = make(map[string]string)
			}
			for k, v := range out {
				portValues[nodeID][k] = v
			}
			emit(FlowEvent{Kind: NodeFinished, NodeID: nodeID, Output: cloneStrMap(out)})
		}
	}

	// Collect declared outputs.
	outputs := make(map[string]string, len(e.flow.Outputs))
	for _, ref := range e.flow.Outputs {
		v, ok := portValues[ref.Node][ref.Port]
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
func LoadCompile(r io.Reader, reg *NodeRegistry, deps Deps) (*Engine, error) {
	f, err := Load(r)
	if err != nil {
		return nil, err
	}
	return Compile(f, reg, deps)
}
