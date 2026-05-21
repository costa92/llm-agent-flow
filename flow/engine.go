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
// every node through the registry, computes a topological layer
// order, and (if a ConditionEvaluator is configured) precompiles
// every Edge.Condition.
//
// Compilation is one-shot — Engine instances are immutable
// post-Compile. Concurrency-safe Run / RunStream.
type Engine struct {
	flow   Flow
	deps   Deps
	nodes  map[string]NodeKind // resolved node runtime
	layers [][]string          // topological layers (node IDs)
	preds  map[string][]Edge   // incoming edges per node

	// edgeCond[edgeIndexInFlow] is the precompiled guard for that
	// edge, or nil for an unconditional edge. Indexed by position in
	// flow.Edges so each edge gets a stable slot.
	edgeCond []Condition

	// maxNodeConcurrency caps the number of nodes that may execute
	// concurrently within a single topological layer. 0 means
	// "unlimited — one goroutine per node" (the default).
	maxNodeConcurrency int
}

// EngineOption configures Compile.
type EngineOption func(*engineConfig)

type engineConfig struct {
	maxNodeConcurrency int
	condEval           ConditionEvaluator
}

// WithMaxNodeConcurrency caps the number of goroutines spawned inside
// a single topological layer. n <= 0 means unlimited. Layers remain
// sequential — a layer's nodes are scheduled only after the previous
// layer has fully completed.
func WithMaxNodeConcurrency(n int) EngineOption {
	return func(c *engineConfig) { c.maxNodeConcurrency = n }
}

// WithConditionEvaluator installs a ConditionEvaluator that Compile
// uses to precompile every non-empty Edge.Condition. When unset, any
// flow containing a non-empty Edge.Condition fails to Compile —
// stdlib-only callers must opt into an evaluator explicitly.
func WithConditionEvaluator(e ConditionEvaluator) EngineOption {
	return func(c *engineConfig) { c.condEval = e }
}

// Compile validates the flow, resolves every Node type through the
// registry, precompiles edge conditions (if any) through the
// configured evaluator, then computes the topological layer ordering.
// Returns a runnable Engine on success.
func Compile(f Flow, reg *NodeRegistry, deps Deps, opts ...EngineOption) (*Engine, error) {
	if reg == nil {
		return nil, errors.New("flow: compile: nil registry")
	}
	if err := Validate(f); err != nil {
		return nil, err
	}
	cfg := engineConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	nodes := make(map[string]NodeKind, len(f.Nodes))
	for _, n := range f.Nodes {
		kind, err := reg.Build(n, deps)
		if err != nil {
			return nil, err
		}
		nodes[n.ID] = kind
	}

	edgeCond := make([]Condition, len(f.Edges))
	for i, e := range f.Edges {
		if e.Condition == "" {
			continue
		}
		if cfg.condEval == nil {
			return nil, fmt.Errorf("flow: compile: edge[%d] %q→%q has a Condition but no ConditionEvaluator was configured — use WithConditionEvaluator", i, e.Source.Node, e.Target.Node)
		}
		c, err := cfg.condEval.Compile(e.Condition)
		if err != nil {
			return nil, fmt.Errorf("flow: compile: edge[%d] condition %q: %w", i, e.Condition, err)
		}
		edgeCond[i] = c
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
		flow:               f,
		deps:               deps,
		nodes:              nodes,
		layers:             layers,
		preds:              preds,
		edgeCond:           edgeCond,
		maxNodeConcurrency: cfg.maxNodeConcurrency,
	}, nil
}

// Run executes the flow synchronously with the caller-supplied inputs
// (keyed by Flow.Inputs[].Name) and returns the declared outputs
// (keyed by Flow.Outputs[].Name).
//
// Outputs from nodes that were skipped (because no incoming edge
// fired) are omitted from the returned map rather than reported as an
// error — this is what makes conditional routing usable.
func (e *Engine) Run(ctx context.Context, inputs map[string]string) (map[string]string, error) {
	out, err := e.run(ctx, inputs, nil)
	return out, err
}

// RunStream executes the flow asynchronously and emits FlowEvents
// describing each node's lifecycle. The returned channel is closed
// after the terminal event (FlowDone or FlowErr). See the package
// doc and the FlowEvent type comment for the ordering contract.
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
	// in parallel.
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

	// portValues[nodeID][portName] = produced value. Populated by
	// declared Flow.Inputs and by fired-edge wiring. Writes serialized
	// through pvMu.
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

	// activated[nodeID] = true if the node will run. Entry nodes (no
	// incoming edges) are pre-activated. Targets of fired edges
	// become activated during layer iteration.
	activated := make(map[string]bool, len(e.flow.Nodes))
	for _, n := range e.flow.Nodes {
		if len(e.preds[n.ID]) == 0 {
			activated[n.ID] = true
		}
	}
	// Flow.Inputs always pre-activate their target nodes — they
	// receive a value from the caller, so the node has work to do
	// even if it has no incoming edges (which is the common entry-
	// node case).
	for _, ref := range e.flow.Inputs {
		v, ok := inputs[ref.Name]
		if !ok {
			err := fmt.Errorf("flow: run: missing required input %q", ref.Name)
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}
		setPort(ref.Node, ref.Port, v)
		activated[ref.Node] = true
	}

	for _, layer := range e.layers {
		if err := ctx.Err(); err != nil {
			emit(FlowEvent{Kind: FlowErr, Err: err})
			return nil, err
		}

		// Build one fanout Task per ACTIVE node. Inactive ones get a
		// NodeSkipped event emitted up-front so the stream still
		// references them; their outgoing edges will not fire.
		tasks := make([]fanout.Task[map[string]string], 0, len(layer))
		layerIDs := make([]string, 0, len(layer))
		for _, nodeID := range layer {
			nodeID := nodeID
			if !activated[nodeID] {
				emit(FlowEvent{Kind: NodeSkipped, NodeID: nodeID})
				continue
			}
			node := e.nodes[nodeID]
			in := getPorts(nodeID) // already populated by upstream layer's fired edges + Flow.Inputs

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
		for i, r := range results {
			if r.Err != nil {
				if errors.Is(r.Err, context.Canceled) && ctx.Err() == nil {
					continue
				}
				_ = layerIDs[i]
				emit(FlowEvent{Kind: FlowErr, Err: r.Err})
				return nil, r.Err
			}
		}

		// Layer is complete. Now fire edges OUT of each just-run
		// active node in this layer. An edge fires iff its source is
		// activated AND its Condition (if any) returns true.
		for _, srcID := range layer {
			if !activated[srcID] {
				continue
			}
			srcPorts := getPorts(srcID)
			for edgeIdx, edge := range e.flow.Edges {
				if edge.Source.Node != srcID {
					continue
				}
				value, ok := srcPorts[edge.Source.Port]
				if !ok {
					// Source ran but didn't emit this port. Don't
					// silently fire a value-less edge.
					continue
				}
				cond := e.edgeCond[edgeIdx]
				if cond != nil {
					fire, evalErr := cond.Evaluate(ctx, CondEnv{Value: value})
					if evalErr != nil {
						wrapped := fmt.Errorf("flow: run: edge[%d] (%s.%s → %s.%s) condition: %w",
							edgeIdx, edge.Source.Node, edge.Source.Port, edge.Target.Node, edge.Target.Port, evalErr)
						emit(FlowEvent{Kind: FlowErr, Err: wrapped})
						return nil, wrapped
					}
					if !fire {
						continue
					}
				}
				setPort(edge.Target.Node, edge.Target.Port, value)
				activated[edge.Target.Node] = true
			}
		}
	}

	// Collect declared outputs. Outputs whose node was skipped are
	// silently omitted so router-style flows can declare every branch
	// output and only the firing branch contributes.
	outputs := make(map[string]string, len(e.flow.Outputs))
	for _, ref := range e.flow.Outputs {
		if !activated[ref.Node] {
			continue
		}
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
