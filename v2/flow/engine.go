package flow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/costa92/llm-agent/pkg/fanout"
)

// Engine compiles a parsed Flow against a NodeRegistry + Deps once,
// then can Run it many times. Compilation runs Validate, resolves every
// node through the registry, computes a topological layer order, and
// (if a ConditionEvaluator is configured) precompiles every
// Edge.Condition.
//
// Compilation is one-shot — Engine instances are immutable post-Compile.
// Concurrency-safe Run / RunStream.
//
// This is a port of the v0.1 flow.Engine to the any port carrier. The
// scheduling semantics (per-node activation, layered fanout, edge-firing
// as a distinct post-fanout stage) are preserved verbatim — see run().
type Engine struct {
	flow   Flow
	deps   Deps
	nodes  map[string]NodeKind // resolved node runtime
	layers [][]string          // topological layers (node IDs)
	preds  map[string][]Edge   // incoming edges per node

	// edgeCond[edgeIndexInFlow] is the precompiled guard for that edge,
	// or nil for an unconditional edge. Indexed by position in
	// flow.Edges so each edge gets a stable slot.
	edgeCond []Condition

	// maxNodeConcurrency caps the number of nodes that may execute
	// concurrently within a single topological layer. 0 means
	// "unlimited — one goroutine per node" (the default).
	maxNodeConcurrency int

	// checkpointStore, when non-nil, enables the resumable engine path:
	// interrupt capture + suspend/resume via RunResumable / Resume.
	checkpointStore CheckpointStore

	// flowHash is the sha256 of the canonical v2 Flow JSON, used to
	// detect a changed flow definition on Resume.
	flowHash string

	// structHash is the sha256 of the canonical STRUCTURAL descriptor of
	// the compiled flow (node port sets + edge wiring, EXCLUDING
	// Node.Config). Used on Resume to allow a config-only change while
	// still rejecting any structural change. See structuralHash.
	structHash string
}

// EngineOption configures Compile.
type EngineOption func(*engineConfig)

type engineConfig struct {
	maxNodeConcurrency int
	condEval           ConditionEvaluator
	checkpointStore    CheckpointStore
}

// WithMaxNodeConcurrency caps the number of goroutines spawned inside a
// single topological layer. n <= 0 means unlimited. Layers remain
// sequential — a layer's nodes are scheduled only after the previous
// layer has fully completed.
func WithMaxNodeConcurrency(n int) EngineOption {
	return func(c *engineConfig) { c.maxNodeConcurrency = n }
}

// WithConditionEvaluator installs a ConditionEvaluator that Compile uses
// to precompile every non-empty Edge.Condition. When unset, any flow
// containing a non-empty Edge.Condition fails to Compile.
func WithConditionEvaluator(e ConditionEvaluator) EngineOption {
	return func(c *engineConfig) { c.condEval = e }
}

// Compile validates the flow, resolves every Node type through the
// registry, precompiles edge conditions (if any), then computes the
// topological layer ordering. Returns a runnable Engine on success.
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

	// FlowHash: sha256 of the canonical v2 Flow JSON. Used on Resume to
	// reject a checkpoint whose flow definition changed. A Marshal failure
	// must fail Compile rather than leave flowHash empty — an empty hash
	// would silently disable the ErrFlowChanged guard.
	b, err := Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("flow: compile: hash flow: %w", err)
	}
	sum := sha256.Sum256(b)
	flowHash := hex.EncodeToString(sum[:])

	// StructHash: sha256 of the canonical structural descriptor built from
	// the RESOLVED nodes (so a config-derived port-set change is caught)
	// plus the edge wiring. Computed here, after nodes are resolved and
	// layers are built, alongside flowHash.
	structHash, err := structuralHash(f, nodes)
	if err != nil {
		return nil, fmt.Errorf("flow: compile: %w", err)
	}

	// Static multi-interrupt check (RV-2): only when a CheckpointStore is
	// configured. At most one interrupt-capable node per layer — otherwise
	// the suspend point is ambiguous. Without a store, interrupt-capable
	// nodes degrade to ordinary nodes and the check is skipped.
	if cfg.checkpointStore != nil {
		for li, layer := range layers {
			count := 0
			for _, id := range layer {
				if ic, ok := nodes[id].(InterruptCapable); ok && ic.CanInterrupt() {
					count++
				}
			}
			if count > 1 {
				return nil, fmt.Errorf("%w: layer %d has %d interrupt-capable nodes", ErrMultipleInterrupts, li, count)
			}
		}
	}

	return &Engine{
		flow:               f,
		deps:               deps,
		nodes:              nodes,
		layers:             layers,
		preds:              preds,
		edgeCond:           edgeCond,
		maxNodeConcurrency: cfg.maxNodeConcurrency,
		checkpointStore:    cfg.checkpointStore,
		flowHash:           flowHash,
		structHash:         structHash,
	}, nil
}

// Run executes the flow synchronously with the caller-supplied inputs
// (keyed by Flow.Inputs[].Name) and returns the declared outputs (keyed
// by Flow.Outputs[].Name). This is the v2 any-carrier entry point.
//
// Outputs from nodes that were skipped (because no incoming edge fired)
// are omitted from the returned map rather than reported as an error —
// this is what makes conditional routing usable.
func (e *Engine) Run(ctx context.Context, inputs map[string]any) (map[string]any, error) {
	out, _, err := e.runCore(ctx, inputs, nil, "", false, nil, nil)
	return out, err
}

// RunStream executes the flow asynchronously and emits Events describing
// each node's lifecycle. The returned channel is closed after the
// terminal event (FlowDone or FlowErr).
func (e *Engine) RunStream(ctx context.Context, inputs map[string]any) (<-chan Event, error) {
	ch := make(chan Event, 16)
	go func() {
		defer close(ch)
		_, _, _ = e.runCore(ctx, inputs, ch, "", false, nil, nil)
	}()
	return ch, nil
}

// resumeSeed carries a restored snapshot into runCore so a suspended run
// can continue mid-graph.
type resumeSeed struct {
	activated  map[string]bool
	portValues map[string]map[string]any
	edgeStates []EdgeFireState
	startLayer int // resume from this layer index (== SuspendLayer+1)
}

// pendingInterrupt records one node's interrupt request captured during a
// layer's fanout.
type pendingInterrupt struct {
	nodeID string
	req    InterruptRequest
}

// runCore is the shared execution core. checkpoint==true enables interrupt
// capture + suspend. seed!=nil resumes from a restored snapshot (skips
// FlowStarted/Inputs init and starts at seed.startLayer). Returns
// (outputs, suspension, err): suspension is non-nil only when the run
// suspended on an interrupt.
// cell carries the run's State (nil when State is unused — the helpers are
// inert and there is zero behavior change for existing flows).
func (e *Engine) runCore(ctx context.Context, inputs map[string]any, ch chan<- Event, runID string, checkpoint bool, seed *resumeSeed, cell *stateCell) (map[string]any, *Suspension, error) {
	// emit serializes channel sends from multiple node goroutines so
	// consumers see one event at a time even though sibling nodes run in
	// parallel.
	var emitMu sync.Mutex
	emit := func(ev Event) {
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

	// portValues[nodeID][portName] = produced value. Populated by
	// declared Flow.Inputs and by fired-edge wiring. Writes serialized
	// through pvMu.
	portValues := make(map[string]map[string]any, len(e.flow.Nodes))
	var pvMu sync.Mutex
	setPort := func(nodeID, port string, value any) {
		pvMu.Lock()
		defer pvMu.Unlock()
		if portValues[nodeID] == nil {
			portValues[nodeID] = make(map[string]any)
		}
		portValues[nodeID][port] = value
	}
	getPorts := func(nodeID string) map[string]any {
		pvMu.Lock()
		defer pvMu.Unlock()
		src := portValues[nodeID]
		if src == nil {
			return map[string]any{}
		}
		out := make(map[string]any, len(src))
		for k, v := range src {
			out[k] = v
		}
		return out
	}

	// activated[nodeID] = true if the node will run. Entry nodes (no
	// incoming edges) are pre-activated. Targets of fired edges become
	// activated during layer iteration.
	activated := make(map[string]bool, len(e.flow.Nodes))
	var edgeStates []EdgeFireState
	var startLayer int

	if seed == nil {
		// Fresh run.
		emit(Event{Kind: FlowStarted, FlowID: e.flow.ID})
		for _, n := range e.flow.Nodes {
			if len(e.preds[n.ID]) == 0 {
				activated[n.ID] = true
			}
		}
		// Flow.Inputs always pre-activate their target nodes — they
		// receive a value from the caller, so the node has work to do
		// even if it has no incoming edges (the common entry-node case).
		for _, ref := range e.flow.Inputs {
			v, ok := inputs[ref.Name]
			if !ok {
				err := fmt.Errorf("flow: run: missing required input %q", ref.Name)
				emit(Event{Kind: FlowErr, Err: err})
				return nil, nil, err
			}
			setPort(ref.Node, ref.Port, v)
			activated[ref.Node] = true
		}
		edgeStates = make([]EdgeFireState, len(e.flow.Edges))
		startLayer = 0
	} else {
		// Resume: adopt the restored snapshot. Do NOT emit FlowStarted,
		// do NOT require/inject Flow.Inputs.
		for k, v := range seed.activated {
			activated[k] = v
		}
		for nodeID, ports := range seed.portValues {
			for port, v := range ports {
				setPort(nodeID, port, v)
			}
		}
		edgeStates = make([]EdgeFireState, len(seed.edgeStates))
		copy(edgeStates, seed.edgeStates)
		startLayer = seed.startLayer
	}

	// pending captures interrupt requests raised during a layer's fanout.
	var intMu sync.Mutex
	var pending []pendingInterrupt

	// fireLayerEdges fires the out-edges of the just-completed layer.
	// interruptNodeID != "" marks that node's out-edges EdgeDeferred
	// instead of firing them. Records each processed edge's EdgeFireState.
	// The per-edge `edgeStates[i] != EdgePending` guard makes a fresh run
	// byte-identical to the prior behavior while preventing resume from
	// re-firing already-processed edges. Returns an error on a conditional
	// edge eval failure / non-string conditional source (whole-run FlowErr).
	fireLayerEdges := func(layer []string, interruptNodeID string) error {
		for _, srcID := range layer {
			if interruptNodeID != "" && srcID == interruptNodeID {
				for i, edge := range e.flow.Edges {
					if edge.Source.Node == srcID && edgeStates[i] == EdgePending {
						edgeStates[i] = EdgeDeferred
					}
				}
				continue
			}
			if !activated[srcID] {
				for i, edge := range e.flow.Edges {
					if edge.Source.Node == srcID && edgeStates[i] == EdgePending {
						edgeStates[i] = EdgeDeadSrcSkip
					}
				}
				continue
			}
			srcPorts := getPorts(srcID)
			for i, edge := range e.flow.Edges {
				if edge.Source.Node != srcID {
					continue
				}
				if edgeStates[i] != EdgePending {
					continue
				}
				value, ok := srcPorts[edge.Source.Port]
				if !ok {
					// Source ran but didn't emit this port. Don't
					// silently fire a value-less edge.
					edgeStates[i] = EdgeDeadNoPort
					continue
				}
				cond := e.edgeCond[i]
				if cond != nil {
					// RV-5: the CEL/JSON route only routes on string
					// values. Project the any value to string for the
					// evaluator; a non-string value on a conditional
					// edge is a run-time error.
					sval, ok := value.(string)
					if !ok {
						return fmt.Errorf("flow: run: edge[%d] (%s.%s → %s.%s) condition: source value is %T, not string — conditional (CEL) edges route on string values only",
							i, edge.Source.Node, edge.Source.Port, edge.Target.Node, edge.Target.Port, value)
					}
					fire, evalErr := cond.Evaluate(ctx, CondEnv{Value: sval})
					if evalErr != nil {
						return fmt.Errorf("flow: run: edge[%d] (%s.%s → %s.%s) condition: %w",
							i, edge.Source.Node, edge.Source.Port, edge.Target.Node, edge.Target.Port, evalErr)
					}
					if !fire {
						edgeStates[i] = EdgeDeadFalse
						continue
					}
				}
				setPort(edge.Target.Node, edge.Target.Port, value)
				activated[edge.Target.Node] = true
				edgeStates[i] = EdgeFired
			}
		}
		return nil
	}

	for layerIdx := startLayer; layerIdx < len(e.layers); layerIdx++ {
		layer := e.layers[layerIdx]
		if err := ctx.Err(); err != nil {
			emit(Event{Kind: FlowErr, Err: err})
			return nil, nil, err
		}

		// Build one fanout Task per ACTIVE node. Inactive ones get a
		// NodeSkipped event emitted up-front so the stream still
		// references them; their outgoing edges will not fire.
		tasks := make([]fanout.Task[map[string]any], 0, len(layer))
		layerIDs := make([]string, 0, len(layer))
		for _, nodeID := range layer {
			nodeID := nodeID
			if !activated[nodeID] {
				emit(Event{Kind: NodeSkipped, NodeID: nodeID})
				continue
			}
			node := e.nodes[nodeID]
			in := getPorts(nodeID) // populated by upstream layer's fired edges + Flow.Inputs

			layerIDs = append(layerIDs, nodeID)
			tasks = append(tasks, func(taskCtx context.Context) (map[string]any, error) {
				emit(Event{Kind: NodeStarted, NodeID: nodeID, Input: cloneAnyMap(in)})
				// Bind this node's id + the run's State cell into ctx so
				// SetState stages under the right node. fanout.Run hands
				// every sibling ONE shared runCtx, so the per-node id can
				// only be set HERE, in the engine closure. A nil cell makes
				// the helpers inert.
				nodeCtx := withStateNode(taskCtx, cell, nodeID)
				out, err := node.Run(nodeCtx, in)
				if err != nil {
					var ie *InterruptError
					if checkpoint && errors.As(err, &ie) {
						ic, ok := node.(InterruptCapable)
						if !ok || !ic.CanInterrupt() {
							wrapped := fmt.Errorf("flow: run: node %q: %w", nodeID, ErrUninstrumentedInterrupt)
							emit(Event{Kind: NodeFinished, NodeID: nodeID, Err: wrapped})
							return nil, wrapped // real error → fails the run via failfast
						}
						intMu.Lock()
						pending = append(pending, pendingInterrupt{nodeID, ie.Request})
						intMu.Unlock()
						emit(Event{Kind: NodeInterrupted, NodeID: nodeID, Request: &ie.Request})
						return nil, nil // return nil so failfast lets siblings drain
					}
					wrapped := fmt.Errorf("flow: run: node %q: %w", nodeID, err)
					emit(Event{Kind: NodeFinished, NodeID: nodeID, Err: wrapped})
					return nil, wrapped
				}
				for k, v := range out {
					setPort(nodeID, k, v)
				}
				emit(Event{Kind: NodeFinished, NodeID: nodeID, Output: cloneAnyMap(out)})
				return out, nil
			})
		}

		results, runErr := fanout.Run(ctx, e.maxNodeConcurrency, tasks, fanout.WithFailFast())
		if runErr != nil {
			emit(Event{Kind: FlowErr, Err: runErr})
			return nil, nil, runErr
		}
		for i, r := range results {
			if r.Err != nil {
				if errors.Is(r.Err, context.Canceled) && ctx.Err() == nil {
					continue
				}
				_ = layerIDs[i]
				emit(Event{Kind: FlowErr, Err: r.Err})
				return nil, nil, r.Err
			}
		}

		// Per-layer State barrier reduce. Pinned AFTER the error checks
		// (an erroring layer early-returns above → commits no State,
		// sibling-error-wins preserved) and BEFORE the suspend/checkpoint
		// splice (so a checkpoint snapshots POST-reduce State). reduce
		// acquires cell.mu and runs on the single engine goroutine, so a
		// node leaking a goroutine that outlives Run cannot race the staged
		// map. A default-reducer collision is treated as a layer error.
		if cell != nil {
			if err := cell.reduce(); err != nil {
				emit(Event{Kind: FlowErr, Err: err})
				return nil, nil, err
			}
		}

		// Suspend splice: if any node in this layer requested an
		// interrupt, fire the non-interrupt siblings' edges, defer the
		// interrupt node's, snapshot a checkpoint, and stop.
		if checkpoint {
			intMu.Lock()
			pend := pending
			pending = nil
			intMu.Unlock()
			if len(pend) > 0 {
				if e.checkpointStore == nil {
					emit(Event{Kind: FlowErr, Err: ErrNoCheckpointStore})
					return nil, nil, ErrNoCheckpointStore
				}
				if len(pend) > 1 {
					emit(Event{Kind: FlowErr, Err: ErrMultipleInterrupts})
					return nil, nil, ErrMultipleInterrupts // no checkpoint written
				}
				if err := fireLayerEdges(layer, pend[0].nodeID); err != nil {
					emit(Event{Kind: FlowErr, Err: err})
					return nil, nil, err // sibling cond eval error → no checkpoint
				}
				cp, err := e.snapshotCheckpoint(runID, layerIdx, pend[0], activated, portValues, edgeStates, inputs)
				if err != nil {
					emit(Event{Kind: FlowErr, Err: err})
					return nil, nil, err // e.g. ErrNotCheckpointable
				}
				if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
					emit(Event{Kind: FlowErr, Err: err})
					return nil, nil, err
				}
				susp := &Suspension{ResumeToken: cp.RunID, NodeID: pend[0].nodeID, Request: pend[0].req}
				emit(Event{Kind: FlowSuspended, NodeID: pend[0].nodeID, ResumeToken: cp.RunID, Request: &pend[0].req})
				return nil, susp, nil
			}
		}

		// Layer is complete. Now fire edges OUT of each just-run active
		// node in this layer. An edge fires iff its source is activated
		// AND its Condition (if any) returns true.
		if err := fireLayerEdges(layer, ""); err != nil {
			emit(Event{Kind: FlowErr, Err: err})
			return nil, nil, err
		}
	}

	// Collect declared outputs. Outputs whose node was skipped are
	// silently omitted so router-style flows can declare every branch
	// output and only the firing branch contributes.
	outputs := make(map[string]any, len(e.flow.Outputs))
	for _, ref := range e.flow.Outputs {
		if !activated[ref.Node] {
			continue
		}
		ports := getPorts(ref.Node)
		v, ok := ports[ref.Port]
		if !ok {
			err := fmt.Errorf("flow: run: output %q awaits %q.%q but it was not emitted", ref.Name, ref.Node, ref.Port)
			emit(Event{Kind: FlowErr, Err: err})
			return nil, nil, err
		}
		key := ref.Name
		if key == "" {
			key = ref.Node + "." + ref.Port
		}
		outputs[key] = v
	}

	emit(Event{Kind: FlowDone, Outputs: cloneAnyMap(outputs)})
	return outputs, nil, nil
}

// snapshotCheckpoint builds a serializable Checkpoint from the live run
// state at a suspend point. It runs AFTER the layer's fanout has joined,
// so no node goroutines are live — a plain read of portValues is safe.
func (e *Engine) snapshotCheckpoint(runID string, layerIdx int, pend pendingInterrupt,
	activated map[string]bool, portValues map[string]map[string]any,
	edgeStates []EdgeFireState, inputs map[string]any) (Checkpoint, error) {

	pv := make(map[string]map[string]json.RawMessage, len(portValues))
	for nodeID, ports := range portValues {
		inner := make(map[string]json.RawMessage, len(ports))
		for port, v := range ports {
			raw, err := encodePortValue(v)
			if err != nil {
				return Checkpoint{}, fmt.Errorf("flow: checkpoint: node %q port %q: %w", nodeID, port, err)
			}
			inner[port] = raw
		}
		pv[nodeID] = inner
	}

	act := make(map[string]bool, len(activated))
	for k, v := range activated {
		act[k] = v
	}
	es := make([]EdgeFireState, len(edgeStates))
	copy(es, edgeStates)

	var origInputs map[string]json.RawMessage
	if inputs != nil {
		origInputs = make(map[string]json.RawMessage, len(inputs))
		for k, v := range inputs {
			raw, err := encodePortValue(v)
			if err != nil {
				return Checkpoint{}, fmt.Errorf("flow: checkpoint: input %q: %w", k, err)
			}
			origInputs[k] = raw
		}
	}

	return Checkpoint{
		Version:         2,
		RunID:           runID,
		FlowID:          e.flow.ID,
		FlowHash:        e.flowHash,
		StructHash:      e.structHash,
		PortValues:      pv,
		Activated:       act,
		EdgeStates:      es,
		SuspendLayer:    layerIdx,
		InterruptNodeID: pend.nodeID,
		InterruptReq:    pend.req,
		OriginalInputs:  origInputs,
		CreatedAt:       time.Now(),
	}, nil
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
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
