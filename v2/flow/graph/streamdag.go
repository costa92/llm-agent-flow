package graph

import (
	"context"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// streamPlan is the compiled descriptor for a graph that streams as a
// BRANCH-DAG: a tree of value/stream nodes with at most branch fan-out, where
// exactly ONE route is taken at runtime. It is built by buildStreamPlan and
// executed by runStreamDAG. Like linearPipeline it NEVER references the engine.
//
// The plan is a read-only topology snapshot plus per-node prebuilt adapters.
// runStreamDAG walks it from entry following the single forward edge of each
// value/stream node, and — at a branchAdapter — only the SELECTED route's
// forward subgraph. Because there is no tee (a stream out-port feeding >1
// edge degrades to box(Invoke), never streams), the runtime path is a single
// dynamically-chosen chain, so the executor stays single-goroutine like
// runLinearStream.
//
// Registry-snapshot note (MAJOR-1): buildStreamPlan reads lookupConcat at
// Compile and snapshots folder availability into the plan implicitly (a
// missing folder for a streamable-branch graph is a Compile error here, so a
// built plan is guaranteed to have every folder it needs). RegisterConcatenator
// called AFTER Compile does NOT change an existing Runnable.
type streamPlan struct {
	nodes   map[string]*streamPlanNode // id -> node
	entryID string
	exitID  string
	outT    reflect.Type // graph output O (tail fold target)
}

// streamPlanNode is one node in a stream plan: its prebuilt adapter, declared
// types, the optional streamNode capability, and its outgoing edges keyed by
// the source port name. For a value/stream node there is exactly one outgoing
// edge on portOut (or zero, for the exit). For a branchAdapter there is one
// outgoing edge per route key (its fromPort).
type streamPlanNode struct {
	id     string
	kind   flow.NodeKind
	stream streamNode // non-nil iff kind implements streamNode
	branch bool       // true iff kind is a *branchAdapter[...]
	inT    reflect.Type
	outT   reflect.Type
	// out maps a source-port name to the single target node id reachable from
	// that port. portOut for ordinary nodes; route-key ports for a branch.
	out map[string]string
}

// buildStreamPlan analyses the graph for the streamable BRANCH-DAG shape and,
// when it matches, returns a plan. It mirrors buildLinearPipeline's 3-way
// contract:
//
//   - (plan, nil): the graph is a provable streamable branch-DAG (single
//     entry/exit, whitelisted node kinds, no fan-in, no tee) AND every
//     stream→value boundary and the tail has a registered folder.
//   - (nil, nil): degrade to box(Invoke) — any shape it can't PROVE
//     streamable (unknown node kind, fan-in, a tee-needing stream out-port,
//     parallel/combine). NO error.
//   - (nil, err): Compile error ONLY when the graph IS a streamable branch-DAG
//     but a required folder is missing (mirrors buildLinearPipeline).
//
// Linear graphs are intentionally left to buildLinearPipeline: buildStreamPlan
// returns (nil, nil) when the graph contains no branch node, so the linear
// path stays the byte-identical oracle.
func (g *Graph[I, O]) buildStreamPlan() (*streamPlan, error) {
	// Index nodes and their prebuilt adapters.
	type built struct {
		gn   graphNode
		kind flow.NodeKind
	}
	byID := make(map[string]built, len(g.nodes))
	hasBranch := false
	for _, n := range g.nodes {
		kind := n.newKind()
		// Whitelist: streamNode (chatModel), branchAdapter, or a value node
		// (lambda/template/tool/passthrough). Anything else (parallel/combine
		// or a future kind) → degrade.
		_, isStream := kind.(streamNode)
		_, isBranch := kind.(branchKind)
		switch {
		case isStream, isBranch:
		default:
			if !isValueNode(kind) {
				return nil, nil // unknown / non-whitelisted kind → degrade
			}
		}
		if isBranch {
			hasBranch = true
		}
		byID[n.ref.id] = built{gn: n, kind: kind}
	}
	// No branch → leave it to the linear path (or degrade if non-linear for
	// another reason). buildStreamPlan only owns the branch-DAG case.
	if !hasBranch {
		return nil, nil
	}

	// Reject combine fan-in and tees. A tee is a single SOURCE PORT feeding
	// >1 edge (a stream out-port feeding >1 downstream needs a Copy — cut this
	// milestone). A node whose multiple in-edges all originate from DISTINCT
	// branch routes is NOT a tee and is safe: in a whitelisted graph
	// ({value, stream, branch}) with no tee (per-port fan-out ≤1) and no
	// combine (toPort==portIn enforced below), the ONLY way a node can have
	// in-degree >1 is branch reconvergence — and a branch fires exactly one
	// route, so at most one in-edge of such a node ever delivers a value at
	// runtime. The executor walk prunes to that single route, so reconvergence
	// is sound and need NOT degrade. (parallel is not whitelisted → already
	// degraded; combine sets toPort!=portIn → degrades below.)
	portFanout := make(map[string]int) // key "node\x00port" -> edge count
	for _, e := range g.edges {
		if e.toPort != portIn {
			return nil, nil // fan-in (combine) → degrade
		}
		portFanout[e.from+"\x00"+e.fromPort]++
	}
	for _, cnt := range portFanout {
		if cnt > 1 {
			return nil, nil // a source port feeding >1 edge = tee → degrade
		}
	}

	// Build the adjacency (per source-port single target) and the plan nodes.
	plan := &streamPlan{
		nodes:   make(map[string]*streamPlanNode, len(g.nodes)),
		entryID: g.entry.id,
		exitID:  g.exit.id,
		outT:    g.outT,
	}
	for id, b := range byID {
		sn, _ := b.kind.(streamNode)
		_, isBranch := b.kind.(branchKind)
		plan.nodes[id] = &streamPlanNode{
			id:     id,
			kind:   b.kind,
			stream: sn,
			branch: isBranch,
			inT:    b.gn.ref.inT,
			outT:   b.gn.ref.outT,
			out:    make(map[string]string),
		}
	}
	for _, e := range g.edges {
		pn, ok := plan.nodes[e.from]
		if !ok {
			return nil, nil // edge from an unknown node → degrade
		}
		pn.out[e.fromPort] = e.to
	}

	// Reachability + single-component + acyclicity check from entry. Every node
	// must be reachable from entry and the walk must terminate at exit with no
	// cycle. A branch node's route ports each lead to a subgraph; we DFS all of
	// them (statically, for validation — runtime prunes to one).
	seen := make(map[string]bool, len(g.nodes))
	var reaches func(id string, stack map[string]bool) error
	reaches = func(id string, stack map[string]bool) error {
		if stack[id] {
			return errCycle
		}
		if seen[id] {
			return nil
		}
		seen[id] = true
		pn := plan.nodes[id]
		stack[id] = true
		for _, target := range pn.out {
			if _, ok := plan.nodes[target]; !ok {
				return errDangling
			}
			if err := reaches(target, stack); err != nil {
				return err
			}
		}
		delete(stack, id)
		return nil
	}
	if err := reaches(g.entry.id, map[string]bool{}); err != nil {
		return nil, nil // cycle / dangling → degrade (Invoke handles/errors it)
	}
	if len(seen) != len(g.nodes) {
		return nil, nil // disconnected component → degrade
	}
	// Exit must be a sink (no outgoing edges) and reachable.
	if exit, ok := plan.nodes[g.exit.id]; !ok || len(exit.out) != 0 {
		return nil, nil
	}

	// Folder validation across ALL possible routes (Compile-error escalation,
	// mirroring buildLinearPipeline). For every edge from a node that emits a
	// stream into a downstream value-consuming node, the downstream In type
	// needs a folder. And if the exit emits a stream, O needs a folder.
	for _, e := range g.edges {
		up := plan.nodes[e.from]
		down := plan.nodes[e.to]
		if up.stream == nil {
			continue // upstream emits a value, never a stream
		}
		// A branch never emits a stream (it passes through a value), and a
		// stream node's downstream always needs a materialised value (no node
		// re-consumes a raw stream). So the downstream In type must fold.
		if _, ok := lookupConcat(down.inT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> %q: no Concatenator for %s",
				e.from, e.to, down.inT)
		}
	}
	// Tail: if the exit node emits a stream, the graph output O must fold.
	if exit := plan.nodes[g.exit.id]; exit.stream != nil {
		if _, ok := lookupConcat(g.outT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> output: no Concatenator for %s",
				g.exit.id, g.outT)
		}
	}

	return plan, nil
}

// branchKind is the capability a branch node exposes to the stream planner: it
// reports its declared route-key output ports so the planner/executor can tell
// a branch apart from a value node. *branchAdapter[T] satisfies it.
type branchKind interface {
	flow.NodeKind
	routeKeys() []string
}

// isValueNode reports whether kind is a plain value node (lambda / passthrough
// / template / tool adapter) — i.e. a flow.NodeKind that is neither a
// streamNode nor a branch. Those are folded-on-input then Run by the executor.
func isValueNode(kind flow.NodeKind) bool {
	if _, ok := kind.(streamNode); ok {
		return false
	}
	if _, ok := kind.(branchKind); ok {
		return false
	}
	return true
}

// sentinel errors for the internal reachability walk (never surfaced).
var (
	errCycle    = fmt.Errorf("graph: stream plan: cycle")
	errDangling = fmt.Errorf("graph: stream plan: dangling edge")
)

// runStreamDAG executes a branch-DAG stream plan: walk from entry, fold-on-input
// at value nodes, re-emit a stream at a streamNode, and at a branch fold the
// input, run the branch to pick ONE route, then continue down ONLY that route's
// forward subgraph. Unselected routes are never allocated/run (pruning). The
// tail is folded (foldTail) when the exit streams and its elem type ≠ O, else
// boxed. Any in-flight stream is Closed on an error return via the shared
// live-carrier cleanup, so the error path never leaks.
func (r *Runnable[I, O]) runStreamDAG(ctx context.Context, in I) (_ StreamReader[O], retErr error) {
	plan := r.streamPlan
	carrier := streamCarrier{value: in} // entry input is always a value

	var live liveCarrier
	defer func() {
		if retErr != nil {
			live.closeIfLive()
		}
	}()

	cur := plan.entryID
	for {
		pn := plan.nodes[cur]

		switch {
		case pn.branch:
			// Fold any upstream stream to the branch's In type (a value), then
			// run the branch to select exactly one route, and follow it.
			if carrier.isStream {
				live.clear()
				v, err := concatToValue(carrier, pn.inT)
				if err != nil {
					return nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.kind.Run(ctx, map[string]any{portIn: carrier.value})
			if err != nil {
				return nil, err
			}
			// branchAdapter.Run emits exactly one {selectedPort: value} entry.
			selPort, selVal, ok := singleEntry(out)
			if !ok {
				return nil, fmt.Errorf("graph: stream: branch %q produced %d outputs, want exactly 1", pn.id, len(out))
			}
			target, ok := pn.out[selPort]
			if !ok {
				return nil, fmt.Errorf("graph: stream: branch %q selected port %q has no edge", pn.id, selPort)
			}
			carrier = streamCarrier{value: selVal}
			cur = target
			continue

		case pn.stream != nil:
			// Stream node: needs a materialised input; fold upstream if needed.
			if carrier.isStream {
				live.clear()
				v, err := concatToValue(carrier, pn.inT)
				if err != nil {
					return nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.stream.streamRun(ctx, carrier)
			if err != nil {
				return nil, err
			}
			carrier = out
			live.set(carrier)

		default:
			// Value node: fold upstream stream to its In type, then Run.
			if carrier.isStream {
				live.clear()
				v, err := concatToValue(carrier, pn.inT)
				if err != nil {
					return nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.kind.Run(ctx, map[string]any{portIn: carrier.value})
			if err != nil {
				return nil, err
			}
			carrier = streamCarrier{value: out[portOut]}
		}

		// Advance to the single forward edge (portOut). The exit is a sink.
		next, ok := pn.out[portOut]
		if !ok {
			break // reached the exit (sink)
		}
		cur = next
	}

	// The tail materialiser takes ownership of any live stream.
	live.clear()

	if carrier.isStream {
		fold, ok := lookupConcat(plan.outT)
		if !ok {
			carrier.stream.Close()
			return nil, fmt.Errorf("graph: stream: no Concatenator for tail output %s", plan.outT)
		}
		return foldTail[O](carrier.stream, fold), nil
	}
	typed, ok := carrier.value.(O)
	if !ok {
		return nil, fmt.Errorf("graph: stream: tail output is %T, not assignable to %s", carrier.value, plan.outT)
	}
	return box(typed), nil
}

// singleEntry returns the sole key/value of m and ok=true iff m has exactly one
// entry. A branchAdapter.Run emits exactly one selected port.
func singleEntry(m map[string]any) (key string, val any, ok bool) {
	if len(m) != 1 {
		return "", nil, false
	}
	for k, v := range m {
		return k, v, true
	}
	return "", nil, false
}
