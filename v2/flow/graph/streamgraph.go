package graph

import (
	"context"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// streamGraphPlan is the compiled descriptor for a Copy-bearing streamable DAG:
// a graph that streams with explicit in-graph fan-out via AddCopy nodes. It is
// built by buildStreamGraphPlan (tried AFTER buildStreamPlan degrades) and
// executed by runStreamGraph (a multi-goroutine executor — one goroutine per
// Copy target subtree). Like streamPlan it NEVER references the engine /
// runCore / checkpoints / resume.
//
// The single-route (branch-DAG) and linear shapes stay on their own
// single-goroutine executors; only a graph that contains a Copy node reaches
// this plan, so the byte-identical oracle paths are untouched.
type streamGraphPlan struct {
	nodes   map[string]*streamGraphNode // id -> node
	entryID string
	exitID  string
	outT    reflect.Type // graph output O (root tail fold target)
}

// streamGraphNode is one node in a stream-graph plan: its prebuilt adapter, the
// optional streamNode / copy / branch capability flags, declared types, and its
// outgoing edges keyed by the source-port name (portOut for ordinary nodes;
// route-key ports for a branch; target-key ports for a copy).
type streamGraphNode struct {
	id     string
	kind   flow.NodeKind
	stream streamNode // non-nil iff kind implements streamNode
	copy   copyKind   // non-nil iff kind is a *copyAdapter[...]
	branch bool       // true iff kind is a branch
	inT    reflect.Type
	outT   reflect.Type
	// out maps a source-port name to the single target node id reachable from
	// that port. portOut for ordinary nodes; target-key ports for a copy.
	out map[string]string
}

// buildStreamGraphPlan analyses the graph for the Copy-bearing streamable-DAG
// shape and, when it matches, returns a plan. It is tried by Compile ONLY after
// buildStreamPlan returns (nil, nil), and follows the same 3-way contract:
//
//   - (plan, nil): the graph contains ≥1 Copy node, every node kind is
//     whitelisted {streamNode, value-node, branch, copy}, has no implicit
//     raw-stream tee (per-port fan-out ≤1), is a single acyclic component, and
//     every stream→value boundary has a registered folder.
//   - (nil, nil): degrade to box(Invoke) — no Copy node, an unknown kind, an
//     implicit tee, a cycle, or a disconnected component. NO error.
//   - (nil, err): Compile error ONLY when the graph IS a streamable Copy-DAG
//     but a required folder is missing (mirrors buildStreamPlan).
func (g *Graph[I, O]) buildStreamGraphPlan() (*streamGraphPlan, error) {
	type built struct {
		gn   graphNode
		kind flow.NodeKind
	}
	byID := make(map[string]built, len(g.nodes))
	hasCopy := false
	for _, n := range g.nodes {
		kind := n.newKind()
		// Whitelist: streamNode (chatModel), copy fan-out, branch, or a value
		// node (lambda/template/tool/passthrough). Anything else → degrade.
		_, isStream := kind.(streamNode)
		_, isCopy := kind.(copyKind)
		_, isBranch := kind.(branchKind)
		switch {
		case isStream, isCopy, isBranch:
		default:
			if !isValueNode(kind) {
				return nil, nil // unknown / non-whitelisted kind → degrade
			}
		}
		if isCopy {
			hasCopy = true
		}
		byID[n.ref.id] = built{gn: n, kind: kind}
	}
	// No Copy node → not our shape. buildStreamPlan already had its chance
	// (linear keeps its pipeline; branch-DAG keeps streamPlan); degrade.
	if !hasCopy {
		return nil, nil
	}

	// Reject fan-in (combine: toPort != portIn) and implicit raw-stream tees (a
	// single SOURCE PORT feeding >1 edge). A Copy node fans out via DISTINCT
	// target-key ports (each feeding exactly one edge), so per-port fan-out
	// stays ≤1; an implicit tee (e.g. a chatmodel out-port feeding 2 edges with
	// NO AddCopy) violates it and degrades — the explicit-node requirement.
	portFanout := make(map[string]int) // "node\x00port" -> edge count
	for _, e := range g.edges {
		if e.toPort != portIn {
			return nil, nil // fan-in (combine) → degrade
		}
		portFanout[e.from+"\x00"+e.fromPort]++
	}
	for _, cnt := range portFanout {
		if cnt > 1 {
			return nil, nil // a source port feeding >1 edge = implicit tee → degrade
		}
	}

	// Build the adjacency and the plan nodes.
	plan := &streamGraphPlan{
		nodes:   make(map[string]*streamGraphNode, len(g.nodes)),
		entryID: g.entry.id,
		exitID:  g.exit.id,
		outT:    g.outT,
	}
	for id, b := range byID {
		sn, _ := b.kind.(streamNode)
		ck, _ := b.kind.(copyKind)
		_, isBranch := b.kind.(branchKind)
		plan.nodes[id] = &streamGraphNode{
			id:     id,
			kind:   b.kind,
			stream: sn,
			copy:   ck,
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

	// Reachability + single-component + acyclicity from entry (static walk over
	// ALL ports of every node, including each Copy target port).
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
		return nil, nil // cycle / dangling → degrade
	}
	if len(seen) != len(g.nodes) {
		return nil, nil // disconnected component → degrade
	}
	// Exit must be a reachable sink (no outgoing edges).
	if exit, ok := plan.nodes[g.exit.id]; !ok || len(exit.out) != 0 {
		return nil, nil
	}

	// Folder validation across the whole DAG (Compile-error escalation,
	// mirroring buildStreamPlan). For every edge whose upstream MAY emit a
	// stream into a value-consuming downstream, the downstream In type needs a
	// folder. Streaminess propagates: a streamNode emits a stream; a Copy node
	// emits a stream iff its (single) upstream does; a branch/value node never
	// emits a raw stream downstream (it folds-on-input). We compute the
	// stream-emitting set by propagating from streamNodes through Copy nodes.
	emitsStream := g.streamEmittingNodes(plan)
	for _, e := range g.edges {
		up := plan.nodes[e.from]
		down := plan.nodes[e.to]
		if !emitsStream[up.id] {
			continue // upstream emits a value, never a stream
		}
		if _, ok := lookupConcat(down.inT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> %q: no Concatenator for %s",
				e.from, e.to, down.inT)
		}
	}
	// Tail: if the exit node may emit a stream, the graph output O must fold.
	if emitsStream[g.exit.id] {
		if _, ok := lookupConcat(g.outT); !ok {
			return nil, fmt.Errorf("graph: compile: stream edge %q -> output: no Concatenator for %s",
				g.exit.id, g.outT)
		}
	}

	return plan, nil
}

// runStreamGraph executes a Copy-bearing stream-graph plan. The executor body
// (walkChain extraction + concurrent Copy fan-out via the Phase-2 broadcaster)
// lands in task 2c; this stub keeps the Stream dispatch wired so 2b can
// classify and assert StreamCapable without yet driving a fan-out.
func (r *Runnable[I, O]) runStreamGraph(_ context.Context, _ I) (StreamReader[O], error) {
	return nil, fmt.Errorf("graph: stream: runStreamGraph not yet implemented (task 2c)")
}

// streamEmittingNodes returns the set of node ids that may emit a raw data
// stream to their downstream edges: every streamNode, plus every Copy node
// transitively fed by a stream-emitting node (a Copy tees its upstream stream
// to each target). Branch and value nodes fold-on-input and never emit a raw
// stream, so they are not in the set.
func (g *Graph[I, O]) streamEmittingNodes(plan *streamGraphPlan) map[string]bool {
	emits := make(map[string]bool, len(plan.nodes))
	// Predecessor map: a node's upstream is the (single, non-tee) source feeding
	// its portIn. Copy streaminess depends on this single predecessor.
	pred := make(map[string]string, len(plan.nodes))
	for _, pn := range plan.nodes {
		for _, target := range pn.out {
			pred[target] = pn.id
		}
	}
	// Seed: streamNodes always emit a stream.
	for id, pn := range plan.nodes {
		if pn.stream != nil {
			emits[id] = true
		}
	}
	// Propagate through Copy nodes: a Copy emits a stream iff its predecessor
	// does. Iterate to a fixed point (a Copy may feed another Copy).
	for changed := true; changed; {
		changed = false
		for id, pn := range plan.nodes {
			if pn.copy == nil || emits[id] {
				continue
			}
			if p, ok := pred[id]; ok && emits[p] {
				emits[id] = true
				changed = true
			}
		}
	}
	return emits
}
