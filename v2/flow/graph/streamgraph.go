package graph

import (
	"context"
	"fmt"
	"reflect"
	"sync"

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

// chainNode accessors for streamGraphNode (see streamdag.go for the
// streamPlanNode counterpart). asCopy is non-nil iff the node is a Copy
// fan-out — the boundary at which walkChain stops.
func (n *streamGraphNode) chainID() string          { return n.id }
func (n *streamGraphNode) chainKind() flow.NodeKind { return n.kind }
func (n *streamGraphNode) chainStream() streamNode  { return n.stream }
func (n *streamGraphNode) chainBranch() bool        { return n.branch }
func (n *streamGraphNode) chainInT() reflect.Type   { return n.inT }
func (n *streamGraphNode) chainCopy() copyKind      { return n.copy }
func (n *streamGraphNode) chainNext(port string) (string, bool) {
	t, ok := n.out[port]
	return t, ok
}
func (n *streamGraphNode) chainTargets() []string {
	ts := make([]string, 0, len(n.out))
	for _, t := range n.out {
		ts = append(ts, t)
	}
	return ts
}

// chainNode is the minimal node view walkChain threads over, satisfied by BOTH
// streamPlanNode (branch-DAG) and streamGraphNode (Copy-DAG). Factoring the walk
// behind it means there is exactly ONE node-step loop, so the branch-DAG path
// (runStreamDAG) and the Copy-DAG path (runStreamGraph) can never diverge on how
// a value/stream/branch node is stepped.
type chainNode interface {
	chainID() string
	chainKind() flow.NodeKind
	chainStream() streamNode
	chainBranch() bool
	chainInT() reflect.Type
	chainCopy() copyKind
	chainNext(port string) (string, bool)
	chainTargets() []string // every forward target id (all out ports)
}

// walkChain walks a SINGLE chain of the plan from startID, threading an entry
// carrier through value/stream/branch nodes exactly as the branch-DAG executor
// always has, registering each live stream in live for deferred cleanup. It
// STOPS and returns when it reaches either:
//
//   - a sink (no portOut edge): returns (carrier, nil, nil) — the caller
//     materialises the O-typed tail (foldTail[O]/box[O]) at the ROOT, once.
//   - a Copy node: returns (carrierIntoCopy, copyNode, nil) WITHOUT running the
//     copy — the caller tees and spawns one goroutine per target subtree.
//
// It is a SUBTREE primitive: it threads from and to a streamCarrier (never the
// graph input I or output O), so a Copy target goroutine can walk its subtree
// from its copyReader entry. ALL O-typed tail logic stays at the root (the
// divergence trap R6 names) — walkChain never touches O.
//
// nodeAt resolves a node id to its chainNode (closes over the concrete plan).
func walkChain(ctx context.Context, nodeAt func(string) chainNode, startID string, entry streamCarrier, live *liveSet) (streamCarrier, chainNode, error) {
	carrier := entry
	cur := startID
	for {
		pn := nodeAt(cur)

		// Copy boundary: stop without running it. The caller tees the carrier
		// flowing INTO the copy and walks each target subtree concurrently.
		if pn.chainCopy() != nil {
			return carrier, pn, nil
		}

		switch {
		case pn.chainBranch():
			// Fold any upstream stream to the branch's In type (a value), then
			// run the branch to select exactly one route, and follow it.
			if carrier.isStream {
				live.unregister(carrier.stream)
				v, err := concatToValue(carrier, pn.chainInT())
				if err != nil {
					return streamCarrier{}, nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.chainKind().Run(ctx, map[string]any{portIn: carrier.value})
			if err != nil {
				return streamCarrier{}, nil, err
			}
			selPort, selVal, ok := singleEntry(out)
			if !ok {
				return streamCarrier{}, nil, fmt.Errorf("graph: stream: branch %q produced %d outputs, want exactly 1", pn.chainID(), len(out))
			}
			target, ok := pn.chainNext(selPort)
			if !ok {
				return streamCarrier{}, nil, fmt.Errorf("graph: stream: branch %q selected port %q has no edge", pn.chainID(), selPort)
			}
			carrier = streamCarrier{value: selVal}
			cur = target
			continue

		case pn.chainStream() != nil:
			// Stream node: needs a materialised input; fold upstream if needed.
			if carrier.isStream {
				live.unregister(carrier.stream)
				v, err := concatToValue(carrier, pn.chainInT())
				if err != nil {
					return streamCarrier{}, nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.chainStream().streamRun(ctx, carrier)
			if err != nil {
				return streamCarrier{}, nil, err
			}
			carrier = out
			live.register(carrier.stream)

		default:
			// Value node: fold upstream stream to its In type, then Run.
			if carrier.isStream {
				live.unregister(carrier.stream)
				v, err := concatToValue(carrier, pn.chainInT())
				if err != nil {
					return streamCarrier{}, nil, err
				}
				carrier = streamCarrier{value: v}
			}
			out, err := pn.chainKind().Run(ctx, map[string]any{portIn: carrier.value})
			if err != nil {
				return streamCarrier{}, nil, err
			}
			carrier = streamCarrier{value: out[portOut]}
		}

		// Advance to the single forward edge (portOut). The exit is a sink.
		next, ok := pn.chainNext(portOut)
		if !ok {
			return carrier, nil, nil // reached the sink
		}
		cur = next
	}
}

// runStreamGraph executes a Copy-bearing stream-graph plan. It walks from entry
// via walkChain to the first Copy boundary; at a Copy node it materialises the
// upstream into a StreamReader[any], tees it with the Phase-2 Copy broadcaster,
// and spawns ONE goroutine PER TARGET SUBTREE, each walking its subtree
// concurrently from its own copyReader. Concurrency is mandatory: the
// broadcaster fills every consumer's buf-1 channel for each frame before
// advancing, so sequential subtree walks would deadlock (round-2 BLOCKER).
//
// The graph's single declared output comes from exactly ONE target subtree (the
// one on the path to the exit sink); the other subtrees are independent leaves
// driven to completion (or cancelled on early-Close). The O-typed tail is
// materialised at the ROOT, once.
func (r *Runnable[I, O]) runStreamGraph(ctx context.Context, in I) (_ StreamReader[O], retErr error) {
	plan := r.streamGraphPlan
	nodeAt := func(id string) chainNode {
		pn, ok := plan.nodes[id]
		if !ok {
			return nil
		}
		return pn
	}

	// One root cancel-ctx owns every subtree goroutine + broadcaster; cancelled
	// on any error return or via the returned reader's Close.
	ctx, cancel := context.WithCancel(ctx)
	live := &liveSet{}
	defer func() {
		if retErr != nil {
			cancel()
			live.closeAll()
		}
	}()

	carrier, copyNode, err := walkChain(ctx, nodeAt, plan.entryID, streamCarrier{value: in}, live)
	if err != nil {
		return nil, err
	}
	if copyNode == nil {
		// No Copy was actually reached at run time (a Copy could sit on an
		// unselected branch route). Materialise the tail like runStreamDAG; the
		// single live stream, if any, is now owned by the tail.
		reader, err := r.materialiseTail(carrier, live)
		if err != nil {
			return nil, err
		}
		cancel() // nothing concurrent to keep alive
		return reader, nil
	}

	// Reached a Copy boundary. Tee the carrier flowing into the copy and walk
	// each target subtree concurrently.
	tailCarrier, err := r.fanOutCopy(ctx, nodeAt, plan, copyNode, carrier, live)
	if err != nil {
		return nil, err
	}
	reader, err := r.materialiseTail(tailCarrier, live)
	if err != nil {
		return nil, err
	}
	// The returned reader owns the root ctx + live set: Close cancels everything.
	return &streamGraphReader[O]{inner: reader, cancel: cancel, live: live}, nil
}

// fanOutCopy tees the carrier flowing into copyNode and drives every target
// subtree CONCURRENTLY (one goroutine per non-tail target), returning the tail
// subtree's final carrier (walked on the calling goroutine). The tail target is
// the one whose subtree statically reaches the graph exit; the others are
// independent leaves driven to completion / cancellation.
//
// Concurrency is the round-2 BLOCKER fix: the broadcaster fills EVERY consumer
// channel (buf 1) for each frame before advancing, so all copyReaders must be
// pulled concurrently — never sibling subtrees one after another on one
// goroutine.
func (r *Runnable[I, O]) fanOutCopy(ctx context.Context, nodeAt func(string) chainNode, plan *streamGraphPlan, copyNode chainNode, in streamCarrier, live *liveSet) (streamCarrier, error) {
	ck := copyNode.chainCopy()
	keys := ck.copyKeys()
	n := len(keys)

	// Materialise the upstream into a StreamReader[any]: a value is boxed, a
	// live stream is teed directly. Its elem type drives the re-stamp on every
	// target carrier (copyReader erases it to chan copyItem[any]).
	var src StreamReader[any]
	var elemT reflect.Type
	if in.isStream {
		live.unregister(in.stream) // ownership moves to the broadcaster
		src = in.stream
		elemT = in.elemT
	} else {
		src = box(in.value)
		elemT = ck.copyElemT() // declared T (the boxed value's type)
	}

	// Tee. The broadcaster takes ownership of src (defer src.Close()).
	readers := Copy[any](src, n)
	for _, rd := range readers {
		live.register(rd) // EVERY copyReader registered independently (mandatory)
	}

	// Identify the single tail target (its subtree reaches the exit).
	tailIdx := -1
	for i, k := range keys {
		target, _ := copyNode.chainNext(k)
		if subtreeReaches(nodeAt, target, plan.exitID) {
			tailIdx = i
			break
		}
	}
	if tailIdx < 0 {
		// No target reaches the exit — the exit is unreachable through this copy.
		// Drain every reader so the broadcaster releases src, then error.
		for _, rd := range readers {
			rd.Close()
		}
		return streamCarrier{}, fmt.Errorf("graph: stream: copy %q: no target subtree reaches the exit", copyNode.chainID())
	}

	// Spawn one goroutine per NON-tail target: each walks its leaf subtree from
	// its copyReader (re-stamped elemT) to completion, draining/closing whatever
	// stream it ends on so the broadcaster can advance. We do NOT wait on them:
	// the tail drives the broadcaster and the leaves finish alongside; the root
	// ctx + liveSet guarantee teardown on error / early-Close.
	for i, k := range keys {
		if i == tailIdx {
			continue
		}
		target, _ := copyNode.chainNext(k)
		entry := streamCarrier{isStream: true, stream: readers[i], elemT: elemT}
		go func(target string, entry streamCarrier) {
			r.driveLeafSubtree(ctx, nodeAt, plan, target, entry, live)
		}(target, entry)
	}

	// Walk the TAIL subtree on this goroutine from its copyReader.
	tailTarget, _ := copyNode.chainNext(keys[tailIdx])
	tailEntry := streamCarrier{isStream: true, stream: readers[tailIdx], elemT: elemT}
	carrier, nextCopy, err := walkChain(ctx, nodeAt, tailTarget, tailEntry, live)
	if err != nil {
		return streamCarrier{}, err
	}
	if nextCopy != nil {
		// Nested Copy on the tail path: recurse.
		return r.fanOutCopy(ctx, nodeAt, plan, nextCopy, carrier, live)
	}
	return carrier, nil
}

// driveLeafSubtree walks a non-tail Copy target subtree to completion and
// discards its result, draining/closing any stream it ends on so the Copy
// broadcaster is not wedged waiting on this consumer. A nested Copy in the leaf
// recurses (its own tail leaf is drained here). Errors are swallowed: a leaf is
// not the graph output, and the root ctx + liveSet handle teardown; the leaf's
// streams are registered in live so a later closeAll releases them regardless.
func (r *Runnable[I, O]) driveLeafSubtree(ctx context.Context, nodeAt func(string) chainNode, plan *streamGraphPlan, startID string, entry streamCarrier, live *liveSet) {
	carrier, nextCopy, err := walkChain(ctx, nodeAt, startID, entry, live)
	if err != nil {
		return
	}
	if nextCopy != nil {
		// Nested copy in a leaf: fan it out too. Its tail carrier is itself a
		// leaf here — drain it.
		carrier, err = r.fanOutCopy(ctx, nodeAt, plan, nextCopy, carrier, live)
		if err != nil {
			return
		}
	}
	// Drain/close whatever the leaf ended on so the broadcaster can complete.
	if carrier.isStream && carrier.stream != nil {
		for {
			if _, err := carrier.stream.Next(); err != nil {
				break
			}
		}
		carrier.stream.Close()
		live.unregister(carrier.stream)
	}
}

// subtreeReaches reports whether the exit id is statically reachable from
// startID by following every node's forward edges (all ports). Used to pick the
// single Copy target subtree that produces the graph tail.
func subtreeReaches(nodeAt func(string) chainNode, startID, exitID string) bool {
	seen := map[string]bool{}
	var dfs func(id string) bool
	dfs = func(id string) bool {
		if id == exitID {
			return true
		}
		if seen[id] {
			return false
		}
		seen[id] = true
		pn := nodeAt(id)
		if pn == nil {
			return false
		}
		for _, t := range pn.chainTargets() {
			if dfs(t) {
				return true
			}
		}
		return false
	}
	return dfs(startID)
}

// materialiseTail turns the root walk's final carrier into a StreamReader[O],
// exactly like runStreamDAG / runLinearStream do at the tail: a stream folds to
// O via the registered concatenator (lazily, so Close-before-Next releases the
// upstream), a value is boxed. The tail stream's ownership moves to the
// returned reader, so it is unregistered from live.
func (r *Runnable[I, O]) materialiseTail(carrier streamCarrier, live *liveSet) (StreamReader[O], error) {
	if carrier.isStream {
		live.unregister(carrier.stream)
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

// streamGraphReader wraps the tail reader so the consumer's Close cancels the
// root ctx (stopping every subtree goroutine + the Copy broadcaster) and Closes
// every still-live stream in the set. Next delegates to the inner tail reader.
type streamGraphReader[O any] struct {
	inner    StreamReader[O]
	cancel   context.CancelFunc
	live     *liveSet
	closeOne sync.Once
}

func (s *streamGraphReader[O]) Next() (O, error) { return s.inner.Next() }

func (s *streamGraphReader[O]) Close() error {
	err := s.inner.Close()
	s.closeOne.Do(func() {
		s.cancel()
		s.live.closeAll()
	})
	return err
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
