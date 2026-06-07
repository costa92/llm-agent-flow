package graph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// merge.go holds the streaming fan-in family: AddStreamMerge (UNORDERED,
// interleaved by arrival) and AddZip (ORDERED, round-robin). They are the
// fan-in duals of AddCopy: N sources of the same element type T converge on a
// single node whose user combine folds the gathered values into R.
//
// Value path (Invoke): both adapters gather the PRESENT source ports in stable
// sorted-key order and apply combine. On Invoke each source contributes exactly
// one value of T; AddStreamMerge gathers them into []T, AddZip into [][]T (one
// 1-element slice per source — the degenerate single-frame case of the stream
// path's per-source frame lists).
//
// Stream path: driven by runStreamGraph's fanInMerge (see streamgraph.go), which
// pulls every source subtree CONCURRENTLY into a StreamReader[any], interleaves
// them with Merge[any]/roundRobinMerge[any], then folds the merged stream back
// to []T / [][]T at the merge boundary and calls the SAME combine. The combine
// is therefore the single value-path/stream-path contract for an R result.
//
// Both nodes are multi-input (inPorts = {key: src.outT == T}) wired via
// AddEdgeTo on named source-key ports, and in-process only (a Go adapter that
// holds a Go combine closure → non-serializable → non-checkpointable), like
// AddCombine and AddCopy.

// AddStreamMerge adds an UNORDERED streaming fan-in node. Its N sources must all
// carry the same element type T. On Invoke it gathers the present source values
// in stable sorted-key order into []T and applies combine to produce R. On the
// stream path each source's stream is interleaved by arrival (Phase-2 Merge),
// drained to []T at the merge boundary, and the same combine produces R. inPorts
// = {key: src.outT (== T)}, outT = R.
func AddStreamMerge[GI, GO, T, R any](
	g *Graph[GI, GO], id string,
	sources map[string]NodeRef,
	combine func(ctx context.Context, items []T) (R, error),
) (NodeRef, error) {
	keys, inPorts, err := validateMergeSources[GI, GO, T](g, "stream merge", id, sources, combine == nil)
	if err != nil {
		return NodeRef{}, err
	}

	elemT := reflect.TypeFor[T]()
	outT := reflect.TypeFor[R]()
	newKind := func() flow.NodeKind {
		return &streamMergeAdapter[T, R]{id: id, keys: keys, inPorts: inPorts, elemT: elemT, outT: outT, combine: combine}
	}
	return finishMergeNode(g, id, sources, inPorts, outT, newKind)
}

// AddZip adds an ORDERED streaming fan-in node via round-robin interleave. Its N
// sources must all carry the same element type T. On Invoke it gathers each
// source's value (sorted-key) into a per-source [][]T — one 1-element slice per
// source — and applies combine to produce R. On the stream path each source's
// stream is round-robin interleaved (roundRobinMerge), drained per source into
// [][]T at the merge boundary, and the same combine produces R. inPorts = {key:
// src.outT (== T)}, outT = R.
func AddZip[GI, GO, T, R any](
	g *Graph[GI, GO], id string,
	sources map[string]NodeRef,
	combine func(ctx context.Context, perSource [][]T) (R, error),
) (NodeRef, error) {
	keys, inPorts, err := validateMergeSources[GI, GO, T](g, "zip", id, sources, combine == nil)
	if err != nil {
		return NodeRef{}, err
	}

	elemT := reflect.TypeFor[T]()
	outT := reflect.TypeFor[R]()
	newKind := func() flow.NodeKind {
		return &zipAdapter[T, R]{id: id, keys: keys, inPorts: inPorts, elemT: elemT, outT: outT, combine: combine}
	}
	return finishMergeNode(g, id, sources, inPorts, outT, newKind)
}

// validateMergeSources checks the shared build-time contract for both merge
// constructors: a non-nil combine, ≥1 source, no empty keys, and HOMOGENEOUS
// element type — every source's outT must be exactly T (heterogeneous-T fan-in
// is AddCombine's job, not a streaming merge). It returns the sorted source keys
// and the inPorts map.
func validateMergeSources[GI, GO, T any](g *Graph[GI, GO], what, id string, sources map[string]NodeRef, nilCombine bool) ([]string, map[string]reflect.Type, error) {
	if nilCombine {
		return nil, nil, g.latch(fmt.Errorf("graph: %s %q: nil combine func", what, id))
	}
	if len(sources) == 0 {
		return nil, nil, g.latch(fmt.Errorf("graph: %s %q: no sources", what, id))
	}
	elemT := reflect.TypeFor[T]()
	inPorts := make(map[string]reflect.Type, len(sources))
	keys := make([]string, 0, len(sources))
	for k, src := range sources {
		if k == "" {
			return nil, nil, g.latch(fmt.Errorf("graph: %s %q: empty source key", what, id))
		}
		// Homogeneous-T: every source must emit exactly T. A streaming merge
		// interleaves a single element type; mixing types is a different node.
		if src.outT != elemT {
			return nil, nil, g.latch(fmt.Errorf("graph: %s %q source %q -> %q: source emits %s, want %s (sources must be homogeneous %s)",
				what, id, k, src.id, src.outT, elemT, elemT))
		}
		inPorts[k] = elemT
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, inPorts, nil
}

// finishMergeNode registers the merge node, marks it in-process, sets inPorts on
// the ref, and wires one edge per source (src.out -> merge.<key>) like
// AddCombine. The merge plan/executor recognises the node via mergeKind.
func finishMergeNode[GI, GO any](g *Graph[GI, GO], id string, sources map[string]NodeRef, inPorts map[string]reflect.Type, outT reflect.Type, newKind func() flow.NodeKind) (NodeRef, error) {
	// inT left zero: a multi-input node resolves port types via inPorts.
	ref, err := g.addNode(id, nil, outT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	ref.inPorts = inPorts
	g.byID[id] = ref
	g.markInProcess(id, "streaming merge fan-in node (in-process only)")

	// Register one edge per source: source.out -> merge.<key>.
	for k, src := range sources {
		g.edges = append(g.edges, graphEdge{from: src.id, to: id, fromPort: portOut, toPort: k})
	}
	return ref, nil
}

// mergeKind is the capability a streaming merge node exposes to the stream
// planner / executor: its declared source-key input ports, the element type T it
// interleaves, and whether it is ORDERED (zip, round-robin) or UNORDERED
// (stream merge, arrival interleave). It is a DISTINCT interface — NOT satisfied
// by combineAdapter — so AddCombine fan-in still degrades to box(Invoke).
// *streamMergeAdapter[T,R] and *zipAdapter[T,R] satisfy it.
type mergeKind interface {
	flow.NodeKind
	mergeKeys() []string
	mergeElemT() reflect.Type
	mergeOrdered() bool
	// mergeFold folds the merged StreamReader[any] (frames of T, tagged by
	// source for ordered drain) into the value the combine expects, then runs
	// combine to produce R as any. It is the merge-node-owned combine->fold
	// bridge: drain merged stream -> []T / [][]T -> combine(ctx, ...). It does
	// NOT route through the global concatRegistry.
	mergeFold(ctx context.Context, merged StreamReader[any]) (any, error)
}

// isMergeSourceKey reports whether key is one of mk's declared source-key input
// ports. The classifier uses it to allow ONLY a merge's declared source keys as
// fan-in toPorts (every other toPort != portIn degrades).
func isMergeSourceKey(mk mergeKind, key string) bool {
	for _, k := range mk.mergeKeys() {
		if k == key {
			return true
		}
	}
	return false
}

// sortMergeSources sorts a merge node's inbound sources by key (insertion sort —
// the slice is tiny). Deterministic source order is the fan-in dual of the
// adapter's sorted-key gather, so the stream path and the value path agree.
func sortMergeSources(s []mergeSource) {
	sort.Slice(s, func(i, j int) bool { return s[i].key < s[j].key })
}

// mergeSrcFrame tags one merged frame with the index of the source it came from,
// so the ordered (zip) fold can reconstruct each source's per-frame list. The
// unordered fold ignores the index. fanInMerge wraps every source reader so its
// frames carry the source index.
type mergeSrcFrame struct {
	srcIdx int
	v      any
}

// streamMergeAdapter backs an UNORDERED streaming merge node. Run gathers the
// present source ports in sorted-key order into []T and calls combine.
type streamMergeAdapter[T, R any] struct {
	id      string
	keys    []string // source-key input port names (stable sorted)
	inPorts map[string]reflect.Type
	elemT   reflect.Type
	outT    reflect.Type
	combine func(ctx context.Context, items []T) (R, error)
}

func (a *streamMergeAdapter[T, R]) Inputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inPorts[k]})
	}
	return ports
}

func (a *streamMergeAdapter[T, R]) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: a.outT}}
}

// Run is the value path (Invoke): gather the PRESENT source values in stable
// sorted-key order into []T, then combine -> R. Sorted-key order makes the
// gather deterministic regardless of port-firing order.
func (a *streamMergeAdapter[T, R]) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	items := make([]T, 0, len(a.keys))
	for _, k := range a.keys {
		raw, ok := in[k]
		if !ok {
			continue // a skipped/conditional source is absent
		}
		v, ok := raw.(T)
		if !ok {
			return nil, fmt.Errorf("graph: stream merge %q: source %q is %T, not %s", a.id, k, raw, a.elemT)
		}
		items = append(items, v)
	}
	out, err := a.combine(ctx, items)
	if err != nil {
		return nil, fmt.Errorf("graph: stream merge %q: %w", a.id, err)
	}
	return map[string]any{portOut: out}, nil
}

func (a *streamMergeAdapter[T, R]) mergeKeys() []string      { return a.keys }
func (a *streamMergeAdapter[T, R]) mergeElemT() reflect.Type { return a.elemT }
func (a *streamMergeAdapter[T, R]) mergeOrdered() bool       { return false }

// mergeFold drains the (unordered) merged stream into []T and runs combine. The
// source index on each frame is ignored — unordered merge cares only about the
// multiset of frames. FULLY BUFFERS []T at the merge boundary (acceptable: the
// merge IS the buffering point).
func (a *streamMergeAdapter[T, R]) mergeFold(ctx context.Context, merged StreamReader[any]) (any, error) {
	items, err := drainMergedFlat[T](merged)
	if err != nil {
		return nil, err
	}
	out, err := a.combine(ctx, items)
	if err != nil {
		return nil, fmt.Errorf("graph: stream merge %q: %w", a.id, err)
	}
	return out, nil
}

// zipAdapter backs an ORDERED (round-robin) streaming merge node. Run gathers
// each source's value (sorted-key) into a per-source [][]T and calls combine.
type zipAdapter[T, R any] struct {
	id      string
	keys    []string // source-key input port names (stable sorted)
	inPorts map[string]reflect.Type
	elemT   reflect.Type
	outT    reflect.Type
	combine func(ctx context.Context, perSource [][]T) (R, error)
}

func (a *zipAdapter[T, R]) Inputs() []flow.Port {
	ports := make([]flow.Port, 0, len(a.keys))
	for _, k := range a.keys {
		ports = append(ports, flow.Port{Name: k, GoType: a.inPorts[k]})
	}
	return ports
}

func (a *zipAdapter[T, R]) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: a.outT}}
}

// Run is the value path (Invoke): gather each PRESENT source's value (sorted-key)
// into a per-source [][]T — one 1-element slice per present source, the
// degenerate single-frame case of the stream path — then combine -> R. A skipped
// source contributes an empty (nil) per-source slot so positions stay aligned
// with the sorted keys.
func (a *zipAdapter[T, R]) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	perSource := make([][]T, 0, len(a.keys))
	for _, k := range a.keys {
		raw, ok := in[k]
		if !ok {
			perSource = append(perSource, nil) // skipped source: empty list
			continue
		}
		v, ok := raw.(T)
		if !ok {
			return nil, fmt.Errorf("graph: zip %q: source %q is %T, not %s", a.id, k, raw, a.elemT)
		}
		perSource = append(perSource, []T{v})
	}
	out, err := a.combine(ctx, perSource)
	if err != nil {
		return nil, fmt.Errorf("graph: zip %q: %w", a.id, err)
	}
	return map[string]any{portOut: out}, nil
}

func (a *zipAdapter[T, R]) mergeKeys() []string      { return a.keys }
func (a *zipAdapter[T, R]) mergeElemT() reflect.Type { return a.elemT }
func (a *zipAdapter[T, R]) mergeOrdered() bool       { return true }

// mergeFold drains the (round-robin) merged stream into a per-source [][]T using
// the source index tag on each frame, then runs combine. The number of sources
// is len(keys), so empty sources still get an (empty) slot, keeping positions
// aligned with the sorted source keys.
func (a *zipAdapter[T, R]) mergeFold(ctx context.Context, merged StreamReader[any]) (any, error) {
	perSource, err := drainMergedPerSource[T](merged, len(a.keys))
	if err != nil {
		return nil, err
	}
	out, err := a.combine(ctx, perSource)
	if err != nil {
		return nil, fmt.Errorf("graph: zip %q: %w", a.id, err)
	}
	return out, nil
}

// drainMergedFlat pulls every frame of the merged stream into a flat []T,
// asserting each frame's element to T. It Closes the stream on exit. A non-EOF
// error from the stream is surfaced (the slice gathered so far is discarded).
func drainMergedFlat[T any](merged StreamReader[any]) ([]T, error) {
	defer merged.Close()
	var items []T
	for {
		raw, err := merged.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return items, nil
			}
			return nil, err
		}
		v, ok := frameValue(raw).(T)
		if !ok {
			return nil, fmt.Errorf("graph: stream: merged frame is %T, not %s", frameValue(raw), reflect.TypeFor[T]())
		}
		items = append(items, v)
	}
}

// drainMergedPerSource pulls every frame of the merged stream into a per-source
// [][]T of length n, using each frame's source-index tag. It Closes the stream
// on exit. A frame must be a mergeSrcFrame (fanInMerge tags every frame); an
// untagged frame is an internal error.
func drainMergedPerSource[T any](merged StreamReader[any], n int) ([][]T, error) {
	defer merged.Close()
	perSource := make([][]T, n)
	for {
		raw, err := merged.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return perSource, nil
			}
			return nil, err
		}
		frame, ok := raw.(mergeSrcFrame)
		if !ok {
			return nil, fmt.Errorf("graph: stream: zip merged frame is %T, not source-tagged", raw)
		}
		if frame.srcIdx < 0 || frame.srcIdx >= n {
			return nil, fmt.Errorf("graph: stream: zip frame source index %d out of range [0,%d)", frame.srcIdx, n)
		}
		v, ok := frame.v.(T)
		if !ok {
			return nil, fmt.Errorf("graph: stream: zip frame is %T, not %s", frame.v, reflect.TypeFor[T]())
		}
		perSource[frame.srcIdx] = append(perSource[frame.srcIdx], v)
	}
}

// frameValue unwraps a mergeSrcFrame to its element, or returns the raw value
// unchanged (the unordered fold accepts both tagged and untagged frames).
func frameValue(raw any) any {
	if f, ok := raw.(mergeSrcFrame); ok {
		return f.v
	}
	return raw
}
