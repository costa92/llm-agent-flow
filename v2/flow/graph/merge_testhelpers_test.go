package graph

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// stringSourceAdapter is a TEST-ONLY streamNode whose value-path output type
// EQUALS its stream element type (string). Unlike the chatmodel (outT =
// llm.Response, elemT = llm.StreamEvent), this lets a streaming merge declare
// T = string and interleave MULTI-FRAME source streams directly (the chatmodel
// would fold each source to a single llm.Response). It emits a fixed sequence of
// string frames on the stream path and the joined string on the value path, so
// Invoke and Stream are observably equivalent.
//
// in["in"] is ignored (the source is driven by the graph input but emits a fixed
// script — sufficient for the executor/leak/equivalence tests). It is
// in-process only.
type stringSourceAdapter struct {
	id     string
	frames []string
	delay  func() // optional per-frame hook (gating for concurrency tests)
}

func (a *stringSourceAdapter) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: reflect.TypeFor[any]()}}
}

func (a *stringSourceAdapter) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: reflect.TypeFor[string]()}}
}

// Run is the value path: join the frames into the single string the stream would
// accumulate.
func (a *stringSourceAdapter) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	var b []byte
	for _, f := range a.frames {
		b = append(b, f...)
	}
	return map[string]any{portOut: string(b)}, nil
}

// streamRun is the stream path: emit the frames as a multi-frame string stream
// (elemT = string == outT), so a merge of T = string interleaves them directly.
func (a *stringSourceAdapter) streamRun(_ context.Context, _ streamCarrier) (streamCarrier, error) {
	return streamCarrier{
		isStream: true,
		stream:   anyStream[string]{&sliceFrameStream{frames: a.frames, delay: a.delay}},
		elemT:    reflect.TypeFor[string](),
	}, nil
}

// sliceFrameStream yields a fixed string sequence then io.EOF, calling an
// optional per-frame delay hook before each frame (for concurrency gating).
type sliceFrameStream struct {
	mu     sync.Mutex
	frames []string
	idx    int
	closed bool
	delay  func()
}

func (s *sliceFrameStream) Next() (string, error) {
	s.mu.Lock()
	if s.closed || s.idx >= len(s.frames) {
		s.mu.Unlock()
		return "", io.EOF
	}
	i := s.idx
	s.idx++
	s.mu.Unlock()
	if s.delay != nil {
		s.delay()
	}
	return s.frames[i], nil
}

func (s *sliceFrameStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// addStringSource adds a stringSourceAdapter node to g (outT = string), wired
// like any single-port node. It is the multi-frame stream source the merge
// stream tests interleave.
func addStringSource[GI, GO any](g *Graph[GI, GO], id string, frames []string, delay func()) (NodeRef, error) {
	outT := reflect.TypeFor[string]()
	newKind := func() flow.NodeKind {
		return &stringSourceAdapter{id: id, frames: frames, delay: delay}
	}
	// inT = any so the graph input (whatever type) is assignable into the source.
	ref, err := g.addNode(id, reflect.TypeFor[any](), outT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "test string stream source (in-process only)")
	return ref, nil
}

// ensure the adapter satisfies streamNode at compile time.
var _ streamNode = (*stringSourceAdapter)(nil)
var _ = fmt.Sprintf
