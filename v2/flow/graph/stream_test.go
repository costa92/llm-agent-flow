package graph

import (
	"context"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"
)

// spyStream is a test llm.StreamReader that emits a fixed sequence of
// EventTextDelta then EventDone, counting Next calls and recording
// whether Close was called. Unlike scriptedStream it emits MULTIPLE text
// deltas so we can prove real per-delta streaming.
type spyStream struct {
	mu        sync.Mutex
	deltas    []string
	step      int
	nextCalls int
	closed    bool
}

func newSpyStream(deltas ...string) *spyStream { return &spyStream{deltas: deltas} }

func (s *spyStream) Next() (llm.StreamEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextCalls++
	if s.closed {
		return llm.StreamEvent{}, io.EOF
	}
	switch {
	case s.step < len(s.deltas):
		d := s.deltas[s.step]
		s.step++
		return llm.StreamEvent{Kind: llm.EventTextDelta, Text: d}, nil
	case s.step == len(s.deltas):
		s.step++
		return llm.StreamEvent{Kind: llm.EventDone, Usage: &llm.Usage{}, FinishReason: llm.FinishReasonStop}, nil
	default:
		return llm.StreamEvent{}, io.EOF
	}
}

func (s *spyStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *spyStream) calls() int  { s.mu.Lock(); defer s.mu.Unlock(); return s.nextCalls }
func (s *spyStream) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// spyModel is a test llm.ChatModel whose Stream returns a shared spyStream
// and records that Stream was invoked. Generate returns the SAME text the
// stream would accumulate (the join of the spy's deltas) so Invoke and Stream
// are observably equivalent — the cross-validator the branch tests rely on.
// generateCalled records whether the value path was taken.
type spyModel struct {
	sr             *spyStream
	streamCalled   bool
	generateCalled bool
}

var _ llm.ChatModel = (*spyModel)(nil)

func (m *spyModel) Generate(context.Context, llm.Request) (llm.Response, error) {
	m.generateCalled = true
	return llm.Response{Text: strings.Join(m.sr.deltas, "")}, nil
}

func (m *spyModel) Stream(context.Context, llm.Request) (llm.StreamReader, error) {
	m.streamCalled = true
	return m.sr, nil
}

func (m *spyModel) Info() llm.ProviderInfo {
	return llm.ProviderInfo{Provider: "spy", Model: "spy-1"}
}

// TestStream_LinearChatModelStreams is the marquee test:
// template -> chatmodel -> parser(Lambda, In=llm.Response). It proves the
// chatmodel streams (Next pulled ≥3 times, Stream invoked), the parser
// receives the concat'd llm.Response{Text:"abc"}, the consumer reader
// yields a single element (parser is a value tail), no goroutine leak, and
// the underlying llm.StreamReader.Close is called.
func TestStream_LinearChatModelStreams(t *testing.T) {
	tmpl, err := prompt.New(prompt.Spec{System: "sys", User: "{q}"})
	if err != nil {
		t.Fatalf("prompt.New: %v", err)
	}
	req, ok := tmpl.(prompt.Requester)
	if !ok {
		t.Fatalf("template not a Requester")
	}

	spy := newSpyStream("a", "b", "c")
	model := &spyModel{sr: spy}

	g := NewGraph[prompt.Vars, string]()
	tNode, _ := AddTemplateNode(g, "tmpl", req)
	cmNode, _ := AddChatModelNode(g, "chat", model)

	var gotText string
	parseN, _ := AddLambdaNode(g, "parse", func(_ context.Context, resp llm.Response) (string, error) {
		gotText = resp.Text
		return resp.Text, nil
	})
	if err := g.AddEdge(tNode, cmNode); err != nil {
		t.Fatalf("AddEdge tmpl->chat: %v", err)
	}
	if err := g.AddEdge(cmNode, parseN); err != nil {
		t.Fatalf("AddEdge chat->parse: %v", err)
	}
	if err := g.Entry(tNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(parseN); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("linear chain must be StreamCapable")
	}

	base := settleGoroutines()

	sr, err := r.Stream(context.Background(), prompt.Vars{"q": "hi"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	got, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got != "abc" {
		t.Fatalf("Next = %q, want %q", got, "abc")
	}
	if _, err := sr.Next(); err != io.EOF {
		t.Fatalf("second Next = %v, want io.EOF (value tail = single element)", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !model.streamCalled {
		t.Fatalf("m.Stream was never called")
	}
	if spy.calls() < 3 {
		t.Fatalf("expected ≥3 Next pulls on the llm stream, got %d", spy.calls())
	}
	if gotText != "abc" {
		t.Fatalf("parser received Text=%q, want %q", gotText, "abc")
	}
	if !spy.wasClosed() {
		t.Fatalf("underlying llm.StreamReader.Close was not called")
	}

	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// TestStream_BranchValueDAGStreams: a value-only Branch DAG (branch ->
// even/odd lambdas reconverging at a passthrough exit) is now a streamable
// branch-DAG (Phase 3 item #1). It is StreamCapable and Stream returns the
// selected route's result, identical to Invoke. (Previously branch graphs
// degraded to box(Invoke); this is the intended behaviour change.)
func TestStream_BranchValueDAGStreams(t *testing.T) {
	build := func() *Graph[int, string] {
		g := NewGraph[int, string]()
		evenN, _ := AddLambdaNode(g, "even", func(_ context.Context, x int) (string, error) { return "even", nil })
		oddN, _ := AddLambdaNode(g, "odd", func(_ context.Context, x int) (string, error) { return "odd", nil })
		br, _ := AddBranch(g, "br", func(_ context.Context, x int) (string, error) {
			if x%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		}, map[string]NodeRef{"even": evenN, "odd": oddN})
		exit, _ := AddPassthroughNode[int, string, string](g, "exit")
		if err := g.AddEdge(evenN, exit); err != nil {
			t.Fatalf("AddEdge even->exit: %v", err)
		}
		if err := g.AddEdge(oddN, exit); err != nil {
			t.Fatalf("AddEdge odd->exit: %v", err)
		}
		if err := g.Entry(br); err != nil {
			t.Fatalf("Entry: %v", err)
		}
		if err := g.Exit(exit); err != nil {
			t.Fatalf("Exit: %v", err)
		}
		return g
	}

	r, err := build().Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("value-only branch DAG must be StreamCapable")
	}

	for _, in := range []int{4, 7} {
		sr, err := r.Stream(context.Background(), in)
		if err != nil {
			t.Fatalf("Stream(%d): %v", in, err)
		}
		got, err := sr.Next()
		if err != nil {
			t.Fatalf("Next(%d): %v", in, err)
		}
		if _, err := sr.Next(); err != io.EOF {
			t.Fatalf("second Next(%d) = %v, want io.EOF", in, err)
		}
		sr.Close()

		// Observable equivalence: Stream result == Invoke result.
		want, err := r.Invoke(context.Background(), in)
		if err != nil {
			t.Fatalf("Invoke(%d): %v", in, err)
		}
		if got != want {
			t.Fatalf("Stream(%d) = %q, want == Invoke %q", in, got, want)
		}
	}
}

// noFolder is a custom type with no registered Concatenator, used to prove
// a missing folder on a stream edge is a Compile error.
type noFolder struct{ s string }

// TestStream_MissingConcatenatorCompileError: a linear chatmodel -> lambda
// whose lambda In is a type with no registered folder must fail Compile.
func TestStream_MissingConcatenatorCompileError(t *testing.T) {
	// A chatmodel emits a stream of llm.StreamEvent; the downstream lambda
	// declares In = noFolder, for which no Concatenator is registered.
	// (The edge itself type-checks because chatmodel out llm.Response is
	// not assignable to noFolder — so we instead make the lambda accept
	// `any` to pass AddEdge, then the stream-edge fold target is `any`...
	// but `any` would also lack a folder. Simpler: build the graph so the
	// edge is llm.Response -> any, and assert no folder for `any`.)
	g := NewGraph[llm.Request, any]()
	spy := newSpyStream("x")
	model := &spyModel{sr: spy}
	cmNode, _ := AddChatModelNode(g, "chat", model)
	// downstream lambda In = any: type-checks (any source/target), but the
	// stream fold target `any` has no registered Concatenator.
	sink, _ := AddLambdaNode(g, "sink", func(_ context.Context, v any) (any, error) { return v, nil })
	if err := g.AddEdge(cmNode, sink); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Entry(cmNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(sink); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	_, err := g.Compile(context.Background())
	if err == nil {
		t.Fatalf("Compile: want missing-concatenator error, got nil")
	}
	if !strings.Contains(err.Error(), "no Concatenator") {
		t.Fatalf("Compile error = %q, want it to mention 'no Concatenator'", err.Error())
	}
}

// TestStream_CloseIdempotentAndEarlyBreak: a chatmodel-tail linear graph
// (O = llm.Response) folds the tail lazily. Closing the reader BEFORE any
// Next (early break) must release the underlying llm stream without
// draining it; a second Close is a safe no-op; no goroutine leaks.
func TestStream_CloseIdempotentAndEarlyBreak(t *testing.T) {
	tmpl, _ := prompt.New(prompt.Spec{System: "s", User: "{q}"})
	req := tmpl.(prompt.Requester)
	spy := newSpyStream("a", "b", "c")
	model := &spyModel{sr: spy}

	g := NewGraph[prompt.Vars, llm.Response]()
	tNode, _ := AddTemplateNode(g, "tmpl", req)
	cmNode, _ := AddChatModelNode(g, "chat", model)
	if err := g.AddEdge(tNode, cmNode); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Entry(tNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(cmNode); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("must be StreamCapable")
	}

	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), prompt.Vars{"q": "hi"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	// Early break: Close before any Next must still release the upstream.
	if err := sr.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
	if _, err := sr.Next(); err != io.EOF {
		t.Fatalf("Next after Close = %v, want io.EOF", err)
	}
	if !spy.wasClosed() {
		t.Fatalf("underlying stream not closed after early break")
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak after early break: base=%d now=%d", base, n)
	}
}

// TestStream_ChatModelTailFolds: a chatmodel-tail linear graph yields the
// folded llm.Response on the single Next, proving the lazy tail folder
// accumulates the 3 deltas.
func TestStream_ChatModelTailFolds(t *testing.T) {
	tmpl, _ := prompt.New(prompt.Spec{System: "s", User: "{q}"})
	req := tmpl.(prompt.Requester)
	spy := newSpyStream("a", "b", "c")
	model := &spyModel{sr: spy}

	g := NewGraph[prompt.Vars, llm.Response]()
	tNode, _ := AddTemplateNode(g, "tmpl", req)
	cmNode, _ := AddChatModelNode(g, "chat", model)
	if err := g.AddEdge(tNode, cmNode); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Entry(tNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(cmNode); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	sr, err := r.Stream(context.Background(), prompt.Vars{"q": "hi"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer sr.Close()
	resp, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if resp.Text != "abc" {
		t.Fatalf("folded tail Text = %q, want %q", resp.Text, "abc")
	}
	if _, err := sr.Next(); err != io.EOF {
		t.Fatalf("second Next = %v, want io.EOF", err)
	}
	if !spy.wasClosed() {
		t.Fatalf("underlying stream not closed after fold")
	}
}

// settleGoroutines lets transient goroutines exit, then returns the count.
// It polls until the count is stable so the leak comparison isn't fooled
// by a still-winding-down runtime goroutine.
func settleGoroutines() int {
	prev := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		runtime.GC()
		time.Sleep(5 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			return n
		}
		prev = n
	}
	return prev
}

// trackingAnyStream is a minimal StreamReader[any] that records whether
// Close was called and how many times.
type trackingAnyStream struct {
	closeCount int
}

func (t *trackingAnyStream) Next() (any, error) { return nil, io.EOF }
func (t *trackingAnyStream) Close() error       { t.closeCount++; return nil }

// TestFoldTail_PanicReleasesUpstream — if the folder panics, foldTailReader
// must still release (Close) the upstream stream exactly once.
func TestFoldTail_PanicReleasesUpstream(t *testing.T) {
	up := &trackingAnyStream{}
	ft := foldTail[string](up, func(StreamReader[any]) (any, error) {
		panic("boom")
	})

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic from folder, got none")
			}
		}()
		_, _ = ft.Next()
	}()

	if up.closeCount != 1 {
		t.Fatalf("upstream closeCount = %d, want 1 (released exactly once on fold panic)", up.closeCount)
	}
	// A subsequent Close must be a no-op (idempotent), not a second release.
	if err := ft.Close(); err != nil {
		t.Fatalf("Close after panic: %v", err)
	}
	if up.closeCount != 1 {
		t.Fatalf("upstream closeCount = %d after extra Close, want still 1", up.closeCount)
	}
}
