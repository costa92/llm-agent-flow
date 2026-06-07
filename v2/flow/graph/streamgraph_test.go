package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
)

// buildCopyFanOutGraph builds a Copy-bearing streamable graph that fans a
// chatmodel STREAM to two folding targets:
//
//	chat(stream) -> copy -> { a: parseA(=exit), b: parseB(leaf) }
//
// Both parse lambdas fold their copyReader (In = llm.Response) to a string and
// record what they saw via the supplied capture funcs, so the test can prove
// BOTH targets observed the full stream. parseA is the declared exit (its
// string is the graph output); parseB is an independent leaf.
func buildCopyFanOutGraph(t *testing.T, m *spyModel, captureA, captureB func(string)) *Graph[llm.Request, string] {
	t.Helper()
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", m)
	parseA, _ := AddLambdaNode(g, "parseA", func(_ context.Context, resp llm.Response) (string, error) {
		if captureA != nil {
			captureA(resp.Text)
		}
		return resp.Text, nil
	})
	parseB, _ := AddLambdaNode(g, "parseB", func(_ context.Context, resp llm.Response) (string, error) {
		if captureB != nil {
			captureB(resp.Text)
		}
		return resp.Text, nil
	})
	cp, err := AddCopy[llm.Request, string, llm.Response](g, "cp", map[string]NodeRef{"a": parseA, "b": parseB})
	if err != nil {
		t.Fatalf("AddCopy: %v", err)
	}
	if err := g.AddEdge(chat, cp); err != nil {
		t.Fatalf("AddEdge chat->cp: %v", err)
	}
	if err := g.Entry(chat); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(parseA); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	return g
}

// TestStreamGraph_CopyFansStreamToTwoConsumers: the Copy node tees the
// chatmodel stream to BOTH targets; each observes the full stream (≥3 deltas
// accumulate to the same text). The graph output (parseA) equals the accumulated
// text; parseB (leaf) saw it too.
func TestStreamGraph_CopyFansStreamToTwoConsumers(t *testing.T) {
	var mu sync.Mutex
	var gotA, gotB string
	bDone := make(chan struct{})
	spy := newSpyStream("a", "b", "c")
	m := &spyModel{sr: spy}
	g := buildCopyFanOutGraph(t, m,
		func(s string) { mu.Lock(); gotA = s; mu.Unlock() },
		func(s string) { mu.Lock(); gotB = s; mu.Unlock(); close(bDone) },
	)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("Copy graph must be StreamCapable")
	}

	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	out, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	// The leaf subtree (parseB) runs in its own goroutine; wait for it to
	// finish folding before asserting what it observed.
	<-bDone
	sr.Close()

	if out != "abc" {
		t.Fatalf("graph output (parseA) = %q, want %q", out, "abc")
	}
	// The source stream was pulled ≥3 times (real streaming) by the broadcaster.
	if spy.calls() < 3 {
		t.Fatalf("source stream pulled only %d times, want ≥3", spy.calls())
	}
	mu.Lock()
	a, b := gotA, gotB
	mu.Unlock()
	if a != "abc" {
		t.Fatalf("target A observed %q, want full stream %q", a, "abc")
	}
	if b != "abc" {
		t.Fatalf("target B (leaf) observed %q, want full stream %q", b, "abc")
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// TestStreamGraph_CopyEquivalenceVsInvoke: Stream output == Invoke output
// (sequence equality — Copy is order-preserving). The cross-validator against
// scheduler divergence.
func TestStreamGraph_CopyEquivalenceVsInvoke(t *testing.T) {
	// Stream run.
	gStream := buildCopyFanOutGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")}, nil, nil)
	rStream, err := gStream.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile (stream): %v", err)
	}
	sr, err := rStream.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	got, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	sr.Close()

	// Invoke run (fresh spies).
	gInvoke := buildCopyFanOutGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")}, nil, nil)
	rInvoke, err := gInvoke.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile (invoke): %v", err)
	}
	want, err := rInvoke.Invoke(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != want {
		t.Fatalf("Stream = %q, want == Invoke %q (Copy order-preserving equivalence)", got, want)
	}
}

// TestStreamGraph_Copy_FullDrain_NoLeak: fully consume the stream then Close;
// the goroutine baseline is restored.
func TestStreamGraph_Copy_FullDrain_NoLeak(t *testing.T) {
	g := buildCopyFanOutGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")}, nil, nil)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for {
		if _, err := sr.Next(); err != nil {
			break
		}
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// TestStreamGraph_Copy_EarlyClose_NoLeak: Close after one Next (before the
// stream is exhausted); the broadcaster + every subtree goroutine exit.
func TestStreamGraph_Copy_EarlyClose_NoLeak(t *testing.T) {
	g := buildCopyFanOutGraph(t, &spyModel{sr: newSpyStream("a", "b", "c", "d", "e")}, nil, nil)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := sr.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// TestStreamGraph_Copy_CloseBeforeNext_NoLeak: Close before any Next; the
// broadcaster + subtree goroutines exit without the consumer ever reading.
func TestStreamGraph_Copy_CloseBeforeNext_NoLeak(t *testing.T) {
	g := buildCopyFanOutGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")}, nil, nil)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close (idempotent): %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}
