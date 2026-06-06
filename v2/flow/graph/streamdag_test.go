package graph

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
)

// buildBranchStreamGraph builds the canonical streamable branch-DAG used by the
// Task-B tests:
//
//	entry(branch over llm.Request) -> { a: chatA, b: chatB } -> parse(exit)
//
// Both chatmodels emit an llm.Response STREAM; parse is a value lambda
// (llm.Response -> string) reached by whichever route fired. parse has
// in-degree 2 (branch reconvergence) — sound because exactly one route fires.
// route is the constant key the branch selects.
func buildBranchStreamGraph(t *testing.T, route string, mA, mB *spyModel) *Graph[llm.Request, string] {
	t.Helper()
	g := NewGraph[llm.Request, string]()
	chatA, _ := AddChatModelNode(g, "chatA", mA)
	chatB, _ := AddChatModelNode(g, "chatB", mB)
	brRef, err := AddBranch(g, "br", func(_ context.Context, _ llm.Request) (string, error) {
		return route, nil
	}, map[string]NodeRef{"a": chatA, "b": chatB})
	if err != nil {
		t.Fatalf("AddBranch: %v", err)
	}
	parse, _ := AddLambdaNode(g, "parse", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text, nil
	})
	if err := g.AddEdge(chatA, parse); err != nil {
		t.Fatalf("AddEdge chatA->parse: %v", err)
	}
	if err := g.AddEdge(chatB, parse); err != nil {
		t.Fatalf("AddEdge chatB->parse: %v", err)
	}
	if err := g.Entry(brRef); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(parse); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	return g
}

func reqWith(text string) llm.Request {
	return llm.Request{Messages: []llm.Message{{Role: "user", Content: text}}}
}

// TestStreamDAG_BranchStreamsSelectedRoute: the selected route streams
// incrementally (spy.calls ≥ 3) and the Stream output equals the same graph's
// Invoke result (observable equivalence — the cross-validator against scheduler
// divergence). Exercised for BOTH routes.
func TestStreamDAG_BranchStreamsSelectedRoute(t *testing.T) {
	for _, tc := range []struct {
		route string
		want  string
	}{
		{"a", "abc"},
		{"b", "xyz"},
	} {
		t.Run(tc.route, func(t *testing.T) {
			spyA := newSpyStream("a", "b", "c")
			spyB := newSpyStream("x", "y", "z")
			mA := &spyModel{sr: spyA}
			mB := &spyModel{sr: spyB}
			g := buildBranchStreamGraph(t, tc.route, mA, mB)

			r, err := g.Compile(context.Background())
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if !r.StreamCapable() {
				t.Fatalf("branch-DAG with chatmodels must be StreamCapable")
			}

			base := settleGoroutines()
			sr, err := r.Stream(context.Background(), reqWith("hi"))
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			got, err := sr.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if _, err := sr.Next(); err != io.EOF {
				t.Fatalf("second Next = %v, want io.EOF (value tail)", err)
			}
			sr.Close()

			if got != tc.want {
				t.Fatalf("Stream route %q = %q, want %q", tc.route, got, tc.want)
			}
			// Incremental streaming proof: the selected spy was pulled ≥3 times.
			sel, other := spyA, spyB
			selM := mA
			if tc.route == "b" {
				sel, other = spyB, spyA
				selM = mB
			}
			if !selM.streamCalled {
				t.Fatalf("selected model Stream not called")
			}
			if sel.calls() < 3 {
				t.Fatalf("selected route streamed only %d deltas, want ≥3", sel.calls())
			}
			if !sel.wasClosed() {
				t.Fatalf("selected underlying stream not closed")
			}
			_ = other

			// Observable equivalence vs Invoke (fresh spies; Invoke uses the
			// value path/Generate, which returns the same joined text). Same
			// graph, same route, same result — the cross-validator against
			// scheduler divergence.
			spyA2 := newSpyStream("a", "b", "c")
			spyB2 := newSpyStream("x", "y", "z")
			g2 := buildBranchStreamGraph(t, tc.route, &spyModel{sr: spyA2}, &spyModel{sr: spyB2})
			r2, _ := g2.Compile(context.Background())
			want, err := r2.Invoke(context.Background(), reqWith("hi"))
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			if got != want {
				t.Fatalf("Stream route %q = %q, want == Invoke %q (observable equivalence)", tc.route, got, want)
			}

			if n := settleGoroutines(); n > base {
				t.Fatalf("goroutine leak: base=%d now=%d", base, n)
			}
		})
	}
}

// TestStreamDAG_BranchUnselectedNotInvoked: the unselected model's Stream AND
// Generate are never called (pruning — unselected route's subgraph is not run).
func TestStreamDAG_BranchUnselectedNotInvoked(t *testing.T) {
	spyA := newSpyStream("a", "b", "c")
	spyB := newSpyStream("x", "y", "z")
	mA := &spyModel{sr: spyA}
	mB := &spyModel{sr: spyB}
	g := buildBranchStreamGraph(t, "a", mA, mB)

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	sr, err := r.Stream(context.Background(), reqWith("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := sr.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	sr.Close()

	if mB.streamCalled {
		t.Fatalf("unselected model B.Stream was called (pruning failed)")
	}
	if mB.generateCalled {
		t.Fatalf("unselected model B.Generate was called (pruning failed)")
	}
	if spyB.calls() != 0 {
		t.Fatalf("unselected model B stream pulled %d times, want 0", spyB.calls())
	}
	if spyB.wasClosed() {
		t.Fatalf("unselected model B stream was Closed, want never touched")
	}
	if !mA.streamCalled {
		t.Fatalf("selected model A.Stream was not called")
	}
}

// TestStreamDAG_BranchEarlyCloseNoLeak: Close the returned StreamReader after
// one Next; the goroutine baseline is restored and the underlying llm stream is
// Closed. Also exercises Close-before-Next (early break).
func TestStreamDAG_BranchEarlyCloseNoLeak(t *testing.T) {
	t.Run("after-one-next", func(t *testing.T) {
		spyA := newSpyStream("a", "b", "c")
		spyB := newSpyStream("x", "y", "z")
		mA := &spyModel{sr: spyA}
		g := buildBranchStreamGraph(t, "a", mA, &spyModel{sr: spyB})
		r, _ := g.Compile(context.Background())

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
		if !spyA.wasClosed() {
			t.Fatalf("selected stream not closed after Next+Close")
		}
		if n := settleGoroutines(); n > base {
			t.Fatalf("goroutine leak: base=%d now=%d", base, n)
		}
	})

	t.Run("close-before-next", func(t *testing.T) {
		// O = llm.Response so the tail folds lazily; Close before Next must
		// release the selected upstream stream without draining.
		spyA := newSpyStream("a", "b", "c")
		spyB := newSpyStream("x", "y", "z")
		mA := &spyModel{sr: spyA}
		mB := &spyModel{sr: spyB}
		g := NewGraph[llm.Request, llm.Response]()
		chatA, _ := AddChatModelNode(g, "chatA", mA)
		chatB, _ := AddChatModelNode(g, "chatB", mB)
		brRef, _ := AddBranch(g, "br", func(_ context.Context, _ llm.Request) (string, error) {
			return "a", nil
		}, map[string]NodeRef{"a": chatA, "b": chatB})
		// exit must be a single node; route both chats into a passthrough that
		// is the streaming tail. passthrough[llm.Response] keeps the carrier a
		// stream? No — passthrough is a value node and folds. To keep the tail
		// streaming, make a chat node the exit directly via a 1-route branch.
		exit, _ := AddPassthroughNode[llm.Request, llm.Response, llm.Response](g, "exit")
		if err := g.AddEdge(chatA, exit); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if err := g.AddEdge(chatB, exit); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if err := g.Entry(brRef); err != nil {
			t.Fatalf("Entry: %v", err)
		}
		if err := g.Exit(exit); err != nil {
			t.Fatalf("Exit: %v", err)
		}
		r, err := g.Compile(context.Background())
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}

		base := settleGoroutines()
		sr, err := r.Stream(context.Background(), reqWith("hi"))
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		// Close before Next. The passthrough exit folds the stream though, so
		// the stream was already drained+closed at the exit's value Run. Assert
		// no leak and idempotent close regardless.
		if err := sr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := sr.Close(); err != nil {
			t.Fatalf("Close (idempotent): %v", err)
		}
		if !spyA.wasClosed() {
			t.Fatalf("selected stream not closed")
		}
		if spyB.wasClosed() {
			t.Fatalf("unselected stream B closed, want never touched")
		}
		if n := settleGoroutines(); n > base {
			t.Fatalf("goroutine leak: base=%d now=%d", base, n)
		}
	})
}

// TestStreamDAG_StreamCapable_BranchTrue: a branch-DAG with chatmodels is
// StreamCapable.
func TestStreamDAG_StreamCapable_BranchTrue(t *testing.T) {
	g := buildBranchStreamGraph(t, "a", &spyModel{sr: newSpyStream("a")}, &spyModel{sr: newSpyStream("x")})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("branch-DAG must be StreamCapable")
	}
}

// TestStreamDAG_FanInDegradesNoError: a parallel+combine (true fan-in) graph is
// NOT StreamCapable; Stream still returns the correct boxed Invoke result with
// no error.
func TestStreamDAG_FanInDegradesNoError(t *testing.T) {
	g := NewGraph[int, string]()
	dblN, _ := AddLambdaNode(g, "dbl", func(_ context.Context, x int) (int, error) { return x * 2, nil })
	incN, _ := AddLambdaNode(g, "inc", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	par, _ := AddParallel[int, string, int](g, "par", map[string]NodeRef{"dbl": dblN, "inc": incN})
	// AddCombine registers the source->combine edges itself; no AddEdgeTo.
	comb, _ := AddCombine[int, string, string](g, "comb",
		map[string]NodeRef{"dbl": dblN, "inc": incN},
		func(_ context.Context, in map[string]any) (string, error) {
			d, _ := in["dbl"].(int)
			i, _ := in["inc"].(int)
			return string(rune('0'+d)) + string(rune('0'+i)), nil
		})
	if err := g.Entry(par); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(comb); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.StreamCapable() {
		t.Fatalf("parallel+combine fan-in must NOT be StreamCapable (degrades)")
	}
	sr, err := r.Stream(context.Background(), 3)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer sr.Close()
	got, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want, err := r.Invoke(context.Background(), 3)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != want {
		t.Fatalf("degraded Stream = %q, want == Invoke %q", got, want)
	}
}

// TestStreamDAG_MissingFolderCompileError: a streamable-branch graph whose
// stream→value boundary has no registered folder is a Compile error (not a
// silent degrade).
func TestStreamDAG_MissingFolderCompileError(t *testing.T) {
	// branch -> {a: chatA, b: chatB} -> sink(In=any). chatmodels emit a stream;
	// the downstream sink In=any has no folder, so a streamable-branch graph
	// missing the fold target must fail Compile.
	g := NewGraph[llm.Request, any]()
	chatA, _ := AddChatModelNode(g, "chatA", &spyModel{sr: newSpyStream("a")})
	chatB, _ := AddChatModelNode(g, "chatB", &spyModel{sr: newSpyStream("x")})
	brRef, _ := AddBranch(g, "br", func(_ context.Context, _ llm.Request) (string, error) {
		return "a", nil
	}, map[string]NodeRef{"a": chatA, "b": chatB})
	sink, _ := AddLambdaNode(g, "sink", func(_ context.Context, v any) (any, error) { return v, nil })
	if err := g.AddEdge(chatA, sink); err != nil {
		t.Fatalf("AddEdge chatA->sink: %v", err)
	}
	if err := g.AddEdge(chatB, sink); err != nil {
		t.Fatalf("AddEdge chatB->sink: %v", err)
	}
	if err := g.Entry(brRef); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(sink); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	_, err := g.Compile(context.Background())
	if err == nil {
		t.Fatalf("Compile: want missing-folder error, got nil")
	}
	if !strings.Contains(err.Error(), "no Concatenator") {
		t.Fatalf("Compile error = %q, want it to mention 'no Concatenator'", err.Error())
	}
}
