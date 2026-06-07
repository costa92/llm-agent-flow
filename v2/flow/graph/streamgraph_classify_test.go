package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
)

// buildCopyStreamGraph builds the canonical Copy-bearing streamable graph:
//
//	entry(chatmodel) -> copy -> { a: parseA, b: <exit> }
//
// The chatmodel emits an llm.Response STREAM; copy fans it to two value
// lambdas. One target leads to the declared exit. Used by the 2b/2c tests.
func buildCopyStreamGraph(t *testing.T, mEntry *spyModel) *Graph[llm.Request, string] {
	t.Helper()
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", mEntry)
	// Two value targets that fold the stream (In = llm.Response) -> string.
	parseA, _ := AddLambdaNode(g, "parseA", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text, nil
	})
	parseB, _ := AddLambdaNode(g, "parseB", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text + "!", nil
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

// TestCopyGraph_IsStreamCapable: a Copy-bearing streamable DAG (chatmodel ->
// copy -> two folding lambdas) is classified to the new stream-graph plan and
// reports StreamCapable.
func TestCopyGraph_IsStreamCapable(t *testing.T) {
	g := buildCopyStreamGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("Copy-bearing streamable graph must be StreamCapable")
	}
	if r.streamGraphPlan == nil {
		t.Fatalf("Copy-bearing graph must populate streamGraphPlan")
	}
	if r.pipeline != nil || r.streamPlan != nil {
		t.Fatalf("Copy-bearing graph must NOT be classified linear or branch-DAG")
	}
}

// TestLinearStillCapable_Regression: a plain linear chatmodel chain stays
// linear (pipeline != nil), NOT routed to the new plan, and Stream is still
// byte-identical to Invoke.
func TestLinearStillCapable_Regression(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", &spyModel{sr: newSpyStream("a", "b", "c")})
	parse, _ := AddLambdaNode(g, "parse", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text, nil
	})
	_ = g.AddEdge(chat, parse)
	_ = g.Entry(chat)
	_ = g.Exit(parse)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.pipeline == nil {
		t.Fatalf("linear chain must keep pipeline != nil")
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("linear chain must NOT populate streamGraphPlan")
	}
	if !r.StreamCapable() {
		t.Fatalf("linear chain must be StreamCapable")
	}
}

// TestBranchDAGStillUsesStreamDAG_Regression: a branch-DAG graph is still
// classified to streamPlan (runStreamDAG), NOT the new stream-graph plan.
func TestBranchDAGStillUsesStreamDAG_Regression(t *testing.T) {
	g := buildBranchStreamGraph(t, "a", &spyModel{sr: newSpyStream("a")}, &spyModel{sr: newSpyStream("x")})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamPlan == nil {
		t.Fatalf("branch-DAG must keep streamPlan != nil (runStreamDAG)")
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("branch-DAG must NOT be classified to streamGraphPlan")
	}
}

// TestUnknownShape_DegradesNoError: a graph with a non-whitelisted node kind
// (parallel/combine, no copy, no stream) degrades to box(Invoke) with no error
// and is not StreamCapable.
func TestUnknownShape_DegradesNoError(t *testing.T) {
	g := NewGraph[int, int]()
	entry, _ := AddPassthroughNode[int, int, int](g, "entry")
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (int, error) { return x * 2, nil })
	par, _ := AddParallel[int, int, int](g, "par", map[string]NodeRef{"a": aN, "b": bN})
	comb, _ := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, in map[string]any) (int, error) { return in["a"].(int) + in["b"].(int), nil })
	_ = par
	_ = g.AddEdge(entry, par)
	_ = g.Entry(entry)
	_ = g.Exit(comb)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.StreamCapable() {
		t.Fatalf("parallel/combine graph must degrade (not StreamCapable)")
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("parallel/combine graph must NOT populate streamGraphPlan")
	}
	// Stream still returns the boxed Invoke result.
	sr, err := r.Stream(context.Background(), 3)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer sr.Close()
	v, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if v != 10 {
		t.Fatalf("degraded Stream = %d, want 10", v)
	}
}

// TestImplicitRawTee_StillDegrades: a chatmodel stream out-port feeding 2 edges
// WITHOUT an AddCopy node (an implicit raw-stream tee) still degrades — it is
// not classified to the stream-graph plan.
func TestImplicitRawTee_StillDegrades(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", &spyModel{sr: newSpyStream("a", "b", "c")})
	parseA, _ := AddLambdaNode(g, "parseA", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text, nil
	})
	parseB, _ := AddLambdaNode(g, "parseB", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text + "!", nil
	})
	// chat.out -> parseA.in AND chat.out -> parseB.in: a raw-stream tee, no copy.
	if err := g.AddEdge(chat, parseA); err != nil {
		t.Fatalf("AddEdge chat->parseA: %v", err)
	}
	if err := g.AddEdge(chat, parseB); err != nil {
		t.Fatalf("AddEdge chat->parseB: %v", err)
	}
	_ = g.Entry(chat)
	_ = g.Exit(parseA)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.StreamCapable() {
		t.Fatalf("implicit raw-stream tee (no AddCopy) must degrade, not be StreamCapable")
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("implicit raw-stream tee must NOT populate streamGraphPlan")
	}
}

// TestMissingFolder_CompileError: a Copy-bearing streamable graph whose
// stream->value boundary has no registered folder is a Compile error (same
// escalation as buildStreamPlan), not a silent degrade.
func TestMissingFolder_CompileError(t *testing.T) {
	// chat (stream) -> copy -> { a: sink(In=any), b: sink2(In=any) }. The
	// downstream In=any has no folder, so a streamable copy graph missing the
	// fold target must fail Compile.
	g := NewGraph[llm.Request, any]()
	chat, _ := AddChatModelNode(g, "chat", &spyModel{sr: newSpyStream("a")})
	sinkA, _ := AddLambdaNode(g, "sinkA", func(_ context.Context, v any) (any, error) { return v, nil })
	sinkB, _ := AddLambdaNode(g, "sinkB", func(_ context.Context, v any) (any, error) { return v, nil })
	cp, err := AddCopy[llm.Request, any, llm.Response](g, "cp", map[string]NodeRef{"a": sinkA, "b": sinkB})
	if err != nil {
		t.Fatalf("AddCopy: %v", err)
	}
	if err := g.AddEdge(chat, cp); err != nil {
		t.Fatalf("AddEdge chat->cp: %v", err)
	}
	_ = g.Entry(chat)
	_ = g.Exit(sinkA)
	_, err = g.Compile(context.Background())
	if err == nil {
		t.Fatalf("Compile: want missing-folder error, got nil")
	}
	if !strings.Contains(err.Error(), "no Concatenator") {
		t.Fatalf("Compile error = %q, want it to mention 'no Concatenator'", err.Error())
	}
}
