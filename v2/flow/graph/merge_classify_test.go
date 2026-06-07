package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
)

// buildStandaloneMergeGraph builds the canonical STANDALONE streaming merge
// graph (no upstream Copy):
//
//	{ a: srcA, b: srcB } -> merge(combine) = exit
//
// Each source emits a MULTI-FRAME string stream from the graph input; the merge
// interleaves the two string streams (T = string) and the combine folds the
// gathered frames to a string. srcA is the graph Entry (every source is fed the
// graph input by fanInMerge; the Entry choice is moot). ordered selects AddZip
// vs AddStreamMerge.
func buildStandaloneMergeGraph(t *testing.T, ordered bool, framesA, framesB []string) *Graph[string, string] {
	t.Helper()
	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", framesA, nil)
	srcB, _ := addStringSource(g, "srcB", framesB, nil)
	sources := map[string]NodeRef{"a": srcA, "b": srcB}

	var mg NodeRef
	var err error
	if ordered {
		mg, err = AddZip[string, string, string, string](g, "mg", sources,
			func(_ context.Context, perSource [][]string) (string, error) {
				return zipInterleave(perSource), nil
			})
	} else {
		mg, err = AddStreamMerge[string, string, string, string](g, "mg", sources,
			func(_ context.Context, items []string) (string, error) {
				return strings.Join(items, ""), nil
			})
	}
	if err != nil {
		t.Fatalf("Add merge: %v", err)
	}
	if err := g.Entry(srcA); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(mg); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	return g
}

// TestMergeGraph_IsStreamCapable: a standalone streaming merge graph is
// classified to the stream-graph plan and reports StreamCapable.
func TestMergeGraph_IsStreamCapable(t *testing.T) {
	g := buildStandaloneMergeGraph(t, false, []string{"a", "b"}, []string{"c", "d"})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("standalone merge graph must be StreamCapable")
	}
	if r.streamGraphPlan == nil {
		t.Fatalf("merge graph must populate streamGraphPlan")
	}
	if r.pipeline != nil || r.streamPlan != nil {
		t.Fatalf("merge graph must NOT be classified linear or branch-DAG")
	}
}

// TestZipGraph_IsStreamCapable: a standalone zip graph is likewise classified to
// the stream-graph plan.
func TestZipGraph_IsStreamCapable(t *testing.T) {
	g := buildStandaloneMergeGraph(t, true, []string{"a", "b"}, []string{"c", "d"})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !r.StreamCapable() {
		t.Fatalf("standalone zip graph must be StreamCapable")
	}
	if r.streamGraphPlan == nil {
		t.Fatalf("zip graph must populate streamGraphPlan")
	}
}

// TestLinearStillStreamCapable_Regression: a plain linear chatmodel chain stays
// linear (pipeline != nil), NOT routed to the stream-graph plan.
func TestLinearStillStreamCapable_Regression(t *testing.T) {
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
}

// TestBranchDAGStillStreamPlan_Regression: a branch-DAG graph stays on streamPlan
// (runStreamDAG), NOT the stream-graph plan.
func TestBranchDAGStillStreamPlan_Regression(t *testing.T) {
	g := buildBranchStreamGraph(t, "a", &spyModel{sr: newSpyStream("a")}, &spyModel{sr: newSpyStream("x")})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamPlan == nil {
		t.Fatalf("branch-DAG must keep streamPlan != nil")
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("branch-DAG must NOT be classified to streamGraphPlan")
	}
}

// TestCopyOnlyStillStreamGraphPlan_Regression: a Copy-only graph (no merge) is
// still classified to the stream-graph plan.
func TestCopyOnlyStillStreamGraphPlan_Regression(t *testing.T) {
	g := buildCopyStreamGraph(t, &spyModel{sr: newSpyStream("a", "b", "c")})
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamGraphPlan == nil {
		t.Fatalf("Copy-only graph must keep streamGraphPlan != nil")
	}
}

// TestCombineFanIn_StillDegrades: AddCombine fan-in (NOT a mergeKind) must still
// degrade — the distinct-interface guard.
func TestCombineFanIn_StillDegrades(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	chatA, _ := AddChatModelNode(g, "chatA", &spyModel{sr: newSpyStream("a")})
	chatB, _ := AddChatModelNode(g, "chatB", &spyModel{sr: newSpyStream("b")})
	// Combine folds two llm.Response sources (each chat stream concats to a
	// Response on the value path) to a string.
	comb, _ := AddCombine[llm.Request, string, string](g, "comb",
		map[string]NodeRef{"a": chatA, "b": chatB},
		func(_ context.Context, in map[string]any) (string, error) {
			return in["a"].(llm.Response).Text + in["b"].(llm.Response).Text, nil
		})
	_ = g.Entry(chatA)
	_ = g.Exit(comb)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("AddCombine fan-in must NOT populate streamGraphPlan (must degrade)")
	}
	if r.StreamCapable() {
		t.Fatalf("AddCombine fan-in must degrade (not StreamCapable)")
	}
}

// TestImplicitRawTee_MergeStillDegrades: a chatmodel STREAM out-port feeding 2
// edges WITHOUT an AddCopy is a raw tee and still degrades, even though the graph
// also contains a merge (the preserved implicit-tee guard, refined to
// stream-emitting ports).
func TestImplicitRawTee_MergeStillDegrades(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", &spyModel{sr: newSpyStream("a", "b")})
	// Two value lambdas fold the SAME chat stream — a raw tee (chat.out -> 2
	// edges). Both feed a merge of strings.
	parseA, _ := AddLambdaNode(g, "parseA", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text, nil
	})
	parseB, _ := AddLambdaNode(g, "parseB", func(_ context.Context, resp llm.Response) (string, error) {
		return resp.Text + "!", nil
	})
	mg, _ := AddStreamMerge[llm.Request, string, string, string](g, "mg",
		map[string]NodeRef{"a": parseA, "b": parseB},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	if err := g.AddEdge(chat, parseA); err != nil {
		t.Fatalf("AddEdge chat->parseA: %v", err)
	}
	if err := g.AddEdge(chat, parseB); err != nil {
		t.Fatalf("AddEdge chat->parseB: %v", err)
	}
	_ = g.Entry(chat)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("implicit raw-stream tee must degrade, not populate streamGraphPlan")
	}
	if r.StreamCapable() {
		t.Fatalf("implicit raw-stream tee must degrade (not StreamCapable)")
	}
}

// TestUpstreamCopyIntoMerge_Degrades: the Copy->{A,B}->Merge DIAMOND is OUT OF
// SCOPE for the standalone MVP — a graph with BOTH a Copy and a Merge must
// DEGRADE (keeping the diamond deferred safely), not be planned.
func TestUpstreamCopyIntoMerge_Degrades(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	chat, _ := AddChatModelNode(g, "chat", &spyModel{sr: newSpyStream("a", "b")})
	// Copy fans the chat stream (llm.Response) to two folding lambdas, both of
	// which feed a merge of strings — the diamond.
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
	mg, _ := AddStreamMerge[llm.Request, string, string, string](g, "mg",
		map[string]NodeRef{"a": parseA, "b": parseB},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	if err := g.AddEdge(chat, cp); err != nil {
		t.Fatalf("AddEdge chat->cp: %v", err)
	}
	_ = g.Entry(chat)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.streamGraphPlan != nil {
		t.Fatalf("upstream-Copy-into-Merge diamond must DEGRADE (deferred), not populate streamGraphPlan")
	}
	if r.StreamCapable() {
		t.Fatalf("diamond must degrade (not StreamCapable)")
	}
}
