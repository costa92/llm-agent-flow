package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-flow/v2/flow"
)

// TestLower_SerializableRoundTrip builds a passthrough-only linear graph,
// asserts it is serializable/checkpointable, lowers it to flow.Flow IR,
// round-trips that IR through Marshal/Load, recompiles against the builtin
// registry, and proves the rebuilt engine forwards the value unchanged.
func TestLower_SerializableRoundTrip(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	p1, err := AddPassthroughNode[string, string, string](g, "p1")
	if err != nil {
		t.Fatalf("AddPassthroughNode p1: %v", err)
	}
	p2, err := AddPassthroughNode[string, string, string](g, "p2")
	if err != nil {
		t.Fatalf("AddPassthroughNode p2: %v", err)
	}
	if err := g.Entry(p1); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.AddEdge(p1, p2); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Exit(p2); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	run, err := g.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !run.Serializable() {
		t.Fatalf("Serializable() = false, want true")
	}
	if !run.Checkpointable() {
		t.Fatalf("Checkpointable() = false, want true")
	}

	got, err := run.Invoke(ctx, "hi")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != "hi" {
		t.Fatalf("Invoke = %q, want %q", got, "hi")
	}

	f, err := run.Lower()
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	b, err := flow.Marshal(f)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	f2, err := flow.Load(strings.NewReader(string(b)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	eng, err := flow.Compile(f2, NewBuiltinRegistry(), flow.Deps{})
	if err != nil {
		t.Fatalf("flow.Compile rebuilt: %v", err)
	}
	out, err := eng.Run(ctx, map[string]any{graphInputKey: "hi"})
	if err != nil {
		t.Fatalf("rebuilt Run: %v", err)
	}
	if out[graphOutputKey] != "hi" {
		t.Fatalf("rebuilt out = %v, want %q", out[graphOutputKey], "hi")
	}
}

// TestLower_LambdaNotSerializable proves a graph holding a Lambda closure is
// not serializable and Lower's error names the node and the word "closure".
func TestLower_LambdaNotSerializable(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[int, int]()
	dbl, err := AddLambdaNode(g, "dbl", func(_ context.Context, x int) (int, error) { return x * 2, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode: %v", err)
	}
	if err := g.Entry(dbl); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(dbl); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	run, err := g.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if run.Serializable() {
		t.Fatalf("Serializable() = true, want false")
	}
	if run.Checkpointable() {
		t.Fatalf("Checkpointable() = true, want false")
	}
	_, err = run.Lower()
	if err == nil {
		t.Fatalf("Lower: want error, got nil")
	}
	if !strings.Contains(err.Error(), "dbl") {
		t.Fatalf("Lower error = %q, want it to mention node id %q", err.Error(), "dbl")
	}
	if !strings.Contains(err.Error(), "closure") {
		t.Fatalf("Lower error = %q, want it to mention %q", err.Error(), "closure")
	}
}

// TestLower_ComponentReason proves a chatmodel node is not serializable and
// Lower's error names the node and the word "injected".
func TestLower_ComponentReason(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[llm.Request, llm.Response]()
	model := &spyModel{sr: newSpyStream("x")}
	chat, err := AddChatModelNode(g, "chat", model)
	if err != nil {
		t.Fatalf("AddChatModelNode: %v", err)
	}
	if err := g.Entry(chat); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(chat); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	run, err := g.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if run.Serializable() {
		t.Fatalf("Serializable() = true, want false")
	}
	_, err = run.Lower()
	if err == nil {
		t.Fatalf("Lower: want error, got nil")
	}
	if !strings.Contains(err.Error(), "chat") {
		t.Fatalf("Lower error = %q, want it to mention node id %q", err.Error(), "chat")
	}
	if !strings.Contains(err.Error(), "injected") {
		t.Fatalf("Lower error = %q, want it to mention %q", err.Error(), "injected")
	}
}

// TestLower_MarshalledIRShape asserts the lowered serializable flow's nodes
// use the real builtin.passthrough type, not the synthetic g0/g1 Compile
// types.
func TestLower_MarshalledIRShape(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	p1, _ := AddPassthroughNode[string, string, string](g, "p1")
	p2, _ := AddPassthroughNode[string, string, string](g, "p2")
	_ = g.Entry(p1)
	_ = g.AddEdge(p1, p2)
	_ = g.Exit(p2)

	run, err := g.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	f, err := run.Lower()
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	if len(f.Nodes) != 2 {
		t.Fatalf("Nodes = %d, want 2", len(f.Nodes))
	}
	for _, n := range f.Nodes {
		if n.Type != builtinPassthrough {
			t.Fatalf("node %q Type = %q, want %q", n.ID, n.Type, builtinPassthrough)
		}
	}
}
