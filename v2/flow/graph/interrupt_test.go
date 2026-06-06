package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// interruptOnce is a typed lambda fn that always requests an interrupt.
func interruptFn(_ context.Context, _ string) (string, error) {
	return "", flow.Interrupt(flow.InterruptRequest{Kind: "approval", Prompt: "ok?"})
}

// TestInterruptibleLambda_IsInterruptCapable: the adapter an
// AddInterruptibleLambdaNode produces satisfies flow.InterruptCapable.
func TestInterruptibleLambda_IsInterruptCapable(t *testing.T) {
	g := NewGraph[string, string]()
	if _, err := AddInterruptibleLambdaNode[string, string, string, string](g, "approve", interruptFn); err != nil {
		t.Fatalf("add: %v", err)
	}
	// White-box: build the node's runtime kind and assert capability.
	var kind flow.NodeKind
	for _, n := range g.nodes {
		if n.ref.id == "approve" {
			kind = n.newKind()
		}
	}
	if kind == nil {
		t.Fatal("node not found")
	}
	ic, ok := kind.(flow.InterruptCapable)
	if !ok {
		t.Fatal("adapter does not satisfy flow.InterruptCapable")
	}
	if !ic.CanInterrupt() {
		t.Fatal("CanInterrupt() == false, want true")
	}
}

// TestInterruptibleLambda_RunResumableSuspends: a typed graph with an
// interruptible lambda suspends at that node and resumes to completion (string
// ports use the built-in codec, so the snapshot succeeds end to end).
func TestInterruptibleLambda_RunResumableSuspends(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	entry, _ := AddPassthroughNode[string, string, string](g, "entry")
	approve, _ := AddInterruptibleLambdaNode[string, string, string, string](g, "approve", interruptFn)
	out, _ := AddPassthroughNode[string, string, string](g, "out")
	if err := g.Entry(entry); err != nil {
		t.Fatalf("entry: %v", err)
	}
	if err := g.AddEdge(entry, approve); err != nil {
		t.Fatalf("edge1: %v", err)
	}
	if err := g.AddEdge(approve, out); err != nil {
		t.Fatalf("edge2: %v", err)
	}
	if err := g.Exit(out); err != nil {
		t.Fatalf("exit: %v", err)
	}
	cs := flow.NewMemoryCheckpointStore()
	run, err := g.Compile(ctx, flow.WithCheckpointStore(cs))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runID := flow.NewRunID()
	res, err := run.engine.RunResumable(ctx, runID, map[string]any{graphInputKey: "x"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil || res.Suspended.NodeID != "approve" {
		t.Fatalf("expected suspension at approve, got %+v", res.Suspended)
	}
	if res.Suspended.Request.Prompt != "ok?" {
		t.Fatalf("request prompt = %q, want ok?", res.Suspended.Request.Prompt)
	}

	// Inject the approve node's output port (portOut) and resume.
	res2, err := run.engine.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{portOut: "approved"})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := res2.Outputs[graphOutputKey]; got != "approved" {
		t.Fatalf("resumed output = %v, want approved", got)
	}
}

// TestPlainLambda_Interrupt_StillUninstrumented: an ordinary AddLambdaNode that
// returns an interrupt is NOT capable, so the run fails — guards that #7 did
// not make every lambda interrupt-capable.
func TestPlainLambda_Interrupt_StillUninstrumented(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	entry, _ := AddPassthroughNode[string, string, string](g, "entry")
	bad, _ := AddLambdaNode[string, string, string, string](g, "bad", interruptFn)
	out, _ := AddPassthroughNode[string, string, string](g, "out")
	_ = g.Entry(entry)
	_ = g.AddEdge(entry, bad)
	_ = g.AddEdge(bad, out)
	_ = g.Exit(out)
	cs := flow.NewMemoryCheckpointStore()
	run, err := g.Compile(ctx, flow.WithCheckpointStore(cs))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = run.engine.RunResumable(ctx, flow.NewRunID(), map[string]any{graphInputKey: "x"})
	if !errors.Is(err, flow.ErrUninstrumentedInterrupt) {
		t.Fatalf("got err %v, want ErrUninstrumentedInterrupt", err)
	}
}

// TestTwoInterruptibleLambdas_SameLayer_Rejected: two interrupt-capable nodes
// in one layer + a checkpoint store → Compile rejects (FC-NEW3 static check
// fires through the typed front-end's lowering).
func TestTwoInterruptibleLambdas_SameLayer_Rejected(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	entry, _ := AddPassthroughNode[string, string, string](g, "entry")
	a, _ := AddInterruptibleLambdaNode[string, string, string, string](g, "a", interruptFn)
	b, _ := AddInterruptibleLambdaNode[string, string, string, string](g, "b", interruptFn)
	out, _ := AddPassthroughNode[string, string, string](g, "out")
	_ = g.Entry(entry)
	_ = g.AddEdge(entry, a) // a, b both depend only on entry → same layer
	_ = g.AddEdge(entry, b)
	_ = g.AddEdge(a, out)
	_ = g.AddEdge(b, out)
	_ = g.Exit(out)
	cs := flow.NewMemoryCheckpointStore()
	_, err := g.Compile(ctx, flow.WithCheckpointStore(cs))
	if !errors.Is(err, flow.ErrMultipleInterrupts) {
		t.Fatalf("got err %v, want ErrMultipleInterrupts", err)
	}
}

// TestInterruptibleLambda_OnePerLayer_OK: one interrupt-capable node per layer
// compiles cleanly with a checkpoint store.
func TestInterruptibleLambda_OnePerLayer_OK(t *testing.T) {
	ctx := context.Background()
	g := NewGraph[string, string]()
	entry, _ := AddPassthroughNode[string, string, string](g, "entry")
	approve, _ := AddInterruptibleLambdaNode[string, string, string, string](g, "approve", interruptFn)
	out, _ := AddPassthroughNode[string, string, string](g, "out")
	_ = g.Entry(entry)
	_ = g.AddEdge(entry, approve)
	_ = g.AddEdge(approve, out)
	_ = g.Exit(out)
	cs := flow.NewMemoryCheckpointStore()
	if _, err := g.Compile(ctx, flow.WithCheckpointStore(cs)); err != nil {
		t.Fatalf("compile: %v", err)
	}
}
