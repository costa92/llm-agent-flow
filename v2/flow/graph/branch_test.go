package graph

import (
	"context"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// TestBranch_RoutesAndSkips drives the key router: the "even"/"odd" key
// selects one of two Lambda branches. The chosen branch runs and reaches
// the common Exit; the unchosen branch is skipped (NodeSkipped event).
func TestBranch_RoutesAndSkips(t *testing.T) {
	g := NewGraph[int, string]()

	evenN, err := AddLambdaNode(g, "even", func(_ context.Context, x int) (string, error) {
		return "even", nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode even: %v", err)
	}
	oddN, err := AddLambdaNode(g, "odd", func(_ context.Context, x int) (string, error) {
		return "odd", nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode odd: %v", err)
	}

	br, err := AddBranch(g, "br", func(_ context.Context, x int) (string, error) {
		if x%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	}, map[string]NodeRef{"even": evenN, "odd": oddN})
	if err != nil {
		t.Fatalf("AddBranch: %v", err)
	}

	// Both branches converge on a common passthrough Exit so the engine
	// always has an output regardless of which branch fired.
	exit, err := AddPassthroughNode[int, string, string](g, "exit")
	if err != nil {
		t.Fatalf("AddPassthroughNode exit: %v", err)
	}
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

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// even input -> "even" output, with the odd branch skipped.
	got, err := r.Invoke(context.Background(), 4)
	if err != nil {
		t.Fatalf("Invoke(4): %v", err)
	}
	if got != "even" {
		t.Fatalf("Invoke(4) = %q, want %q", got, "even")
	}

	// odd input -> "odd" output, with the even branch skipped, asserted
	// via the RunStream event log.
	skipped := streamSkipped(t, r, 7)
	if !skipped["even"] {
		t.Fatalf("odd input: want even branch skipped, skipped=%v", skipped)
	}
	if skipped["odd"] {
		t.Fatalf("odd input: odd branch must run, but was skipped")
	}
}

// streamSkipped runs the graph via RunStream and returns the set of node
// IDs that emitted a NodeSkipped event.
func streamSkipped(t *testing.T, r *Runnable[int, string], in int) map[string]bool {
	t.Helper()
	ch, err := r.Events(context.Background(), in)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	skipped := make(map[string]bool)
	for ev := range ch {
		if ev.Kind == flow.NodeSkipped {
			skipped[ev.NodeID] = true
		}
		if ev.Kind == flow.FlowErr {
			t.Fatalf("Stream FlowErr: %v", ev.Err)
		}
	}
	return skipped
}

// TestBranch_RouteTargetTypeMismatch rejects a route whose target cannot
// accept the branch input type T at build time.
func TestBranch_RouteTargetTypeMismatch(t *testing.T) {
	g := NewGraph[int, string]()
	// route target consumes string, but the branch input T is int.
	bad, err := AddLambdaNode(g, "bad", func(_ context.Context, s string) (string, error) {
		return s, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode bad: %v", err)
	}
	if _, err := AddBranch(g, "br", func(_ context.Context, x int) (string, error) {
		return "k", nil
	}, map[string]NodeRef{"k": bad}); err == nil {
		t.Fatalf("AddBranch with int->string route: want error, got nil")
	}
}
