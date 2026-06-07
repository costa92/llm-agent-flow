package graph

import (
	"context"
	"testing"
)

// TestAddCopy_InvokeForwardsToAll drives a copy fan-out: a passthrough entry
// feeds a copy node that forwards its int IN FULL to both target lambdas; a
// combine sums the two target results. The copy node emits the SAME value on
// every route port (parallelAdapter semantics) so both targets see the input.
// Run 200× to assert byte-identical determinism on the value path.
func TestAddCopy_InvokeForwardsToAll(t *testing.T) {
	build := func() *Runnable[int, int] {
		g := NewGraph[int, int]()
		entry, err := AddPassthroughNode[int, int, int](g, "entry")
		if err != nil {
			t.Fatalf("AddPassthroughNode entry: %v", err)
		}
		aN, err := AddLambdaNode(g, "a", func(_ context.Context, x int) (int, error) {
			return x + 1, nil
		})
		if err != nil {
			t.Fatalf("AddLambdaNode a: %v", err)
		}
		bN, err := AddLambdaNode(g, "b", func(_ context.Context, x int) (int, error) {
			return x * 2, nil
		})
		if err != nil {
			t.Fatalf("AddLambdaNode b: %v", err)
		}
		cp, err := AddCopy[int, int, int](g, "cp", map[string]NodeRef{"a": aN, "b": bN})
		if err != nil {
			t.Fatalf("AddCopy: %v", err)
		}
		comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"a": aN, "b": bN},
			func(_ context.Context, in map[string]any) (int, error) {
				return in["a"].(int) + in["b"].(int), nil
			})
		if err != nil {
			t.Fatalf("AddCombine: %v", err)
		}
		if err := g.AddEdge(entry, cp); err != nil {
			t.Fatalf("AddEdge entry->cp: %v", err)
		}
		if err := g.Entry(entry); err != nil {
			t.Fatalf("Entry: %v", err)
		}
		if err := g.Exit(comb); err != nil {
			t.Fatalf("Exit: %v", err)
		}
		r, err := g.Compile(context.Background())
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return r
	}

	r := build()
	// x=3 -> a:(3+1)=4, b:(3*2)=6, combine sum = 10. Both targets received 3.
	for i := 0; i < 200; i++ {
		got, err := r.Invoke(context.Background(), 3)
		if err != nil {
			t.Fatalf("Invoke(3) iter %d: %v", i, err)
		}
		if got != 10 {
			t.Fatalf("Invoke(3) iter %d = %d, want 10 (both targets received the value)", i, got)
		}
	}
}

// TestAddCopy_RejectsUnassignableTarget proves AddCopy validates each target's
// input type against T at build time: a string-input target wired under a
// T=int copy must latch an error (like AddParallel).
func TestAddCopy_RejectsUnassignableTarget(t *testing.T) {
	g := NewGraph[int, int]()
	strTarget, err := AddLambdaNode(g, "strTarget", func(_ context.Context, s string) (string, error) {
		return s, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode strTarget: %v", err)
	}
	if _, err := AddCopy[int, int, int](g, "cp", map[string]NodeRef{"s": strTarget}); err == nil {
		t.Fatalf("AddCopy with int->string target: want error, got nil")
	}
}

// TestAddCopy_NoTargets rejects a copy node with an empty target map.
func TestAddCopy_NoTargets(t *testing.T) {
	g := NewGraph[int, int]()
	if _, err := AddCopy[int, int, int](g, "cp", map[string]NodeRef{}); err == nil {
		t.Fatalf("AddCopy with no targets: want error, got nil")
	}
}

// TestAddCopy_EmptyKey rejects an empty target key.
func TestAddCopy_EmptyKey(t *testing.T) {
	g := NewGraph[int, int]()
	tgt, _ := AddLambdaNode(g, "t", func(_ context.Context, x int) (int, error) { return x, nil })
	if _, err := AddCopy[int, int, int](g, "cp", map[string]NodeRef{"": tgt}); err == nil {
		t.Fatalf("AddCopy with empty target key: want error, got nil")
	}
}
