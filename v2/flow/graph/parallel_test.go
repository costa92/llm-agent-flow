package graph

import (
	"context"
	"testing"
)

// TestAddParallel_FanOutRunsAllBranches drives a fan-out: a passthrough entry
// feeds a parallel node that forwards its int to both branch lambdas; a
// combine sums the two branch results. Asserting the combine output proves
// BOTH branches ran and both reached the combiner.
func TestAddParallel_FanOutRunsAllBranches(t *testing.T) {
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

	par, err := AddParallel[int, int, int](g, "par", map[string]NodeRef{"a": aN, "b": bN})
	if err != nil {
		t.Fatalf("AddParallel: %v", err)
	}
	comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, in map[string]any) (int, error) {
			return in["a"].(int) + in["b"].(int), nil
		})
	if err != nil {
		t.Fatalf("AddCombine: %v", err)
	}

	if err := g.AddEdge(entry, par); err != nil {
		t.Fatalf("AddEdge entry->par: %v", err)
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

	// x=3 -> a:(3+1)=4, b:(3*2)=6, combine sum = 10.
	got, err := r.Invoke(context.Background(), 3)
	if err != nil {
		t.Fatalf("Invoke(3): %v", err)
	}
	if got != 10 {
		t.Fatalf("Invoke(3) = %d, want 10 (both branches ran)", got)
	}
}

// TestAddCombine_GathersAllSources is the canonical diamond: entry -> parallel
// -> {A: x+1, B: x*2} -> combine(sum) -> exit. Invoke(3) = (3+1)+(3*2) = 10.
func TestAddCombine_GathersAllSources(t *testing.T) {
	g := NewGraph[int, int]()

	entry, err := AddPassthroughNode[int, int, int](g, "entry")
	if err != nil {
		t.Fatalf("AddPassthroughNode entry: %v", err)
	}
	aN, err := AddLambdaNode(g, "A", func(_ context.Context, x int) (int, error) {
		return x + 1, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode A: %v", err)
	}
	bN, err := AddLambdaNode(g, "B", func(_ context.Context, x int) (int, error) {
		return x * 2, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode B: %v", err)
	}
	par, err := AddParallel[int, int, int](g, "par", map[string]NodeRef{"A": aN, "B": bN})
	if err != nil {
		t.Fatalf("AddParallel: %v", err)
	}
	comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"A": aN, "B": bN},
		func(_ context.Context, in map[string]any) (int, error) {
			sum := 0
			for _, v := range in {
				sum += v.(int)
			}
			return sum, nil
		})
	if err != nil {
		t.Fatalf("AddCombine: %v", err)
	}

	if err := g.AddEdge(entry, par); err != nil {
		t.Fatalf("AddEdge entry->par: %v", err)
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
	got, err := r.Invoke(context.Background(), 3)
	if err != nil {
		t.Fatalf("Invoke(3): %v", err)
	}
	if got != 10 {
		t.Fatalf("Invoke(3) = %d, want 10", got)
	}
}

// TestAddCombine_TypeMismatch_FailsAtBuild proves AddEdgeTo into a combiner
// port latches a type error when the from-node's output type cannot feed the
// declared port type. AddCombine derives the port type FROM its source, so
// the mismatch arises when a DIFFERENT, wrong-typed node is wired into a port
// via AddEdgeTo.
func TestAddCombine_TypeMismatch_FailsAtBuild(t *testing.T) {
	g := NewGraph[int, int]()

	// Source declared for the combine ports is an int producer.
	intSrc, err := AddLambdaNode(g, "intSrc", func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode intSrc: %v", err)
	}
	comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"k": intSrc},
		func(_ context.Context, in map[string]any) (int, error) {
			return in["k"].(int), nil
		})
	if err != nil {
		t.Fatalf("AddCombine: %v", err)
	}

	// A string-producing node wired into the int "k" port must latch.
	strN, err := AddLambdaNode(g, "strN", func(_ context.Context, x int) (string, error) {
		return "x", nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode strN: %v", err)
	}
	if err := g.AddEdgeTo(strN, comb, "k"); err == nil {
		t.Fatalf("AddEdgeTo string->int port: want error, got nil")
	}

	// And the error must surface at Compile even after wiring the rest.
	entry, _ := AddPassthroughNode[int, int, int](g, "entry")
	_ = g.AddEdge(entry, intSrc)
	_ = g.Entry(entry)
	_ = g.Exit(comb)
	if _, err := g.Compile(context.Background()); err == nil {
		t.Fatalf("Compile: want latched type error, got nil")
	}
}

// TestAddCombine_UnknownPort rejects AddEdgeTo to a combiner port name that
// was never declared.
func TestAddCombine_UnknownPort(t *testing.T) {
	g := NewGraph[int, int]()
	src, _ := AddLambdaNode(g, "src", func(_ context.Context, x int) (int, error) { return x, nil })
	comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"k": src},
		func(_ context.Context, in map[string]any) (int, error) { return 0, nil })
	if err != nil {
		t.Fatalf("AddCombine: %v", err)
	}
	other, _ := AddLambdaNode(g, "other", func(_ context.Context, x int) (int, error) { return x, nil })
	if err := g.AddEdgeTo(other, comb, "nope"); err == nil {
		t.Fatalf("AddEdgeTo to undeclared port: want error, got nil")
	}
}

// TestParallel_NotStreamCapable confirms a graph with parallel/combine is not
// stream-capable yet Stream still returns the correct boxed result.
func TestParallel_NotStreamCapable(t *testing.T) {
	g := NewGraph[int, int]()
	entry, _ := AddPassthroughNode[int, int, int](g, "entry")
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (int, error) { return x * 2, nil })
	par, err := AddParallel[int, int, int](g, "par", map[string]NodeRef{"a": aN, "b": bN})
	if err != nil {
		t.Fatalf("AddParallel: %v", err)
	}
	_ = par
	comb, err := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, in map[string]any) (int, error) {
			return in["a"].(int) + in["b"].(int), nil
		})
	if err != nil {
		t.Fatalf("AddCombine: %v", err)
	}
	_ = g.AddEdge(entry, par)
	_ = g.Entry(entry)
	_ = g.Exit(comb)

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if r.StreamCapable() {
		t.Fatalf("StreamCapable() = true, want false for fan-out/fan-in graph")
	}

	sr, err := r.Stream(context.Background(), 3)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer sr.Close()
	v, err := sr.Next()
	if err != nil {
		t.Fatalf("Stream.Next: %v", err)
	}
	if v != 10 {
		t.Fatalf("Stream result = %d, want 10 (boxed Invoke)", v)
	}
}

// TestParallel_TransparentToExpectedValue asserts the sugar is transparent:
// the AddParallel/AddCombine diamond computes the same value a hand-wired
// fan-out/fan-in would. (x+1)+(x*2) for x=5 is 6+10 = 16.
func TestParallel_TransparentToExpectedValue(t *testing.T) {
	g := NewGraph[int, int]()
	entry, _ := AddPassthroughNode[int, int, int](g, "entry")
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (int, error) { return x * 2, nil })
	par, _ := AddParallel[int, int, int](g, "par", map[string]NodeRef{"a": aN, "b": bN})
	comb, _ := AddCombine[int, int, int](g, "comb", map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, in map[string]any) (int, error) {
			return in["a"].(int) + in["b"].(int), nil
		})
	_ = par
	_ = g.AddEdge(entry, par)
	_ = g.Entry(entry)
	_ = g.Exit(comb)

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), 5)
	if err != nil {
		t.Fatalf("Invoke(5): %v", err)
	}
	want := (5 + 1) + (5 * 2) // 16
	if got != want {
		t.Fatalf("Invoke(5) = %d, want %d", got, want)
	}
}
