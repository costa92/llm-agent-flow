package graph

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// TestStatefulLambda_RunningSummary chains three stateful lambda nodes that
// each append their tag to a running []string State. Because the chain is
// linear, each node is the single writer in its layer (default reduce, no
// collision), so the final State is all three contributions in order. State
// here is RUNTIME-ONLY (the typed route is non-serializable / non-checkpointed).
func TestStatefulLambda_RunningSummary(t *testing.T) {
	g := NewGraph[string, []string]()

	// Three nodes append to the running []string State. n1 takes the graph's
	// string input; n2/n3 take the []string flowing along the chain. Each
	// reads State from the cell, appends its tag, SetStates, and emits the
	// summary as data flow too.
	n1, err := AddStatefulLambdaNode(g, "n1", func(ctx context.Context, _ string, state []string) ([]string, error) {
		next := append(append([]string{}, state...), "a")
		flow.SetState(ctx, next)
		return next, nil
	})
	if err != nil {
		t.Fatalf("add n1: %v", err)
	}
	n2, err := AddStatefulLambdaNode(g, "n2", func(ctx context.Context, _ []string, state []string) ([]string, error) {
		next := append(append([]string{}, state...), "b")
		flow.SetState(ctx, next)
		return next, nil
	})
	if err != nil {
		t.Fatalf("add n2: %v", err)
	}
	n3, err := AddStatefulLambdaNode(g, "n3", func(ctx context.Context, _ []string, state []string) ([]string, error) {
		next := append(append([]string{}, state...), "c")
		flow.SetState(ctx, next)
		return next, nil
	})
	if err != nil {
		t.Fatalf("add n3: %v", err)
	}
	if err := g.AddEdge(n1, n2); err != nil {
		t.Fatalf("edge n1->n2: %v", err)
	}
	if err := g.AddEdge(n2, n3); err != nil {
		t.Fatalf("edge n2->n3: %v", err)
	}
	if err := g.Entry(n1); err != nil {
		t.Fatalf("entry: %v", err)
	}
	if err := g.Exit(n3); err != nil {
		t.Fatalf("exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// The final node's Out is the running summary read from State, so the
	// returned O reflects all three contributions in order.
	got, err := r.InvokeWithState(context.Background(), "start", flow.WithInitialState([]string{}))
	if err != nil {
		t.Fatalf("InvokeWithState: %v", err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("running summary = %v, want %v", got, want)
	}
}

// TestStatefulLambda_StatelessInvokeInert asserts a stateful node run via the
// bare Invoke (no State opts) behaves inertly: StateFromContext returns
// (zero,false) inside the node and SetState is a no-op, so the node still
// produces its output from the zero State.
func TestStatefulLambda_StatelessInvokeInert(t *testing.T) {
	g := NewGraph[string, string]()
	var sawOK bool
	n, err := AddStatefulLambdaNode(g, "n", func(ctx context.Context, in string, state []string) (string, error) {
		// Confirm the cell-less ctx yields no State.
		_, ok := flow.StateFromContext[[]string](ctx)
		sawOK = ok
		flow.SetState(ctx, []string{"should-be-dropped"}) // no-op without a cell
		return in + ":" + strings.Join(state, ","), nil
	})
	if err != nil {
		t.Fatalf("add n: %v", err)
	}
	if err := g.Entry(n); err != nil {
		t.Fatalf("entry: %v", err)
	}
	if err := g.Exit(n); err != nil {
		t.Fatalf("exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), "x")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if sawOK {
		t.Fatalf("StateFromContext returned ok=true on a State-less Invoke")
	}
	if got != "x:" { // empty State joined → ""
		t.Fatalf("output %q, want %q (inert State)", got, "x:")
	}
}
