package graph

import (
	"context"
	"strings"
	"testing"
)

// --- local model types for the type-alignment tests (no contract dep) ---

// typeA and typeB are two unrelated concrete struct types used to drive
// the mismatch case: an edge from a typeA producer to a typeB consumer
// must be rejected.
type typeA struct{ n int }
type typeB struct{ s string }

// greeter is an interface implemented by concreteGreeter (pointer
// receiver), used to drive the interface-assignability rule.
type greeter interface{ Greet() string }

type concreteGreeter struct{ name string }

func (c *concreteGreeter) Greet() string { return "hi " + c.name }

func TestInvoke_LambdaChain(t *testing.T) {
	g := NewGraph[int, int]()
	inc1, err := AddLambdaNode(g, "inc1", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode inc1: %v", err)
	}
	inc2, err := AddLambdaNode(g, "inc2", func(_ context.Context, x int) (int, error) { return x + 1, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode inc2: %v", err)
	}
	if err := g.AddEdge(inc1, inc2); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Entry(inc1); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(inc2); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), 1)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != 3 {
		t.Fatalf("Invoke(1) = %d, want 3", got)
	}
}

func TestAddEdge_TypeMismatch(t *testing.T) {
	g := NewGraph[typeA, typeB]()
	a, err := AddLambdaNode(g, "a", func(_ context.Context, in typeA) (typeA, error) { return in, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode a: %v", err)
	}
	b, err := AddLambdaNode(g, "b", func(_ context.Context, in typeB) (typeB, error) { return in, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode b: %v", err)
	}
	// a.outT == typeA, b.inT == typeB: not assignable.
	if err := g.AddEdge(a, b); err == nil {
		t.Fatalf("AddEdge with mismatched types: want error, got nil")
	}
	// The latched error must also surface from Compile.
	_ = g.Entry(a)
	_ = g.Exit(b)
	if _, err := g.Compile(context.Background()); err == nil {
		t.Fatalf("Compile with latched mismatch: want error, got nil")
	}
}

func TestAddEdge_InterfaceRule(t *testing.T) {
	g := NewGraph[string, string]()
	// producer outputs *concreteGreeter (implements greeter via pointer recv).
	prod, err := AddLambdaNode(g, "prod", func(_ context.Context, name string) (*concreteGreeter, error) {
		return &concreteGreeter{name: name}, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode prod: %v", err)
	}
	// consumer accepts the greeter interface.
	cons, err := AddLambdaNode(g, "cons", func(_ context.Context, gr greeter) (string, error) {
		return gr.Greet(), nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode cons: %v", err)
	}
	if err := g.AddEdge(prod, cons); err != nil {
		t.Fatalf("AddEdge concrete->interface: want nil, got %v", err)
	}
	if err := g.Entry(prod); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(cons); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), "bob")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != "hi bob" {
		t.Fatalf("Invoke = %q, want %q", got, "hi bob")
	}
}

func TestAddEdge_AnyDeferredToRuntime(t *testing.T) {
	g := NewGraph[int, string]()
	// producer outputs any (statically). AddEdge must accept (deferred).
	prod, err := AddLambdaNode(g, "prod", func(_ context.Context, x int) (any, error) {
		// returns a string, which the downstream int consumer can't take.
		return "not-an-int", nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode prod: %v", err)
	}
	cons, err := AddLambdaNode(g, "cons", func(_ context.Context, x int) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode cons: %v", err)
	}
	// any -> int: build-time OK (deferred to runtime).
	if err := g.AddEdge(prod, cons); err != nil {
		t.Fatalf("AddEdge any->int: want nil (deferred), got %v", err)
	}
	if err := g.Entry(prod); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(cons); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// At runtime the actual value is a string, cons wants int -> error.
	if _, err := r.Invoke(context.Background(), 1); err == nil {
		t.Fatalf("Invoke with runtime type mismatch: want error, got nil")
	} else if !strings.Contains(err.Error(), "cons") {
		t.Logf("runtime error (ok): %v", err)
	}
}

func TestPassthrough_Identity(t *testing.T) {
	g := NewGraph[string, string]()
	p, err := AddPassthroughNode[string, string, string](g, "p")
	if err != nil {
		t.Fatalf("AddPassthroughNode: %v", err)
	}
	if err := g.Entry(p); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(p); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != "echo" {
		t.Fatalf("passthrough = %q, want %q", got, "echo")
	}
}

func TestEntryExit_TypeMismatch(t *testing.T) {
	g := NewGraph[int, int]()
	// node takes/returns string; graph I/O is int -> Entry & Exit mismatch.
	n, err := AddLambdaNode(g, "n", func(_ context.Context, s string) (string, error) { return s, nil })
	if err != nil {
		t.Fatalf("AddLambdaNode: %v", err)
	}
	if err := g.Entry(n); err == nil {
		t.Fatalf("Entry int->string node: want error, got nil")
	}
	if err := g.Exit(n); err == nil {
		t.Fatalf("Exit string-node->int: want error, got nil")
	}
}
