package typedgraph_test

import (
	"context"
	"testing"

	typedgraph "github.com/costa92/llm-agent-flow/v2/examples/typed_graph"
)

// TestExample_TypedGraphRoundTrip is the "did the typed v2 stack actually
// run" gate. It builds the template -> chatmodel -> extract -> tool graph,
// Invokes it, and asserts the deterministic result. If this fails the
// typed front-end's lower-to-engine path is broken.
func TestExample_TypedGraphRoundTrip(t *testing.T) {
	got, err := typedgraph.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := "5"; got != want {
		t.Fatalf("Run = %q, want %q", got, want)
	}
}
