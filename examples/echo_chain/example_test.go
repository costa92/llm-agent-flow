package echochain_test

import (
	"context"
	"os"
	"testing"

	"github.com/costa92/llm-agent-flow/examples/echo_chain"
	"github.com/costa92/llm-agent-flow/flow"
)

// TestExampleFlowRoundTrip is the canonical "did the v0 stack
// actually run" gate. It reads the bundled flow.json from disk, wires
// in the bundled tools, runs the engine, and asserts the deterministic
// output. If this test fails, every subsequent commit is at risk.
func TestExampleFlowRoundTrip(t *testing.T) {
	f, err := os.Open("flow.json")
	if err != nil {
		t.Fatalf("open flow.json: %v", err)
	}
	defer f.Close()

	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := flow.FromAgentTools(echochain.Tools())
	engine, err := flow.LoadCompile(f, reg, flow.Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	out, err := engine.Run(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, want := out["out"], "OLLEH"; got != want {
		t.Fatalf("out=%q, want %q", got, want)
	}
}
