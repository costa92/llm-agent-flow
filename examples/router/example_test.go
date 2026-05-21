// Package router_test drives the router example end-to-end: load
// flow.json, register the bundled tools, wire the CEL evaluator, then
// run two inputs and confirm only the matching branch fires.
package router_test

import (
	"context"
	"os"
	"testing"

	"github.com/costa92/llm-agent-flow/examples/router"
	"github.com/costa92/llm-agent-flow/flow"
	cond "github.com/costa92/llm-agent-flow/flow/cond/cel"
)

func newRouterEngine(t *testing.T) *flow.Engine {
	t.Helper()
	f, err := os.Open("flow.json")
	if err != nil {
		t.Fatalf("open flow.json: %v", err)
	}
	defer f.Close()

	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := flow.FromAgentTools(router.Tools())
	celEval, err := cond.NewEvaluator()
	if err != nil {
		t.Fatalf("cel.NewEvaluator: %v", err)
	}
	eng, err := flow.LoadCompile(f, reg, flow.Deps{Tools: tools}, flow.WithConditionEvaluator(celEval))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	return eng
}

func TestRouterGreetBranch(t *testing.T) {
	eng := newRouterEngine(t)
	out, err := eng.Run(context.Background(), map[string]string{"in": "hello there"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, ok := out["greeting"]; !ok || got != "Hello! Nice to see you." {
		t.Fatalf("greeting = (%q,%v), want canonical greeting", got, ok)
	}
	if _, ok := out["other"]; ok {
		t.Fatalf("other branch unexpectedly present: %+v", out)
	}
}

func TestRouterOtherBranch(t *testing.T) {
	eng := newRouterEngine(t)
	out, err := eng.Run(context.Background(), map[string]string{"in": "what time is it"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, ok := out["other"]; !ok || got == "" {
		t.Fatalf("other = (%q,%v), want non-empty fallback", got, ok)
	}
	if _, ok := out["greeting"]; ok {
		t.Fatalf("greet branch unexpectedly present: %+v", out)
	}
}
