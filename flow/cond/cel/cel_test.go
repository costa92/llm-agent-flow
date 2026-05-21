package cel_test

import (
	"context"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
	"github.com/costa92/llm-agent-flow/flow/cond/cel"
)

func mustCompile(t *testing.T, expr string) flow.Condition {
	t.Helper()
	eval, err := cel.NewEvaluator()
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	c, err := eval.Compile(expr)
	if err != nil {
		t.Fatalf("Compile(%q): %v", expr, err)
	}
	return c
}

func evalCond(t *testing.T, c flow.Condition, value string) bool {
	t.Helper()
	out, err := c.Evaluate(context.Background(), flow.CondEnv{Value: value})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	return out
}

func TestCELEqualityComparison(t *testing.T) {
	c := mustCompile(t, `value == "go"`)
	if !evalCond(t, c, "go") {
		t.Fatal("value=='go' on 'go' = false, want true")
	}
	if evalCond(t, c, "stop") {
		t.Fatal("value=='go' on 'stop' = true, want false")
	}
}

func TestCELStringFunctions(t *testing.T) {
	c := mustCompile(t, `value.startsWith("hello")`)
	if !evalCond(t, c, "hello world") {
		t.Fatal("startsWith hello on 'hello world' = false")
	}
	if evalCond(t, c, "goodbye") {
		t.Fatal("startsWith hello on 'goodbye' = true")
	}
}

func TestCELRegexMatches(t *testing.T) {
	c := mustCompile(t, `value.matches("^[A-Z]+$")`)
	if !evalCond(t, c, "HELLO") {
		t.Fatal("matches /^[A-Z]+$/ on HELLO = false")
	}
	if evalCond(t, c, "Hello") {
		t.Fatal("matches /^[A-Z]+$/ on Hello = true")
	}
}

func TestCELSizeAndLogic(t *testing.T) {
	c := mustCompile(t, `size(value) > 3 && value != "skip"`)
	if !evalCond(t, c, "hello") {
		t.Fatal("len>3 && !=skip on hello = false")
	}
	if evalCond(t, c, "skip") {
		t.Fatal("len>3 && !=skip on skip = true")
	}
	if evalCond(t, c, "hi") {
		t.Fatal("len>3 && !=skip on hi = true")
	}
}

func TestCELCompileSyntaxError(t *testing.T) {
	eval, _ := cel.NewEvaluator()
	_, err := eval.Compile(`value ==`)
	if err == nil || !strings.Contains(err.Error(), "compile") {
		t.Fatalf("Compile(syntax error) = %v, want compile error", err)
	}
}

func TestCELCompileRejectsNonBool(t *testing.T) {
	eval, _ := cel.NewEvaluator()
	_, err := eval.Compile(`value`)
	if err == nil || !strings.Contains(err.Error(), "want bool") {
		t.Fatalf("Compile(non-bool) = %v, want type error", err)
	}
}

func TestCELCompileRejectsUnknownVar(t *testing.T) {
	eval, _ := cel.NewEvaluator()
	_, err := eval.Compile(`nope == "x"`)
	if err == nil {
		t.Fatal("Compile(unknown var) = nil, want error")
	}
}

// TestCELIntegrationWithEngine: round-trips a CEL-conditioned flow
// through the engine to confirm the evaluator and engine wiring work
// end-to-end against the real flow.Compile path.
func TestCELIntegrationWithEngine(t *testing.T) {
	const src = `{
		"id":"router",
		"nodes":[
			{"id":"src","type":"tool","config":{"tool":"src"}},
			{"id":"left","type":"tool","config":{"tool":"echo"}},
			{"id":"right","type":"tool","config":{"tool":"echo"}}
		],
		"edges":[
			{"source":{"node":"src","port":"output"},"target":{"node":"left","port":"input"}, "condition":"value == \"go\""},
			{"source":{"node":"src","port":"output"},"target":{"node":"right","port":"input"},"condition":"value != \"go\""}
		],
		"outputs":[
			{"name":"L","node":"left","port":"output"},
			{"name":"R","node":"right","port":"output"}
		]
	}`
	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := flow.ToolMap{
		"src":  &literalTool{name: "src", v: "go"},
		"echo": &literalTool{name: "echo"},
	}
	eval, err := cel.NewEvaluator()
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	eng, err := flow.LoadCompile(strings.NewReader(src), reg, flow.Deps{Tools: tools},
		flow.WithConditionEvaluator(eval))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	out, err := eng.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if v, ok := out["L"]; !ok || v != "go" {
		t.Fatalf("L = (%q,%v), want (go,true)", v, ok)
	}
	if _, ok := out["R"]; ok {
		t.Fatalf("R unexpectedly present: %+v", out)
	}
}
