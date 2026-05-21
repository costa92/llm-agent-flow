package flow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// stubEvaluator is a deterministic in-package ConditionEvaluator used
// to exercise the activation algorithm without pulling cel-go into
// the core flow package. Supported syntax:
//
//	==<lit>      → fire iff env.Value == lit
//	!=<lit>      → fire iff env.Value != lit
//	bad          → Compile returns an error
//	boom         → Compile returns ok, Evaluate returns an error
//	always       → always fire
//	never        → never fire
type stubEvaluator struct{}

func (stubEvaluator) Compile(expr string) (Condition, error) {
	switch {
	case expr == "bad":
		return nil, errors.New("stub: bad expression")
	case expr == "boom":
		return stubCond{kind: "boom"}, nil
	case expr == "always":
		return stubCond{kind: "always"}, nil
	case expr == "never":
		return stubCond{kind: "never"}, nil
	case strings.HasPrefix(expr, "=="):
		return stubCond{kind: "eq", lit: strings.TrimPrefix(expr, "==")}, nil
	case strings.HasPrefix(expr, "!="):
		return stubCond{kind: "ne", lit: strings.TrimPrefix(expr, "!=")}, nil
	}
	return nil, errors.New("stub: unknown expression " + expr)
}

type stubCond struct {
	kind string
	lit  string
}

func (s stubCond) Evaluate(_ context.Context, env CondEnv) (bool, error) {
	switch s.kind {
	case "always":
		return true, nil
	case "never":
		return false, nil
	case "boom":
		return false, errors.New("stub: evaluate boom")
	case "eq":
		return env.Value == s.lit, nil
	case "ne":
		return env.Value != s.lit, nil
	}
	return false, errors.New("stub: bad kind")
}

// passthroughTool returns input["input"] verbatim (or the static
// "literal" config value if set). Useful as the source of a string
// value to test conditions against.
type passthroughTool struct {
	name    string
	literal string
}

func (t *passthroughTool) Name() string { return t.name }
func (t *passthroughTool) Execute(_ context.Context, args json.RawMessage) (string, error) {
	if t.literal != "" {
		return t.literal, nil
	}
	var p struct {
		Input string `json:"input"`
	}
	_ = json.Unmarshal(args, &p)
	return p.Input, nil
}

// routerFlowJSON is a small "router" diamond:
//
//	source → (==go) → left → joinL  → out_left
//	source → (!=go) → right → joinR  → out_right
//
// "source" emits a literal; the two outgoing edges have opposing
// conditions so exactly one branch activates per run.
const routerFlowJSON = `{
  "id": "router",
  "nodes": [
    { "id": "source",  "type": "tool", "config": { "tool": "src" } },
    { "id": "left",    "type": "tool", "config": { "tool": "echo" } },
    { "id": "right",   "type": "tool", "config": { "tool": "echo" } },
    { "id": "joinL",   "type": "tool", "config": { "tool": "echo" } },
    { "id": "joinR",   "type": "tool", "config": { "tool": "echo" } }
  ],
  "edges": [
    { "source": { "node": "source", "port": "output" }, "target": { "node": "left",  "port": "input" }, "condition": "==go" },
    { "source": { "node": "source", "port": "output" }, "target": { "node": "right", "port": "input" }, "condition": "!=go" },
    { "source": { "node": "left",   "port": "output" }, "target": { "node": "joinL", "port": "input" } },
    { "source": { "node": "right",  "port": "output" }, "target": { "node": "joinR", "port": "input" } }
  ],
  "outputs": [
    { "name": "from_left",  "node": "joinL", "port": "output" },
    { "name": "from_right", "node": "joinR", "port": "output" }
  ]
}`

func newRouterEngine(t *testing.T, sourceValue string) *Engine {
	t.Helper()
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := ToolMap{
		"src":  &passthroughTool{name: "src", literal: sourceValue},
		"echo": &passthroughTool{name: "echo"},
	}
	eng, err := LoadCompile(strings.NewReader(routerFlowJSON), reg, Deps{Tools: tools},
		WithConditionEvaluator(stubEvaluator{}))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	return eng
}

func TestConditionalRouterFiresLeftWhenValueMatches(t *testing.T) {
	eng := newRouterEngine(t, "go")
	out, err := eng.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, ok := out["from_left"]; !ok || got != "go" {
		t.Fatalf("from_left = (%q,%v), want (go,true)", got, ok)
	}
	if _, ok := out["from_right"]; ok {
		t.Fatalf("from_right unexpectedly present: %+v", out)
	}
}

func TestConditionalRouterFiresRightWhenValueDiffers(t *testing.T) {
	eng := newRouterEngine(t, "stop")
	out, err := eng.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, ok := out["from_right"]; !ok || got != "stop" {
		t.Fatalf("from_right = (%q,%v), want (stop,true)", got, ok)
	}
	if _, ok := out["from_left"]; ok {
		t.Fatalf("from_left unexpectedly present: %+v", out)
	}
}

func TestConditionalRouterSkipPropagatesDownstream(t *testing.T) {
	// Both edges always-fire OR never-fire — but here we use never on
	// the left so joinL must also be skipped (no incoming edge fires).
	const src = `{
		"id":"x",
		"nodes":[
			{"id":"a","type":"tool","config":{"tool":"src"}},
			{"id":"b","type":"tool","config":{"tool":"echo"}},
			{"id":"c","type":"tool","config":{"tool":"echo"}}
		],
		"edges":[
			{"source":{"node":"a","port":"output"},"target":{"node":"b","port":"input"},"condition":"never"},
			{"source":{"node":"b","port":"output"},"target":{"node":"c","port":"input"}}
		],
		"outputs":[{"name":"c_out","node":"c","port":"output"}]
	}`
	reg := NewNodeRegistry()
	_ = RegisterToolNode(reg)
	tools := ToolMap{
		"src":  &passthroughTool{name: "src", literal: "x"},
		"echo": &passthroughTool{name: "echo"},
	}
	eng, err := LoadCompile(strings.NewReader(src), reg, Deps{Tools: tools}, WithConditionEvaluator(stubEvaluator{}))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	ch, err := eng.RunStream(context.Background(), nil)
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	skipped := map[string]bool{}
	for ev := range ch {
		if ev.Kind == NodeSkipped {
			skipped[ev.NodeID] = true
		}
		if ev.Kind == FlowErr {
			t.Fatalf("FlowErr: %v", ev.Err)
		}
	}
	if !skipped["b"] || !skipped["c"] {
		t.Fatalf("expected b and c to be skipped, got %+v", skipped)
	}
}

func TestCompileRejectsConditionWithoutEvaluator(t *testing.T) {
	reg := NewNodeRegistry()
	_ = RegisterToolNode(reg)
	tools := ToolMap{
		"src":  &passthroughTool{name: "src", literal: "go"},
		"echo": &passthroughTool{name: "echo"},
	}
	_, err := LoadCompile(strings.NewReader(routerFlowJSON), reg, Deps{Tools: tools})
	if err == nil || !strings.Contains(err.Error(), "ConditionEvaluator") {
		t.Fatalf("LoadCompile(conds, no eval) = %v, want evaluator-required error", err)
	}
}

func TestCompileSurfacesEvaluatorParseError(t *testing.T) {
	const src = `{
		"id":"x",
		"nodes":[{"id":"a","type":"tool","config":{"tool":"src"}}],
		"edges":[{"source":{"node":"a","port":"output"},"target":{"node":"a","port":"input"},"condition":"bad"}],
		"outputs":[]
	}`
	// Note: this flow has a self-loop, Validate will reject first;
	// craft a 2-node flow instead.
	const src2 = `{
		"id":"x",
		"nodes":[
			{"id":"a","type":"tool","config":{"tool":"src"}},
			{"id":"b","type":"tool","config":{"tool":"echo"}}
		],
		"edges":[{"source":{"node":"a","port":"output"},"target":{"node":"b","port":"input"},"condition":"bad"}],
		"outputs":[]
	}`
	_ = src
	reg := NewNodeRegistry()
	_ = RegisterToolNode(reg)
	tools := ToolMap{"src": &passthroughTool{name: "src", literal: "x"}, "echo": &passthroughTool{name: "echo"}}
	_, err := LoadCompile(strings.NewReader(src2), reg, Deps{Tools: tools}, WithConditionEvaluator(stubEvaluator{}))
	if err == nil || !strings.Contains(err.Error(), "bad expression") {
		t.Fatalf("LoadCompile(bad cond) = %v, want compile-time parse error", err)
	}
}

func TestEvaluatorRuntimeErrorSurfacesAsFlowErr(t *testing.T) {
	const src = `{
		"id":"x",
		"nodes":[
			{"id":"a","type":"tool","config":{"tool":"src"}},
			{"id":"b","type":"tool","config":{"tool":"echo"}}
		],
		"edges":[{"source":{"node":"a","port":"output"},"target":{"node":"b","port":"input"},"condition":"boom"}],
		"outputs":[]
	}`
	reg := NewNodeRegistry()
	_ = RegisterToolNode(reg)
	tools := ToolMap{"src": &passthroughTool{name: "src", literal: "x"}, "echo": &passthroughTool{name: "echo"}}
	eng, err := LoadCompile(strings.NewReader(src), reg, Deps{Tools: tools}, WithConditionEvaluator(stubEvaluator{}))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	_, err = eng.Run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "evaluate boom") {
		t.Fatalf("Run() = %v, want evaluate-boom error", err)
	}
}

func TestBackwardCompatNoConditionsNoEvaluatorStillWorks(t *testing.T) {
	// Re-verify the v0.0.3 echo chain compiles+runs without any
	// evaluator configured, proving the additive nature of Phase 4.
	eng := newTestEngine(t, sampleJSON)
	out, err := eng.Run(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out["out"] != "OLLEH" {
		t.Fatalf("out=%q, want OLLEH", out["out"])
	}
}
