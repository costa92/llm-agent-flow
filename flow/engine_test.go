package flow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// fakeTool is the smallest possible Tool implementation for tests.
type fakeTool struct {
	name string
	fn   func(in string) string
}

func (t *fakeTool) Name() string { return t.name }
func (t *fakeTool) Execute(_ context.Context, args json.RawMessage) (string, error) {
	var p struct {
		Input string `json:"input"`
	}
	_ = json.Unmarshal(args, &p)
	return t.fn(p.Input), nil
}

func newTestEngine(t *testing.T, src string) *Engine {
	t.Helper()
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := ToolMap{
		"upper":   &fakeTool{name: "upper", fn: strings.ToUpper},
		"reverse": &fakeTool{name: "reverse", fn: func(s string) string {
			r := []rune(s)
			for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
				r[i], r[j] = r[j], r[i]
			}
			return string(r)
		}},
	}
	e, err := LoadCompile(strings.NewReader(src), reg, Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	return e
}

func TestEngineRunsLinearChain(t *testing.T) {
	e := newTestEngine(t, sampleJSON)
	out, err := e.Run(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out["out"] != "OLLEH" {
		t.Fatalf("out=%q, want OLLEH", out["out"])
	}
}

func TestEngineStreamEmitsExpectedSequence(t *testing.T) {
	e := newTestEngine(t, sampleJSON)
	ch, err := e.RunStream(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	var kinds []FlowEventKind
	var finalOutputs map[string]string
	for ev := range ch {
		kinds = append(kinds, ev.Kind)
		if ev.Kind == FlowDone {
			finalOutputs = ev.Outputs
		}
		if ev.Kind == FlowErr {
			t.Fatalf("unexpected FlowErr: %v", ev.Err)
		}
	}
	want := []FlowEventKind{
		FlowStarted,
		NodeStarted, NodeFinished,
		NodeStarted, NodeFinished,
		FlowDone,
	}
	if len(kinds) != len(want) {
		t.Fatalf("event kinds = %v, want %v", kinds, want)
	}
	for i, k := range want {
		if kinds[i] != k {
			t.Fatalf("event[%d] = %d, want %d (full = %v)", i, kinds[i], k, kinds)
		}
	}
	if finalOutputs["out"] != "OLLEH" {
		t.Fatalf("FlowDone outputs = %+v, want out=OLLEH", finalOutputs)
	}
}

func TestEngineRejectsMissingInput(t *testing.T) {
	e := newTestEngine(t, sampleJSON)
	_, err := e.Run(context.Background(), map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "missing required input") {
		t.Fatalf("Run() = %v, want missing-input error", err)
	}
}

func TestCompileRejectsUnknownNodeType(t *testing.T) {
	const src = `{"id":"x","nodes":[{"id":"a","type":"nope"}],"edges":[]}`
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	_, err := LoadCompile(strings.NewReader(src), reg, Deps{})
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("LoadCompile(unknown) = %v, want unknown-type error", err)
	}
}
