package flow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// drainEvents collects all events from a RunStream channel into a slice
// keyed by NodeID for the lifecycle assertions.
func drainEvents(ch <-chan Event) []Event {
	var out []Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func kindsForNode(events []Event, nodeID string) []EventKind {
	var ks []EventKind
	for _, ev := range events {
		if ev.NodeID == nodeID {
			ks = append(ks, ev.Kind)
		}
	}
	return ks
}

func hasKind(ks []EventKind, want EventKind) bool {
	for _, k := range ks {
		if k == want {
			return true
		}
	}
	return false
}

// TestConformance_LinearChain: A→B→C string flow through the string
// shim produces the v0.1 semantics: input threads through three
// passthrough nodes to the declared output.
func TestConformance_LinearChain(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"C": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "linear",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}, {ID: "C", Type: "C"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}},
			{Source: PortRef{"B", "out"}, Target: PortRef{"C", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"C", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.RunString(context.Background(), map[string]string{"x": "hello"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["y"] != "hello" {
		t.Fatalf("got %q, want %q", out["y"], "hello")
	}
}

// TestConformance_ConditionalBranch: A fans to B (cond Value=="go") and
// C (cond Value=="stop"). With input "go", B fires and C is skipped
// (NodeSkipped emitted, no output). Mirrors v0.1 conditional routing.
func TestConformance_ConditionalBranch(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"C": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "branch",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}, {ID: "C", Type: "C"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}, Condition: "==go"},
			{Source: PortRef{"A", "out"}, Target: PortRef{"C", "in"}, Condition: "==stop"},
		},
		Inputs: []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{
			{Name: "b", PortRef: PortRef{"B", "out"}},
			{Name: "c", PortRef: PortRef{"C", "out"}},
		},
	}
	eng, err := Compile(f, reg, Deps{}, WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ch, err := eng.RunStreamString(context.Background(), map[string]string{"x": "go"})
	if err != nil {
		t.Fatalf("runstream: %v", err)
	}
	events := drainEvents(ch)

	// B ran: NodeFinished present. C skipped: NodeSkipped present, no Finished.
	if !hasKind(kindsForNode(events, "B"), NodeFinished) {
		t.Errorf("B should have finished; kinds=%v", kindsForNode(events, "B"))
	}
	if !hasKind(kindsForNode(events, "C"), NodeSkipped) {
		t.Errorf("C should be skipped; kinds=%v", kindsForNode(events, "C"))
	}
	if hasKind(kindsForNode(events, "C"), NodeFinished) {
		t.Errorf("C should NOT have finished")
	}

	// Output: b present == "go", c omitted (skipped).
	out, err := eng.RunString(context.Background(), map[string]string{"x": "go"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["b"] != "go" {
		t.Errorf("b = %q, want %q", out["b"], "go")
	}
	if _, ok := out["c"]; ok {
		t.Errorf("c should be omitted (C skipped), got %q", out["c"])
	}
}

// TestConformance_ParallelFanout: two nodes B and C in the same layer
// both fan out from A and run; both complete. Asserts concurrent
// scheduling produces both outputs.
func TestConformance_ParallelFanout(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"C": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "fanout",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}, {ID: "C", Type: "C"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}},
			{Source: PortRef{"A", "out"}, Target: PortRef{"C", "in"}},
		},
		Inputs: []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{
			{Name: "b", PortRef: PortRef{"B", "out"}},
			{Name: "c", PortRef: PortRef{"C", "out"}},
		},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.RunString(context.Background(), map[string]string{"x": "v"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["b"] != "v" || out["c"] != "v" {
		t.Fatalf("fanout: b=%q c=%q, want both %q", out["b"], out["c"], "v")
	}
}

// TestConformance_FanIn: two parents A,B feed a single node M on two
// distinct ports. Any incoming edge activates M (v0.1: any input edge
// fire => activated), and both ports are supplied. M concatenates them.
func TestConformance_FanIn(t *testing.T) {
	merge := &lambdaNode{
		inPorts:  []Port{{Name: "l"}, {Name: "r"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			l, _ := in["l"].(string)
			r, _ := in["r"].(string)
			return map[string]any{"out": l + r}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"M": merge,
	})
	f := Flow{
		ID:    "fanin",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}, {ID: "M", Type: "M"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"M", "l"}},
			{Source: PortRef{"B", "out"}, Target: PortRef{"M", "r"}},
		},
		Inputs: []NamedPortRef{
			{Name: "a", PortRef: PortRef{"A", "in"}},
			{Name: "b", PortRef: PortRef{"B", "in"}},
		},
		Outputs: []NamedPortRef{{Name: "m", PortRef: PortRef{"M", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.RunString(context.Background(), map[string]string{"a": "foo", "b": "bar"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["m"] != "foobar" {
		t.Fatalf("fanin: m=%q, want %q", out["m"], "foobar")
	}
}

// typedPayload is a non-string structured value that must survive the
// any carrier across two nodes.
type typedPayload struct {
	N     int
	Label string
}

// TestConformance_TypedAnyPipeline: a non-string struct value threads
// through two nodes via the any carrier. The first increments N, the
// second appends to Label. Proves the carrier holds structured values,
// not just strings, end-to-end through Run (any path).
func TestConformance_TypedAnyPipeline(t *testing.T) {
	inc := &lambdaNode{
		inPorts:  []Port{{Name: "p"}},
		outPorts: []Port{{Name: "p"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			p := in["p"].(typedPayload)
			p.N++
			return map[string]any{"p": p}, nil
		},
	}
	label := &lambdaNode{
		inPorts:  []Port{{Name: "p"}},
		outPorts: []Port{{Name: "p"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			p := in["p"].(typedPayload)
			p.Label += "!"
			return map[string]any{"p": p}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"INC": inc, "LBL": label})
	f := Flow{
		ID:    "typed",
		Nodes: []Node{{ID: "INC", Type: "INC"}, {ID: "LBL", Type: "LBL"}},
		Edges: []Edge{
			{Source: PortRef{"INC", "p"}, Target: PortRef{"LBL", "p"}},
		},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"INC", "p"}}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"LBL", "p"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.Run(context.Background(), map[string]any{"in": typedPayload{N: 1, Label: "x"}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got, ok := out["out"].(typedPayload)
	if !ok {
		t.Fatalf("output is %T, want typedPayload", out["out"])
	}
	if got.N != 2 || got.Label != "x!" {
		t.Fatalf("typed pipeline: got %+v, want {N:2 Label:x!}", got)
	}
}

// errNode returns an error from Run.
func errNode() *lambdaNode {
	return &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return nil, errors.New("boom")
		},
	}
}

// TestConformance_NodeError: a node returning error fails the run and
// emits a terminal FlowErr event.
func TestConformance_NodeError(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": errNode(),
	})
	f := Flow{
		ID:    "err",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"B", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Sync path: run fails.
	if _, err := eng.RunString(context.Background(), map[string]string{"x": "v"}); err == nil {
		t.Fatalf("expected run error, got nil")
	}

	// Stream path: a FlowErr event is emitted.
	ch, err := eng.RunStreamString(context.Background(), map[string]string{"x": "v"})
	if err != nil {
		t.Fatalf("runstream: %v", err)
	}
	events := drainEvents(ch)
	var sawFlowErr bool
	for _, ev := range events {
		if ev.Kind == FlowErr {
			sawFlowErr = true
		}
	}
	if !sawFlowErr {
		t.Fatalf("expected FlowErr event; events=%v", events)
	}
}

// TestConformance_UnactivatedSkip: a node whose only incoming edge does
// not fire (condition false) is skipped — no Run, no output.
func TestConformance_UnactivatedSkip(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "skip",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}, Condition: "==yes"},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"B", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{}, WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Input "no" => condition "==yes" false => B not activated => skipped.
	out, err := eng.RunString(context.Background(), map[string]string{"x": "no"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, ok := out["y"]; ok {
		t.Fatalf("y should be omitted (B skipped), got %q", out["y"])
	}
}

// TestShim_NonStringOutputError: the string shim refuses a structured
// (non-string) output value rather than silently corrupting it.
func TestShim_NonStringOutputError(t *testing.T) {
	produce := &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return map[string]any{"out": typedPayload{N: 1}}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"A": produce})
	f := Flow{
		ID:      "nonstring",
		Nodes:   []Node{{ID: "A", Type: "A"}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"A", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := eng.RunString(context.Background(), map[string]string{"x": "v"}); err == nil {
		t.Fatalf("expected string-shim error for non-string output")
	}
}

// TestEngine_CELOnNonStringPort_RunError: RV-5 — a conditional edge over
// a non-string source value errors at run time (CEL routes on strings
// only).
func TestEngine_CELOnNonStringPort_RunError(t *testing.T) {
	produce := &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return map[string]any{"out": typedPayload{N: 1}}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{
		"A": produce,
		"B": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "celnonstring",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}, Condition: "==go"},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"B", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{}, WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := eng.Run(context.Background(), map[string]any{"x": "v"}); err == nil {
		t.Fatalf("expected run error for CEL condition on non-string port")
	}
}

// TestMarshalLoad_RoundTrip: a Flow marshals deterministically and
// reloads to an equal IR.
func TestMarshalLoad_RoundTrip(t *testing.T) {
	f := Flow{
		ID:      "rt",
		Nodes:   []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}},
		Edges:   []Edge{{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"B", "out"}}},
	}
	b, err := Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b2, err := Marshal(f)
	if err != nil {
		t.Fatalf("marshal 2: %v", err)
	}
	if string(b) != string(b2) {
		t.Fatalf("marshal not deterministic")
	}
	g, err := Load(bytesReader(b))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if g.ID != f.ID || len(g.Nodes) != len(f.Nodes) || len(g.Edges) != len(f.Edges) {
		t.Fatalf("round-trip mismatch: %+v", g)
	}
}
