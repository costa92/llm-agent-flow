package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// interruptNode is a test NodeKind that returns Interrupt(req) on Run and
// declares InterruptCapable. fn (if set) runs on resume legs where the
// node's output ports are already populated — but in practice the engine
// injects humanInput and never re-runs the node, so Run is only hit on the
// suspending leg.
type interruptNode struct {
	inPorts  []Port
	outPorts []Port
	req      InterruptRequest
}

func (n *interruptNode) Inputs() []Port  { return n.inPorts }
func (n *interruptNode) Outputs() []Port { return n.outPorts }
func (n *interruptNode) CanInterrupt() bool { return true }
func (n *interruptNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	return nil, Interrupt(n.req)
}

// newInterruptNode builds an interrupt-capable single-in single-out node.
func newInterruptNode(inPort, outPort string, req InterruptRequest) *interruptNode {
	return &interruptNode{
		inPorts:  []Port{{Name: inPort}},
		outPorts: []Port{{Name: outPort}},
		req:      req,
	}
}

// uninstrumentedInterrupt is a lambdaNode whose fn returns an
// InterruptError but the node does NOT implement InterruptCapable.
func uninstrumentedInterrupt(inPort, outPort string) *lambdaNode {
	return &lambdaNode{
		inPorts:  []Port{{Name: inPort}},
		outPorts: []Port{{Name: outPort}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return nil, Interrupt(InterruptRequest{Kind: "approval", Prompt: "ok?"})
		},
	}
}

// registerMixed builds a registry where each Flow.Node.Type is the node
// ID, resolving to the supplied NodeKind.
func registerMixed(fns map[string]NodeKind) *NodeRegistry {
	reg := NewNodeRegistry()
	for id, nk := range fns {
		nk := nk
		_ = reg.Register(id, func(cfg json.RawMessage, deps Deps) (NodeKind, error) { return nk, nil })
	}
	return reg
}

// fanInNode counts how many times it runs and joins two input ports.
type fanInNode struct {
	inA, inB string
	out      string
	calls    *int
}

func (n *fanInNode) Inputs() []Port  { return []Port{{Name: n.inA}, {Name: n.inB}} }
func (n *fanInNode) Outputs() []Port { return []Port{{Name: n.out}} }
func (n *fanInNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	if n.calls != nil {
		*n.calls++
	}
	a, _ := in[n.inA].(string)
	b, _ := in[n.inB].(string)
	return map[string]any{n.out: a + "+" + b}, nil
}

// didRunNode records whether it executed.
type didRunNode struct {
	in, out string
	ran     *bool
}

func (n *didRunNode) Inputs() []Port  { return []Port{{Name: n.in}} }
func (n *didRunNode) Outputs() []Port { return []Port{{Name: n.out}} }
func (n *didRunNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	if n.ran != nil {
		*n.ran = true
	}
	return map[string]any{n.out: in[n.in]}, nil
}

// errEvaluator is a ConditionEvaluator whose Evaluate always errors.
type errEvaluator struct{}

func (errEvaluator) Compile(expr string) (Condition, error) { return errCondition{}, nil }

type errCondition struct{}

func (errCondition) Evaluate(ctx context.Context, env CondEnv) (bool, error) {
	return false, errors.New("eval boom")
}

// --- Tests -----------------------------------------------------------------

// 1. Two interrupt-capable nodes in the same layer are rejected at compile
//    time WHEN a store is configured; the same flow compiles fine without.
func TestCompile_TwoInterruptCapableSameLayer_Rejected(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"I1": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"I2": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
	})
	f := Flow{
		ID:    "two-int",
		Nodes: []Node{{ID: "I1", Type: "I1"}, {ID: "I2", Type: "I2"}},
		// no edges between them → same layer 0
	}
	store := NewMemoryCheckpointStore()
	if _, err := Compile(f, reg, Deps{}, WithCheckpointStore(store)); !errors.Is(err, ErrMultipleInterrupts) {
		t.Fatalf("with store: got err %v, want ErrMultipleInterrupts", err)
	}
	if _, err := Compile(f, reg, Deps{}); err != nil {
		t.Fatalf("without store: got err %v, want nil", err)
	}
}

// 2. One interrupt-capable node per layer compiles with the store.
func TestCompile_OneInterruptPerLayer_OK(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"I1": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"I2": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
	})
	f := Flow{
		ID:    "chain-int",
		Nodes: []Node{{ID: "I1", Type: "I1"}, {ID: "I2", Type: "I2"}},
		Edges: []Edge{{Source: PortRef{"I1", "out"}, Target: PortRef{"I2", "in"}}},
	}
	store := NewMemoryCheckpointStore()
	if _, err := Compile(f, reg, Deps{}, WithCheckpointStore(store)); err != nil {
		t.Fatalf("compile: %v", err)
	}
}

// 3. A non-capable node returning InterruptError fails the run.
func TestRun_UninstrumentedInterrupt_FailsRun(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"E": uninstrumentedInterrupt("in", "out"),
	})
	f := Flow{
		ID:      "uninstrumented",
		Nodes:   []Node{{ID: "E", Type: "E"}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"E", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"E", "out"}}},
	}
	store := NewMemoryCheckpointStore()
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.RunResumable(context.Background(), NewRunID(), map[string]any{"x": "v"})
	if !errors.Is(err, ErrUninstrumentedInterrupt) {
		t.Fatalf("got err %v, want ErrUninstrumentedInterrupt", err)
	}
}

// 4. An interrupt-capable node interrupting but engine compiled WITHOUT a
//    store → RunResumable returns ErrNoCheckpointStore.
func TestRunResumable_NoStore_Errors(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"I": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
	})
	f := Flow{
		ID:      "no-store",
		Nodes:   []Node{{ID: "I", Type: "I"}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"I", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"I", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{}) // no store
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.RunResumable(context.Background(), NewRunID(), map[string]any{"x": "v"})
	if !errors.Is(err, ErrNoCheckpointStore) {
		t.Fatalf("got err %v, want ErrNoCheckpointStore", err)
	}
}

// headlineFlow: entry → approve(interrupt) → out, all string passthrough.
func headlineFlow(t *testing.T, approve NodeKind) (Flow, *NodeRegistry) {
	t.Helper()
	reg := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": approve,
		"out":     passthrough("in", "out"),
	})
	f := Flow{
		ID: "headline",
		Nodes: []Node{
			{ID: "entry", Type: "entry"},
			{ID: "approve", Type: "approve"},
			{ID: "out", Type: "out"},
		},
		Edges: []Edge{
			{Source: PortRef{"entry", "out"}, Target: PortRef{"approve", "in"}},
			{Source: PortRef{"approve", "out"}, Target: PortRef{"out", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"entry", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"out", "out"}}},
	}
	return f, reg
}

// 5. The headline invariant: resume reproduces the non-interrupt baseline.
func TestResume_HeadlineInvariant(t *testing.T) {
	ctx := context.Background()

	// Baseline: approve is a plain passthrough.
	bf, breg := headlineFlow(t, passthrough("in", "out"))
	beng, err := Compile(bf, breg, Deps{})
	if err != nil {
		t.Fatalf("baseline compile: %v", err)
	}
	base, err := beng.Run(ctx, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}

	// Interrupt variant.
	store := NewMemoryCheckpointStore()
	f, reg := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Kind: "approval", Prompt: "approve?"}))
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	if res.Suspended.NodeID != "approve" {
		t.Fatalf("suspended at %q, want approve", res.Suspended.NodeID)
	}
	if res.Suspended.Request.Prompt != "approve?" {
		t.Fatalf("prompt %q, want approve?", res.Suspended.Request.Prompt)
	}

	res2, err := eng.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res2.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
	if res2.Outputs["y"] != base["y"] {
		t.Fatalf("resume output %v, want baseline %v", res2.Outputs["y"], base["y"])
	}
}

// 6. Fan-in where one parent suspends: D runs exactly once, output matches
//    the non-interrupt baseline.
func TestResume_FanInOneParentSuspends(t *testing.T) {
	ctx := context.Background()

	// Baseline: S is a passthrough.
	var baseCalls int
	bd := &fanInNode{inA: "x", inB: "y", out: "z", calls: &baseCalls}
	breg := registerMixed(map[string]NodeKind{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"S": passthrough("in", "out"),
		"D": bd,
	})
	bf := fanInFlow()
	beng, err := Compile(bf, breg, Deps{})
	if err != nil {
		t.Fatalf("baseline compile: %v", err)
	}
	base, err := beng.Run(ctx, map[string]any{"in": "v"})
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	if baseCalls != 1 {
		t.Fatalf("baseline D calls %d, want 1", baseCalls)
	}

	// Interrupt variant.
	var dCalls int
	d := &fanInNode{inA: "x", inB: "y", out: "z", calls: &dCalls}
	store := NewMemoryCheckpointStore()
	reg := registerMixed(map[string]NodeKind{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"S": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"D": d,
	})
	f := fanInFlow()
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"in": "v"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil || res.Suspended.NodeID != "S" {
		t.Fatalf("expected suspension at S, got %+v", res.Suspended)
	}
	if dCalls != 0 {
		t.Fatalf("D ran before resume: %d", dCalls)
	}
	res2, err := eng.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "v"})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if dCalls != 1 {
		t.Fatalf("D calls after resume %d, want 1", dCalls)
	}
	if res2.Outputs["z"] != base["z"] {
		t.Fatalf("resume output %v, want baseline %v", res2.Outputs["z"], base["z"])
	}
}

// fanInFlow: A → {B, S}; B → D.x; S → D.y; D is fan-in in a later layer.
func fanInFlow() Flow {
	return Flow{
		ID: "fanin-int",
		Nodes: []Node{
			{ID: "A", Type: "A"},
			{ID: "B", Type: "B"},
			{ID: "S", Type: "S"},
			{ID: "D", Type: "D"},
		},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"B", "in"}},
			{Source: PortRef{"A", "out"}, Target: PortRef{"S", "in"}},
			{Source: PortRef{"B", "out"}, Target: PortRef{"D", "x"}},
			{Source: PortRef{"S", "out"}, Target: PortRef{"D", "y"}},
		},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "z", PortRef: PortRef{"D", "z"}}},
	}
}

// 7. A sibling dead-false conditional edge at suspend time is not
//    re-evaluated on resume.
func TestResume_SiblingDeadFalseEdgeNotReEvaluated(t *testing.T) {
	ctx := context.Background()
	var calls int

	// Flow: A → S(interrupt) and A → C via a conditional edge that is
	//       false (A emits "v", cond wants "go"). S → out (string).
	store := NewMemoryCheckpointStore()
	reg := registerMixed(map[string]NodeKind{
		"A":   passthrough("in", "out"),
		"S":   newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"C":   passthrough("in", "out"),
		"out": passthrough("in", "out"),
	})
	f := Flow{
		ID: "deadfalse",
		Nodes: []Node{
			{ID: "A", Type: "A"},
			{ID: "S", Type: "S"},
			{ID: "C", Type: "C"},
			{ID: "out", Type: "out"},
		},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"S", "in"}},
			{Source: PortRef{"A", "out"}, Target: PortRef{"C", "in"}, Condition: "==go"},
			{Source: PortRef{"S", "out"}, Target: PortRef{"out", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"A", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"out", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{calls: &calls}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"in": "v"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	suspendCalls := calls // cond evaluated once at suspend (false)
	if suspendCalls == 0 {
		t.Fatalf("expected cond evaluated at suspend")
	}
	if _, err := eng.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "v"}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if calls != suspendCalls {
		t.Fatalf("dead-false edge re-evaluated on resume: %d → %d", suspendCalls, calls)
	}
}

// 8. A sibling conditional edge whose Evaluate errors at suspend time →
//    RunResumable returns the error AND no checkpoint is saved.
func TestSuspend_SiblingCondEvalError_NoCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	reg := registerMixed(map[string]NodeKind{
		"A": passthrough("in", "out"),
		"S": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"C": passthrough("in", "out"),
	})
	f := Flow{
		ID: "conderr",
		Nodes: []Node{
			{ID: "A", Type: "A"},
			{ID: "S", Type: "S"},
			{ID: "C", Type: "C"},
		},
		Edges: []Edge{
			{Source: PortRef{"A", "out"}, Target: PortRef{"S", "in"}},
			{Source: PortRef{"A", "out"}, Target: PortRef{"C", "in"}, Condition: "anything"},
		},
		Inputs: []NamedPortRef{{Name: "in", PortRef: PortRef{"A", "in"}}},
	}
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(errEvaluator{}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	_, err = eng.RunResumable(ctx, runID, map[string]any{"in": "v"})
	if err == nil {
		t.Fatalf("expected eval error, got nil")
	}
	if _, lerr := store.LoadCheckpoint(ctx, runID); !errors.Is(lerr, ErrCheckpointNotFound) {
		t.Fatalf("expected no checkpoint saved, got %v", lerr)
	}
}

// 9. Resume with an unknown humanInput key is rejected.
func TestResume_UnknownHumanInputKey_Rejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	f, reg := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}))
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"x": "v"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	_, err = eng.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"bogus": "v"})
	if !errors.Is(err, ErrInvalidHumanInput) {
		t.Fatalf("got err %v, want ErrInvalidHumanInput", err)
	}
}

// 10. Resume against an engine compiled from a different flow → ErrFlowChanged.
func TestResume_FlowChanged(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f1, reg1 := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}))
	eng1, err := Compile(f1, reg1, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile1: %v", err)
	}
	runID := NewRunID()
	res, err := eng1.RunResumable(ctx, runID, map[string]any{"x": "v"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}

	// A structurally different flow → different FlowHash.
	f2, reg2 := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}))
	f2.Description = "changed"
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	_, err = eng2.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "v"})
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("got err %v, want ErrFlowChanged", err)
	}
}

// 11. Resume with an unknown token → ErrCheckpointNotFound.
func TestResume_CheckpointNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	f, reg := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}))
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.Resume(ctx, "run_missing", "run_missing", map[string]any{"out": "v"})
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("got err %v, want ErrCheckpointNotFound", err)
	}
}

// 12. At suspend, a node in the layer after the interrupt did NOT run.
func TestSuspend_StopsBeforeNextLayer(t *testing.T) {
	ctx := context.Background()
	var ran bool
	store := NewMemoryCheckpointStore()
	reg := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"out":     &didRunNode{in: "in", out: "out", ran: &ran},
	})
	f := Flow{
		ID: "stops",
		Nodes: []Node{
			{ID: "entry", Type: "entry"},
			{ID: "approve", Type: "approve"},
			{ID: "out", Type: "out"},
		},
		Edges: []Edge{
			{Source: PortRef{"entry", "out"}, Target: PortRef{"approve", "in"}},
			{Source: PortRef{"approve", "out"}, Target: PortRef{"out", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"entry", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"out", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := eng.RunResumable(ctx, NewRunID(), map[string]any{"x": "v"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	if ran {
		t.Fatalf("downstream node ran before resume")
	}
}

// 13. Codec round-trip for string + a registered struct; unregistered → error.
type codecPayload struct {
	N     int    `json:"n"`
	Label string `json:"label"`
}

func TestCodec_RoundTrip(t *testing.T) {
	RegisterCodec[codecPayload]("codecPayload")

	// string (built-in)
	raw, err := encodePortValue("hi")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	v, err := decodePortValue(raw)
	if err != nil {
		t.Fatalf("decode string: %v", err)
	}
	if v != "hi" {
		t.Fatalf("string round-trip: %v", v)
	}

	// struct
	want := codecPayload{N: 7, Label: "z"}
	raw, err = encodePortValue(want)
	if err != nil {
		t.Fatalf("encode struct: %v", err)
	}
	v, err = decodePortValue(raw)
	if err != nil {
		t.Fatalf("decode struct: %v", err)
	}
	if got, ok := v.(codecPayload); !ok || got != want {
		t.Fatalf("struct round-trip: got %#v", v)
	}

	// unregistered type
	type unreg struct{ X int }
	if _, err := encodePortValue(unreg{X: 1}); !errors.Is(err, ErrNotCheckpointable) {
		t.Fatalf("unregistered: got %v, want ErrNotCheckpointable", err)
	}
}

// 14. A normal Run over a conditional-routing flow still behaves the same
//     (guards the runCore refactor).
func TestFreshRun_Unchanged(t *testing.T) {
	ctx := context.Background()
	reg := registerLambdas(map[string]*lambdaNode{
		"A": passthrough("in", "out"),
		"B": passthrough("in", "out"),
		"C": passthrough("in", "out"),
	})
	f := Flow{
		ID:    "router",
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
	out, err := eng.Run(ctx, map[string]any{"x": "go"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["b"] != "go" {
		t.Fatalf("b=%v, want go", out["b"])
	}
	if _, ok := out["c"]; ok {
		t.Fatalf("c should be omitted (skipped branch), got %v", out["c"])
	}
}

// TestMemoryCheckpointStore_LoadReturnsCopy — a Checkpoint loaded from the
// store must be an independent deep copy: mutating the loaded Activated /
// PortValues / EdgeStates must NOT corrupt the store's retained copy.
func TestMemoryCheckpointStore_LoadReturnsCopy(t *testing.T) {
	ctx := context.Background()
	cs := NewMemoryCheckpointStore()
	orig := Checkpoint{
		RunID:      "r1",
		FlowID:     "f1",
		Activated:  map[string]bool{"n1": true},
		PortValues: map[string]map[string]json.RawMessage{"n1": {"out": json.RawMessage(`"v"`)}},
		EdgeStates: []EdgeFireState{EdgeFired, EdgePending},
		OriginalInputs: map[string]json.RawMessage{"in": json.RawMessage(`"x"`)},
	}
	if err := cs.SaveCheckpoint(ctx, orig); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := cs.LoadCheckpoint(ctx, "r1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// Mutate the loaded copy.
	loaded.Activated["n1"] = false
	loaded.Activated["n2"] = true
	loaded.PortValues["n1"]["out"] = json.RawMessage(`"mutated"`)
	loaded.PortValues["n3"] = map[string]json.RawMessage{"p": json.RawMessage(`"z"`)}
	loaded.EdgeStates[0] = EdgeDeadFalse
	loaded.OriginalInputs["in"] = json.RawMessage(`"corrupt"`)

	// Re-load and assert the store copy is unmutated.
	again, err := cs.LoadCheckpoint(ctx, "r1")
	if err != nil {
		t.Fatalf("re-load: %v", err)
	}
	if again.Activated["n1"] != true {
		t.Fatalf("Activated[n1] = %v, want true (store corrupted)", again.Activated["n1"])
	}
	if _, ok := again.Activated["n2"]; ok {
		t.Fatalf("Activated[n2] present, want absent (store corrupted)")
	}
	if string(again.PortValues["n1"]["out"]) != `"v"` {
		t.Fatalf("PortValues[n1][out] = %s, want \"v\" (store corrupted)", again.PortValues["n1"]["out"])
	}
	if _, ok := again.PortValues["n3"]; ok {
		t.Fatalf("PortValues[n3] present, want absent (store corrupted)")
	}
	if again.EdgeStates[0] != EdgeFired {
		t.Fatalf("EdgeStates[0] = %v, want EdgeFired (store corrupted)", again.EdgeStates[0])
	}
	if string(again.OriginalInputs["in"]) != `"x"` {
		t.Fatalf("OriginalInputs[in] = %s, want \"x\" (store corrupted)", again.OriginalInputs["in"])
	}
}

// errLoadStore is a CheckpointStore whose LoadCheckpoint returns a custom
// (non-not-found) error, simulating a genuine store/IO failure.
type errLoadStore struct{ err error }

func (s errLoadStore) SaveCheckpoint(context.Context, Checkpoint) error { return nil }
func (s errLoadStore) LoadCheckpoint(context.Context, string) (Checkpoint, error) {
	return Checkpoint{}, s.err
}
func (s errLoadStore) DeleteCheckpoint(context.Context, string) error { return nil }
func (s errLoadStore) ListCheckpoints(context.Context, string, int) ([]CheckpointMeta, error) {
	return nil, nil
}

// TestResume_StoreErrorNotMislabeledNotFound — a genuine store error from
// LoadCheckpoint must surface as an error that is NOT errors.Is
// ErrCheckpointNotFound (only the sentinel itself maps to not-found).
func TestResume_StoreErrorNotMislabeledNotFound(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"I": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
	})
	f := Flow{
		ID:      "store-err",
		Nodes:   []Node{{ID: "I", Type: "I"}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"I", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"I", "out"}}},
	}
	ioErr := errors.New("disk on fire")
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(errLoadStore{err: ioErr}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, rerr := eng.Resume(context.Background(), "r1", "r1", nil)
	if rerr == nil {
		t.Fatal("Resume returned nil error, want store error")
	}
	if errors.Is(rerr, ErrCheckpointNotFound) {
		t.Fatalf("Resume err %v mislabeled as ErrCheckpointNotFound", rerr)
	}
	if !errors.Is(rerr, ioErr) {
		t.Fatalf("Resume err %v does not wrap the underlying store error", rerr)
	}
}
