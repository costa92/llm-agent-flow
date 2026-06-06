package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// configPortNode is a NodeKind whose declared ports are FIXED (in/out)
// regardless of Config — used to prove a Node.Config change is excluded
// from the struct hash.
type configPortNode struct{}

func (configPortNode) Inputs() []Port  { return []Port{{Name: "in"}} }
func (configPortNode) Outputs() []Port { return []Port{{Name: "out"}} }
func (configPortNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	return map[string]any{"out": in["in"]}, nil
}

// structHashTestFlow builds entry → approve(interrupt) → out, with `out`'s
// Node.Config set to cfg. approve is the supplied interrupt-capable kind so
// the flow can suspend/resume.
func structHashTestFlow(approve NodeKind, outConfig string) (Flow, *NodeRegistry) {
	reg := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": approve,
		"out":     configPortNode{},
	})
	f := Flow{
		ID: "structhash",
		Nodes: []Node{
			{ID: "entry", Type: "entry"},
			{ID: "approve", Type: "approve"},
			{ID: "out", Type: "out", Config: json.RawMessage(outConfig)},
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

// suspendOnFlow compiles eng over f with the shared store and drives a
// RunResumable to the suspend point, returning runID and the resume token.
func suspendOnFlow(t *testing.T, f Flow, reg *NodeRegistry, store CheckpointStore) (string, string) {
	t.Helper()
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("suspend compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(context.Background(), runID, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	return runID, res.Suspended.ResumeToken
}

// TestResume_ConfigChangeAllowed — engine2's flow differs ONLY by the `out`
// node's Config blob. Resume SUCCEEDS and yields the same output a
// non-interrupted run of flowA would.
func TestResume_ConfigChangeAllowed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	// Non-interrupt baseline over flowA (config "1") to fix the invariant.
	bf, breg := structHashTestFlow(passthrough("in", "out"), `{"v":1}`)
	beng, err := Compile(bf, breg, Deps{})
	if err != nil {
		t.Fatalf("baseline compile: %v", err)
	}
	base, err := beng.Run(ctx, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}

	// engine1 suspends over flowA (config "1").
	f1, reg1 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	runID, token := suspendOnFlow(t, f1, reg1, store)

	// engine2 over flowA with ONLY a different out.Config blob.
	f2, reg2 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":2}`)
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	res, err := eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("resume after config-only change: %v", err)
	}
	if res.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
	if res.Outputs["y"] != base["y"] {
		t.Fatalf("resume output %v, want baseline %v", res.Outputs["y"], base["y"])
	}
}

// TestResume_PortSetChanged_Rejected — engine2 resolves the `out` node to a
// DIFFERENT port set. Resume → ErrFlowChanged.
func TestResume_PortSetChanged_Rejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f1, reg1 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	runID, token := suspendOnFlow(t, f1, reg1, store)

	// engine2: same IR shape but `out` resolves to a node with a different
	// output port name ("out2" instead of "out").
	reg2 := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"out":     passthrough("in", "out2"),
	})
	f2, _ := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	// Repoint the output ref to the new port so f2 still validates.
	f2.Outputs = []NamedPortRef{{Name: "y", PortRef: PortRef{"out", "out2"}}}
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	_, err = eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("got err %v, want ErrFlowChanged", err)
	}
}

// TestResume_EdgeAddedShiftsIndices_Rejected — engine2 inserts an extra edge
// before the region edges, shifting EdgeStates indices. Resume → ErrFlowChanged.
func TestResume_EdgeAddedShiftsIndices_Rejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f1, reg1 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	runID, token := suspendOnFlow(t, f1, reg1, store)

	// engine2: add a new sink node `extra` and an edge entry→extra at index
	// 0, shifting the region edges' indices by one. The cursor (EdgeStates)
	// indexed by position would be corrupted → must be rejected.
	reg2 := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"out":     configPortNode{},
		"extra":   passthrough("in", "out"),
	})
	f2, _ := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	f2.Nodes = append(f2.Nodes, Node{ID: "extra", Type: "extra"})
	f2.Edges = append([]Edge{
		{Source: PortRef{"entry", "out"}, Target: PortRef{"extra", "in"}},
	}, f2.Edges...)
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	_, err = eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("got err %v, want ErrFlowChanged", err)
	}
}

// TestResume_EdgeConditionChanged_Rejected — engine2 changed an edge's
// Condition string. Resume → ErrFlowChanged.
func TestResume_EdgeConditionChanged_Rejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f1, reg1 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	runID, token := suspendOnFlow(t, f1, reg1, store)

	f2, reg2 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	// Add a condition to the approve→out edge (index 1).
	f2.Edges[1].Condition = "==approved"
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	_, err = eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("got err %v, want ErrFlowChanged", err)
	}
}

// TestResume_IdenticalFlow_StillResumes — engine2 == flowA exactly. Resume
// succeeds (fast path: identical FlowHash).
func TestResume_IdenticalFlow_StillResumes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f1, reg1 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	runID, token := suspendOnFlow(t, f1, reg1, store)

	f2, reg2 := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	eng2, err := Compile(f2, reg2, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile2: %v", err)
	}
	res, err := eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("resume identical flow: %v", err)
	}
	if res.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
}

// TestResume_V1CheckpointNoStructHash_StrictFallback — a hand-saved v1
// Checkpoint (no StructHash) against a flow whose FlowHash differs from the
// engine's → strict fallback → ErrFlowChanged.
func TestResume_V1CheckpointNoStructHash_StrictFallback(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	f, reg := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Craft a v1 checkpoint: empty StructHash, a FlowHash that differs from
	// the engine's, version 1.
	cp := Checkpoint{
		Version:         1,
		RunID:           "v1run",
		FlowID:          f.ID,
		FlowHash:        "deadbeef-different",
		StructHash:      "", // v1: no struct hash
		InterruptNodeID: "approve",
		Activated:       map[string]bool{},
		EdgeStates:      make([]EdgeFireState, len(f.Edges)),
	}
	if err := store.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("save v1 checkpoint: %v", err)
	}
	_, err = eng.Resume(ctx, "v1run", "v1run", map[string]any{"out": "approved"})
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("got err %v, want ErrFlowChanged (strict v1 fallback)", err)
	}
}

// TestStructuralHash_StableAcrossConfigOnlyChange — unit test: structuralHash
// is identical for two flows differing only by Config, and differs when a
// port or edge changes.
func TestStructuralHash_StableAcrossConfigOnlyChange(t *testing.T) {
	approve := newInterruptNode("in", "out", InterruptRequest{Prompt: "p"})

	fA, regA := structHashTestFlow(approve, `{"v":1}`)
	engA, err := Compile(fA, regA, Deps{}, WithCheckpointStore(NewMemoryCheckpointStore()))
	if err != nil {
		t.Fatalf("compile A: %v", err)
	}

	// Config-only change → same struct hash.
	fB, regB := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":99}`)
	engB, err := Compile(fB, regB, Deps{}, WithCheckpointStore(NewMemoryCheckpointStore()))
	if err != nil {
		t.Fatalf("compile B: %v", err)
	}
	if engA.structHash != engB.structHash {
		t.Fatalf("config-only change altered struct hash:\n A=%s\n B=%s", engA.structHash, engB.structHash)
	}
	// Sanity: FlowHash DID change (config is in the full flow JSON).
	if engA.flowHash == engB.flowHash {
		t.Fatalf("expected flowHash to differ on config change")
	}

	// Edge change → different struct hash.
	fC, regC := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	fC.Edges[1].Condition = "==go"
	engC, err := Compile(fC, regC, Deps{}, WithCheckpointStore(NewMemoryCheckpointStore()), WithConditionEvaluator(staticEvaluator{}))
	if err != nil {
		t.Fatalf("compile C: %v", err)
	}
	if engA.structHash == engC.structHash {
		t.Fatalf("edge condition change did not alter struct hash")
	}

	// Port change → different struct hash.
	regD := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}),
		"out":     passthrough("in", "out2"),
	})
	fD, _ := structHashTestFlow(newInterruptNode("in", "out", InterruptRequest{Prompt: "p"}), `{"v":1}`)
	fD.Outputs = []NamedPortRef{{Name: "y", PortRef: PortRef{"out", "out2"}}}
	engD, err := Compile(fD, regD, Deps{}, WithCheckpointStore(NewMemoryCheckpointStore()))
	if err != nil {
		t.Fatalf("compile D: %v", err)
	}
	if engA.structHash == engD.structHash {
		t.Fatalf("port change did not alter struct hash")
	}
}
