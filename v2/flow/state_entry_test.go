package flow

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// listWriterNode writes a []string into State (matching an S=[]string
// run) and passes its input through.
type listWriterNode struct {
	inPort, outPort string
	write           []string
}

func (n *listWriterNode) Inputs() []Port  { return []Port{{Name: n.inPort}} }
func (n *listWriterNode) Outputs() []Port { return []Port{{Name: n.outPort}} }
func (n *listWriterNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	SetState(ctx, n.write)
	return map[string]any{n.outPort: in[n.inPort]}, nil
}

// TestRunWithState_ThreadsCell asserts RunWithState builds a cell from the
// options and threads it: a node writing State in layer N is visible to a
// reader in layer N+1, and the run returns the declared outputs.
func TestRunWithState_ThreadsCell(t *testing.T) {
	ctx := context.Background()
	var seen []string
	f := Flow{
		ID: "chain",
		Nodes: []Node{
			{ID: "w", Type: "w"},
			{ID: "r", Type: "r"},
		},
		Edges:   []Edge{{Source: PortRef{"w", "out"}, Target: PortRef{"r", "in"}}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"w", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"r", "out"}}},
	}
	reg := registerMixed(map[string]NodeKind{
		"w": &listWriterNode{inPort: "in", outPort: "out", write: []string{"from-w"}},
		"r": &stateReaderNode{inPort: "in", outPort: "out", seen: &seen},
	})
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.RunWithState(ctx, map[string]any{"x": "v"},
		WithInitialState([]string{}), WithReducer(func(prev []string, writes [][]string) []string {
			for _, w := range writes {
				prev = append(prev, w...)
			}
			return prev
		}))
	if err != nil {
		t.Fatalf("RunWithState: %v", err)
	}
	if out["y"] != "v" {
		t.Fatalf("output y = %v, want v", out["y"])
	}
	if !reflect.DeepEqual(seen, []string{"from-w"}) {
		t.Fatalf("reader saw %v, want [from-w]", seen)
	}
}

// TestRunWithState_NoOptionsInert asserts RunWithState with no options is
// byte-identical to bare Run (no cell, helpers inert).
func TestRunWithState_NoOptionsInert(t *testing.T) {
	ctx := context.Background()
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", write: "b"},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	bare, err := eng.Run(ctx, map[string]any{"x": "v"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	withState, err := eng.RunWithState(ctx, map[string]any{"x": "v"})
	if err != nil {
		t.Fatalf("RunWithState: %v", err)
	}
	if !reflect.DeepEqual(bare, withState) {
		t.Fatalf("RunWithState (no opts) = %v, want byte-identical to Run %v", withState, bare)
	}
}

// TestRunResumableWithState_Conflict asserts RunResumableWithState threads
// the cell so the default reject-on-collision fires through it.
func TestRunResumableWithState_Conflict(t *testing.T) {
	ctx := context.Background()
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", write: "b"},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.RunResumableWithState(ctx, NewRunID(), map[string]any{"x": "v"},
		WithInitialState([]string{})) // no reducer → reject-on-collision
	if !errors.Is(err, ErrStateWriteConflict) {
		t.Fatalf("RunResumableWithState err = %v, want ErrStateWriteConflict", err)
	}
}

// TestResumeWithState_Smoke asserts ResumeWithState is callable and behaves
// like Resume for a non-State resume (State restore is task 4).
func TestResumeWithState_Smoke(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	f, reg := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Kind: "approval", Prompt: "approve?"}))
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumableWithState(ctx, runID, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	res2, err := eng.ResumeWithState(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("ResumeWithState: %v", err)
	}
	if res2.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
	if res2.Outputs["y"] != "approved" {
		t.Fatalf("resume output %v, want approved", res2.Outputs["y"])
	}
}
