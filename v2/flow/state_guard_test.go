package flow

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// ckStateB is a SECOND registered State type with a DIFFERENT codec tag,
// used to exercise the different-tag resume guard.
type ckStateB struct {
	Other string `json:"other"`
}

func init() { RegisterCodec[ckStateB]("ckStateB") }

// suspendWithState runs stateCheckpointFlow to its interrupt with the given
// WithInitialState seed and returns the engine + store + runID + suspension.
func suspendWithState[S any](t *testing.T, store *MemoryCheckpointStore, seed S) (*Engine, string, *Suspension) {
	t.Helper()
	reg := registerMixed(map[string]NodeKind{
		"w":       &stateWriterReaderNode{inPort: "in", outPort: "out", write: &ckState{Note: "pre"}},
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "ok?"}),
		"r":       passthrough("in", "out"),
	})
	eng, err := Compile(stateCheckpointFlow(), reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumableWithState(context.Background(), runID, map[string]any{"x": "v"},
		WithInitialState(seed))
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	return eng, runID, res.Suspended
}

// different-TAG: suspend with S=ckState (the writer commits ckState), resume
// with a cell of S=ckStateB → the codec tag differs → ErrFlowChanged.
func TestStateGuard_DifferentTag_FlowChanged(t *testing.T) {
	store := NewMemoryCheckpointStore()
	eng, runID, susp := suspendWithState(t, store, ckState{})

	_, err := eng.ResumeWithState(context.Background(), runID, susp.ResumeToken,
		map[string]any{"out": "v"}, WithInitialState(ckStateB{}))
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("resume with different-tag S = %v, want ErrFlowChanged", err)
	}
}

// same-TAG/different-SHAPE (the MAJOR-4 hole): the checkpoint's StateCodec
// carries tag "ckState" with a fingerprint of a DIFFERENT shape than the
// resume run's ckState. RegisterCodec is first-write-wins so two shapes can't
// share a tag in-process; we synthesize the mismatch by mutating the stored
// StateCodec to a different fingerprint under the same tag, then assert the
// FINGERPRINT compare rejects it.
func TestStateGuard_SameTagDifferentShape_FlowChanged(t *testing.T) {
	store := NewMemoryCheckpointStore()
	eng, runID, susp := suspendWithState(t, store, ckState{})

	cp, err := store.LoadCheckpoint(context.Background(), runID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// Build a StateCodec with the SAME "ckState" tag but the fingerprint of a
	// differently-shaped struct — exactly the same-tag/different-shape swap.
	type ckStateV2 struct {
		Note  string `json:"note"`
		Extra int    `json:"extra"`
	}
	mismatched := "state:ckState:" + stateFingerprint(reflect.TypeFor[ckStateV2]())
	if mismatched == cp.StateCodec {
		t.Fatalf("test setup: ckStateV2 fingerprint collides with ckState — not a valid different-shape case")
	}
	cp.StateCodec = mismatched
	if err := store.SaveCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	_, err = eng.ResumeWithState(context.Background(), runID, susp.ResumeToken,
		map[string]any{"out": "v"}, WithInitialState(ckState{}))
	if !errors.Is(err, ErrFlowChanged) {
		t.Fatalf("resume with same-tag/different-shape StateCodec = %v, want ErrFlowChanged", err)
	}
}

// identical S → resume succeeds (the guard does not over-reject).
func TestStateGuard_IdenticalShape_Resumes(t *testing.T) {
	store := NewMemoryCheckpointStore()
	eng, runID, susp := suspendWithState(t, store, ckState{})

	res, err := eng.ResumeWithState(context.Background(), runID, susp.ResumeToken,
		map[string]any{"out": "v"}, WithInitialState(ckState{}))
	if err != nil {
		t.Fatalf("resume with identical S: %v", err)
	}
	if res.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
}
