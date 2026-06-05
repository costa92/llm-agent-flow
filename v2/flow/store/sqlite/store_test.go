package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/costa92/llm-agent-flow/v2/flow"
	"github.com/costa92/llm-agent-flow/v2/flow/store/sqlite"
)

func newMemStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// sampleCheckpoint hand-builds a representative, non-trivial Checkpoint
// with a type-tagged PortValues map (the codec wire form: {"type":...,
// "value":...}). Mirrors what a real string-port suspend would store.
func sampleCheckpoint(runID, flowID string) flow.Checkpoint {
	tagged := func(v string) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"type": "string", "value": v})
		return b
	}
	return flow.Checkpoint{
		Version:  1,
		RunID:    runID,
		FlowID:   flowID,
		FlowHash: "deadbeefcafe",
		PortValues: map[string]map[string]json.RawMessage{
			"entry":   {"out": tagged("hello")},
			"approve": {"in": tagged("hello")},
		},
		Activated:    map[string]bool{"entry": true, "approve": true},
		EdgeStates:   []flow.EdgeFireState{flow.EdgeFired, flow.EdgeDeferred},
		SuspendLayer: 1,
		InterruptNodeID: "approve",
		InterruptReq:    flow.InterruptRequest{Kind: "approval", Prompt: "ok?"},
		OriginalInputs:  map[string]json.RawMessage{"x": tagged("hello")},
		CreatedAt:       time.Unix(0, time.Now().UnixNano()),
	}
}

func TestCheckpointStore_SaveLoadRoundTrip(t *testing.T) {
	s := newMemStore(t)
	ctx := context.Background()
	cp := sampleCheckpoint("run_a", "flowA")

	if err := s.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	got, err := s.LoadCheckpoint(ctx, "run_a")
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}

	if got.RunID != cp.RunID || got.FlowID != cp.FlowID || got.FlowHash != cp.FlowHash {
		t.Fatalf("ids mismatch: got %+v", got)
	}
	if got.SuspendLayer != cp.SuspendLayer || got.InterruptNodeID != cp.InterruptNodeID {
		t.Fatalf("suspend/interrupt mismatch: got %+v", got)
	}
	if !reflect.DeepEqual(got.Activated, cp.Activated) {
		t.Fatalf("Activated mismatch: got %v want %v", got.Activated, cp.Activated)
	}
	if !reflect.DeepEqual(got.EdgeStates, cp.EdgeStates) {
		t.Fatalf("EdgeStates mismatch: got %v want %v", got.EdgeStates, cp.EdgeStates)
	}
	if !reflect.DeepEqual(got.PortValues, cp.PortValues) {
		t.Fatalf("PortValues mismatch: got %v want %v", got.PortValues, cp.PortValues)
	}
}

func TestCheckpointStore_LoadMissing_NotFound(t *testing.T) {
	s := newMemStore(t)
	_, err := s.LoadCheckpoint(context.Background(), "nope")
	if !errors.Is(err, flow.ErrCheckpointNotFound) {
		t.Fatalf("got %v, want ErrCheckpointNotFound", err)
	}
}

func TestCheckpointStore_Delete(t *testing.T) {
	s := newMemStore(t)
	ctx := context.Background()
	cp := sampleCheckpoint("run_d", "flowA")
	if err := s.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := s.DeleteCheckpoint(ctx, "run_d"); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if _, err := s.LoadCheckpoint(ctx, "run_d"); !errors.Is(err, flow.ErrCheckpointNotFound) {
		t.Fatalf("post-delete Load = %v, want ErrCheckpointNotFound", err)
	}
	// Deleting a non-existent token is not an error.
	if err := s.DeleteCheckpoint(ctx, "run_d"); err != nil {
		t.Fatalf("delete-twice = %v, want nil", err)
	}
}

func TestCheckpointStore_ListByFlow(t *testing.T) {
	s := newMemStore(t)
	ctx := context.Background()

	// 3 checkpoints across 2 flows, with distinct, increasing CreatedAt.
	base := time.Unix(0, 1_000_000_000)
	mk := func(run, flowID string, off time.Duration) flow.Checkpoint {
		cp := sampleCheckpoint(run, flowID)
		cp.CreatedAt = base.Add(off)
		return cp
	}
	if err := s.SaveCheckpoint(ctx, mk("a1", "flowA", 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(ctx, mk("a2", "flowA", 2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(ctx, mk("b1", "flowB", 1*time.Second)); err != nil {
		t.Fatal(err)
	}

	list, err := s.ListCheckpoints(ctx, "flowA", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len = %d, want 2 (only flowA)", len(list))
	}
	// newest-first: a2 before a1.
	if list[0].RunID != "a2" || list[1].RunID != "a1" {
		t.Fatalf("order = [%s, %s], want [a2, a1]", list[0].RunID, list[1].RunID)
	}
	for _, m := range list {
		if m.FlowID != "flowA" {
			t.Fatalf("got flowID %q, want flowA", m.FlowID)
		}
	}

	// limit caps the count.
	capped, err := s.ListCheckpoints(ctx, "flowA", 1)
	if err != nil {
		t.Fatalf("ListCheckpoints(limit=1): %v", err)
	}
	if len(capped) != 1 || capped[0].RunID != "a2" {
		t.Fatalf("limited list = %+v, want [a2]", capped)
	}
}
