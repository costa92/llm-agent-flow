package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

func seedRun(t *testing.T) (runID string, store flowstore.Store) {
	t.Helper()
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	id, err := s.StartRun(ctx, "f", map[string]string{"in": "x"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return id, s
}

func TestAppendAndListRunEvents(t *testing.T) {
	runID, s := seedRun(t)
	ctx := context.Background()
	kinds := []flowstore.RunEventKind{
		flowstore.RunEventFlowStarted,
		flowstore.RunEventNodeStarted,
		flowstore.RunEventNodeFinished,
		flowstore.RunEventFlowDone,
	}
	payloads := [][]byte{
		[]byte(`{"flow":"f"}`),
		[]byte(`{"node":"a","input":{"in":"x"}}`),
		[]byte(`{"node":"a","output":{"out":"X"}}`),
		[]byte(`{"outputs":{"out":"X"}}`),
	}
	for i, k := range kinds {
		node := ""
		if k == flowstore.RunEventNodeStarted || k == flowstore.RunEventNodeFinished {
			node = "a"
		}
		if err := s.AppendRunEvent(ctx, runID, k, node, payloads[i]); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}
	got, err := s.ListRunEvents(ctx, runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4 (%+v)", len(got), got)
	}
	for i, ev := range got {
		if ev.Seq != i+1 {
			t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, i+1)
		}
		if ev.Kind != kinds[i] {
			t.Fatalf("event[%d].Kind = %q, want %q", i, ev.Kind, kinds[i])
		}
		if ev.Timestamp.IsZero() {
			t.Fatalf("event[%d].Timestamp not populated", i)
		}
	}

	// Confirm payload round-trips through json.RawMessage.
	var p struct {
		Outputs map[string]string `json:"outputs"`
	}
	if err := json.Unmarshal(got[3].Payload, &p); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if p.Outputs["out"] != "X" {
		t.Fatalf("FlowDone payload = %+v, want out=X", p)
	}
}

func TestListRunEventsEmpty(t *testing.T) {
	runID, s := seedRun(t)
	got, err := s.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestAppendRunEventUnknownRunIsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	err := s.AppendRunEvent(context.Background(), "missing-run", flowstore.RunEventFlowStarted, "", nil)
	if !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("Append(unknown) = %v, want ErrNotFound", err)
	}
}

func TestAppendNilPayloadStoresNoPayload(t *testing.T) {
	runID, s := seedRun(t)
	if err := s.AppendRunEvent(context.Background(), runID, flowstore.RunEventNodeSkipped, "skipped_node", nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, _ := s.ListRunEvents(context.Background(), runID, 0)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if len(got[0].Payload) != 0 {
		t.Fatalf("Payload = %q, want empty", got[0].Payload)
	}
	if got[0].NodeID != "skipped_node" {
		t.Fatalf("NodeID = %q, want skipped_node", got[0].NodeID)
	}
}

func TestListRunEventsLimit(t *testing.T) {
	runID, s := seedRun(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := s.AppendRunEvent(ctx, runID, flowstore.RunEventNodeStarted, "n", []byte(`{}`)); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}
	got, err := s.ListRunEvents(ctx, runID, 2)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("seqs = [%d %d], want [1 2]", got[0].Seq, got[1].Seq)
	}
}

func TestEventsSurviveFlowDelete(t *testing.T) {
	// Same audit-safety property as runs themselves: deleting the
	// flow keeps the event history.
	runID, s := seedRun(t)
	ctx := context.Background()
	if err := s.AppendRunEvent(ctx, runID, flowstore.RunEventFlowStarted, "", []byte(`{}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.DeleteFlow(ctx, "f"); err != nil {
		t.Fatalf("DeleteFlow: %v", err)
	}
	got, err := s.ListRunEvents(ctx, runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents post-delete: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
}

func TestAppendRunEventRejectsEmptyRunID(t *testing.T) {
	s := newTestStore(t)
	err := s.AppendRunEvent(context.Background(), "", flowstore.RunEventFlowStarted, "", nil)
	if err == nil || !strings.Contains(err.Error(), "empty run_id") {
		t.Fatalf("err = %v, want empty run_id rejection", err)
	}
}
