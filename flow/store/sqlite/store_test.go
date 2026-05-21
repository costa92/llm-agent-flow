package sqlite_test

import (
	"context"
	"errors"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	"github.com/costa92/llm-agent-flow/flow/store/sqlite"
)

func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutAndGetFlowRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	body := []byte(`{"id":"x","nodes":[],"edges":[]}`)
	rec, err := s.PutFlow(ctx, "x", "demo flow", body, true)
	if err != nil {
		t.Fatalf("PutFlow create: %v", err)
	}
	if rec.ID != "x" || rec.Name != "demo flow" || string(rec.JSON) != string(body) {
		t.Fatalf("rec = %+v, want round-tripped", rec)
	}
	if rec.CreatedAt.IsZero() || rec.UpdatedAt.IsZero() {
		t.Fatalf("timestamps unset: %+v", rec)
	}

	got, err := s.GetFlow(ctx, "x")
	if err != nil {
		t.Fatalf("GetFlow: %v", err)
	}
	if string(got.JSON) != string(body) {
		t.Fatalf("GetFlow.JSON = %q, want %q", got.JSON, body)
	}
}

func TestPutFlowCreateRejectsDuplicate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	body := []byte(`{}`)
	if _, err := s.PutFlow(ctx, "x", "first", body, true); err != nil {
		t.Fatalf("first PutFlow: %v", err)
	}
	_, err := s.PutFlow(ctx, "x", "second", body, true)
	if !errors.Is(err, flowstore.ErrAlreadyExists) {
		t.Fatalf("PutFlow(create, dup) = %v, want ErrAlreadyExists", err)
	}
}

func TestPutFlowReplaceUpdatesFields(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	first := []byte(`{"v":1}`)
	if _, err := s.PutFlow(ctx, "x", "v1", first, true); err != nil {
		t.Fatalf("PutFlow first: %v", err)
	}
	second := []byte(`{"v":2}`)
	rec, err := s.PutFlow(ctx, "x", "v2", second, false)
	if err != nil {
		t.Fatalf("PutFlow replace: %v", err)
	}
	if rec.Name != "v2" || string(rec.JSON) != string(second) {
		t.Fatalf("rec = %+v, want v2", rec)
	}
	// CreatedAt should be preserved across replace.
	got, _ := s.GetFlow(ctx, "x")
	if !got.UpdatedAt.After(got.CreatedAt) && !got.UpdatedAt.Equal(got.CreatedAt) {
		t.Fatalf("UpdatedAt %v before CreatedAt %v", got.UpdatedAt, got.CreatedAt)
	}
}

func TestGetFlowMissingReturnsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetFlow(context.Background(), "nope")
	if !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("GetFlow(missing) = %v, want ErrNotFound", err)
	}
}

func TestListFlowsOrdersByUpdatedAtDesc(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := s.PutFlow(ctx, id, id, []byte(`{}`), true); err != nil {
			t.Fatalf("PutFlow %q: %v", id, err)
		}
	}
	// Touch "a" so it's most-recently-updated.
	if _, err := s.PutFlow(ctx, "a", "a-bumped", []byte(`{}`), false); err != nil {
		t.Fatalf("PutFlow bump a: %v", err)
	}
	list, err := s.ListFlows(ctx, 10)
	if err != nil {
		t.Fatalf("ListFlows: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len = %d, want 3", len(list))
	}
	if list[0].ID != "a" {
		t.Fatalf("first id = %q, want a (most recently updated)", list[0].ID)
	}
}

func TestDeleteFlow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "x", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	if err := s.DeleteFlow(ctx, "x"); err != nil {
		t.Fatalf("DeleteFlow: %v", err)
	}
	if _, err := s.GetFlow(ctx, "x"); !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("post-delete GetFlow = %v, want ErrNotFound", err)
	}
	if err := s.DeleteFlow(ctx, "x"); !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("delete-twice = %v, want ErrNotFound", err)
	}
}

func TestRunLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "x", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	runID, err := s.StartRun(ctx, "x", map[string]string{"in": "hi"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if runID == "" {
		t.Fatal("empty runID")
	}

	rec, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun (running): %v", err)
	}
	if rec.Status != flowstore.RunStatusRunning || rec.FinishedAt != nil {
		t.Fatalf("running state wrong: %+v", rec)
	}
	if rec.Inputs["in"] != "hi" {
		t.Fatalf("inputs = %+v, want in=hi", rec.Inputs)
	}

	if err := s.FinishRun(ctx, runID, map[string]string{"out": "HI"}, ""); err != nil {
		t.Fatalf("FinishRun (done): %v", err)
	}
	rec, _ = s.GetRun(ctx, runID)
	if rec.Status != flowstore.RunStatusDone || rec.FinishedAt == nil || rec.Outputs["out"] != "HI" {
		t.Fatalf("done state wrong: %+v", rec)
	}

	// FinishRun on terminal row is a no-op (idempotent).
	if err := s.FinishRun(ctx, runID, nil, "ignored"); err != nil {
		t.Fatalf("FinishRun idempotent: %v", err)
	}
	rec2, _ := s.GetRun(ctx, runID)
	if rec2.Error != "" {
		t.Fatalf("idempotent FinishRun overwrote: %+v", rec2)
	}
}

func TestRunFailedPath(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, _ = s.PutFlow(ctx, "x", "", []byte(`{}`), true)
	runID, _ := s.StartRun(ctx, "x", nil)
	if err := s.FinishRun(ctx, runID, nil, "boom"); err != nil {
		t.Fatalf("FinishRun failed: %v", err)
	}
	rec, _ := s.GetRun(ctx, runID)
	if rec.Status != flowstore.RunStatusFailed || rec.Error != "boom" {
		t.Fatalf("failed state wrong: %+v", rec)
	}
}

func TestListRunsByFlowOrdersByStartedAtDesc(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, _ = s.PutFlow(ctx, "f", "", []byte(`{}`), true)
	var ids []string
	for i := 0; i < 3; i++ {
		id, err := s.StartRun(ctx, "f", nil)
		if err != nil {
			t.Fatalf("StartRun #%d: %v", i, err)
		}
		ids = append(ids, id)
	}
	list, err := s.ListRuns(ctx, "f", 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len = %d, want 3", len(list))
	}
	if list[0].ID != ids[len(ids)-1] {
		t.Fatalf("first id = %q, want most-recent %q", list[0].ID, ids[len(ids)-1])
	}
}

func TestGetRunMissingReturnsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetRun(context.Background(), "nope")
	if !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("GetRun(missing) = %v, want ErrNotFound", err)
	}
}

func TestDeleteFlowKeepsRunHistory(t *testing.T) {
	// Deleting a flow does NOT cascade to its runs — historical
	// records survive so audit / debugging keep working.
	s := newTestStore(t)
	ctx := context.Background()
	_, _ = s.PutFlow(ctx, "f", "", []byte(`{}`), true)
	runID, _ := s.StartRun(ctx, "f", nil)
	_ = s.FinishRun(ctx, runID, map[string]string{"x": "y"}, "")
	if err := s.DeleteFlow(ctx, "f"); err != nil {
		t.Fatalf("DeleteFlow: %v", err)
	}
	rec, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun after flow delete: %v", err)
	}
	if rec.Status != flowstore.RunStatusDone {
		t.Fatalf("run %+v lost data after flow delete", rec)
	}
}
