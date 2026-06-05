package sqlite

// White-box tests for the SuspendRun lifecycle + widened FinishRun. White-box
// because resume_token / interrupt_node are not surfaced by GetRun, so we
// read them directly off s.db.

import (
	"context"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

func openMem(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// queryRunCols reads the persisted suspend columns directly.
func queryRunCols(t *testing.T, s *Store, runID string) (status, resumeToken, interruptNode string) {
	t.Helper()
	if err := s.db.QueryRow(
		`SELECT status, resume_token, interrupt_node FROM runs WHERE id = ?`, runID,
	).Scan(&status, &resumeToken, &interruptNode); err != nil {
		t.Fatalf("query run cols: %v", err)
	}
	return
}

func TestSuspendRun_RunningToSuspended(t *testing.T) {
	s := openMem(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	runID, err := s.StartRun(ctx, "f", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.SuspendRun(ctx, runID, "tok-123", "approve"); err != nil {
		t.Fatalf("SuspendRun: %v", err)
	}

	rec, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if rec.Status != flowstore.RunStatusSuspended {
		t.Fatalf("status = %q, want suspended", rec.Status)
	}
	status, tok, node := queryRunCols(t, s, runID)
	if status != "suspended" || tok != "tok-123" || node != "approve" {
		t.Fatalf("cols = (%q, %q, %q), want (suspended, tok-123, approve)", status, tok, node)
	}

	// Re-suspend is idempotent.
	if err := s.SuspendRun(ctx, runID, "tok-456", "approve2"); err != nil {
		t.Fatalf("re-SuspendRun: %v", err)
	}
	_, tok, node = queryRunCols(t, s, runID)
	if tok != "tok-456" || node != "approve2" {
		t.Fatalf("re-suspend cols = (%q, %q), want (tok-456, approve2)", tok, node)
	}
}

func TestSuspendRun_MissingRun_NotFound(t *testing.T) {
	s := openMem(t)
	if err := s.SuspendRun(context.Background(), "nope", "t", "n"); err != flowstore.ErrNotFound {
		t.Fatalf("SuspendRun(missing) = %v, want ErrNotFound", err)
	}
}

func TestFinishRun_SuspendedToDone(t *testing.T) {
	s := openMem(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	runID, _ := s.StartRun(ctx, "f", nil)
	if err := s.SuspendRun(ctx, runID, "tok", "node"); err != nil {
		t.Fatalf("SuspendRun: %v", err)
	}
	if err := s.FinishRun(ctx, runID, map[string]string{"out": "ok"}, ""); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	rec, _ := s.GetRun(ctx, runID)
	if rec.Status != flowstore.RunStatusDone || rec.FinishedAt == nil || rec.Outputs["out"] != "ok" {
		t.Fatalf("done state wrong: %+v", rec)
	}
}

func TestFinishRun_RunningToDone_StillWorks(t *testing.T) {
	s := openMem(t)
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	runID, _ := s.StartRun(ctx, "f", nil)
	if err := s.FinishRun(ctx, runID, map[string]string{"out": "ok"}, ""); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	rec, _ := s.GetRun(ctx, runID)
	if rec.Status != flowstore.RunStatusDone || rec.Outputs["out"] != "ok" {
		t.Fatalf("running→done broke: %+v", rec)
	}
}
