package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	"github.com/costa92/llm-agent-flow/flow/store/sqlite"
)

// newDiskStore opens an on-disk SQLite Store under t.TempDir(). Used
// by batch and WAL tests that need real-file behaviour (sidecar files,
// WAL mode, etc.) rather than the :memory: shortcut.
func newDiskStore(t *testing.T) (*sqlite.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "flow.db")
	s, err := sqlite.Open(dsn)
	if err != nil {
		t.Fatalf("Open(disk): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dsn
}

// seedDiskRun mirrors seedRun but uses an on-disk store so batch
// tests can run under WAL once it lands.
func seedDiskRun(t *testing.T) (runID string, s *sqlite.Store) {
	t.Helper()
	st, _ := newDiskStore(t)
	ctx := context.Background()
	if _, err := st.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		t.Fatalf("PutFlow: %v", err)
	}
	id, err := st.StartRun(ctx, "f", map[string]string{"in": "x"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return id, st
}

func TestAppendRunEventsBatchHappyPath(t *testing.T) {
	runID, s := seedRun(t)
	ctx := context.Background()
	items := []flowstore.RunEventBatchItem{
		{Kind: flowstore.RunEventFlowStarted, Payload: []byte(`{"flow":"f"}`)},
		{Kind: flowstore.RunEventNodeStarted, NodeID: "a", Payload: []byte(`{"node":"a"}`)},
		{Kind: flowstore.RunEventNodeFinished, NodeID: "a", Payload: []byte(`{"node":"a"}`)},
		{Kind: flowstore.RunEventFlowDone, Payload: []byte(`{"outputs":{"out":"X"}}`)},
	}
	batcher := s.(interface {
		AppendRunEvents(ctx context.Context, runID string, items []flowstore.RunEventBatchItem) error
	})
	if err := batcher.AppendRunEvents(ctx, runID, items); err != nil {
		t.Fatalf("AppendRunEvents: %v", err)
	}
	got, _ := s.ListRunEvents(ctx, runID, 0)
	if len(got) != len(items) {
		t.Fatalf("len = %d, want %d", len(got), len(items))
	}
	for i, ev := range got {
		if ev.Seq != i+1 {
			t.Fatalf("seq[%d] = %d, want %d", i, ev.Seq, i+1)
		}
		if ev.Kind != items[i].Kind {
			t.Fatalf("kind[%d] = %q, want %q", i, ev.Kind, items[i].Kind)
		}
	}
}

func TestAppendRunEventsBatchEmptyIsNoOp(t *testing.T) {
	runID, s := seedRun(t)
	batcher := s.(interface {
		AppendRunEvents(ctx context.Context, runID string, items []flowstore.RunEventBatchItem) error
	})
	if err := batcher.AppendRunEvents(context.Background(), runID, nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	got, _ := s.ListRunEvents(context.Background(), runID, 0)
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestAppendRunEventsBatchUnknownRunErrNotFound(t *testing.T) {
	// newTestStore returns *sqlite.Store directly so AppendRunEvents
	// is callable without the interface assertion.
	s := newTestStore(t)
	err := s.AppendRunEvents(context.Background(), "nope",
		[]flowstore.RunEventBatchItem{{Kind: flowstore.RunEventFlowStarted}})
	if !errors.Is(err, flowstore.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestAppendRunEventsBatch_LargeBatch_OneSQLStatement locks two
// contracts that the multi-VALUES rewrite must keep:
//
//  1. All 50 events are persisted with seq 1..50 in order, with
//     payload / kind round-tripped byte-for-byte.
//  2. The entire batch shares ONE server-side timestamp (the engine
//     samples nowUnix() once, not per row). Replay clients depend on
//     this to render a batch as a single point on a timeline.
func TestAppendRunEventsBatch_LargeBatch_OneSQLStatement(t *testing.T) {
	runID, s := seedDiskRun(t)
	ctx := context.Background()

	const n = 50
	items := make([]flowstore.RunEventBatchItem, n)
	for i := 0; i < n; i++ {
		items[i] = flowstore.RunEventBatchItem{
			Kind:    flowstore.RunEventNodeStarted,
			NodeID:  fmt.Sprintf("node-%d", i),
			Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	if err := s.AppendRunEvents(ctx, runID, items); err != nil {
		t.Fatalf("AppendRunEvents: %v", err)
	}

	got, err := s.ListRunEvents(ctx, runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	firstTS := got[0].Timestamp
	for i, ev := range got {
		if ev.Seq != i+1 {
			t.Fatalf("seq[%d] = %d, want %d", i, ev.Seq, i+1)
		}
		if ev.Kind != items[i].Kind {
			t.Fatalf("kind[%d] = %q, want %q", i, ev.Kind, items[i].Kind)
		}
		if string(ev.Payload) != string(items[i].Payload) {
			t.Fatalf("payload[%d] = %q, want %q", i, ev.Payload, items[i].Payload)
		}
		if !ev.Timestamp.Equal(firstTS) {
			t.Fatalf("timestamp[%d] = %v, want %v (whole batch must share one ts)",
				i, ev.Timestamp, firstTS)
		}
	}
}

// TestAppendRunEventsBatch_ExceedsParamLimit_ChunksTransparently
// pushes 200 events through. Whatever per-statement chunk size the
// implementation picks, the caller sees one monotonic seq 1..200 and
// per-row NodeID / Payload one-to-one. This is the off-by-one canary
// for any chunk-boundary seq arithmetic.
func TestAppendRunEventsBatch_ExceedsParamLimit_ChunksTransparently(t *testing.T) {
	runID, s := seedDiskRun(t)
	ctx := context.Background()

	const n = 200
	items := make([]flowstore.RunEventBatchItem, n)
	for i := 0; i < n; i++ {
		items[i] = flowstore.RunEventBatchItem{
			Kind:    flowstore.RunEventNodeFinished,
			NodeID:  fmt.Sprintf("n%03d", i),
			Payload: []byte(fmt.Sprintf(`{"idx":%d}`, i)),
		}
	}
	if err := s.AppendRunEvents(ctx, runID, items); err != nil {
		t.Fatalf("AppendRunEvents: %v", err)
	}

	got, err := s.ListRunEvents(ctx, runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	for i, ev := range got {
		if ev.Seq != i+1 {
			t.Fatalf("seq[%d] = %d, want %d (off-by-one across chunk boundary)",
				i, ev.Seq, i+1)
		}
		wantNode := fmt.Sprintf("n%03d", i)
		if ev.NodeID != wantNode {
			t.Fatalf("node[%d] = %q, want %q", i, ev.NodeID, wantNode)
		}
		wantPayload := fmt.Sprintf(`{"idx":%d}`, i)
		if string(ev.Payload) != wantPayload {
			t.Fatalf("payload[%d] = %q, want %q", i, ev.Payload, wantPayload)
		}
	}
}

func TestAppendRunEventsContinuesSeqAfterSingleAppend(t *testing.T) {
	runID, s := seedRun(t)
	ctx := context.Background()
	if err := s.AppendRunEvent(ctx, runID, flowstore.RunEventFlowStarted, "", []byte(`{}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	batcher := s.(interface {
		AppendRunEvents(ctx context.Context, runID string, items []flowstore.RunEventBatchItem) error
	})
	if err := batcher.AppendRunEvents(ctx, runID, []flowstore.RunEventBatchItem{
		{Kind: flowstore.RunEventNodeStarted, NodeID: "a"},
		{Kind: flowstore.RunEventNodeFinished, NodeID: "a"},
	}); err != nil {
		t.Fatalf("AppendRunEvents: %v", err)
	}
	got, _ := s.ListRunEvents(ctx, runID, 0)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, ev := range got {
		if ev.Seq != i+1 {
			t.Fatalf("seq[%d] = %d, want %d", i, ev.Seq, i+1)
		}
	}
}
