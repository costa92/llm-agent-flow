package sqlite_test

import (
	"context"
	"errors"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

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
