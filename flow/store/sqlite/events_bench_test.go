package sqlite_test

// Benchmarks measuring the v0.1.2 perf change:
//
//   - BenchmarkAppendRunEvents_Batch_600 — one AppendRunEvents call
//     with 600 items. After the multi-VALUES rewrite this is expected
//     to drop to roughly a third of the prepared-loop baseline, on
//     top of the WAL speedup the on-disk store also gets.
//
//   - BenchmarkAppendRunEvent_Single_600 — 600 sequential
//     AppendRunEvent calls. WAL+NORMAL alone gives this path the
//     ~5x speedup; the multi-VALUES rewrite does not touch the
//     single-row path.
//
// No hard thresholds: CI noise + machine-to-machine variance makes
// strict assertions a flake source. The numbers themselves go into
// the commit message and CHANGELOG for posterity.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	"github.com/costa92/llm-agent-flow/flow/store/sqlite"
)

func benchSeedDiskRun(b *testing.B) (string, *sqlite.Store) {
	b.Helper()
	dir := b.TempDir()
	dsn := filepath.Join(dir, "bench.db")
	s, err := sqlite.Open(dsn)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if _, err := s.PutFlow(ctx, "f", "", []byte(`{}`), true); err != nil {
		b.Fatalf("PutFlow: %v", err)
	}
	id, err := s.StartRun(ctx, "f", nil)
	if err != nil {
		b.Fatalf("StartRun: %v", err)
	}
	return id, s
}

func BenchmarkAppendRunEvents_Batch_600(b *testing.B) {
	const n = 600
	items := make([]flowstore.RunEventBatchItem, n)
	for i := 0; i < n; i++ {
		items[i] = flowstore.RunEventBatchItem{
			Kind:    flowstore.RunEventNodeStarted,
			NodeID:  fmt.Sprintf("n%04d", i),
			Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID, s := benchSeedDiskRun(b)
		b.StartTimer()
		if err := s.AppendRunEvents(context.Background(), runID, items); err != nil {
			b.Fatalf("AppendRunEvents: %v", err)
		}
	}
}

func BenchmarkAppendRunEvent_Single_600(b *testing.B) {
	const n = 600
	payload := []byte(`{"i":0}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID, s := benchSeedDiskRun(b)
		b.StartTimer()
		for j := 0; j < n; j++ {
			if err := s.AppendRunEvent(context.Background(), runID,
				flowstore.RunEventNodeStarted, "n", payload); err != nil {
				b.Fatalf("AppendRunEvent: %v", err)
			}
		}
	}
}
