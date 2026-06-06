package flow

import (
	"context"
	"errors"
	"testing"
)

// summary is a sample user State type with a slice field, used to exercise
// the determinism + copy-on-read contracts.
type summary struct {
	Lines []string
	Count int
}

// TestState_NoCellInert asserts both helpers are silent no-ops when no cell
// is bound into the context — State-less flows stay completely inert.
func TestState_NoCellInert(t *testing.T) {
	ctx := context.Background()

	if _, ok := StateFromContext[summary](ctx); ok {
		t.Fatalf("StateFromContext on a cell-less ctx returned ok=true")
	}
	// Must not panic / must be a silent no-op.
	SetState(ctx, summary{Count: 1})
}

// TestState_LastWritePerNode asserts the last SetState for a given nodeID
// wins (staged write is overwritten, not appended).
func TestState_LastWritePerNode(t *testing.T) {
	cell := newStateCell(summary{}, nil)
	ctx := withStateNode(context.Background(), cell, "n1")

	SetState(ctx, summary{Count: 1})
	SetState(ctx, summary{Count: 2})

	if got := cell.staged["n1"].(summary).Count; got != 2 {
		t.Fatalf("staged write = %d, want 2 (last-write-per-node)", got)
	}
	if len(cell.staged) != 1 {
		t.Fatalf("staged map has %d entries, want 1", len(cell.staged))
	}
}

// TestState_RejectOnCollisionDefault asserts that with NO reducer supplied,
// two nodes writing in one layer is a conflict (BLOCKER-1).
func TestState_RejectOnCollisionDefault(t *testing.T) {
	cell := newStateCell(summary{}, nil)
	SetState(withStateNode(context.Background(), cell, "n1"), summary{Count: 1})
	SetState(withStateNode(context.Background(), cell, "n2"), summary{Count: 2})

	if err := cell.reduce(); !errors.Is(err, ErrStateWriteConflict) {
		t.Fatalf("reduce with 2 writers and no reducer = %v, want ErrStateWriteConflict", err)
	}
}

// TestState_SingleWriterDefaultApplies asserts that with no reducer and a
// single writer (or zero), reduce applies the write with no error.
func TestState_SingleWriterDefaultApplies(t *testing.T) {
	cell := newStateCell(summary{}, nil)

	// Zero writers: no error, committed unchanged.
	if err := cell.reduce(); err != nil {
		t.Fatalf("reduce with 0 writers = %v, want nil", err)
	}

	SetState(withStateNode(context.Background(), cell, "n1"), summary{Count: 7})
	if err := cell.reduce(); err != nil {
		t.Fatalf("reduce with 1 writer = %v, want nil", err)
	}
	if got := cell.committed.(summary).Count; got != 7 {
		t.Fatalf("committed = %d, want 7", got)
	}
	if len(cell.staged) != 0 {
		t.Fatalf("staged not cleared after reduce: %d entries", len(cell.staged))
	}
}

// TestState_WithReducerMerges asserts a supplied reducer merges 2 writes
// deterministically and never errors on collision.
func TestState_WithReducerMerges(t *testing.T) {
	reduce := func(prev summary, writes []summary) summary {
		for _, w := range writes {
			prev.Lines = append(prev.Lines, w.Lines...)
			prev.Count += w.Count
		}
		return prev
	}
	cell := newStateCell(summary{}, func(prev any, writes []any) (any, error) {
		ss := make([]summary, len(writes))
		for i, w := range writes {
			ss[i] = w.(summary)
		}
		return reduce(prev.(summary), ss), nil
	})

	SetState(withStateNode(context.Background(), cell, "n1"), summary{Lines: []string{"a"}, Count: 1})
	SetState(withStateNode(context.Background(), cell, "n2"), summary{Lines: []string{"b"}, Count: 2})

	if err := cell.reduce(); err != nil {
		t.Fatalf("reduce with reducer = %v, want nil", err)
	}
	got := cell.committed.(summary)
	if got.Count != 3 {
		t.Fatalf("merged Count = %d, want 3", got.Count)
	}
	if len(got.Lines) != 2 || got.Lines[0] != "a" || got.Lines[1] != "b" {
		t.Fatalf("merged Lines = %v, want [a b] (nodeID-sorted)", got.Lines)
	}
}

// TestState_ReduceNodeIDSortedStable asserts the reducer receives writes in
// nodeID-sorted order regardless of staging order — the determinism rule.
func TestState_ReduceNodeIDSortedStable(t *testing.T) {
	makeCell := func() *stateCell {
		return newStateCell([]string(nil), func(prev any, writes []any) (any, error) {
			out := append([]string(nil), prev.([]string)...)
			for _, w := range writes {
				out = append(out, w.(string))
			}
			return out, nil
		})
	}

	// Stage in order z, a, m.
	c1 := makeCell()
	SetState(withStateNode(context.Background(), c1, "z"), "z")
	SetState(withStateNode(context.Background(), c1, "a"), "a")
	SetState(withStateNode(context.Background(), c1, "m"), "m")
	if err := c1.reduce(); err != nil {
		t.Fatal(err)
	}

	// Stage in reverse order m, a, z.
	c2 := makeCell()
	SetState(withStateNode(context.Background(), c2, "m"), "m")
	SetState(withStateNode(context.Background(), c2, "a"), "a")
	SetState(withStateNode(context.Background(), c2, "z"), "z")
	if err := c2.reduce(); err != nil {
		t.Fatal(err)
	}

	g1 := c1.committed.([]string)
	g2 := c2.committed.([]string)
	want := []string{"a", "m", "z"}
	for i := range want {
		if g1[i] != want[i] || g2[i] != want[i] {
			t.Fatalf("reduce order not nodeID-sorted: c1=%v c2=%v want %v", g1, g2, want)
		}
	}
}

// TestState_CopyOnRead asserts StateFromContext returns a value copy:
// mutating the returned struct does not change the committed snapshot.
func TestState_CopyOnRead(t *testing.T) {
	cell := newStateCell(summary{Count: 5}, nil)
	ctx := withStateNode(context.Background(), cell, "n1")

	got, ok := StateFromContext[summary](ctx)
	if !ok {
		t.Fatalf("StateFromContext ok=false, want true")
	}
	got.Count = 999

	again, _ := StateFromContext[summary](ctx)
	if again.Count != 5 {
		t.Fatalf("committed mutated through returned value: %d, want 5", again.Count)
	}
}
