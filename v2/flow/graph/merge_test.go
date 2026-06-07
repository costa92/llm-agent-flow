package graph

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// buildStreamMergeInvokeGraph builds a value-path stream-merge graph:
//
//	entry(copy fan-out) -> { a: lambdaA, b: lambdaB } -> merge(combine) = exit
//
// On Invoke each source lambda turns the int into a string; the merge gathers
// the two source strings (sorted-key) and joins them with "|". A Copy fans the
// entry value to both sources so both fire on every Invoke. (This is the value
// path only — Invoke, no streaming.)
func buildStreamMergeInvokeGraph(t *testing.T) *Runnable[int, string] {
	t.Helper()
	g := NewGraph[int, string]()
	entry, _ := AddPassthroughNode[int, string, int](g, "entry")
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (string, error) {
		return fmt.Sprintf("a%d", x), nil
	})
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (string, error) {
		return fmt.Sprintf("b%d", x), nil
	})
	cp, err := AddCopy[int, string, int](g, "cp", map[string]NodeRef{"a": aN, "b": bN})
	if err != nil {
		t.Fatalf("AddCopy: %v", err)
	}
	mg, err := AddStreamMerge[int, string, string, string](g, "mg",
		map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	if err != nil {
		t.Fatalf("AddStreamMerge: %v", err)
	}
	if err := g.AddEdge(entry, cp); err != nil {
		t.Fatalf("AddEdge entry->cp: %v", err)
	}
	if err := g.Entry(entry); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(mg); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// TestAddStreamMerge_InvokeGathersSortedDeterministic drives the value path 200×
// and asserts byte-identical R: the gather is in sorted-key order ("a" before
// "b") regardless of port-firing order.
func TestAddStreamMerge_InvokeGathersSortedDeterministic(t *testing.T) {
	r := buildStreamMergeInvokeGraph(t)
	for i := 0; i < 200; i++ {
		got, err := r.Invoke(context.Background(), 3)
		if err != nil {
			t.Fatalf("Invoke iter %d: %v", i, err)
		}
		if got != "a3|b3" {
			t.Fatalf("Invoke iter %d = %q, want %q (sorted-key gather a before b)", i, got, "a3|b3")
		}
	}
}

// TestAddStreamMerge_SkippedSourceAbsent proves a conditionally-skipped source
// (selected away by a branch) is ABSENT from the gathered []T: the merge folds
// only the present source.
func TestAddStreamMerge_SkippedSourceAbsent(t *testing.T) {
	g := NewGraph[int, string]()
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (string, error) {
		return "A", nil
	})
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (string, error) {
		return "B", nil
	})
	// A branch selects exactly ONE of a/b, so the merge sees a single source.
	br, _ := AddBranch(g, "br", func(_ context.Context, x int) (string, error) {
		if x%2 == 0 {
			return "a", nil
		}
		return "b", nil
	}, map[string]NodeRef{"a": aN, "b": bN})
	mg, err := AddStreamMerge[int, string, string, string](g, "mg",
		map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	if err != nil {
		t.Fatalf("AddStreamMerge: %v", err)
	}
	if err := g.Entry(br); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(mg); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// x even -> only "a" fires -> merge gathers ["A"] -> "A".
	got, err := r.Invoke(context.Background(), 2)
	if err != nil {
		t.Fatalf("Invoke(2): %v", err)
	}
	if got != "A" {
		t.Fatalf("Invoke(2) = %q, want %q (only present source a)", got, "A")
	}
	// x odd -> only "b" fires -> merge gathers ["B"] -> "B".
	got, err = r.Invoke(context.Background(), 3)
	if err != nil {
		t.Fatalf("Invoke(3): %v", err)
	}
	if got != "B" {
		t.Fatalf("Invoke(3) = %q, want %q (only present source b)", got, "B")
	}
}

// buildZipInvokeGraph builds a value-path zip graph:
//
//	entry(copy fan-out) -> { a: lambdaA, b: lambdaB } -> zip(combine) = exit
//
// On Invoke the zip gathers each source's value into a per-source [][]T (sorted
// key) and the combine round-robin-interleaves position 0 of each, joined with
// ",". With two single-frame sources the result is "a3,b3".
func buildZipInvokeGraph(t *testing.T) *Runnable[int, string] {
	t.Helper()
	g := NewGraph[int, string]()
	entry, _ := AddPassthroughNode[int, string, int](g, "entry")
	aN, _ := AddLambdaNode(g, "a", func(_ context.Context, x int) (string, error) {
		return fmt.Sprintf("a%d", x), nil
	})
	bN, _ := AddLambdaNode(g, "b", func(_ context.Context, x int) (string, error) {
		return fmt.Sprintf("b%d", x), nil
	})
	cp, err := AddCopy[int, string, int](g, "cp", map[string]NodeRef{"a": aN, "b": bN})
	if err != nil {
		t.Fatalf("AddCopy: %v", err)
	}
	zp, err := AddZip[int, string, string, string](g, "zp",
		map[string]NodeRef{"a": aN, "b": bN},
		func(_ context.Context, perSource [][]string) (string, error) {
			return zipInterleave(perSource), nil
		})
	if err != nil {
		t.Fatalf("AddZip: %v", err)
	}
	if err := g.AddEdge(entry, cp); err != nil {
		t.Fatalf("AddEdge entry->cp: %v", err)
	}
	if err := g.Entry(entry); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(zp); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// zipInterleave round-robin-interleaves the per-source frame lists in source
// order, skipping an exhausted source, and joins them with ",". It is the
// deterministic zip oracle reused by the value- and stream-path tests.
func zipInterleave(perSource [][]string) string {
	var out []string
	maxLen := 0
	for _, s := range perSource {
		if len(s) > maxLen {
			maxLen = len(s)
		}
	}
	for i := 0; i < maxLen; i++ {
		for _, s := range perSource {
			if i < len(s) {
				out = append(out, s[i])
			}
		}
	}
	return strings.Join(out, ",")
}

// TestAddZip_InvokeRoundRobinDeterministic drives the zip value path 200× and
// asserts byte-identical R: sources are gathered in sorted-key order and
// round-robin-interleaved deterministically.
func TestAddZip_InvokeRoundRobinDeterministic(t *testing.T) {
	r := buildZipInvokeGraph(t)
	for i := 0; i < 200; i++ {
		got, err := r.Invoke(context.Background(), 3)
		if err != nil {
			t.Fatalf("Invoke iter %d: %v", i, err)
		}
		if got != "a3,b3" {
			t.Fatalf("Invoke iter %d = %q, want %q (round-robin a then b)", i, got, "a3,b3")
		}
	}
}

// TestMerge_HeteroT_BuildRejected proves a merge whose sources have DIFFERENT
// element types is a build error: a streaming merge interleaves a single T.
func TestMerge_HeteroT_BuildRejected(t *testing.T) {
	g := NewGraph[int, string]()
	strN, _ := AddLambdaNode(g, "s", func(_ context.Context, x int) (string, error) { return "s", nil })
	intN, _ := AddLambdaNode(g, "i", func(_ context.Context, x int) (int, error) { return x, nil })
	// T=string but source "i" emits int -> reject.
	if _, err := AddStreamMerge[int, string, string, string](g, "mg",
		map[string]NodeRef{"s": strN, "i": intN},
		func(_ context.Context, items []string) (string, error) { return "", nil }); err == nil {
		t.Fatalf("AddStreamMerge with heterogeneous source types: want error, got nil")
	}
	// Same for zip.
	g2 := NewGraph[int, string]()
	strN2, _ := AddLambdaNode(g2, "s", func(_ context.Context, x int) (string, error) { return "s", nil })
	intN2, _ := AddLambdaNode(g2, "i", func(_ context.Context, x int) (int, error) { return x, nil })
	if _, err := AddZip[int, string, string, string](g2, "zp",
		map[string]NodeRef{"s": strN2, "i": intN2},
		func(_ context.Context, perSource [][]string) (string, error) { return "", nil }); err == nil {
		t.Fatalf("AddZip with heterogeneous source types: want error, got nil")
	}
}

// TestMerge_EmptySources_BuildRejected rejects a merge / zip node with no
// sources, mirroring AddCopy/AddCombine.
func TestMerge_EmptySources_BuildRejected(t *testing.T) {
	g := NewGraph[int, string]()
	if _, err := AddStreamMerge[int, string, string, string](g, "mg",
		map[string]NodeRef{},
		func(_ context.Context, items []string) (string, error) { return "", nil }); err == nil {
		t.Fatalf("AddStreamMerge with no sources: want error, got nil")
	}
	g2 := NewGraph[int, string]()
	if _, err := AddZip[int, string, string, string](g2, "zp",
		map[string]NodeRef{},
		func(_ context.Context, perSource [][]string) (string, error) { return "", nil }); err == nil {
		t.Fatalf("AddZip with no sources: want error, got nil")
	}
}
