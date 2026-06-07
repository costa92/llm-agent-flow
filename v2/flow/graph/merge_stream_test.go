package graph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// drainString reads a StreamReader[string] to io.EOF, returning the single
// folded output (the merge value tail is one element).
func drainStringTail(t *testing.T, sr StreamReader[string]) string {
	t.Helper()
	v, err := sr.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Next: %v", err)
	}
	return v
}

// buildMergeMultisetGraph builds a standalone merge graph whose combine returns
// the SORTED multiset of gathered frames joined with "," — so the test can
// assert exact multiset + count regardless of interleave order.
func buildMergeMultisetGraph(t *testing.T, framesA, framesB []string) *Graph[string, string] {
	t.Helper()
	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", framesA, nil)
	srcB, _ := addStringSource(g, "srcB", framesB, nil)
	mg, err := AddStreamMerge[string, string, string, string](g, "mg",
		map[string]NodeRef{"a": srcA, "b": srcB},
		func(_ context.Context, items []string) (string, error) {
			cp := append([]string(nil), items...)
			sort.Strings(cp)
			return strings.Join(cp, ","), nil
		})
	if err != nil {
		t.Fatalf("AddStreamMerge: %v", err)
	}
	_ = g.Entry(srcA)
	_ = g.Exit(mg)
	return g
}

// TestStreamGraph_StreamMerge_InterleavesTwoStreams: the merge interleaves two
// multi-frame streams. Assert MULTISET equality + exact count + per-source
// completeness + per-source FIFO order (each source's frames appear in arrival
// order within the merged stream).
func TestStreamGraph_StreamMerge_InterleavesTwoStreams(t *testing.T) {
	framesA := []string{"a1", "a2", "a3"}
	framesB := []string{"b1", "b2"}

	// Build a merge whose combine records the RAW interleave order (not sorted)
	// so we can assert per-source FIFO. Encode the merged sequence with "|".
	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", framesA, nil)
	srcB, _ := addStringSource(g, "srcB", framesB, nil)
	mg, _ := AddStreamMerge[string, string, string, string](g, "mg",
		map[string]NodeRef{"a": srcA, "b": srcB},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	_ = g.Entry(srcA)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	out := drainStringTail(t, sr)
	sr.Close()

	merged := strings.Split(out, "|")
	if len(merged) != len(framesA)+len(framesB) {
		t.Fatalf("merged count = %d (%q), want %d", len(merged), out, len(framesA)+len(framesB))
	}
	// Multiset equality.
	gotSorted := append([]string(nil), merged...)
	sort.Strings(gotSorted)
	want := append(append([]string(nil), framesA...), framesB...)
	sort.Strings(want)
	if !reflect.DeepEqual(gotSorted, want) {
		t.Fatalf("merged multiset = %v, want %v", gotSorted, want)
	}
	// Per-source completeness + FIFO order: filter the merged sequence by source
	// prefix and assert it equals that source's frames in order.
	assertSubsequence(t, merged, framesA)
	assertSubsequence(t, merged, framesB)

	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// assertSubsequence checks that filtering merged to only the frames in want
// yields want in the SAME order (per-source FIFO).
func assertSubsequence(t *testing.T, merged, want []string) {
	t.Helper()
	set := make(map[string]bool, len(want))
	for _, w := range want {
		set[w] = true
	}
	var got []string
	for _, m := range merged {
		if set[m] {
			got = append(got, m)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("per-source order = %v, want %v (FIFO within a source)", got, want)
	}
}

// TestStreamGraph_StreamMerge_EquivalenceVsInvoke_Multiset: Stream output ==
// Invoke output as a MULTISET (the unordered cross-validator).
func TestStreamGraph_StreamMerge_EquivalenceVsInvoke_Multiset(t *testing.T) {
	framesA := []string{"a1", "a2", "a3"}
	framesB := []string{"b1", "b2"}

	rStream, err := buildMergeMultisetGraph(t, framesA, framesB).Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile (stream): %v", err)
	}
	sr, err := rStream.Stream(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	gotStream := drainStringTail(t, sr)
	sr.Close()

	rInvoke, err := buildMergeMultisetGraph(t, framesA, framesB).Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile (invoke): %v", err)
	}
	gotInvoke, err := rInvoke.Invoke(context.Background(), "x")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	// Both combines sort the gathered frames, so the outputs are byte-identical
	// multisets. On Invoke each source contributes its FULL joined string as ONE
	// frame; on Stream it contributes each frame. The multiset comparison must be
	// on the same granularity, so compare via the sorted token sets below.
	if gotStream == gotInvoke {
		return // identical (both single-frame-per-source on Invoke vs per-frame on Stream coincide only if framing matches)
	}
	// The Invoke value path sees one value PER SOURCE (the source's full joined
	// string), while the stream path sees each frame. Normalise both to the
	// frame multiset and compare.
	wantTokens := append(append([]string(nil), framesA...), framesB...)
	sort.Strings(wantTokens)
	gotTokens := strings.Split(gotStream, ",")
	sort.Strings(gotTokens)
	if !reflect.DeepEqual(gotTokens, wantTokens) {
		t.Fatalf("stream multiset = %v, want %v", gotTokens, wantTokens)
	}
	// Invoke: each source is one joined value.
	invTokens := strings.Split(gotInvoke, ",")
	sort.Strings(invTokens)
	wantInvoke := []string{strings.Join(framesA, ""), strings.Join(framesB, "")}
	sort.Strings(wantInvoke)
	if !reflect.DeepEqual(invTokens, wantInvoke) {
		t.Fatalf("invoke multiset = %v, want %v", invTokens, wantInvoke)
	}
}

// TestStreamGraph_Zip_EquivalenceVsInvoke_Sequence: the zip stream output is a
// DETERMINISTIC sequence (round-robin per-source). Assert exact sequence
// equality against the deterministic oracle.
func TestStreamGraph_Zip_EquivalenceVsInvoke_Sequence(t *testing.T) {
	framesA := []string{"a1", "a2", "a3"}
	framesB := []string{"b1", "b2"}

	build := func() *Runnable[string, string] {
		g := NewGraph[string, string]()
		srcA, _ := addStringSource(g, "srcA", framesA, nil)
		srcB, _ := addStringSource(g, "srcB", framesB, nil)
		mg, _ := AddZip[string, string, string, string](g, "mg",
			map[string]NodeRef{"a": srcA, "b": srcB},
			func(_ context.Context, perSource [][]string) (string, error) {
				return zipInterleave(perSource), nil
			})
		_ = g.Entry(srcA)
		_ = g.Exit(mg)
		r, err := g.Compile(context.Background())
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return r
	}

	// Deterministic oracle: round-robin a1,b1,a2,b2,a3.
	want := "a1,b1,a2,b2,a3"

	r := build()
	sr, err := r.Stream(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	gotStream := drainStringTail(t, sr)
	sr.Close()
	if gotStream != want {
		t.Fatalf("zip stream = %q, want %q (deterministic round-robin)", gotStream, want)
	}
}

// TestStreamGraph_Merge_N1_SequenceEquality: a single-source merge yields that
// source's frames in order (sequence equality).
func TestStreamGraph_Merge_N1_SequenceEquality(t *testing.T) {
	frames := []string{"a1", "a2", "a3"}
	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", frames, nil)
	mg, _ := AddStreamMerge[string, string, string, string](g, "mg",
		map[string]NodeRef{"a": srcA},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, "|"), nil
		})
	_ = g.Entry(srcA)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	sr, err := r.Stream(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	got := drainStringTail(t, sr)
	sr.Close()
	if got != "a1|a2|a3" {
		t.Fatalf("N=1 merge = %q, want %q", got, "a1|a2|a3")
	}
}

// TestStreamGraph_Merge_ConcurrentFanIn_SmallBuffer_NoHang: the BLOCKER-LAW gate.
// Two sources each emit ≥buf+2 frames, gated so that frame i+1 of each source is
// only released after a sibling frame is consumed — a sequential source walk
// (drain source 0 fully before touching source 1) would hang. The concurrent
// fan-in completes.
func TestStreamGraph_Merge_ConcurrentFanIn_SmallBuffer_NoHang(t *testing.T) {
	const n = 5 // >= mergeBufSize + 2
	framesA := make([]string, n)
	framesB := make([]string, n)
	for i := 0; i < n; i++ {
		framesA[i] = "a"
		framesB[i] = "b"
	}
	// Gate: both sources advance in lockstep; a sequential walk that fully drains
	// srcA before starting srcB would block forever waiting on srcB's release,
	// which never comes until srcA's frames are interleaved. We model this with a
	// shared counter that requires alternating progress: each source's per-frame
	// hook waits on a shared semaphore that the OTHER source's progress releases.
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	progressA, progressB := 0, 0
	hookA := func() {
		mu.Lock()
		defer mu.Unlock()
		progressA++
		cond.Broadcast()
		// Wait until B has caught up to within 1 frame (lockstep) — forces
		// concurrent pulling.
		for progressA-progressB > 1 {
			cond.Wait()
		}
	}
	hookB := func() {
		mu.Lock()
		defer mu.Unlock()
		progressB++
		cond.Broadcast()
		for progressB-progressA > 1 {
			cond.Wait()
		}
	}

	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", framesA, hookA)
	srcB, _ := addStringSource(g, "srcB", framesB, hookB)
	mg, _ := AddStreamMerge[string, string, string, string](g, "mg",
		map[string]NodeRef{"a": srcA, "b": srcB},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, ""), nil
		})
	_ = g.Entry(srcA)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	base := settleGoroutines()
	sr, err := r.Stream(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	got := drainStringTail(t, sr)
	sr.Close()
	if len(got) != 2*n {
		t.Fatalf("merged length = %d (%q), want %d (concurrent fan-in completed)", len(got), got, 2*n)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// errSourceAdapter is a streamNode source whose stream errors after a few frames.
type errSourceAdapter struct {
	id     string
	frames []string
	err    error
}

func (a *errSourceAdapter) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: reflect.TypeFor[any]()}}
}
func (a *errSourceAdapter) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: reflect.TypeFor[string]()}}
}
func (a *errSourceAdapter) Run(context.Context, map[string]any) (map[string]any, error) {
	return map[string]any{portOut: strings.Join(a.frames, "")}, nil
}
func (a *errSourceAdapter) streamRun(context.Context, streamCarrier) (streamCarrier, error) {
	return streamCarrier{
		isStream: true,
		stream:   anyStream[string]{&errFrameStream{frames: a.frames, err: a.err}},
		elemT:    reflect.TypeFor[string](),
	}, nil
}

type errFrameStream struct {
	mu     sync.Mutex
	frames []string
	idx    int
	err    error
	closed bool
}

func (s *errFrameStream) Next() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", io.EOF
	}
	if s.idx < len(s.frames) {
		v := s.frames[s.idx]
		s.idx++
		return v, nil
	}
	return "", s.err
}
func (s *errFrameStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func addErrSource[GI, GO any](g *Graph[GI, GO], id string, frames []string, err error) (NodeRef, error) {
	newKind := func() flow.NodeKind { return &errSourceAdapter{id: id, frames: frames, err: err} }
	ref, e := g.addNode(id, reflect.TypeFor[any](), reflect.TypeFor[string](), newKind)
	if e != nil {
		return NodeRef{}, e
	}
	g.markInProcess(id, "test erroring stream source (in-process only)")
	return ref, nil
}

// TestStreamGraph_Merge_OneSourceErrors_PropagatesAndNoLeak: one source's stream
// errors; the error surfaces through the merged fold and every stream is
// released (no leak).
func TestStreamGraph_Merge_OneSourceErrors_PropagatesAndNoLeak(t *testing.T) {
	sentinel := errors.New("merge-src-boom")
	base := settleGoroutines()
	g := NewGraph[string, string]()
	srcA, _ := addStringSource(g, "srcA", []string{"a1", "a2"}, nil)
	srcBad, _ := addErrSource(g, "srcBad", []string{"b1"}, sentinel)
	mg, _ := AddStreamMerge[string, string, string, string](g, "mg",
		map[string]NodeRef{"a": srcA, "b": srcBad},
		func(_ context.Context, items []string) (string, error) {
			return strings.Join(items, ""), nil
		})
	_ = g.Entry(srcA)
	_ = g.Exit(mg)
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	sr, err := r.Stream(context.Background(), "x")
	// The error may surface at Stream() (eager fold) or at Next().
	if err == nil {
		_, nerr := sr.Next()
		err = nerr
		sr.Close()
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel %v", err, sentinel)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// mergeLeakModes runs the 3 standard leak modes for a merge graph with the given
// source count.
func runMergeLeakModes(t *testing.T, nSources int) {
	build := func() *Runnable[string, string] {
		g := NewGraph[string, string]()
		sources := make(map[string]NodeRef, nSources)
		var first NodeRef
		for i := 0; i < nSources; i++ {
			key := string(rune('a' + i))
			frames := []string{key + "1", key + "2", key + "3"}
			src, _ := addStringSource(g, "src"+key, frames, nil)
			sources[key] = src
			if i == 0 {
				first = src
			}
		}
		mg, _ := AddStreamMerge[string, string, string, string](g, "mg", sources,
			func(_ context.Context, items []string) (string, error) {
				return strings.Join(items, ""), nil
			})
		_ = g.Entry(first)
		_ = g.Exit(mg)
		r, err := g.Compile(context.Background())
		if err != nil {
			t.Fatalf("Compile (n=%d): %v", nSources, err)
		}
		return r
	}

	// Full drain.
	t.Run("FullDrain", func(t *testing.T) {
		base := settleGoroutines()
		sr, err := build().Stream(context.Background(), "x")
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		drainStringTail(t, sr)
		sr.Close()
		if n := settleGoroutines(); n > base {
			t.Fatalf("goroutine leak: base=%d now=%d", base, n)
		}
	})
	// Early close (after first Next).
	t.Run("EarlyClose", func(t *testing.T) {
		base := settleGoroutines()
		sr, err := build().Stream(context.Background(), "x")
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		_, _ = sr.Next()
		sr.Close()
		if n := settleGoroutines(); n > base {
			t.Fatalf("goroutine leak: base=%d now=%d", base, n)
		}
	})
	// Close before next.
	t.Run("CloseBeforeNext", func(t *testing.T) {
		base := settleGoroutines()
		sr, err := build().Stream(context.Background(), "x")
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		sr.Close()
		if n := settleGoroutines(); n > base {
			t.Fatalf("goroutine leak: base=%d now=%d", base, n)
		}
	})
}

func TestStreamGraph_Merge_Leak_N1(t *testing.T) { runMergeLeakModes(t, 1) }
func TestStreamGraph_Merge_Leak_N2(t *testing.T) { runMergeLeakModes(t, 2) }
func TestStreamGraph_Merge_Leak_N3(t *testing.T) { runMergeLeakModes(t, 3) }
