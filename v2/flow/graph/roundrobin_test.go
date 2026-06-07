package graph

import (
	"errors"
	"io"
	"sync"
	"testing"
)

// blockingSliceStream is a sliceStream whose Next BLOCKS on a per-element gate
// until the corresponding gate channel is closed. It models a source feeding off
// a shared upstream Copy: source 0 can only advance once a frame is broadcast,
// which only happens once source 1's prior frame has been consumed. A
// serialize-pull merge (drain source 0 fully before touching source 1) would
// deadlock; a concurrent per-source puller does not.
type blockingSliceStream struct {
	mu         sync.Mutex
	items      []int
	idx        int
	gates      []chan struct{} // gates[i] must be closed before item i is returned
	closeCount int
}

func (s *blockingSliceStream) Next() (int, error) {
	s.mu.Lock()
	i := s.idx
	s.mu.Unlock()
	if i < len(s.items) {
		<-s.gates[i] // block until released
		s.mu.Lock()
		v := s.items[s.idx]
		s.idx++
		s.mu.Unlock()
		return v, nil
	}
	return 0, io.EOF
}

func (s *blockingSliceStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return nil
}

// anyOf erases a StreamReader[int] to a StreamReader[any] for roundRobinMerge,
// which operates on the erased carrier type the executor uses.
func anyOf(src StreamReader[int]) StreamReader[any] { return anyStream[int]{src} }

// drainAny reads a StreamReader[any] to io.EOF, asserting each frame to int.
func drainAny(t *testing.T, sr StreamReader[any]) ([]int, error) {
	t.Helper()
	var got []int
	for {
		v, err := sr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return got, nil
			}
			return got, err
		}
		got = append(got, v.(int))
	}
}

// TestRoundRobinMerge_DeterministicOrder: equal-length sources interleave in a
// FIXED sequence (s0[0], s1[0], s0[1], s1[1], …) over 100 runs — SEQUENCE
// equality, the determinism the zip cross-validator needs.
func TestRoundRobinMerge_DeterministicOrder(t *testing.T) {
	want := []int{10, 20, 11, 21, 12, 22}
	for run := 0; run < 100; run++ {
		a := newSliceStream(10, 11, 12)
		b := newSliceStream(20, 21, 22)
		m := roundRobinMerge[any](anyOf(a), anyOf(b))
		got, err := drainAny(t, m)
		if err != nil {
			t.Fatalf("run %d drain: %v", run, err)
		}
		if len(got) != len(want) {
			t.Fatalf("run %d got %v, want %v", run, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("run %d got %v, want %v (deterministic round-robin)", run, got, want)
			}
		}
		m.Close()
	}
}

// TestRoundRobinMerge_RaggedLengths: an exhausted source is skipped; the longer
// source's remaining frames drain in order. No hang on mismatch.
func TestRoundRobinMerge_RaggedLengths(t *testing.T) {
	a := newSliceStream(1, 2, 3, 4) // longer
	b := newSliceStream(5)          // shorter
	m := roundRobinMerge[any](anyOf(a), anyOf(b))
	got, err := drainAny(t, m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	// s0[0], s1[0], then s1 exhausted -> s0[1], s0[2], s0[3].
	want := []int{1, 5, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v (ragged drain skips exhausted source)", got, want)
		}
	}
	m.Close()
}

// TestRoundRobinMerge_ErrorPropagates: a non-EOF error from a source surfaces
// through Next.
func TestRoundRobinMerge_ErrorPropagates(t *testing.T) {
	sentinel := errors.New("rr-boom")
	a := newSliceStream(1, 2)
	bad := newErrSliceStream(sentinel, 3)
	m := roundRobinMerge[any](anyOf(a), anyOf(bad))
	var sawErr bool
	for {
		_, err := m.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, sentinel) {
				sawErr = true
				break
			}
			t.Fatalf("unexpected err: %v", err)
		}
	}
	if !sawErr {
		t.Fatalf("sentinel never surfaced through Next")
	}
	m.Close()
}

// TestRoundRobinMerge_NoSources_EOF: zero sources -> immediate io.EOF.
func TestRoundRobinMerge_NoSources_EOF(t *testing.T) {
	base := settleGoroutines()
	m := roundRobinMerge[any]()
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("roundRobinMerge() Next = %v, want io.EOF", err)
	}
	m.Close()
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// TestRoundRobinMerge_FullDrain_NoLeak: fully consume then Close; baseline
// restored and every source Closed exactly once.
func TestRoundRobinMerge_FullDrain_NoLeak(t *testing.T) {
	base := settleGoroutines()
	a := newSliceStream(1, 2, 3)
	b := newSliceStream(4, 5, 6)
	c := newSliceStream(7, 8, 9)
	m := roundRobinMerge[any](anyOf(a), anyOf(b), anyOf(c))
	if _, err := drainAny(t, m); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if a.closes() != 1 || b.closes() != 1 || c.closes() != 1 {
		t.Fatalf("source closes a=%d b=%d c=%d, want all 1", a.closes(), b.closes(), c.closes())
	}
}

// TestRoundRobinMerge_EarlyClose_NoLeak: Close mid-stream without draining (the
// round-2 leak gate — the prototype leaked 2 goroutines until the stage-2 drains
// were added). Every source must be Closed.
func TestRoundRobinMerge_EarlyClose_NoLeak(t *testing.T) {
	base := settleGoroutines()
	mk := func() *sliceStream[int] {
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		return newSliceStream(items...)
	}
	a, b, c := mk(), mk(), mk()
	m := roundRobinMerge[any](anyOf(a), anyOf(b), anyOf(c))
	if _, err := m.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v, want io.EOF", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if a.closes() != 1 || b.closes() != 1 || c.closes() != 1 {
		t.Fatalf("source closes a=%d b=%d c=%d, want all 1", a.closes(), b.closes(), c.closes())
	}
}

// TestRoundRobinMerge_CloseBeforeNext_NoLeak: Close before a single Next.
func TestRoundRobinMerge_CloseBeforeNext_NoLeak(t *testing.T) {
	base := settleGoroutines()
	a := newSliceStream(1, 2, 3)
	b := newSliceStream(4, 5, 6)
	m := roundRobinMerge[any](anyOf(a), anyOf(b))
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v, want io.EOF", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if a.closes() != 1 || b.closes() != 1 {
		t.Fatalf("source closes a=%d b=%d, want all 1", a.closes(), b.closes())
	}
}

// TestRoundRobinMerge_ConcurrentPull_NoDeadlock: two sources off a SHARED gate
// schedule that would deadlock a serialize-pull merge. Source 0's frame i is
// released only after source 1's frame i-1 has been consumed, and vice versa, so
// pulling source 0 to EOF before touching source 1 would wedge. The concurrent
// per-source pullers + buf-1 lookahead complete.
func TestRoundRobinMerge_ConcurrentPull_NoDeadlock(t *testing.T) {
	base := settleGoroutines()
	const n = 8
	itemsA := make([]int, n)
	itemsB := make([]int, n)
	gatesA := make([]chan struct{}, n)
	gatesB := make([]chan struct{}, n)
	for i := 0; i < n; i++ {
		itemsA[i] = 100 + i
		itemsB[i] = 200 + i
		gatesA[i] = make(chan struct{})
		gatesB[i] = make(chan struct{})
	}
	a := &blockingSliceStream{items: itemsA, gates: gatesA}
	b := &blockingSliceStream{items: itemsB, gates: gatesB}

	// Release frames in lockstep: a[0], b[0], a[1], b[1], … — exactly the
	// round-robin consumption order. This proves the reader consumes the
	// sources concurrently (it never gets ahead of the gate it just opened).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			close(gatesA[i])
			close(gatesB[i])
		}
	}()

	m := roundRobinMerge[any](anyOf(a), anyOf(b))
	got, err := drainAny(t, m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	<-done
	if len(got) != 2*n {
		t.Fatalf("got %d frames, want %d (concurrent pull completed)", len(got), 2*n)
	}
	m.Close()
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}
