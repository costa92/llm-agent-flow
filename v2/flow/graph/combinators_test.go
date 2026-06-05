package graph

import (
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
)

// sliceStream is a test StreamReader[T] that yields a fixed slice then
// io.EOF, optionally terminating with a non-EOF error in place of EOF. It
// records how many times Close was called (under a mutex, since Copy/Merge
// pull and Close from a producer goroutine concurrently with the test).
type sliceStream[T any] struct {
	mu         sync.Mutex
	items      []T
	idx        int
	err        error // non-nil: returned (instead of io.EOF) once items exhausted
	closeCount int
}

func newSliceStream[T any](items ...T) *sliceStream[T] {
	return &sliceStream[T]{items: items}
}

func newErrSliceStream[T any](err error, items ...T) *sliceStream[T] {
	return &sliceStream[T]{items: items, err: err}
}

func (s *sliceStream[T]) Next() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var zero T
	if s.idx < len(s.items) {
		v := s.items[s.idx]
		s.idx++
		return v, nil
	}
	if s.err != nil {
		return zero, s.err
	}
	return zero, io.EOF
}

func (s *sliceStream[T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return nil
}

func (s *sliceStream[T]) closes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

// drainInts reads a StreamReader[int] to io.EOF, returning the values and
// the terminal error (nil if it was a clean io.EOF).
func drainInts(t *testing.T, sr StreamReader[int]) ([]int, error) {
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
		got = append(got, v)
	}
}

func TestCopy_AllConsumersSeeAllElements(t *testing.T) {
	base := settleGoroutines()
	src := newSliceStream(1, 2, 3)
	readers := Copy[int](src, 2)
	if len(readers) != 2 {
		t.Fatalf("Copy returned %d readers, want 2", len(readers))
	}

	// Bounded shared-rate backpressure: the broadcaster blocks on the
	// slowest consumer, so consumers must be drained CONCURRENTLY (draining
	// one fully before the other would stall the broadcaster). Each goroutine
	// drains its own reader and checks it saw all 3 in order.
	want := []int{1, 2, 3}
	results := make([][]int, len(readers))
	var wg sync.WaitGroup
	for i, r := range readers {
		wg.Add(1)
		go func(i int, r StreamReader[int]) {
			defer wg.Done()
			got, err := drainInts(t, r)
			if err != nil {
				t.Errorf("reader %d drain err: %v", i, err)
				return
			}
			results[i] = got
			if _, err := r.Next(); !errors.Is(err, io.EOF) {
				t.Errorf("reader %d post-drain Next = %v, want io.EOF", i, err)
			}
		}(i, r)
	}
	wg.Wait()
	for i, got := range results {
		if len(got) != len(want) {
			t.Fatalf("reader %d got %v, want %v", i, got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("reader %d got %v, want %v (in order)", i, got, want)
			}
		}
	}
	for _, r := range readers {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

func TestCopy_CloseAllReleasesSource_NoLeak(t *testing.T) {
	base := settleGoroutines()
	src := newSliceStream(1, 2, 3)
	readers := Copy[int](src, 3)
	// Drain concurrently (bounded shared-rate: the broadcaster blocks on the
	// slowest consumer).
	var wg sync.WaitGroup
	for _, r := range readers {
		wg.Add(1)
		go func(r StreamReader[int]) {
			defer wg.Done()
			if _, err := drainInts(t, r); err != nil {
				t.Errorf("drain: %v", err)
			}
		}(r)
	}
	wg.Wait()
	for _, r := range readers {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if c := src.closes(); c != 1 {
		t.Fatalf("src.Close called %d times, want 1", c)
	}
}

func TestCopy_EarlyConsumerClose_NoLeak(t *testing.T) {
	base := settleGoroutines()
	// Many elements so the broadcaster is mid-stream when one consumer
	// abandons without draining.
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	src := newSliceStream(items...)
	readers := Copy[int](src, 2)

	// Reader 0 abandons early WITHOUT draining.
	if err := readers[0].Close(); err != nil {
		t.Fatalf("early Close: %v", err)
	}
	// Reader 1 still drains cleanly — the broadcaster must not stall on the
	// abandoned reader[0].
	got, err := drainInts(t, readers[1])
	if err != nil {
		t.Fatalf("reader 1 drain: %v", err)
	}
	if len(got) != len(items) {
		t.Fatalf("reader 1 got %d elements, want %d", len(got), len(items))
	}
	if err := readers[1].Close(); err != nil {
		t.Fatalf("Close reader 1: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if c := src.closes(); c != 1 {
		t.Fatalf("src.Close called %d times, want 1", c)
	}
}

func TestCopy_SourceError_PropagatesToAll(t *testing.T) {
	base := settleGoroutines()
	sentinel := errors.New("boom")
	src := newErrSliceStream(sentinel, 1, 2)
	readers := Copy[int](src, 2)
	// Drain concurrently; each consumer must see the 2 values then the
	// sentinel error.
	var wg sync.WaitGroup
	for i, r := range readers {
		wg.Add(1)
		go func(i int, r StreamReader[int]) {
			defer wg.Done()
			got, err := drainInts(t, r)
			if !errors.Is(err, sentinel) {
				t.Errorf("reader %d terminal err = %v, want %v", i, err, sentinel)
			}
			if len(got) != 2 {
				t.Errorf("reader %d got %d elements before err, want 2", i, len(got))
			}
		}(i, r)
	}
	wg.Wait()
	for _, r := range readers {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

func TestCopy_ZeroOrNegative(t *testing.T) {
	src := newSliceStream(1, 2, 3)
	if r := Copy[int](src, 0); len(r) != 0 {
		t.Fatalf("Copy(src,0) = %d readers, want 0", len(r))
	}
	if r := Copy[int](src, -1); len(r) != 0 {
		t.Fatalf("Copy(src,-1) = %d readers, want 0", len(r))
	}
}

func TestMerge_InterleavesAllSources(t *testing.T) {
	base := settleGoroutines()
	a := newSliceStream(1, 2, 3)
	b := newSliceStream(4, 5)
	c := newSliceStream(6, 7, 8, 9)
	m := Merge[int](a, b, c)

	got, err := drainInts(t, m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	sort.Ints(got)
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged multiset = %v, want %v", got, want)
		}
	}
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("post-drain Next = %v, want io.EOF", err)
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

func TestMerge_CloseMidStream_NoLeak(t *testing.T) {
	base := settleGoroutines()
	mk := func() *sliceStream[int] {
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		return newSliceStream(items...)
	}
	a, b, c := mk(), mk(), mk()
	m := Merge[int](a, b, c)

	// Pull a couple then Close mid-stream without draining.
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

func TestMerge_SourceErrorPropagates(t *testing.T) {
	base := settleGoroutines()
	sentinel := errors.New("merge-boom")
	a := newSliceStream(1, 2)
	bad := newErrSliceStream(sentinel, 3)
	c := newSliceStream(4, 5)
	m := Merge[int](a, bad, c)

	// Drain until the sentinel surfaces. Other elements may interleave
	// before it; we only require that the error eventually surfaces and the
	// other sources are still released on Close.
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
		t.Fatalf("sentinel error never surfaced through Next")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
	if a.closes() != 1 || bad.closes() != 1 || c.closes() != 1 {
		t.Fatalf("source closes a=%d bad=%d c=%d, want all 1", a.closes(), bad.closes(), c.closes())
	}
}

func TestMerge_ZeroSources_EOF(t *testing.T) {
	base := settleGoroutines()
	m := Merge[int]()
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Merge() Next = %v, want io.EOF", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}
