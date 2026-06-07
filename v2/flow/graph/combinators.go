package graph

import (
	"context"
	"io"
	"sync"
)

// This file holds the standalone StreamReader[T] combinators: Copy (tee:
// one reader fanned out to N) and Merge (interleave: N readers folded to
// one). They operate on ALREADY-MATERIALISED stream values — e.g. the
// output of Runnable.Stream, or any StreamReader a node author holds — and
// are NOT wired as in-graph engine-scheduled nodes. Combinator *nodes* that
// stream from concurrent mid-graph branches need the partial-barrier
// scheduler rework and are out of scope here.
//
// Both reuse the pipedReader leak-discipline template documented in
// stream.go: a producer goroutine that does `defer src.Close()`, a
// two-stage Close (cancel the producer context, then drain the internal
// channel so the producer's send never blocks), and a Next that selects on
// {item, ctx.Done()} and surfaces io.EOF on a clean end. NO goroutine may
// block forever once its consumer stops and Closes.

// copyBufSize is the bounded per-consumer buffer for Copy. Small and fixed:
// the broadcaster blocks on the slowest consumer, so a slow/abandoned
// consumer slows ALL siblings until it Closes (bounded shared-rate
// backpressure). This is deliberate — an unbounded per-consumer buffer is
// an OOM/leak footgun. A consumer that stops reading MUST Close so the
// broadcaster's send to it drains and its siblings can make progress.
const copyBufSize = 1

// mergeBufSize is the bounded shared buffer for Merge's pullers.
const mergeBufSize = 1

// copyItem carries one broadcast element plus the terminal error. When err
// is non-nil it is a terminal frame (no value) delivered to every consumer
// after the last value; consumers surface err on the Next that reads it and
// io.EOF thereafter.
type copyItem[T any] struct {
	v   T
	err error
}

// Copy tees src into n independent StreamReaders, each yielding every
// element of src in order followed by the same terminal error (a non-EOF
// src error is delivered to every consumer, then io.EOF on the next read).
//
// A single broadcaster goroutine pulls src and fans each frame out to n
// bounded per-consumer channels (bounded shared-rate backpressure: see
// copyBufSize). The broadcaster does `defer src.Close()`. Each consumer's
// Close is idempotent, marks its branch done, and DRAINS its own channel so
// the broadcaster's send to it can never block forever; when ALL n
// consumers have Closed, the broadcaster's context is cancelled so it exits
// and releases src. The broadcaster's send selects on {consumerCh,
// ctx.Done()} so a global cancel always unblocks it.
//
// n <= 0 returns nil (no consumers, src is not pulled and the caller still
// owns it). n == 1 is a valid single tee.
func Copy[T any](src StreamReader[T], n int) []StreamReader[T] {
	if n <= 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	shared := &copyShared{cancel: cancel, remaining: n}

	readers := make([]StreamReader[T], n)
	chans := make([]chan copyItem[T], n)
	for i := range readers {
		ch := make(chan copyItem[T], copyBufSize)
		chans[i] = ch
		readers[i] = &copyReader[T]{ch: ch, ctx: ctx, shared: shared}
	}

	go broadcastCopy(ctx, src, chans)
	return readers
}

// copyShared is the shared close-bookkeeping for one Copy fan-out: cancel
// stops the broadcaster, remaining counts consumers not yet Closed. When it
// reaches zero the broadcaster is cancelled.
type copyShared struct {
	mu        sync.Mutex
	cancel    context.CancelFunc
	remaining int
}

func (s *copyShared) consumerClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining > 0 {
		s.remaining--
		if s.remaining == 0 {
			s.cancel()
		}
	}
}

// broadcastCopy is the single producer goroutine: it pulls src and fans
// each frame to every consumer channel, then closes the channels so each
// consumer's Next surfaces io.EOF. `defer src.Close()` releases the source
// when the broadcaster exits (clean end, src error, or global cancel).
func broadcastCopy[T any](ctx context.Context, src StreamReader[T], chans []chan copyItem[T]) {
	defer func() {
		for _, ch := range chans {
			close(ch)
		}
	}()
	defer src.Close()

	for {
		v, err := src.Next()
		frame := copyItem[T]{v: v, err: err}
		for _, ch := range chans {
			select {
			case ch <- frame:
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			// Terminal frame delivered to all; broadcaster is done.
			return
		}
	}
}

// copyReader is one tee consumer. It drains its own channel on Next and,
// once a terminal frame (err != nil, including io.EOF) is seen, returns
// io.EOF for all subsequent Next calls.
type copyReader[T any] struct {
	mu     sync.Mutex
	ch     chan copyItem[T]
	ctx    context.Context
	shared *copyShared
	closed bool
	done   bool // terminal frame already surfaced
}

func (r *copyReader[T]) Next() (T, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	if r.closed || r.done {
		return zero, io.EOF
	}
	frame, ok := <-r.ch
	if !ok {
		// Broadcaster closed the channel without a terminal frame reaching
		// us (e.g. global cancel) — treat as clean end.
		r.done = true
		return zero, io.EOF
	}
	if frame.err != nil {
		r.done = true
		return zero, frame.err
	}
	return frame.v, nil
}

func (r *copyReader[T]) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	// Tell the fan-out one consumer is gone (cancels the broadcaster once
	// all consumers have Closed).
	r.shared.consumerClosed()

	// Stage 2: drain our own channel so the broadcaster's send to us can
	// never block forever. The broadcaster either keeps sending until it
	// observes the global cancel (when all consumers Closed) or until it
	// exits and closes the channel; either way this drain terminates.
	go func() {
		for range r.ch {
		}
	}()
	return nil
}

// Merge interleaves srcs into a single StreamReader yielding elements from
// all sources as they arrive (unordered); it returns a clean io.EOF only
// after EVERY source has hit io.EOF. A non-EOF error from any source is
// surfaced through Next; Close still releases all sources.
//
// One puller goroutine per source pulls into a SHARED bounded channel; Next
// reads that channel. Each puller does `defer src.Close()`. Merge's Close
// cancels the shared context (stage 1) then drains the shared channel
// (stage 2) so every puller's send unblocks and its defer releases its
// source. Close is idempotent. When all pullers finish (all sources EOF),
// the shared channel is closed so Next returns io.EOF.
//
// Zero srcs returns an immediately-EOF reader.
func Merge[T any](srcs ...StreamReader[T]) StreamReader[T] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan mergeItem[T], mergeBufSize)
	m := &mergeReader[T]{ch: ch, ctx: ctx, cancel: cancel}

	var wg sync.WaitGroup
	wg.Add(len(srcs))
	for _, src := range srcs {
		go pullMerge(ctx, src, ch, &wg)
	}
	// Closer goroutine: once every puller has exited, close the shared
	// channel so Next surfaces io.EOF on a clean drain.
	go func() {
		wg.Wait()
		close(ch)
	}()
	return m
}

type mergeItem[T any] struct {
	v   T
	err error
}

// pullMerge pulls one source into the shared channel until the source ends
// (io.EOF, an error, or the shared context is cancelled). It forwards a
// non-EOF error as a terminal frame, then returns. `defer src.Close()`
// releases the source on every exit path.
func pullMerge[T any](ctx context.Context, src StreamReader[T], ch chan<- mergeItem[T], wg *sync.WaitGroup) {
	defer wg.Done()
	defer src.Close()
	for {
		v, err := src.Next()
		if err != nil {
			if err == io.EOF {
				return
			}
			// Forward the non-EOF error, then this source is done.
			select {
			case ch <- mergeItem[T]{err: err}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case ch <- mergeItem[T]{v: v}:
		case <-ctx.Done():
			return
		}
	}
}

type mergeReader[T any] struct {
	mu     sync.Mutex
	ch     chan mergeItem[T]
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
}

func (m *mergeReader[T]) Next() (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var zero T
	if m.closed {
		return zero, io.EOF
	}
	frame, ok := <-m.ch
	if !ok {
		// All pullers finished and the closer closed the channel: clean EOF.
		return zero, io.EOF
	}
	if frame.err != nil {
		return zero, frame.err
	}
	return frame.v, nil
}

func (m *mergeReader[T]) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	// Stage 1: signal all pullers to stop.
	m.cancel()
	// Stage 2: drain the shared channel so any puller mid-send unblocks; its
	// `defer src.Close()` then releases the source. The closer goroutine
	// closes ch once all pullers exit, ending this drain.
	go func() {
		for range m.ch {
		}
	}()
	return nil
}

// roundRobinMerge interleaves srcs into a single StreamReader with a
// DETERMINISTIC output order: source 0's next frame, source 1's next frame, …,
// repeating; a source at io.EOF is skipped; ragged lengths drain the remaining
// sources. It returns a clean io.EOF only after EVERY source has hit io.EOF. A
// non-EOF error from any source is surfaced through Next. Close releases all
// sources.
//
// Determinism with concurrency: ONE puller goroutine per source feeds a
// PER-SOURCE bounded channel (buf 1 — the lookahead); Next selects from those
// channels in fixed source-index order. Because the pullers run concurrently and
// each has a buf-1 lookahead, a source feeding off a shared upstream Copy never
// wedges the broadcaster while Next is blocked receiving from a sibling — the
// round-2 R-ZIPDEADLOCK mitigation. Output order is fixed by source index +
// frame index, independent of arrival timing, so the merged sequence is
// reproducible (the zip cross-validator).
//
// Zero srcs returns an immediately-EOF reader.
func roundRobinMerge[T any](srcs ...StreamReader[T]) StreamReader[T] {
	ctx, cancel := context.WithCancel(context.Background())
	chans := make([]chan mergeItem[T], len(srcs))
	for i, src := range srcs {
		ch := make(chan mergeItem[T], mergeBufSize)
		chans[i] = ch
		// One puller per source into its OWN channel; the puller closes its
		// channel on exit so Next detects an exhausted source as a closed
		// channel and skips it.
		go pullRoundRobin(ctx, src, ch)
	}
	return &roundRobinReader[T]{chans: chans, ctx: ctx, cancel: cancel, exhausted: make([]bool, len(srcs))}
}

// pullRoundRobin pulls one source into its OWN per-source channel until the
// source ends (io.EOF, an error, or the shared context is cancelled), then
// closes the channel so the reader's Next sees the source is done. It forwards a
// non-EOF error as a terminal frame, then closes. `defer src.Close()` releases
// the source on every exit path; `defer close(ch)` lets Next skip the exhausted
// source. It is the per-source analogue of pullMerge (which shares one channel
// closed by a separate closer after a WaitGroup).
func pullRoundRobin[T any](ctx context.Context, src StreamReader[T], ch chan mergeItem[T]) {
	defer close(ch)
	defer src.Close()
	for {
		v, err := src.Next()
		if err != nil {
			if err == io.EOF {
				return
			}
			select {
			case ch <- mergeItem[T]{err: err}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case ch <- mergeItem[T]{v: v}:
		case <-ctx.Done():
			return
		}
	}
}

// roundRobinReader is the roundRobinMerge reader. It selects frames from its
// per-source channels in fixed index order, skipping exhausted (closed-channel)
// sources, and applies the Phase-2 two-stage Close.
type roundRobinReader[T any] struct {
	mu        sync.Mutex
	chans     []chan mergeItem[T]
	ctx       context.Context
	cancel    context.CancelFunc
	cur       int    // next source index to try (round-robin cursor)
	exhausted []bool // exhausted[i] == true once source i's channel is closed
	closed    bool
}

func (m *roundRobinReader[T]) Next() (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var zero T
	if m.closed {
		return zero, io.EOF
	}
	n := len(m.chans)
	if n == 0 {
		return zero, io.EOF
	}
	// Round-robin over the sources: advance cur to the next non-exhausted
	// source and receive its next frame. A closed channel marks that source
	// exhausted; when ALL are exhausted, return io.EOF.
	for {
		// Find the next non-exhausted source starting at cur.
		picked := -1
		for off := 0; off < n; off++ {
			i := (m.cur + off) % n
			if !m.exhausted[i] {
				picked = i
				break
			}
		}
		if picked < 0 {
			return zero, io.EOF // every source exhausted
		}
		frame, ok := <-m.chans[picked]
		if !ok {
			// Source done (puller exited, channel closed). Skip it and keep the
			// cursor where it is so the next non-exhausted source is tried.
			m.exhausted[picked] = true
			m.cur = picked
			continue
		}
		// Advance the cursor PAST the picked source so the next Next tries the
		// following source (true round-robin).
		m.cur = (picked + 1) % n
		if frame.err != nil {
			return zero, frame.err
		}
		return frame.v, nil
	}
}

func (m *roundRobinReader[T]) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	// Stage 1: signal all pullers to stop.
	m.cancel()
	// Stage 2: drain EVERY per-source channel so any puller mid-send unblocks;
	// its `defer src.Close()` then releases the source. Each per-source closer
	// closes its channel once its puller exits, ending each drain. This is the
	// round-2 leak gate (the prototype leaked until these stage-2 drains existed).
	for _, ch := range m.chans {
		go func(ch chan mergeItem[T]) {
			for range ch {
			}
		}(ch)
	}
	return nil
}
