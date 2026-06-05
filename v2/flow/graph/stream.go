package graph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/costa92/llm-agent-contract/llm"
)

// StreamReader is the graph-layer data-stream iterator. Next returns
// io.EOF (from package "io") when the stream ends cleanly; Close is
// idempotent and MUST be called by every consumer (typically via
// `defer sr.Close()`) to release the underlying source and prevent
// goroutine leaks. It mirrors llm.StreamReader's shape but is generic
// over the element type T so the typed graph layer can carry concrete
// values (llm.StreamEvent, string, …) through a linear pipeline.
type StreamReader[T any] interface {
	Next() (T, error)
	Close() error
}

// bridgeLLMStream wraps an llm.StreamReader as a graph StreamReader of
// llm.StreamEvent. It is a pure passthrough: Next and Close delegate
// 1:1 to the underlying reader with NO goroutine. The underlying
// reader's Close contract (idempotent, must-call) is inherited.
func bridgeLLMStream(sr llm.StreamReader) StreamReader[llm.StreamEvent] {
	return &llmStreamBridge{sr: sr}
}

type llmStreamBridge struct{ sr llm.StreamReader }

func (b *llmStreamBridge) Next() (llm.StreamEvent, error) { return b.sr.Next() }
func (b *llmStreamBridge) Close() error                   { return b.sr.Close() }

// box returns a single-element StreamReader yielding v once then io.EOF.
// It uses a mutex to guard the done/closed flags and has NO goroutine —
// it is the degrade path for non-streaming results (Invoke -> box -> O)
// and for value-tailed linear pipelines.
func box[T any](v T) StreamReader[T] {
	return &boxReader[T]{v: v}
}

type boxReader[T any] struct {
	mu     sync.Mutex
	v      T
	done   bool
	closed bool
}

func (b *boxReader[T]) Next() (T, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var zero T
	if b.closed || b.done {
		return zero, io.EOF
	}
	b.done = true
	return b.v, nil
}

func (b *boxReader[T]) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// streamCarrier is the value-or-stream union threaded between nodes by
// the LINEAR data-stream executor only. The Invoke / layered engine path
// never sees it — they always deal in plain any values.
//
// Checkpoint note: a stream in flight is NOT checkpoint-eligible (it is a
// live, single-pass, side-effecting iterator). Only the seams between
// nodes where isStream == false carry a materialised value that a future
// checkpoint mechanism could snapshot. This is documented intent only;
// task 5 implements no checkpointing.
type streamCarrier struct {
	isStream bool
	value    any                  // valid when isStream == false
	stream   StreamReader[any]    // valid when isStream == true
	elemT    reflect.Type         // element type of stream (for concatenator lookup)
}

// Concatenator folds a stream of T into a single value. The PRODUCED
// type may differ from the element type T (e.g. llm.StreamEvent ->
// llm.Response): the produced value is returned as any so the registry
// can hold heterogeneous folders keyed by the downstream target type.
type Concatenator[T any] func(StreamReader[T]) (any, error)

// concatRegistry maps a downstream target Go type (the type the folded
// value must satisfy) to a type-erased folder that consumes a
// StreamReader[any] and produces that target value.
//
// Selection is driven by the DOWNSTREAM PORT's declared In type (OQ-1),
// not merely the element type: a stream of llm.StreamEvent feeding a port
// declared In = llm.Response selects the Accumulate folder; the same
// stream feeding In = llm.StreamEvent would (in principle) select a
// passthrough. The element type is recorded on the carrier and validated
// at run time when the folder unwraps each item.
var concatRegistry = map[reflect.Type]func(StreamReader[any]) (any, error){}

var concatRegistryMu sync.RWMutex

// registerConcat installs a folder for target type targetT. Internal
// helper shared by the built-ins and the exported RegisterConcatenator.
func registerConcat(targetT reflect.Type, fn func(StreamReader[any]) (any, error)) {
	concatRegistryMu.Lock()
	defer concatRegistryMu.Unlock()
	concatRegistry[targetT] = fn
}

// RegisterConcatenator registers a folder that produces a value of type R
// from a stream of elements of type T. The folder is selected when a
// downstream node port declares its input type as R. Re-registering R
// replaces the prior folder.
func RegisterConcatenator[T, R any](fn Concatenator[T]) {
	targetT := reflect.TypeFor[R]()
	registerConcat(targetT, func(in StreamReader[any]) (any, error) {
		return fn(adaptStream[T](in))
	})
}

// lookupConcat returns the folder for downstream target type targetT.
func lookupConcat(targetT reflect.Type) (func(StreamReader[any]) (any, error), bool) {
	concatRegistryMu.RLock()
	defer concatRegistryMu.RUnlock()
	fn, ok := concatRegistry[targetT]
	return fn, ok
}

// adaptStream re-types a StreamReader[any] to a StreamReader[T] by
// asserting each item. Used by registered concatenators to recover the
// concrete element type from the erased carrier.
func adaptStream[T any](in StreamReader[any]) StreamReader[T] {
	return &typedStream[T]{in: in}
}

type typedStream[T any] struct{ in StreamReader[any] }

func (t *typedStream[T]) Next() (T, error) {
	var zero T
	v, err := t.in.Next()
	if err != nil {
		return zero, err
	}
	typed, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("graph: stream: element is %T, not %s", v, reflect.TypeFor[T]())
	}
	return typed, nil
}

func (t *typedStream[T]) Close() error { return t.in.Close() }

func init() {
	// Built-in: llm.StreamEvent stream -> llm.Response (accumulate). Note
	// the produced type (llm.Response) differs from the element type
	// (llm.StreamEvent); selection keys on the downstream In = llm.Response.
	RegisterConcatenator[llm.StreamEvent, llm.Response](func(sr StreamReader[llm.StreamEvent]) (any, error) {
		return llm.AccumulateStream(llmStreamFromGraph{sr})
	})
	// Built-in: string stream -> string (join). Element type == produced
	// type; selection keys on the downstream In = string.
	RegisterConcatenator[string, string](func(sr StreamReader[string]) (any, error) {
		defer sr.Close()
		var b []byte
		for {
			s, err := sr.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return string(b), nil
				}
				return string(b), err
			}
			b = append(b, s...)
		}
	})
}

// llmStreamFromGraph adapts a graph StreamReader[llm.StreamEvent] back to
// an llm.StreamReader so llm.AccumulateStream (which drains + closes it)
// can fold it. AccumulateStream's defer Close reaches the graph reader,
// which in turn closes the underlying llm.StreamReader.
type llmStreamFromGraph struct{ sr StreamReader[llm.StreamEvent] }

func (a llmStreamFromGraph) Next() (llm.StreamEvent, error) { return a.sr.Next() }
func (a llmStreamFromGraph) Close() error                   { return a.sr.Close() }

// concatToValue folds carrier's stream down to a single value targeting
// downstream type targetT, using the registered concatenator. It is
// called from the linear executor when an upstream stream meets a node
// that needs a materialised value. The folder drains AND closes the
// stream (per each folder's contract).
func concatToValue(c streamCarrier, targetT reflect.Type) (any, error) {
	fn, ok := lookupConcat(targetT)
	if !ok {
		return nil, fmt.Errorf("graph: stream: no Concatenator for %s", targetT)
	}
	return fn(c.stream)
}

// foldTail is the tail materialiser when a linear pipeline ends on a
// streamNode whose stream element type differs from the graph output O.
// It is a single-element StreamReader[O] that folds LAZILY: the first
// Next runs the concatenator (which drains + closes the underlying
// stream) and returns the folded O; Close before any Next closes the
// underlying stream directly, so an early break never leaks the source.
//
// Lazy (rather than fold-then-box at pipeline end) so that a consumer who
// Closes without ever reading does not pay the drain — and still releases
// the upstream.
func foldTail[O any](stream StreamReader[any], fold func(StreamReader[any]) (any, error)) StreamReader[O] {
	return &foldTailReader[O]{stream: stream, fold: fold}
}

type foldTailReader[O any] struct {
	mu     sync.Mutex
	stream StreamReader[any]
	fold   func(StreamReader[any]) (any, error)
	done   bool
	closed bool
}

func (f *foldTailReader[O]) Next() (O, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero O
	if f.closed || f.done {
		return zero, io.EOF
	}
	f.done = true
	v, err := f.fold(f.stream) // drains + closes the underlying stream
	if err != nil {
		return zero, err
	}
	typed, ok := v.(O)
	if !ok {
		return zero, fmt.Errorf("graph: stream: folded tail is %T, not %s", v, reflect.TypeFor[O]())
	}
	return typed, nil
}

func (f *foldTailReader[O]) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	if !f.done {
		// Folded yet? No — release the upstream directly so an early break
		// (Close before Next) does not leak the source.
		return f.stream.Close()
	}
	return nil
}

// pipedReader is a DOCUMENTED TEMPLATE for a future stream-through node
// (a node that transforms a stream lazily, emitting as it pulls). It is
// NOT used by the task-5 MVP: linear concatenate is synchronous and
// spawns zero producer goroutines. It is kept here so the eventual
// stream-through implementation has a vetted cancel + two-stage Close +
// `defer upstream.Close()` skeleton to copy.
//
// Shape (when implemented):
//   - a producer goroutine pulls from upstream, transforms, and sends on
//     an internal channel; it does `defer upstream.Close()` so the
//     upstream source is always released when the producer exits.
//   - Close cancels the producer's context (stage 1) then drains the
//     internal channel so the producer's send never blocks (stage 2),
//     making Close safe to call while the producer is mid-send.
//   - Next selects on {item, ctx.Done()} and surfaces io.EOF on close.
//
// The MVP omits all of this: concatenate runs inline on the consumer's
// goroutine, so there is no producer to leak.
type pipedReader[T any] struct {
	mu       sync.Mutex
	ch       chan T
	cancel   context.CancelFunc
	closed   bool
	upstream interface{ Close() error }
}

func (p *pipedReader[T]) Next() (T, error) {
	var zero T
	return zero, io.EOF // template only; never constructed in task 5
}

func (p *pipedReader[T]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	// stage 1: signal the producer to stop.
	if p.cancel != nil {
		p.cancel()
	}
	// stage 2: a real implementation would drain p.ch here so the
	// producer's pending send unblocks, then the producer's
	// `defer upstream.Close()` releases the source.
	return nil
}
