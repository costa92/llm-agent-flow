package graph

import "sync"

// streamexec.go holds shared data-stream execution helpers used by BOTH the
// linear executor (runLinearStream) and the branch-DAG executor
// (runStreamDAG). It NEVER touches the engine, runCore, checkpoints,
// EdgeFireState, or resume — the streaming path is reachable only via
// Runnable.Stream and is disjoint from Invoke by construction.

// liveCarrier tracks the single in-flight stream carrier a streaming walk is
// currently threading, so an error return BEFORE the tail is materialised can
// release it deterministically. There is at most one live stream at any point
// in either executor (the linear chain folds before re-streaming; the branch
// DAG walks one selected route), so a single slot suffices.
//
// Discipline:
//   - set(c) records the current carrier after each node step;
//   - clear() is called once the carrier has been HANDED OFF (folded into the
//     next node's input, or materialised into the returned tail reader) so the
//     deferred cleanup does not double-close an already-consumed stream;
//   - closeIfLive() (deferred under an error sentinel) closes the recorded
//     stream iff it is still live and unconsumed.
//
// Close is idempotent on every StreamReader the carriers hold, so a redundant
// close on a racing success path is a harmless no-op — but clear() keeps the
// common path from relying on that.
type liveCarrier struct {
	c    streamCarrier
	held bool
}

// set records carrier c as the current in-flight carrier.
func (l *liveCarrier) set(c streamCarrier) {
	l.c = c
	l.held = true
}

// clear marks the current carrier as handed off (folded or materialised into
// the tail), so a later closeIfLive is a no-op for it.
func (l *liveCarrier) clear() {
	l.held = false
	l.c = streamCarrier{}
}

// closeIfLive releases the recorded carrier's stream iff it is still held and
// is a live stream. Intended to run under a deferred error guard:
//
//	var live liveCarrier
//	defer func() {
//		if retErr != nil {
//			live.closeIfLive()
//		}
//	}()
//
// It is safe to call unconditionally because clear() zeroes the slot once the
// carrier is consumed, and Close is idempotent regardless.
func (l *liveCarrier) closeIfLive() {
	if l.held && l.c.isStream && l.c.stream != nil {
		_ = l.c.stream.Close()
	}
	l.clear()
}

// liveSet tracks EVERY in-flight stream a streaming walk currently owns, so an
// error return or early-Close before the tail is materialised can release them
// all. The single-goroutine executors (runStreamDAG, the linear chain) never
// hold more than one live stream at a time, but the Copy-bearing executor
// (runStreamGraph) fans out to N concurrent subtree goroutines, each with its
// own live stream PLUS every copyReader from the Phase-2 broadcaster — so a set,
// not a single slot, is required.
//
// Discipline (mirrors liveCarrier, generalised to N):
//   - register(c) records a stream after it is produced (streamRun output, a
//     copyReader, or a broadcaster source we materialised);
//   - unregister(c) drops it once HANDED OFF (folded into a downstream value, or
//     materialised into the returned tail) so closeAll does not double-close;
//   - closeAll() (under a deferred error/early-Close guard) Closes every stream
//     still registered. ORDER is irrelevant — the Phase-2 two-stage Close drains
//     each broadcaster/reader independently (round-2 verified leak-free in both
//     Close orders) — but COMPLETENESS is mandatory: every copyReader is
//     registered independently so a subtree that errors before consuming its
//     copyReader cannot wedge the broadcaster.
//
// Close is idempotent on every StreamReader, so a redundant close on a racing
// success path is a harmless no-op.
type liveSet struct {
	mu      sync.Mutex
	streams []StreamReader[any]
}

// register records s as an in-flight stream the walk now owns. A nil stream is
// ignored so callers can register a carrier's stream unconditionally.
func (l *liveSet) register(s StreamReader[any]) {
	if s == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.streams = append(l.streams, s)
}

// unregister drops s (handed off: folded or materialised into the tail) so a
// later closeAll is a no-op for it. Removal is by pointer identity.
func (l *liveSet) unregister(s StreamReader[any]) {
	if s == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, cur := range l.streams {
		if cur == s {
			l.streams = append(l.streams[:i], l.streams[i+1:]...)
			return
		}
	}
}

// closeAll Closes every stream still registered and empties the set. Safe to
// call more than once (idempotent Close + the set is drained). Intended under a
// deferred error guard, exactly like liveCarrier.closeIfLive.
func (l *liveSet) closeAll() {
	l.mu.Lock()
	streams := l.streams
	l.streams = nil
	l.mu.Unlock()
	for _, s := range streams {
		_ = s.Close()
	}
}
