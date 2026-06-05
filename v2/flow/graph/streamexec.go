package graph

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
