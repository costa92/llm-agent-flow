package flow

// EventKind enumerates the typed-union variants emitted by
// Runner.RunStream. Field population is gated by Kind (mirrors the
// llm.StreamEvent idiom: typed union with a stable per-node ID).
//
//	Kind = FlowStarted:  FlowID != ""
//	Kind = NodeStarted:  NodeID != "" ; Input != nil
//	Kind = NodeFinished: NodeID != "" ; Output != nil ; Err non-nil only on failure
//	Kind = NodeSkipped:  NodeID != ""   (no incoming edge fired)
//	Kind = FlowDone:     Outputs != nil
//	Kind = FlowErr:      Err != nil
//	Kind = NodeInterrupted: NodeID != "" ; Request != nil
//	Kind = FlowSuspended:   NodeID != "" ; ResumeToken != "" ; Request != nil
type EventKind uint8

const (
	FlowStarted     EventKind = iota // engine accepted the flow + inputs
	NodeStarted                      // a node is about to execute
	NodeFinished                     // a node finished (Output set, or Err non-nil on failure)
	NodeSkipped                      // no incoming edge fired; node elided
	FlowDone                         // terminal success; Outputs populated
	FlowErr                          // terminal failure; Err populated
	NodeInterrupted                  // a node requested a human interrupt
	FlowSuspended                    // terminal-for-this-leg: run suspended awaiting human input
)

// Event is the typed-union element of a Runner.RunStream channel. The
// any-carrier maps mirror the engine's port store.
type Event struct {
	Kind     EventKind
	FlowID   string
	NodeID   string
	Input    map[string]any    // current node's input port -> value
	Output   map[string]any    // current node's output port -> value
	Outputs  map[string]any    // terminal: declared flow outputs keyed by Name
	Metadata map[string]string // optional side-channel pairs (reserved)
	Err      error

	Request     *InterruptRequest // NodeInterrupted/FlowSuspended: the human-interaction payload
	ResumeToken string            // FlowSuspended: token to pass to Resume
}
