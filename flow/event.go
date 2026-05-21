package flow

// FlowEventKind enumerates the typed-union variants emitted by
// Engine.RunStream. Field population is gated by Kind (mirrors the
// llm.StreamEvent K1 idiom: typed union with stable per-node ID
// across deltas).
//
//	Kind = FlowStarted:  FlowID != ""
//	Kind = NodeStarted:  NodeID != "" ; Input != nil (post-decode)
//	Kind = NodeFinished: NodeID != "" ; Output != nil ; Err is non-nil only on failure
//	Kind = FlowDone:     Outputs != nil
//	Kind = FlowErr:      Err != nil
type FlowEventKind uint8

const (
	FlowStarted  FlowEventKind = iota // engine accepted the flow + inputs
	NodeStarted                       // a node is about to execute
	NodeFinished                      // a node finished (Output set, or Err non-nil on failure)
	FlowDone                          // terminal success; Outputs populated
	FlowErr                           // terminal failure; Err populated
)

// FlowEvent is the typed-union element of an Engine.RunStream channel.
type FlowEvent struct {
	Kind    FlowEventKind
	FlowID  string
	NodeID  string
	Input   map[string]string // current node's input port -> value
	Output  map[string]string // current node's output port -> value
	Outputs map[string]string // terminal: declared flow outputs keyed by Name
	Err     error
}
