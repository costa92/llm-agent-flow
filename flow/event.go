package flow

// FlowEventKind enumerates the typed-union variants emitted by
// Engine.RunStream. Field population is gated by Kind (mirrors the
// llm.StreamEvent K1 idiom: typed union with stable per-node ID
// across deltas).
//
//	Kind = FlowStarted:  FlowID != ""
//	Kind = NodeStarted:  NodeID != "" ; Input != nil (post-decode)
//	Kind = NodeFinished: NodeID != "" ; Output != nil ; Err is non-nil only on failure
//	                     Metadata is populated when the node implements
//	                     MetadataAware (HTTP status, exec exit code,
//	                     token usage, etc.); nil otherwise.
//	Kind = NodeSkipped:  NodeID != ""   (no incoming edge fired)
//	Kind = FlowDone:     Outputs != nil
//	Kind = FlowErr:      Err != nil
type FlowEventKind uint8

const (
	FlowStarted  FlowEventKind = iota // engine accepted the flow + inputs
	NodeStarted                       // a node is about to execute
	NodeFinished                      // a node finished (Output set, or Err non-nil on failure)
	NodeSkipped                       // a conditional edge or upstream skip elided this node
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

	// Metadata carries optional side-channel key/value pairs about a
	// node's execution — e.g. HTTP status code, exec exit code, LLM
	// token usage. Populated only on NodeFinished events emitted by
	// nodes that implement MetadataAware (see node.go). Nil for nodes
	// that don't, and on every other Kind. Both success and error
	// paths preserve Metadata so failed runs still surface debugging
	// signals (e.g. HTTP 500 with a status code attached).
	Metadata map[string]string
}
