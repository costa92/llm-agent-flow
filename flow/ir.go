package flow

import (
	"encoding/json"
	"fmt"
	"io"
)

// Flow is the top-level serializable shape: a named DAG of nodes
// connected by edges, with explicit input and output port references.
//
// The IR is intentionally minimal at v0:
//   - Inputs declare which node ports receive the caller-supplied
//     inputs at run time, keyed by name (Outputs do the same for the
//     final values).
//   - Each Node has a Type that resolves through a NodeRegistry; the
//     Config blob is decoded by the registered factory.
//   - Each Edge carries a SourceRef → TargetRef pair. Conditional
//     routing is reserved for a later version.
type Flow struct {
	ID          string  `json:"id"`
	Name        string  `json:"name,omitempty"`
	Description string  `json:"description,omitempty"`
	Nodes       []Node  `json:"nodes"`
	Edges       []Edge  `json:"edges"`
	Inputs      []NamedPortRef `json:"inputs,omitempty"`
	Outputs     []NamedPortRef `json:"outputs,omitempty"`
}

// Node is one vertex in the flow DAG. Type resolves through a
// NodeRegistry; Config is the type-specific JSON blob the registered
// factory consumes.
type Node struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Config json.RawMessage `json:"config,omitempty"`
}

// Edge connects one source port on one node to one target port on
// another. Source.Node == Target.Node is rejected by Validate.
//
// Condition (optional) is a guard expression evaluated at run time
// against an environment containing at least `value` (the source port
// value as string). When non-empty, the edge fires only if the
// expression evaluates to true; a false result skips the edge — its
// target node will NOT receive this value, and if no incoming edge
// fires the target is "skipped" (no Run, no outputs). An empty
// Condition (the default) means the edge always fires, preserving
// the v0.0.x DAG behavior.
//
// The expression syntax is determined by the ConditionEvaluator
// supplied at Compile time; the bundled flow/cond/cel package uses
// Google CEL.
type Edge struct {
	Source    PortRef `json:"source"`
	Target    PortRef `json:"target"`
	Condition string  `json:"condition,omitempty"`
}

// PortRef names a (Node, Port) pair. Port is the port-name string
// declared by the resolved node type.
type PortRef struct {
	Node string `json:"node"`
	Port string `json:"port"`
}

// NamedPortRef pairs a PortRef with an external Name — the key used
// at Run() time to thread inputs in and outputs out.
type NamedPortRef struct {
	Name string `json:"name,omitempty"`
	PortRef
}

// Port is a static descriptor a Node type publishes about its inputs
// and outputs. Engine uses Ports to validate edges and to thread
// values between nodes at run time.
type Port struct {
	Name string `json:"name"`
	// Type is a JSON-schema-style descriptor, narrowed at v0 to a
	// small set of well-known constants ("string", "json"). Reserved
	// for later expansion; Validate currently rejects only mismatched
	// non-empty Type pairs.
	Type string `json:"type,omitempty"`
}

// Load parses a Flow from r (JSON). It validates JSON syntax but not
// the graph itself — call Validate(flow) afterward.
func Load(r io.Reader) (Flow, error) {
	var f Flow
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return Flow{}, fmt.Errorf("flow: load: %w", err)
	}
	return f, nil
}

// Marshal serializes a Flow back to JSON. Round-trip safe with Load.
func Marshal(f Flow) ([]byte, error) {
	return json.MarshalIndent(f, "", "  ")
}
