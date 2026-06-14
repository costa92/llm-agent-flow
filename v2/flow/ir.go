package flow

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// Flow is the top-level serializable shape of a v2 flow: a named DAG of
// nodes connected by edges, with explicit input and output port
// references.
//
// The v2 IR mirrors v0.1's structure but its port carrier is any (not
// string) — see node.go. The JSON shape is a superset of v0.1's: the
// optional Port.GoType metadata the typed front-end attaches is not
// serialized, so all v0.1 flow JSON loads here unchanged.
type Flow struct {
	ID          string         `json:"id"`
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
	Nodes       []Node         `json:"nodes"`
	Edges       []Edge         `json:"edges"`
	Mappings    []Mapping      `json:"mappings,omitempty"`
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
// against an environment containing `value` — the source port value
// projected to string (RV-5: the CEL/JSON route only routes on string
// values). An empty Condition (the default) means the edge always
// fires, preserving the v0.1 DAG behavior.
type Edge struct {
	Source    PortRef `json:"source"`
	Target    PortRef `json:"target"`
	Condition string  `json:"condition,omitempty"`
}

// Mapping copies a selected value from either a caller input or a node output
// port into a target node input port. When Target.Path is set, multiple
// mappings can assemble one structured input object without a glue node.
type Mapping struct {
	Source MappingSource `json:"source"`
	Target MappingTarget `json:"target"`
}

// MappingSource selects either an external Flow input (Input) or a node port
// (Node+Port). Path selects a nested field inside that source value.
type MappingSource struct {
	Input string   `json:"input,omitempty"`
	Node  string   `json:"node,omitempty"`
	Port  string   `json:"port,omitempty"`
	Path  []string `json:"path,omitempty"`
}

// MappingTarget names the destination node input port. Path, when non-empty,
// writes into a nested map rooted at that port.
type MappingTarget struct {
	Node string   `json:"node"`
	Port string   `json:"port"`
	Path []string `json:"path,omitempty"`
}

// PortRef names a (Node, Port) pair. Port is the port-name string
// declared by the resolved node type.
type PortRef struct {
	Node string `json:"node"`
	Port string `json:"port"`
}

// NamedPortRef pairs a PortRef with an external Name — the key used at
// Run() time to thread inputs in and outputs out.
type NamedPortRef struct {
	Name string `json:"name,omitempty"`
	PortRef
}

// Port is a static descriptor a Node type publishes about its inputs
// and outputs. Engine uses Ports to thread values between nodes at run
// time.
//
// GoType carries the declared Go reflect.Type set by the typed
// front-end; it is nil for the JSON/string route, which uses Schema
// instead. Both routes share this struct. GoType is not serialized —
// it is build-time metadata only (the JSON IR stays language-neutral).
type Port struct {
	Name   string       `json:"name"`
	Schema string       `json:"schema,omitempty"`
	GoType reflect.Type `json:"-"`
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

// Marshal serializes a Flow back to JSON, deterministically (indented,
// stable field order via encoding/json). Round-trip safe with Load.
func Marshal(f Flow) ([]byte, error) {
	return json.MarshalIndent(f, "", "  ")
}
