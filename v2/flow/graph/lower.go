package graph

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// builtinPassthrough is the globally registered node-type string for the
// serializable identity node. It is the only builtin type in this task.
const builtinPassthrough = "builtin.passthrough"

// NewBuiltinRegistry returns a fresh flow.NodeRegistry pre-populated with
// the builtin node types a serializable (lowered) graph references. Lower's
// flow.Flow IR recompiles against it.
func NewBuiltinRegistry() *flow.NodeRegistry {
	reg := flow.NewNodeRegistry()
	// The passthrough factory ignores cfg/deps and forwards any value; id is
	// only used in error messages, so the rebuilt-from-JSON form leaves it
	// empty and operates on any element type.
	_ = reg.Register(builtinPassthrough, func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
		return &passthroughAdapter{id: "", t: nil}, nil
	})
	return reg
}

// lower builds the serializable flow.Flow IR for g, or returns an error
// listing every node that is not serializable (node-level reasons). It is
// computed at Compile; the result is stored on the Runnable.
func (g *Graph[I, O]) lower() (flow.Flow, error) {
	var reasons []string
	for _, gn := range g.nodes {
		id := gn.ref.id
		if g.lowering[id] != nil {
			continue
		}
		reason := g.reasons[id]
		if reason == "" {
			reason = "not serializable"
		}
		reasons = append(reasons, fmt.Sprintf("%q: %s", id, reason))
	}
	if len(reasons) > 0 {
		return flow.Flow{}, fmt.Errorf("graph: not serializable: %s", strings.Join(reasons, "; "))
	}

	irNodes := make([]flow.Node, 0, len(g.nodes))
	for _, gn := range g.nodes {
		l := g.lowering[gn.ref.id]
		irNodes = append(irNodes, flow.Node{ID: gn.ref.id, Type: l.typ, Config: l.config})
	}

	irEdges := make([]flow.Edge, 0, len(g.edges))
	for _, e := range g.edges {
		irEdges = append(irEdges, flow.Edge{
			Source: flow.PortRef{Node: e.from, Port: e.fromPort},
			Target: flow.PortRef{Node: e.to, Port: e.toPort},
		})
	}

	return flow.Flow{
		ID:    "graph",
		Nodes: irNodes,
		Edges: irEdges,
		Inputs: []flow.NamedPortRef{{
			Name:    graphInputKey,
			PortRef: flow.PortRef{Node: g.entry.id, Port: portIn},
		}},
		Outputs: []flow.NamedPortRef{{
			Name:    graphOutputKey,
			PortRef: flow.PortRef{Node: g.exit.id, Port: portOut},
		}},
	}, nil
}
