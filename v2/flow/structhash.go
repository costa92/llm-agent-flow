package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// structuralHash computes a canonical, deterministic sha256 hex digest of a
// compiled flow's STRUCTURE — the part of the graph that, if changed, would
// corrupt a resume cursor. It deliberately EXCLUDES Node.Config: a Config
// change that does not alter the resolved port sets or the edge wiring is
// resume-safe, so two flows differing only by Config hash identically.
//
// The descriptor captures, per node (sorted by node ID):
//   - id, type, and the RESOLVED runtime node's sorted Inputs()/Outputs()
//     port names. Ports come from nodes[id].Inputs()/Outputs() (not the
//     static IR) so a Config-derived port-set change IS detected — this is
//     the safety-critical bit.
//
// And, per edge (in flow.Edges slice order, WITH index): the index, the
// source/target node+port, and the condition string. Edges keep slice order
// because the resume cursor (Checkpoint.EdgeStates) is indexed by position,
// so any add/remove/reorder of edges must change the hash.
//
// MAJOR-4(b): the graph State type S is intentionally NOT folded into this
// hash. On the only checkpointable path (the pure-engine route) S is a RUN
// option, not build-known, so it isn't available here; and the typed
// AddStatefulLambdaNode route is non-serializable (holds a Go closure) → never
// checkpointed, so folding S there would be dead protection. The State-type
// guard lives entirely in Checkpoint.StateCodec (the namespaced tag +
// shape fingerprint, compared on resume — see statecodec.go / resume.go).
func structuralHash(f Flow, nodes map[string]NodeKind) (string, error) {
	type portDesc = string // just the port name

	type nodeDesc struct {
		ID      string     `json:"id"`
		Type    string     `json:"type"`
		Inputs  []portDesc `json:"inputs"`
		Outputs []portDesc `json:"outputs"`
	}
	type edgeDesc struct {
		Index      int    `json:"index"`
		SourceNode string `json:"source_node"`
		SourcePort string `json:"source_port"`
		TargetNode string `json:"target_node"`
		TargetPort string `json:"target_port"`
		Condition  string `json:"condition"`
	}
	type mappingDesc struct {
		Index      int      `json:"index"`
		SourceIn   string   `json:"source_input,omitempty"`
		SourceNode string   `json:"source_node,omitempty"`
		SourcePort string   `json:"source_port,omitempty"`
		SourcePath []string `json:"source_path,omitempty"`
		TargetNode string   `json:"target_node"`
		TargetPort string   `json:"target_port"`
		TargetPath []string `json:"target_path,omitempty"`
	}
	type descriptor struct {
		Nodes    []nodeDesc    `json:"nodes"`
		Edges    []edgeDesc    `json:"edges"`
		Mappings []mappingDesc `json:"mappings,omitempty"`
	}

	portNames := func(ports []Port) []portDesc {
		names := make([]portDesc, 0, len(ports))
		for _, p := range ports {
			names = append(names, p.Name)
		}
		sort.Strings(names)
		return names
	}

	// Nodes sorted by ID for determinism.
	nodeIDs := make([]string, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		nodeIDs = append(nodeIDs, n.ID)
	}
	sort.Strings(nodeIDs)

	// Map IR node ID -> Type for the descriptor (resolved kind has no Type).
	typeByID := make(map[string]string, len(f.Nodes))
	for _, n := range f.Nodes {
		typeByID[n.ID] = n.Type
	}

	nds := make([]nodeDesc, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nk := nodes[id]
		if nk == nil {
			// Should not happen: Compile resolved every node before hashing.
			return "", fmt.Errorf("flow: structural hash: node %q not resolved", id)
		}
		nds = append(nds, nodeDesc{
			ID:      id,
			Type:    typeByID[id],
			Inputs:  portNames(nk.Inputs()),
			Outputs: portNames(nk.Outputs()),
		})
	}

	eds := make([]edgeDesc, 0, len(f.Edges))
	for i, e := range f.Edges {
		eds = append(eds, edgeDesc{
			Index:      i,
			SourceNode: e.Source.Node,
			SourcePort: e.Source.Port,
			TargetNode: e.Target.Node,
			TargetPort: e.Target.Port,
			Condition:  e.Condition,
		})
	}
	maps := make([]mappingDesc, 0, len(f.Mappings))
	for i, m := range f.Mappings {
		maps = append(maps, mappingDesc{
			Index:      i,
			SourceIn:   m.Source.Input,
			SourceNode: m.Source.Node,
			SourcePort: m.Source.Port,
			SourcePath: append([]string(nil), m.Source.Path...),
			TargetNode: m.Target.Node,
			TargetPort: m.Target.Port,
			TargetPath: append([]string(nil), m.Target.Path...),
		})
	}

	b, err := json.Marshal(descriptor{Nodes: nds, Edges: eds, Mappings: maps})
	if err != nil {
		return "", fmt.Errorf("flow: structural hash: marshal descriptor: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
