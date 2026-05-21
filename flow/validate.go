package flow

import (
	"errors"
	"fmt"
)

// ErrEmptyFlow is returned by Validate when a Flow declares no nodes.
var ErrEmptyFlow = errors.New("flow: validate: no nodes")

// ValidateError aggregates per-issue diagnostics so the caller sees
// every fault in one pass rather than the first one only.
type ValidateError struct {
	Issues []string
}

func (e *ValidateError) Error() string {
	if len(e.Issues) == 1 {
		return "flow: validate: " + e.Issues[0]
	}
	return fmt.Sprintf("flow: validate: %d issues: %v", len(e.Issues), e.Issues)
}

// Validate runs all static checks against a parsed Flow. It does NOT
// resolve node types — that happens at Engine construction. The
// checks here only need the IR shape:
//
//   - non-empty node list
//   - unique node IDs
//   - non-empty node IDs and types
//   - no self-loop edges (Source.Node == Target.Node)
//   - every Edge references an existing node
//   - no duplicate edges (same source/target pair)
//   - no cycles in the directed node graph
func Validate(f Flow) error {
	if len(f.Nodes) == 0 {
		return ErrEmptyFlow
	}
	var issues []string

	ids := make(map[string]struct{}, len(f.Nodes))
	for i, n := range f.Nodes {
		if n.ID == "" {
			issues = append(issues, fmt.Sprintf("node[%d]: empty id", i))
			continue
		}
		if n.Type == "" {
			issues = append(issues, fmt.Sprintf("node[%q]: empty type", n.ID))
		}
		if _, dup := ids[n.ID]; dup {
			issues = append(issues, fmt.Sprintf("node[%q]: duplicate id", n.ID))
			continue
		}
		ids[n.ID] = struct{}{}
	}

	seenEdge := make(map[string]struct{}, len(f.Edges))
	for i, e := range f.Edges {
		if _, ok := ids[e.Source.Node]; !ok {
			issues = append(issues, fmt.Sprintf("edge[%d]: source node %q not found", i, e.Source.Node))
		}
		if _, ok := ids[e.Target.Node]; !ok {
			issues = append(issues, fmt.Sprintf("edge[%d]: target node %q not found", i, e.Target.Node))
		}
		if e.Source.Node == e.Target.Node {
			issues = append(issues, fmt.Sprintf("edge[%d]: self-loop on node %q", i, e.Source.Node))
		}
		key := e.Source.Node + "." + e.Source.Port + " -> " + e.Target.Node + "." + e.Target.Port
		if _, dup := seenEdge[key]; dup {
			issues = append(issues, fmt.Sprintf("edge[%d]: duplicate edge %s", i, key))
		}
		seenEdge[key] = struct{}{}
	}

	for i, ref := range f.Inputs {
		if _, ok := ids[ref.Node]; !ok {
			issues = append(issues, fmt.Sprintf("inputs[%d]: node %q not found", i, ref.Node))
		}
	}
	for i, ref := range f.Outputs {
		if _, ok := ids[ref.Node]; !ok {
			issues = append(issues, fmt.Sprintf("outputs[%d]: node %q not found", i, ref.Node))
		}
	}

	if cycle := findCycle(f); len(cycle) > 0 {
		issues = append(issues, fmt.Sprintf("cycle detected: %v", cycle))
	}

	if len(issues) > 0 {
		return &ValidateError{Issues: issues}
	}
	return nil
}

// findCycle walks the directed edge set with iterative DFS and
// returns one cycle (as the visited node-id sequence) if one exists.
// Returns nil when the graph is acyclic.
func findCycle(f Flow) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(f.Nodes))
	for _, n := range f.Nodes {
		color[n.ID] = white
	}
	out := make(map[string][]string, len(f.Nodes))
	for _, e := range f.Edges {
		out[e.Source.Node] = append(out[e.Source.Node], e.Target.Node)
	}
	var parent = make(map[string]string)
	var cycle []string
	var visit func(string, string) bool
	visit = func(u, from string) bool {
		color[u] = gray
		parent[u] = from
		for _, v := range out[u] {
			switch color[v] {
			case white:
				if visit(v, u) {
					return true
				}
			case gray:
				// reconstruct cycle v ← parents... ← v
				cur := u
				cycle = []string{v}
				for cur != v && cur != "" {
					cycle = append(cycle, cur)
					cur = parent[cur]
				}
				cycle = append(cycle, v)
				// reverse to print in traversal order
				for i, j := 0, len(cycle)-1; i < j; i, j = i+1, j-1 {
					cycle[i], cycle[j] = cycle[j], cycle[i]
				}
				return true
			}
		}
		color[u] = black
		return false
	}
	for _, n := range f.Nodes {
		if color[n.ID] == white {
			if visit(n.ID, "") {
				return cycle
			}
		}
	}
	return nil
}
