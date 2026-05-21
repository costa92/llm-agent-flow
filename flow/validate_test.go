package flow

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateRejectsEmptyFlow(t *testing.T) {
	if err := Validate(Flow{}); !errors.Is(err, ErrEmptyFlow) {
		t.Fatalf("Validate(empty) = %v, want ErrEmptyFlow", err)
	}
}

func TestValidateDetectsCycle(t *testing.T) {
	f := Flow{
		Nodes: []Node{{ID: "a", Type: "tool"}, {ID: "b", Type: "tool"}},
		Edges: []Edge{
			{Source: PortRef{Node: "a", Port: "output"}, Target: PortRef{Node: "b", Port: "input"}},
			{Source: PortRef{Node: "b", Port: "output"}, Target: PortRef{Node: "a", Port: "input"}},
		},
	}
	err := Validate(f)
	if err == nil {
		t.Fatal("Validate(cycle) = nil, want error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Validate(cycle) = %v, want 'cycle' in message", err)
	}
}

func TestValidateRejectsSelfLoop(t *testing.T) {
	f := Flow{
		Nodes: []Node{{ID: "a", Type: "tool"}},
		Edges: []Edge{{Source: PortRef{Node: "a", Port: "output"}, Target: PortRef{Node: "a", Port: "input"}}},
	}
	err := Validate(f)
	if err == nil || !strings.Contains(err.Error(), "self-loop") {
		t.Fatalf("Validate(self-loop) = %v, want self-loop error", err)
	}
}

func TestValidateRejectsDanglingEdge(t *testing.T) {
	f := Flow{
		Nodes: []Node{{ID: "a", Type: "tool"}},
		Edges: []Edge{{Source: PortRef{Node: "a", Port: "output"}, Target: PortRef{Node: "missing", Port: "input"}}},
	}
	err := Validate(f)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Validate(dangling) = %v, want not-found error", err)
	}
}

func TestValidateRejectsDuplicateNodeID(t *testing.T) {
	f := Flow{Nodes: []Node{{ID: "a", Type: "tool"}, {ID: "a", Type: "tool"}}}
	err := Validate(f)
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Fatalf("Validate(dup id) = %v, want duplicate-id error", err)
	}
}

func TestValidateAcceptsLinearChain(t *testing.T) {
	f := Flow{
		Nodes: []Node{
			{ID: "a", Type: "tool"},
			{ID: "b", Type: "tool"},
			{ID: "c", Type: "tool"},
		},
		Edges: []Edge{
			{Source: PortRef{Node: "a", Port: "output"}, Target: PortRef{Node: "b", Port: "input"}},
			{Source: PortRef{Node: "b", Port: "output"}, Target: PortRef{Node: "c", Port: "input"}},
		},
	}
	if err := Validate(f); err != nil {
		t.Fatalf("Validate(linear) = %v, want nil", err)
	}
}
