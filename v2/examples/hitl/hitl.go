// Package hitl is the v2 human-in-the-loop demo: a pre -> gate -> post
// flow where the gate node interrupts the run for human approval, the
// engine checkpoints, and a later Resume injects the human decision to
// drive the run to completion.
//
// Like the v0.1 examples it is a library package with no main; the bundled
// example_test.go runs the flow and asserts the suspend point and the
// resumed output. The whole demo runs against an in-memory CheckpointStore
// so it needs no external state.
package hitl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// node-type strings registered for this flow.
const (
	typePassthrough = "passthrough"
	typeApproval    = "approval"
)

// passthroughNode forwards its "in" port to its "out" port unchanged. It
// is the plain transport node for the pre/post stages.
type passthroughNode struct{}

func (passthroughNode) Inputs() []flow.Port  { return []flow.Port{{Name: "in"}} }
func (passthroughNode) Outputs() []flow.Port { return []flow.Port{{Name: "decision"}} }

func (passthroughNode) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	v, ok := in["in"]
	if !ok {
		return nil, fmt.Errorf("hitl: passthrough: missing input port %q", "in")
	}
	return map[string]any{"decision": v}, nil
}

// postNode is the terminal passthrough; it forwards "in" to "out".
type postNode struct{}

func (postNode) Inputs() []flow.Port  { return []flow.Port{{Name: "in"}} }
func (postNode) Outputs() []flow.Port { return []flow.Port{{Name: "out"}} }

func (postNode) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	v, ok := in["in"]
	if !ok {
		return nil, fmt.Errorf("hitl: post: missing input port %q", "in")
	}
	return map[string]any{"out": v}, nil
}

// approvalNode is the interrupt-capable gate. Its Run always returns
// Interrupt(...), so the engine suspends the run there; on Resume the
// engine injects the human input as this node's "decision" output port
// (Run is never re-executed).
type approvalNode struct{}

func (approvalNode) Inputs() []flow.Port  { return []flow.Port{{Name: "in"}} }
func (approvalNode) Outputs() []flow.Port { return []flow.Port{{Name: "decision"}} }
func (approvalNode) CanInterrupt() bool   { return true }

func (approvalNode) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	return nil, flow.Interrupt(flow.InterruptRequest{
		Kind:   "approval",
		Prompt: "approve this action?",
	})
}

var _ flow.InterruptCapable = approvalNode{}

// Flow holds the compiled engine + its shared checkpoint store so a Start
// (which suspends) and a later Resume operate against the same run state.
type Flow struct {
	engine *flow.Engine
}

// New compiles the pre -> gate -> post flow against an in-memory
// checkpoint store and returns a ready-to-run Flow. The edge
// gate.decision -> post.in carries the human decision once Resume injects
// it.
func New() (*Flow, error) {
	reg := flow.NewNodeRegistry()
	if err := reg.Register(typePassthrough, func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
		return passthroughNode{}, nil
	}); err != nil {
		return nil, fmt.Errorf("hitl: register passthrough: %w", err)
	}
	if err := reg.Register("post", func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
		return postNode{}, nil
	}); err != nil {
		return nil, fmt.Errorf("hitl: register post: %w", err)
	}
	if err := reg.Register(typeApproval, func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
		return approvalNode{}, nil
	}); err != nil {
		return nil, fmt.Errorf("hitl: register approval: %w", err)
	}

	f := flow.Flow{
		ID: "hitl",
		Nodes: []flow.Node{
			{ID: "pre", Type: typePassthrough},
			{ID: "gate", Type: typeApproval},
			{ID: "post", Type: "post"},
		},
		Edges: []flow.Edge{
			{Source: flow.PortRef{Node: "pre", Port: "decision"}, Target: flow.PortRef{Node: "gate", Port: "in"}},
			{Source: flow.PortRef{Node: "gate", Port: "decision"}, Target: flow.PortRef{Node: "post", Port: "in"}},
		},
		Inputs:  []flow.NamedPortRef{{Name: "in", PortRef: flow.PortRef{Node: "pre", Port: "in"}}},
		Outputs: []flow.NamedPortRef{{Name: "out", PortRef: flow.PortRef{Node: "post", Port: "out"}}},
	}

	engine, err := flow.Compile(f, reg, flow.Deps{}, flow.WithCheckpointStore(flow.NewMemoryCheckpointStore()))
	if err != nil {
		return nil, fmt.Errorf("hitl: compile: %w", err)
	}
	return &Flow{engine: engine}, nil
}

// Start kicks off a fresh run. The gate node interrupts immediately, so
// Start returns the run's id plus the Suspension describing where it
// paused; outputs are nil until Resume.
func (fl *Flow) Start(ctx context.Context) (runID string, susp *flow.Suspension, err error) {
	runID = flow.NewRunID()
	res, err := fl.engine.RunResumable(ctx, runID, map[string]any{"in": "request"})
	if err != nil {
		return "", nil, fmt.Errorf("hitl: start: %w", err)
	}
	return runID, res.Suspended, nil
}

// Resume injects the human decision as the gate node's "decision" output
// port and drives the run to completion, returning the final outputs.
func (fl *Flow) Resume(ctx context.Context, runID, token, decision string) (map[string]any, error) {
	res, err := fl.engine.Resume(ctx, runID, token, map[string]any{"decision": decision})
	if err != nil {
		return nil, fmt.Errorf("hitl: resume: %w", err)
	}
	if res.Suspended != nil {
		return nil, fmt.Errorf("hitl: resume: run re-suspended at %q", res.Suspended.NodeID)
	}
	return res.Outputs, nil
}
