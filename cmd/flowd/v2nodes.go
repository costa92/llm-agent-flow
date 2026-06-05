package main

import (
	"context"
	"encoding/json"

	v2flow "github.com/costa92/llm-agent-flow/v2/flow"
)

// registerV2Nodes installs the MVP v2 node kinds into reg. These are the
// minimal set required by the flowd resumable (HITL) path — tool /
// component nodes are NOT ported yet (deferred).
func registerV2Nodes(reg *v2flow.NodeRegistry) error {
	if err := reg.Register("passthrough", func(_ json.RawMessage, _ v2flow.Deps) (v2flow.NodeKind, error) {
		return passthroughNode{}, nil
	}); err != nil {
		return err
	}
	if err := reg.Register("approval", func(_ json.RawMessage, _ v2flow.Deps) (v2flow.NodeKind, error) {
		return approvalNode{}, nil
	}); err != nil {
		return err
	}
	return nil
}

// passthroughNode copies its single "in" port to its single "out" port.
type passthroughNode struct{}

func (passthroughNode) Inputs() []v2flow.Port  { return []v2flow.Port{{Name: "in"}} }
func (passthroughNode) Outputs() []v2flow.Port { return []v2flow.Port{{Name: "out"}} }

func (passthroughNode) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	return map[string]any{"out": in["in"]}, nil
}

// approvalNode is a HITL gate: it always requests a human interrupt. On
// resume the engine injects humanInput["decision"] as this node's
// "decision" output port.
type approvalNode struct{}

func (approvalNode) Inputs() []v2flow.Port  { return []v2flow.Port{{Name: "in"}} }
func (approvalNode) Outputs() []v2flow.Port { return []v2flow.Port{{Name: "decision"}} }

func (approvalNode) CanInterrupt() bool { return true }

func (approvalNode) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	return nil, v2flow.Interrupt(v2flow.InterruptRequest{Kind: "approval", Prompt: "approve?"})
}
