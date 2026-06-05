package flow

import (
	"context"
	"encoding/json"
	"fmt"
)

// lambdaNode is a test-only stub NodeKind: it runs a user-supplied Go
// function over the any-typed input map. Enough to drive the
// conformance suite without any real component node. Registered via
// registerLambdas so a Flow IR can reference it by node ID.
type lambdaNode struct {
	inPorts  []Port
	outPorts []Port
	fn       func(ctx context.Context, in map[string]any) (map[string]any, error)
}

func (n *lambdaNode) Inputs() []Port  { return n.inPorts }
func (n *lambdaNode) Outputs() []Port { return n.outPorts }
func (n *lambdaNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	return n.fn(ctx, in)
}

// registerLambdas builds a NodeRegistry where each Flow.Node.Type is the
// node ID, resolving to the lambda registered under that ID in fns. The
// factory ignores Config — the behavior is the supplied closure.
func registerLambdas(fns map[string]*lambdaNode) *NodeRegistry {
	reg := NewNodeRegistry()
	for id, ln := range fns {
		ln := ln
		_ = reg.Register(id, func(cfg json.RawMessage, deps Deps) (NodeKind, error) {
			return ln, nil
		})
	}
	return reg
}

// passthrough makes a single-in single-out lambda that copies inPort ->
// outPort verbatim (any value preserved, no string assumption).
func passthrough(inPort, outPort string) *lambdaNode {
	return &lambdaNode{
		inPorts:  []Port{{Name: inPort}},
		outPorts: []Port{{Name: outPort}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			v, ok := in[inPort]
			if !ok {
				return nil, fmt.Errorf("passthrough: missing input %q", inPort)
			}
			return map[string]any{outPort: v}, nil
		},
	}
}

// staticEvaluator is a test ConditionEvaluator: each condition string
// is a literal "key==value" form compared against CondEnv.Value. Only
// the small set the conformance suite needs.
type staticEvaluator struct {
	calls *int // optional: counts Evaluate invocations
}

func (e staticEvaluator) Compile(expr string) (Condition, error) {
	return &eqCondition{expr: expr, calls: e.calls}, nil
}

// eqCondition fires when CondEnv.Value equals the want string. expr is
// of the form `==go` meaning "fire iff Value == \"go\"".
type eqCondition struct {
	expr  string
	calls *int
}

func (c *eqCondition) Evaluate(ctx context.Context, env CondEnv) (bool, error) {
	if c.calls != nil {
		*c.calls++
	}
	want := c.expr[len("=="):]
	return env.Value == want, nil
}
