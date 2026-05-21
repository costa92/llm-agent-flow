package flow

import "context"

// Runner is the runtime contract every flow executor satisfies. It
// abstracts Engine so decorators (e.g. github.com/costa92/llm-agent-otel/otelflow.Wrap)
// can compose over the engine without depending on its concrete type.
//
// *Engine satisfies Runner via its method set; downstream code that
// wants to wrap, mock, or substitute the engine targets this interface.
type Runner interface {
	// Run executes the flow synchronously with the caller-supplied
	// inputs (keyed by Flow.Inputs[].Name) and returns the declared
	// outputs (keyed by Flow.Outputs[].Name).
	Run(ctx context.Context, inputs map[string]string) (map[string]string, error)

	// RunStream executes the flow asynchronously and emits FlowEvents
	// describing each node's lifecycle. See FlowEvent's documentation
	// for the ordering contract.
	RunStream(ctx context.Context, inputs map[string]string) (<-chan FlowEvent, error)
}

// Compile-time assertion that *Engine satisfies Runner. Wrappers in
// other modules rely on this — keep it green.
var _ Runner = (*Engine)(nil)

// FlowID returns the id of the compiled flow. Useful as a span
// attribute or log field in wrappers.
func (e *Engine) FlowID() string { return e.flow.ID }

// FlowName returns the declared name of the compiled flow (may be
// empty).
func (e *Engine) FlowName() string { return e.flow.Name }
