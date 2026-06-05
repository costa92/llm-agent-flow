package flow

import "context"

// Runner is the runtime contract every v2 flow executor satisfies. It
// abstracts Engine so decorators can compose over the engine without
// depending on its concrete type.
//
// Unlike v0.1's string-keyed Runner, the v2 Runner threads any-typed
// port maps. The string-keyed convenience entry (Engine.Run /
// Engine.RunStream string shim) is layered on top, not on this
// interface.
//
// ResumableRunner is intentionally NOT defined here — it lands with the
// checkpoint task.
type Runner interface {
	// Run executes the flow synchronously with the caller-supplied
	// inputs (keyed by Flow.Inputs[].Name) and returns the declared
	// outputs (keyed by Flow.Outputs[].Name).
	Run(ctx context.Context, inputs map[string]any) (map[string]any, error)

	// RunStream executes the flow asynchronously and emits Events
	// describing each node's lifecycle. See Event's documentation for
	// the ordering contract.
	RunStream(ctx context.Context, inputs map[string]any) (<-chan Event, error)
}

// Compile-time assertion that *Engine satisfies Runner.
var _ Runner = (*Engine)(nil)

// FlowID returns the id of the compiled flow.
func (e *Engine) FlowID() string { return e.flow.ID }

// FlowName returns the declared name of the compiled flow (may be empty).
func (e *Engine) FlowName() string { return e.flow.Name }
