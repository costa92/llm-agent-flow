package flow

import "context"

// ConditionEvaluator compiles edge guard expressions. The flow
// library does not bind any specific expression language — callers
// plug a concrete evaluator in via WithConditionEvaluator. The
// bundled flow/cond/cel package provides a Google CEL-backed
// implementation; users who prefer a different language (plain Go,
// regex, JSONata, ...) can implement this interface themselves.
//
// Implementations must be safe for concurrent use from multiple
// goroutines once returned from a constructor — Compile is called
// once per Edge.Condition at flow Compile time, and the returned
// Condition.Evaluate is called many times concurrently across runs.
type ConditionEvaluator interface {
	// Compile parses an expression and returns a runnable Condition.
	// It is called once per non-empty Edge.Condition at
	// flow.Compile time; syntactic errors surface here so flow load
	// fails fast rather than during a run.
	Compile(expr string) (Condition, error)
}

// Condition is one compiled edge guard. Evaluate runs it against a
// per-edge environment and returns whether the edge should fire.
type Condition interface {
	// Evaluate runs the compiled expression. ctx propagates from
	// Engine.Run / RunStream so evaluators that talk to external
	// systems (a slow JSONPath engine, an HTTP-backed policy, etc.)
	// can honor cancellation. The boolean return value is consulted;
	// any error short-circuits the run with FlowErr.
	Evaluate(ctx context.Context, env CondEnv) (bool, error)
}

// CondEnv is the environment available to an Edge.Condition. At v0
// only Value is exposed — the source port's outbound value as a
// string. Future revisions will widen this to whole-node outputs,
// flow inputs, run metadata, etc. — added fields are additive only
// so existing expressions keep working.
type CondEnv struct {
	// Value is the value flowing through this edge (the source
	// port's output for this run).
	Value string
}
