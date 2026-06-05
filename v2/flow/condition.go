package flow

import "context"

// ConditionEvaluator compiles edge guard expressions. The flow library
// does not bind any specific expression language — callers plug a
// concrete evaluator in via WithConditionEvaluator.
//
// Implementations must be safe for concurrent use once returned from a
// constructor — Compile is called once per Edge.Condition at
// flow.Compile time, and the returned Condition.Evaluate is called many
// times concurrently across runs.
type ConditionEvaluator interface {
	// Compile parses an expression and returns a runnable Condition.
	// Syntactic errors surface here so flow load fails fast.
	Compile(expr string) (Condition, error)
}

// Condition is one compiled edge guard. Evaluate runs it against a
// per-edge environment and returns whether the edge should fire.
type Condition interface {
	// Evaluate runs the compiled expression. ctx propagates from
	// Engine.Run / RunStream so evaluators can honor cancellation. Any
	// error short-circuits the run with FlowErr.
	Evaluate(ctx context.Context, env CondEnv) (bool, error)
}

// CondEnv is the environment available to an Edge.Condition.
//
// RV-5: Value stays string even though the v2 port carrier is any. The
// JSON/CEL routing path projects the source port's any value to string
// (v.(string)) before evaluation, so existing CEL expressions like
// value.startsWith("hello") keep working verbatim. Typed non-string
// routing uses the Go-key Branch front-end, not this CEL path.
type CondEnv struct {
	// Value is the source port's output value for this run, projected
	// to string.
	Value string
}
