// Package cel is the Google CEL-backed ConditionEvaluator for
// llm-agent-flow.
//
// Importing this package pulls in github.com/google/cel-go and its
// transitive deps (protobuf, antlr runtime). The flow library itself
// stays stdlib-only — applications opt into CEL by importing this
// subpackage explicitly:
//
//	import (
//	    "github.com/costa92/llm-agent-flow/flow"
//	    cond "github.com/costa92/llm-agent-flow/flow/cond/cel"
//	)
//
//	eval, _ := cond.NewEvaluator()
//	engine, err := flow.LoadCompile(r, reg, deps, flow.WithConditionEvaluator(eval))
//
// The evaluator declares a single variable, `value` (string), holding
// the source port's outbound value for the edge under evaluation.
// CEL's standard library (size, startsWith, contains, regex via
// matches(), etc.) is available.
//
// Example expressions:
//
//	value == "go"
//	value.startsWith("hello")
//	value.matches("^[A-Z]+$")
//	size(value) > 0
package cel

import (
	"context"
	"errors"
	"fmt"

	"github.com/costa92/llm-agent-flow/flow"
	celpkg "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

// Evaluator is a CEL-backed flow.ConditionEvaluator. Zero value is
// not usable; construct with NewEvaluator.
type Evaluator struct {
	env *celpkg.Env
}

// NewEvaluator returns a CEL evaluator with `value` (string) declared
// in the environment. Returns an error only if cel-go fails to build
// its base environment (in practice, never).
func NewEvaluator() (*Evaluator, error) {
	env, err := celpkg.NewEnv(
		celpkg.Variable("value", celpkg.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("flow/cond/cel: env: %w", err)
	}
	return &Evaluator{env: env}, nil
}

// MustNewEvaluator is the panic-on-error variant for callers who
// cannot meaningfully recover (e.g. process startup).
func MustNewEvaluator() *Evaluator {
	e, err := NewEvaluator()
	if err != nil {
		panic(err)
	}
	return e
}

// Compile implements flow.ConditionEvaluator.
//
// The returned flow.Condition is safe for concurrent Evaluate calls —
// CEL programs are immutable after compilation.
func (e *Evaluator) Compile(expr string) (flow.Condition, error) {
	ast, issues := e.env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile %q: %w", expr, issues.Err())
	}
	if got := ast.OutputType(); !got.IsAssignableType(celpkg.BoolType) {
		return nil, fmt.Errorf("compile %q: expression returns %s, want bool", expr, got)
	}
	prog, err := e.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program %q: %w", expr, err)
	}
	return &compiledCondition{prog: prog, expr: expr}, nil
}

type compiledCondition struct {
	prog celpkg.Program
	expr string
}

func (c *compiledCondition) Evaluate(ctx context.Context, env flow.CondEnv) (bool, error) {
	out, _, err := c.prog.ContextEval(ctx, map[string]any{"value": env.Value})
	if err != nil {
		return false, fmt.Errorf("evaluate %q: %w", c.expr, err)
	}
	return refToBool(out)
}

func refToBool(v ref.Val) (bool, error) {
	if v == nil {
		return false, errors.New("nil result")
	}
	if b, ok := v.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("expected bool, got %T", v.Value())
}
