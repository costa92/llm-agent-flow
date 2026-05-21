// Package router provides the three demo tools the
// examples/router/flow.json references — a classifier and two
// branch-specific responders.
//
// classify reads input.input ("hello" / "what time is it" / ...) and
// emits the literal "greet" or "other". make_greeting + say_other
// produce the per-branch human-readable output.
package router

import (
	"context"
	"encoding/json"
	"strings"

	agents "github.com/costa92/llm-agent"
)

// Tools returns the three demo tools by name.
func Tools() []agents.Tool {
	classify := agents.NewFuncTool(
		"classify",
		"Classify the input into one of: greet, other.",
		json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
		func(_ context.Context, args json.RawMessage) (string, error) {
			var p struct {
				Input string `json:"input"`
			}
			if err := json.Unmarshal(args, &p); err != nil {
				return "", err
			}
			low := strings.ToLower(p.Input)
			if strings.Contains(low, "hello") || strings.Contains(low, "hi") || strings.Contains(low, "你好") {
				return "greet", nil
			}
			return "other", nil
		},
	)
	greet := agents.NewFuncTool(
		"make_greeting",
		"Render a greeting reply.",
		json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
		func(_ context.Context, _ json.RawMessage) (string, error) {
			return "Hello! Nice to see you.", nil
		},
	)
	other := agents.NewFuncTool(
		"say_other",
		"Fallback responder when no specific intent matches.",
		json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
		func(_ context.Context, _ json.RawMessage) (string, error) {
			return "Sorry — I do not know how to handle that yet.", nil
		},
	)
	return []agents.Tool{classify, greet, other}
}
