// Package echochain provides the two trivial tools the
// examples/echo_chain demo flow references by name.
//
// The example deliberately uses agents.NewFuncTool — it is the
// canonical zero-dep mock for downstream tests that don't want to
// stand up a real provider.
package echochain

import (
	"context"
	"encoding/json"

	agents "github.com/costa92/llm-agent"
)

// Tools returns the two demo tools by name.
func Tools() []agents.Tool {
	upper := agents.NewFuncTool(
		"upper",
		"Uppercase its input string.",
		json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
		func(_ context.Context, args json.RawMessage) (string, error) {
			var p struct {
				Input string `json:"input"`
			}
			if err := json.Unmarshal(args, &p); err != nil {
				return "", err
			}
			return uppercase(p.Input), nil
		},
	)
	reverse := agents.NewFuncTool(
		"reverse",
		"Reverse its input string.",
		json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
		func(_ context.Context, args json.RawMessage) (string, error) {
			var p struct {
				Input string `json:"input"`
			}
			if err := json.Unmarshal(args, &p); err != nil {
				return "", err
			}
			return reverseStr(p.Input), nil
		},
	)
	return []agents.Tool{upper, reverse}
}

func uppercase(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			r -= 32
		}
		out = append(out, r)
	}
	return string(out)
}

func reverseStr(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
