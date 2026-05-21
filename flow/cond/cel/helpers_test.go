package cel_test

import (
	"context"
	"encoding/json"
)

// literalTool returns its v field on every Execute, ignoring inputs.
// Mirrors the helper in flow/engine_cond_test.go but kept private here
// so the cel package's test file is self-contained.
type literalTool struct {
	name string
	v    string
}

func (t *literalTool) Name() string { return t.name }
func (t *literalTool) Execute(_ context.Context, args json.RawMessage) (string, error) {
	if t.v != "" {
		return t.v, nil
	}
	var p struct {
		Input string `json:"input"`
	}
	_ = json.Unmarshal(args, &p)
	return p.Input, nil
}
