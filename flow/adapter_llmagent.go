package flow

import (
	"context"
	"encoding/json"

	agents "github.com/costa92/llm-agent"
)

// FromAgentTool adapts a github.com/costa92/llm-agent/agents.Tool to
// flow's local Tool shape. The adapter is a thin shim — flow keeps
// its own narrow Tool interface so the library API does not depend on
// any specific Tool constructor or shape.
func FromAgentTool(t agents.Tool) Tool {
	return &agentToolAdapter{inner: t}
}

type agentToolAdapter struct {
	inner agents.Tool
}

func (a *agentToolAdapter) Name() string { return a.inner.Name() }

func (a *agentToolAdapter) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	return a.inner.Execute(ctx, args)
}

// FromAgentTools batch-adapts a slice of agents.Tool into a ToolMap
// keyed by Name(). Duplicates overwrite earlier entries.
func FromAgentTools(ts []agents.Tool) ToolMap {
	out := make(ToolMap, len(ts))
	for _, t := range ts {
		if t == nil {
			continue
		}
		out[t.Name()] = FromAgentTool(t)
	}
	return out
}
