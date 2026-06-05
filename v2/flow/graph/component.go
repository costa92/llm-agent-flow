package graph

import (
	"context"
	"encoding/json"

	"github.com/costa92/llm-agent-contract/agents"
	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"
)

// AddChatModelNode adds a node that calls m.Generate. It takes an
// llm.Request on its "in" port and emits the llm.Response on "out". The
// node reuses the single-port lambda adapter — the component call is just
// a typed closure.
func AddChatModelNode[GI, GO any](g *Graph[GI, GO], id string, m llm.ChatModel) (NodeRef, error) {
	return AddLambdaNode[GI, GO, llm.Request, llm.Response](g, id,
		func(ctx context.Context, req llm.Request) (llm.Response, error) {
			return m.Generate(ctx, req)
		})
}

// AddTemplateNode adds a node that calls t.FormatRequest. It takes
// prompt.Vars on "in" and emits the assembled llm.Request on "out".
// Conversation history is not wired in this task (passed as nil).
func AddTemplateNode[GI, GO any](g *Graph[GI, GO], id string, t prompt.Requester) (NodeRef, error) {
	return AddLambdaNode[GI, GO, prompt.Vars, llm.Request](g, id,
		func(ctx context.Context, vars prompt.Vars) (llm.Request, error) {
			return t.FormatRequest(ctx, vars, nil)
		})
}

// AddToolNode adds a node that calls t.Execute. It takes the call
// arguments as json.RawMessage on "in" and emits the tool's string result
// on "out".
func AddToolNode[GI, GO any](g *Graph[GI, GO], id string, t agents.Tool) (NodeRef, error) {
	return AddLambdaNode[GI, GO, json.RawMessage, string](g, id,
		func(ctx context.Context, args json.RawMessage) (string, error) {
			return t.Execute(ctx, args)
		})
}
