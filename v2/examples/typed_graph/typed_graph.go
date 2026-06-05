// Package typedgraph is the v2 typed-graph demo: it builds the headline
// template -> chatmodel -> extract -> tool chain through the typed graph
// front-end and Invokes it end to end.
//
// Like the v0.1 examples it is a library package with no main; the bundled
// example_test.go runs Run and asserts the deterministic output. The
// ChatModel is a deterministic scripted stand-in and the tool is an inline
// agents.Tool, so the demo needs no live provider.
package typedgraph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/costa92/llm-agent-contract/agents"
	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"

	"github.com/costa92/llm-agent-flow/v2/flow/graph"
)

// calcTool is an inline agents.Tool that adds the two operands it is
// handed and returns the sum as a string. It mirrors the zero-dep tool
// pattern used across the v2 graph tests.
type calcTool struct{}

func (calcTool) Name() string        { return "calc" }
func (calcTool) Description() string { return "Add two numbers a and b." }
func (calcTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
}

func (calcTool) Execute(_ context.Context, args json.RawMessage) (string, error) {
	var p struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	if err := json.Unmarshal(args, &p); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", p.A+p.B), nil
}

var _ agents.Tool = calcTool{}

// Run builds the typed graph template -> chatmodel -> extract -> tool and
// Invokes it. The template formats the user's expression into an
// llm.Request; the scripted ChatModel answers with a single calc tool
// call; the extract lambda pulls that call's JSON arguments out of the
// response; the tool computes the sum. The graph input is prompt.Vars and
// the output is the tool's string result.
func Run(ctx context.Context) (string, error) {
	tmpl, err := prompt.New(prompt.Spec{
		System: "You are a helpful assistant.",
		User:   "Compute {expr}",
	})
	if err != nil {
		return "", fmt.Errorf("prompt.New: %w", err)
	}
	req, ok := tmpl.(prompt.Requester)
	if !ok {
		return "", fmt.Errorf("prompt template does not implement Requester")
	}

	// Deterministic scripted model: always answers with a single calc
	// tool call over the operands the example asks about.
	model := llm.NewScriptedLLM(
		llm.WithProvider("scripted"),
		llm.WithModel("test-1"),
		llm.WithResponses(llm.ToolCallResponse("calc", `{"a":2,"b":3}`)),
	)

	g := graph.NewGraph[prompt.Vars, string]()

	tNode, err := graph.AddTemplateNode(g, "tmpl", req)
	if err != nil {
		return "", fmt.Errorf("AddTemplateNode: %w", err)
	}
	cmNode, err := graph.AddChatModelNode(g, "chat", model)
	if err != nil {
		return "", fmt.Errorf("AddChatModelNode: %w", err)
	}
	exNode, err := graph.AddLambdaNode(g, "extract",
		func(_ context.Context, resp llm.Response) (json.RawMessage, error) {
			if len(resp.ToolCalls) == 0 {
				return nil, fmt.Errorf("model returned no tool call")
			}
			return resp.ToolCalls[0].Arguments, nil
		})
	if err != nil {
		return "", fmt.Errorf("AddLambdaNode extract: %w", err)
	}
	toolNode, err := graph.AddToolNode(g, "tool", calcTool{})
	if err != nil {
		return "", fmt.Errorf("AddToolNode: %w", err)
	}

	if err := g.AddEdge(tNode, cmNode); err != nil {
		return "", fmt.Errorf("AddEdge tmpl->chat: %w", err)
	}
	if err := g.AddEdge(cmNode, exNode); err != nil {
		return "", fmt.Errorf("AddEdge chat->extract: %w", err)
	}
	if err := g.AddEdge(exNode, toolNode); err != nil {
		return "", fmt.Errorf("AddEdge extract->tool: %w", err)
	}
	if err := g.Entry(tNode); err != nil {
		return "", fmt.Errorf("Entry: %w", err)
	}
	if err := g.Exit(toolNode); err != nil {
		return "", fmt.Errorf("Exit: %w", err)
	}

	run, err := g.Compile(ctx)
	if err != nil {
		return "", fmt.Errorf("Compile: %w", err)
	}

	out, err := run.Invoke(ctx, prompt.Vars{"expr": "2+3"})
	if err != nil {
		return "", fmt.Errorf("Invoke: %w", err)
	}
	return out, nil
}
