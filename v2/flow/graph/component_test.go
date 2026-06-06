package graph

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-contract/agents"
	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"
)

// fakeTool is an inline agents.Tool that echoes a fixed string regardless
// of its arguments.
type fakeTool struct{ result string }

func (fakeTool) Name() string            { return "fake" }
func (fakeTool) Description() string     { return "a fake tool" }
func (fakeTool) Schema() json.RawMessage { return json.RawMessage(`{}`) }
func (f fakeTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	return f.result, nil
}

var _ agents.Tool = fakeTool{}

// TestComponentChain_TemplateChatModelTool is the marquee end-to-end test:
// template -> chatmodel -> lambda(extract tool-call args) -> tool, all
// wired through typed component nodes.
func TestComponentChain_TemplateChatModelTool(t *testing.T) {
	tmpl, err := prompt.New(prompt.Spec{
		System: "You are a helpful assistant.",
		User:   "Compute {expr}",
	})
	if err != nil {
		t.Fatalf("prompt.New: %v", err)
	}
	req, ok := tmpl.(prompt.Requester)
	if !ok {
		t.Fatalf("prompt template does not implement Requester")
	}

	// scripted model returns a single tool-call response whose arguments
	// the downstream tool consumes.
	model := llm.NewScriptedLLM(
		llm.WithProvider("scripted"),
		llm.WithModel("test-1"),
		llm.WithResponses(
			llm.ToolCallResponse("calc", `{"a":2,"b":3}`),
		),
	)

	tool := fakeTool{result: "5"}

	g := NewGraph[prompt.Vars, string]()

	tNode, err := AddTemplateNode(g, "tmpl", req)
	if err != nil {
		t.Fatalf("AddTemplateNode: %v", err)
	}
	cmNode, err := AddChatModelNode(g, "chat", model)
	if err != nil {
		t.Fatalf("AddChatModelNode: %v", err)
	}
	// extract: pull the first tool call's Arguments out of the Response.
	exNode, err := AddLambdaNode(g, "extract", func(_ context.Context, resp llm.Response) (json.RawMessage, error) {
		if len(resp.ToolCalls) == 0 {
			return nil, nil
		}
		return resp.ToolCalls[0].Arguments, nil
	})
	if err != nil {
		t.Fatalf("AddLambdaNode extract: %v", err)
	}
	toolNode, err := AddToolNode(g, "tool", tool)
	if err != nil {
		t.Fatalf("AddToolNode: %v", err)
	}

	if err := g.AddEdge(tNode, cmNode); err != nil {
		t.Fatalf("AddEdge tmpl->chat: %v", err)
	}
	if err := g.AddEdge(cmNode, exNode); err != nil {
		t.Fatalf("AddEdge chat->extract: %v", err)
	}
	if err := g.AddEdge(exNode, toolNode); err != nil {
		t.Fatalf("AddEdge extract->tool: %v", err)
	}
	if err := g.Entry(tNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(toolNode); err != nil {
		t.Fatalf("Exit: %v", err)
	}

	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := r.Invoke(context.Background(), prompt.Vars{"expr": "2+3"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != "5" {
		t.Fatalf("Invoke = %q, want %q", got, "5")
	}
}

// TestChatModelToTool_TypeMismatch asserts that wiring a ChatModel node
// (out llm.Response) directly into a Tool node (in json.RawMessage) is
// rejected at AddEdge time — llm.Response is not assignable to
// json.RawMessage.
func TestChatModelToTool_TypeMismatch(t *testing.T) {
	g := NewGraph[llm.Request, string]()
	model := llm.NewScriptedLLM(llm.WithResponses(llm.TextResponse("hi")))
	cmNode, err := AddChatModelNode(g, "chat", model)
	if err != nil {
		t.Fatalf("AddChatModelNode: %v", err)
	}
	toolNode, err := AddToolNode(g, "tool", fakeTool{result: "x"})
	if err != nil {
		t.Fatalf("AddToolNode: %v", err)
	}
	if err := g.AddEdge(cmNode, toolNode); err == nil {
		t.Fatalf("AddEdge Response->RawMessage: want error, got nil")
	} else if !strings.Contains(err.Error(), "assignable") {
		t.Logf("mismatch error (ok): %v", err)
	}
}
