package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/costa92/llm-agent-contract/agents"
	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

// AddChatModelNode adds a node that calls the ChatModel. It takes an
// llm.Request on its "in" port and emits the llm.Response on "out".
//
// Unlike the other component nodes it uses a dedicated chatModelAdapter
// rather than the generic lambda adapter, because it carries TWO paths:
//   - Run (value path): used by Invoke / the layered engine — calls
//     m.Generate, behaviourally identical to the prior lambda wrapper.
//   - streamRun (data-stream path): used only by the linear data-stream
//     executor — calls m.Stream and emits a StreamReader[llm.StreamEvent].
func AddChatModelNode[GI, GO any](g *Graph[GI, GO], id string, m llm.ChatModel) (NodeRef, error) {
	inT := reflect.TypeFor[llm.Request]()
	outT := reflect.TypeFor[llm.Response]()
	newKind := func() flow.NodeKind {
		return &chatModelAdapter{id: id, m: m, inT: inT, outT: outT}
	}
	ref, err := g.addNode(id, inT, outT, newKind)
	if err != nil {
		return NodeRef{}, err
	}
	g.markInProcess(id, "chatmodel node holds an injected llm.ChatModel (use the JSON front-end with Deps-resolved nodes for a checkpointable flow)")
	return ref, nil
}

// chatModelAdapter is the dedicated flow.NodeKind for a ChatModel node.
// It publishes a single in (llm.Request) / out (llm.Response) port and
// implements the optional streamNode interface so the linear data-stream
// executor can drive m.Stream instead of m.Generate.
type chatModelAdapter struct {
	id   string
	m    llm.ChatModel
	inT  reflect.Type
	outT reflect.Type
}

func (a *chatModelAdapter) Inputs() []flow.Port {
	return []flow.Port{{Name: portIn, GoType: a.inT}}
}

func (a *chatModelAdapter) Outputs() []flow.Port {
	return []flow.Port{{Name: portOut, GoType: a.outT}}
}

// Run is the value path: assert the boxed input to llm.Request and call
// Generate. Behaviour is identical to the prior lambda wrapper so the
// existing component/Invoke tests stay green.
func (a *chatModelAdapter) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	raw, ok := in[portIn]
	if !ok {
		return nil, fmt.Errorf("graph: node %q: missing input port %q", a.id, portIn)
	}
	req, ok := raw.(llm.Request)
	if !ok {
		return nil, fmt.Errorf("graph: node %q: input is %T, not assignable to %s", a.id, raw, a.inT)
	}
	resp, err := a.m.Generate(ctx, req)
	if err != nil {
		return nil, err
	}
	return map[string]any{portOut: resp}, nil
}

// streamRun is the data-stream path: it needs a materialised llm.Request
// input (the linear executor concats any upstream stream to a value
// before calling this), calls m.Stream, and emits a carrier wrapping a
// StreamReader[llm.StreamEvent].
func (a *chatModelAdapter) streamRun(ctx context.Context, in streamCarrier) (streamCarrier, error) {
	if in.isStream {
		return streamCarrier{}, fmt.Errorf("graph: node %q: chatmodel stream input must be a materialised llm.Request", a.id)
	}
	req, ok := in.value.(llm.Request)
	if !ok {
		return streamCarrier{}, fmt.Errorf("graph: node %q: input is %T, not %s", a.id, in.value, a.inT)
	}
	sr, err := a.m.Stream(ctx, req)
	if err != nil {
		return streamCarrier{}, err
	}
	bridged := bridgeLLMStream(sr)
	return streamCarrier{
		isStream: true,
		stream:   anyStream[llm.StreamEvent]{bridged},
		elemT:    reflect.TypeFor[llm.StreamEvent](),
	}, nil
}

// streamNode is the optional interface a node implements when it can
// produce a data stream directly. Only chatModelAdapter implements it;
// every other node (lambda/branch/template/tool/passthrough) goes through
// the executor's "concat upstream stream -> value -> Run -> value" path.
type streamNode interface {
	streamRun(ctx context.Context, in streamCarrier) (streamCarrier, error)
}

// anyStream erases a StreamReader[T] to a StreamReader[any] so it can ride
// the streamCarrier. Next boxes each T into any; Close delegates.
type anyStream[T any] struct{ sr StreamReader[T] }

func (a anyStream[T]) Next() (any, error) {
	v, err := a.sr.Next()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (a anyStream[T]) Close() error { return a.sr.Close() }

// AddTemplateNode adds a node that calls t.FormatRequest. It takes
// prompt.Vars on "in" and emits the assembled llm.Request on "out".
// Conversation history is not wired in this task (passed as nil).
func AddTemplateNode[GI, GO any](g *Graph[GI, GO], id string, t prompt.Requester) (NodeRef, error) {
	ref, err := AddLambdaNode[GI, GO, prompt.Vars, llm.Request](g, id,
		func(ctx context.Context, vars prompt.Vars) (llm.Request, error) {
			return t.FormatRequest(ctx, vars, nil)
		})
	if err != nil {
		return NodeRef{}, err
	}
	// Overwrite the generic lambda reason with the specific component one.
	g.markInProcess(id, "template node holds an injected prompt.Requester (use the JSON front-end with Deps-resolved nodes for a checkpointable flow)")
	return ref, nil
}

// AddToolNode adds a node that calls t.Execute. It takes the call
// arguments as json.RawMessage on "in" and emits the tool's string result
// on "out".
func AddToolNode[GI, GO any](g *Graph[GI, GO], id string, t agents.Tool) (NodeRef, error) {
	ref, err := AddLambdaNode[GI, GO, json.RawMessage, string](g, id,
		func(ctx context.Context, args json.RawMessage) (string, error) {
			return t.Execute(ctx, args)
		})
	if err != nil {
		return NodeRef{}, err
	}
	// Overwrite the generic lambda reason with the specific component one.
	g.markInProcess(id, "tool node holds an injected agents.Tool (use the JSON front-end with Deps-resolved nodes for a checkpointable flow)")
	return ref, nil
}
