package flow

import (
	"errors"
	"testing"

	contractagents "github.com/costa92/llm-agent-contract/agents"
)

func TestRunEventFromFlowEventNodeStarted(t *testing.T) {
	got := RunEventFromFlowEvent(Event{
		Kind:     NodeStarted,
		FlowID:   "flow-1",
		NodeID:   "node-1",
		Input:    map[string]any{"in": "value"},
		Metadata: map[string]string{"k": "v"},
	})
	if got.Kind != contractagents.RunEventFlowNodeStarted {
		t.Fatalf("kind = %s, want %s", got.Kind, contractagents.RunEventFlowNodeStarted)
	}
	if got.FlowID != "flow-1" || got.NodeID != "node-1" {
		t.Fatalf("ids = (%q,%q), want flow-1/node-1", got.FlowID, got.NodeID)
	}
	if got.Input["in"] != "value" || got.Metadata["k"] != "v" {
		t.Fatalf("payload = input:%v metadata:%v", got.Input, got.Metadata)
	}
}

func TestRunEventFromFlowEventError(t *testing.T) {
	want := errors.New("boom")
	got := RunEventFromFlowEvent(Event{Kind: FlowErr, Err: want})
	if got.Kind != contractagents.RunEventFlowError {
		t.Fatalf("kind = %s, want %s", got.Kind, contractagents.RunEventFlowError)
	}
	if !errors.Is(got.Err, want) {
		t.Fatalf("err = %v, want %v", got.Err, want)
	}
}

func TestRunEventFromFlowEventInterruptAndSuspended(t *testing.T) {
	req := InterruptRequest{Kind: "approval", Prompt: "approve?"}
	interrupted := RunEventFromFlowEvent(Event{Kind: NodeInterrupted, NodeID: "gate", Request: &req})
	if interrupted.Kind != contractagents.RunEventInterrupt {
		t.Fatalf("interrupt kind = %s, want %s", interrupted.Kind, contractagents.RunEventInterrupt)
	}
	if payload, ok := interrupted.Payload.(InterruptRequest); !ok || payload.Prompt != "approve?" {
		t.Fatalf("interrupt payload = %#v, want InterruptRequest approve?", interrupted.Payload)
	}

	suspended := RunEventFromFlowEvent(Event{
		Kind:        FlowSuspended,
		NodeID:      "gate",
		Request:     &req,
		ResumeToken: "run-1",
	})
	if suspended.Kind != contractagents.RunEventSuspended {
		t.Fatalf("suspended kind = %s, want %s", suspended.Kind, contractagents.RunEventSuspended)
	}
	if suspended.ResumeToken != "run-1" {
		t.Fatalf("resume token = %q, want run-1", suspended.ResumeToken)
	}
}
