package flow

import "testing"

// TestFlowEvent_HasMetadataField pins the v0.1.3 additive shape:
// FlowEvent must carry a Metadata map so downstream consumers can
// surface side-channel information (HTTP status, exec exit code,
// LLM token usage, etc.) without subverting the typed-union design.
func TestFlowEvent_HasMetadataField(t *testing.T) {
	ev := FlowEvent{
		Kind:     NodeFinished,
		Metadata: map[string]string{"http_status": "200"},
	}
	if got := ev.Metadata["http_status"]; got != "200" {
		t.Fatalf("Metadata[http_status]=%q, want 200", got)
	}
}
