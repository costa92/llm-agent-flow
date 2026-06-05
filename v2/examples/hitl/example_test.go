package hitl_test

import (
	"context"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/examples/hitl"
)

// TestExample_HITLSuspendResume is the "did checkpoint/interrupt/resume
// actually work" gate. It starts the flow, asserts it suspends at the gate
// node, then resumes with an approval decision and asserts the flow drives
// to completion with that decision as the output.
func TestExample_HITLSuspendResume(t *testing.T) {
	ctx := context.Background()

	fl, err := hitl.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runID, susp, err := fl.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if susp == nil {
		t.Fatalf("Start: expected the run to suspend at the gate, got no suspension")
	}
	if susp.NodeID != "gate" {
		t.Fatalf("suspended at %q, want %q", susp.NodeID, "gate")
	}

	out, err := fl.Resume(ctx, runID, susp.ResumeToken, "approved")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got, want := out["out"], "approved"; got != want {
		t.Fatalf("out=%v, want %v", got, want)
	}
}
