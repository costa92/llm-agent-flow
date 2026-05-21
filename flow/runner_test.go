package flow

import (
	"strings"
	"testing"
)

func TestRunnerInterfaceSatisfiedByEngine(t *testing.T) {
	// Compile-time check is the var _ Runner = (*Engine)(nil) in
	// runner.go; this is the runtime-time check.
	eng := newTestEngine(t, sampleJSON)
	var r Runner = eng
	if r == nil {
		t.Fatal("nil Runner assignment")
	}
}

func TestEngineFlowIDAndName(t *testing.T) {
	eng := newTestEngine(t, sampleJSON)
	if got := eng.FlowID(); got != "echo_chain" {
		t.Fatalf("FlowID = %q, want echo_chain", got)
	}
	if got := eng.FlowName(); got != "echo chain" {
		t.Fatalf("FlowName = %q, want %q", got, "echo chain")
	}
	_ = strings.Contains
}
