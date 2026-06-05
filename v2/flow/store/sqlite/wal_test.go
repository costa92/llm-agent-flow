package sqlite_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/costa92/llm-agent-flow/v2/flow"
	"github.com/costa92/llm-agent-flow/v2/flow/store/sqlite"
)

// passNode is a single-in single-out string passthrough NodeKind.
type passNode struct{ in, out string }

func (n *passNode) Inputs() []flow.Port  { return []flow.Port{{Name: n.in}} }
func (n *passNode) Outputs() []flow.Port { return []flow.Port{{Name: n.out}} }
func (n *passNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	return map[string]any{n.out: in[n.in]}, nil
}

// interruptNode is interrupt-capable: Run always requests a human
// interrupt. The engine injects humanInput on resume and never re-runs it.
type interruptNode struct {
	in, out string
	req     flow.InterruptRequest
}

func (n *interruptNode) Inputs() []flow.Port  { return []flow.Port{{Name: n.in}} }
func (n *interruptNode) Outputs() []flow.Port { return []flow.Port{{Name: n.out}} }
func (n *interruptNode) CanInterrupt() bool   { return true }
func (n *interruptNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	return nil, flow.Interrupt(n.req)
}

// headlineRegistry builds entry → approve → out over string ports. approve
// resolves to the supplied NodeKind (interrupt-capable or plain passthrough
// for the baseline).
func headlineRegistry(t *testing.T, approve flow.NodeKind) *flow.NodeRegistry {
	t.Helper()
	reg := flow.NewNodeRegistry()
	must := func(typ string, nk flow.NodeKind) {
		if err := reg.Register(typ, func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
			return nk, nil
		}); err != nil {
			t.Fatalf("register %q: %v", typ, err)
		}
	}
	must("entry", &passNode{in: "in", out: "out"})
	must("approve", approve)
	must("out", &passNode{in: "in", out: "out"})
	return reg
}

func headlineFlow() flow.Flow {
	return flow.Flow{
		ID: "headline-wal",
		Nodes: []flow.Node{
			{ID: "entry", Type: "entry"},
			{ID: "approve", Type: "approve"},
			{ID: "out", Type: "out"},
		},
		Edges: []flow.Edge{
			{Source: flow.PortRef{Node: "entry", Port: "out"}, Target: flow.PortRef{Node: "approve", Port: "in"}},
			{Source: flow.PortRef{Node: "approve", Port: "out"}, Target: flow.PortRef{Node: "out", Port: "in"}},
		},
		Inputs:  []flow.NamedPortRef{{Name: "x", PortRef: flow.PortRef{Node: "entry", Port: "in"}}},
		Outputs: []flow.NamedPortRef{{Name: "y", PortRef: flow.PortRef{Node: "out", Port: "out"}}},
	}
}

// TestWALResumeToCompletion proves a checkpoint persisted to an on-disk WAL
// DB survives a Close()/Open() cycle (a simulated process restart) and the
// run resumes to completion from disk, matching the non-interrupt baseline.
func TestWALResumeToCompletion(t *testing.T) {
	ctx := context.Background()

	// Baseline: approve is a plain passthrough, no store, plain Run.
	bEng, err := flow.Compile(headlineFlow(), headlineRegistry(t, &passNode{in: "in", out: "out"}), flow.Deps{})
	if err != nil {
		t.Fatalf("baseline compile: %v", err)
	}
	base, err := bEng.Run(ctx, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}

	dsn := filepath.Join(t.TempDir(), "cp.db")

	// First process: compile with on-disk checkpoint store, run, suspend.
	cs, err := sqlite.Open(dsn)
	if err != nil {
		t.Fatalf("Open cs: %v", err)
	}
	intNode := &interruptNode{in: "in", out: "out", req: flow.InterruptRequest{Kind: "approval", Prompt: "approve?"}}
	eng, err := flow.Compile(headlineFlow(), headlineRegistry(t, intNode), flow.Deps{}, flow.WithCheckpointStore(cs))
	if err != nil {
		t.Fatalf("compile eng: %v", err)
	}
	runID := flow.NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("RunResumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension")
	}
	if res.Suspended.NodeID != "approve" {
		t.Fatalf("suspended at %q, want approve", res.Suspended.NodeID)
	}
	token := res.Suspended.ResumeToken

	// Simulate process restart: close the store.
	if err := cs.Close(); err != nil {
		t.Fatalf("cs.Close: %v", err)
	}

	// Second process: reopen the SAME DB, recompile a FRESH engine from
	// the SAME flow (→ same FlowHash), resume from disk.
	cs2, err := sqlite.Open(dsn)
	if err != nil {
		t.Fatalf("Open cs2: %v", err)
	}
	t.Cleanup(func() { _ = cs2.Close() })
	intNode2 := &interruptNode{in: "in", out: "out", req: flow.InterruptRequest{Kind: "approval", Prompt: "approve?"}}
	eng2, err := flow.Compile(headlineFlow(), headlineRegistry(t, intNode2), flow.Deps{}, flow.WithCheckpointStore(cs2))
	if err != nil {
		t.Fatalf("compile eng2: %v", err)
	}
	res2, err := eng2.Resume(ctx, runID, token, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res2.Suspended != nil {
		t.Fatalf("unexpected re-suspension: %+v", res2.Suspended)
	}
	if res2.Outputs["y"] != base["y"] {
		t.Fatalf("resume output %v, want baseline %v", res2.Outputs["y"], base["y"])
	}
}
