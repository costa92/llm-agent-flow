package flow

import (
	"context"
	"errors"
	"testing"
)

// ckState is a registered State type used by the checkpoint tests. It is
// registered via RegisterCodec so a State-bearing run is checkpointable.
type ckState struct {
	Note string `json:"note"`
}

func init() { RegisterCodec[ckState]("ckState") }

// stateWriterReaderNode writes a fixed ckState into State (when write set)
// and records the ckState it observes (when seen set), passing input through.
type stateWriterReaderNode struct {
	inPort, outPort string
	write           *ckState
	seen            *ckState
	sawState        *bool
}

func (n *stateWriterReaderNode) Inputs() []Port  { return []Port{{Name: n.inPort}} }
func (n *stateWriterReaderNode) Outputs() []Port { return []Port{{Name: n.outPort}} }
func (n *stateWriterReaderNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	if n.write != nil {
		SetState(ctx, *n.write)
	}
	if n.seen != nil {
		s, ok := StateFromContext[ckState](ctx)
		*n.seen = s
		if n.sawState != nil {
			*n.sawState = ok
		}
	}
	return map[string]any{n.outPort: in[n.inPort]}, nil
}

// stateCheckpointFlow: w (writes State, layer 0) → approve (interrupt, layer
// 1) → r (reads State, layer 2). State is written in a NON-interrupt node so
// it is staged+reduced before the suspend snapshot.
func stateCheckpointFlow() Flow {
	return Flow{
		ID: "state-ckpt",
		Nodes: []Node{
			{ID: "w", Type: "w"},
			{ID: "approve", Type: "approve"},
			{ID: "r", Type: "r"},
		},
		Edges: []Edge{
			{Source: PortRef{"w", "out"}, Target: PortRef{"approve", "in"}},
			{Source: PortRef{"approve", "out"}, Target: PortRef{"r", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"w", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"r", "out"}}},
	}
}

// T-RESUME: State written pre-suspend is observable post-resume. Testable
// only on the engine/JSON route (not the typed-lambda route, which is
// non-serializable).
func TestStateCheckpoint_ResumeRestoresState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	var seen ckState
	var saw bool
	reg := registerMixed(map[string]NodeKind{
		"w":       &stateWriterReaderNode{inPort: "in", outPort: "out", write: &ckState{Note: "pre-suspend"}},
		"approve": newInterruptNode("in", "out", InterruptRequest{Kind: "approval", Prompt: "ok?"}),
		"r":       &stateWriterReaderNode{inPort: "in", outPort: "out", seen: &seen, sawState: &saw},
	})
	eng, err := Compile(stateCheckpointFlow(), reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumableWithState(ctx, runID, map[string]any{"x": "v"},
		WithInitialState(ckState{}))
	if err != nil {
		t.Fatalf("run resumable with state: %v", err)
	}
	if res.Suspended == nil {
		t.Fatalf("expected suspension at approve")
	}

	// Resume with a fresh WithInitialState seed — the durable checkpoint
	// State must OVERRIDE it so r observes the pre-suspend write.
	res2, err := eng.ResumeWithState(ctx, runID, res.Suspended.ResumeToken,
		map[string]any{"out": "v"}, WithInitialState(ckState{Note: "ignored-seed"}))
	if err != nil {
		t.Fatalf("resume with state: %v", err)
	}
	if res2.Suspended != nil {
		t.Fatalf("unexpected re-suspension")
	}
	if !saw {
		t.Fatalf("reader did not observe State on resume")
	}
	if seen.Note != "pre-suspend" {
		t.Fatalf("reader saw State %q, want pre-suspend (durable State should override the resume seed)", seen.Note)
	}
}

// v2 back-compat: a Version-2 checkpoint (no State) resumes fine on the
// State-less Resume path.
func TestStateCheckpoint_V2BackCompat(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	f, reg := headlineFlow(t, newInterruptNode("in", "out", InterruptRequest{Kind: "approval", Prompt: "approve?"}))
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(ctx, runID, map[string]any{"x": "approved"})
	if err != nil {
		t.Fatalf("run resumable: %v", err)
	}
	// Rewrite the stored checkpoint as a v2 (empty State) record to simulate
	// a checkpoint written before State support.
	cp, err := store.LoadCheckpoint(ctx, runID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cp.Version = 2
	cp.State = nil
	cp.StateCodec = ""
	if err := store.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("save: %v", err)
	}
	res2, err := eng.Resume(ctx, runID, res.Suspended.ResumeToken, map[string]any{"out": "approved"})
	if err != nil {
		t.Fatalf("resume v2 checkpoint: %v", err)
	}
	if res2.Outputs["y"] != "approved" {
		t.Fatalf("output %v, want approved", res2.Outputs["y"])
	}
}

// missing-codec: a State-bearing run with an unregistered State type → the
// suspend snapshot errors wrapping ErrNotCheckpointable.
func TestStateCheckpoint_MissingCodec_NotCheckpointable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()
	type unregState struct{ V int }
	reg := registerMixed(map[string]NodeKind{
		"w":       &unregWriterNode{inPort: "in", outPort: "out"},
		"approve": newInterruptNode("in", "out", InterruptRequest{Prompt: "ok?"}),
		"r":       passthrough("in", "out"),
	})
	eng, err := Compile(stateCheckpointFlow(), reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.RunResumableWithState(ctx, NewRunID(), map[string]any{"x": "v"},
		WithInitialState(unregState{}))
	if !errors.Is(err, ErrNotCheckpointable) {
		t.Fatalf("got err %v, want ErrNotCheckpointable", err)
	}
}

// unregWriterNode writes an unregistered State type (unregState defined in
// the test above) — used to exercise the missing-codec path. It writes a
// distinct unexported type so no codec is registered.
type unregWriterNode struct{ inPort, outPort string }

func (n *unregWriterNode) Inputs() []Port  { return []Port{{Name: n.inPort}} }
func (n *unregWriterNode) Outputs() []Port { return []Port{{Name: n.outPort}} }
func (n *unregWriterNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	type unregState struct{ V int }
	SetState(ctx, unregState{V: 1})
	return map[string]any{n.outPort: in[n.inPort]}, nil
}

// clone safety: cloning a Checkpoint with State does not alias the bytes.
func TestStateCheckpoint_CloneNoAlias(t *testing.T) {
	orig := Checkpoint{
		RunID:      "r1",
		State:      []byte(`{"t":"ckState","v":{"note":"a"}}`),
		StateCodec: "state:ckState:deadbeef",
	}
	cl := orig.clone()
	// Mutate the clone's State bytes.
	cl.State[0] = 'X'
	if orig.State[0] == 'X' {
		t.Fatalf("clone aliased State bytes — mutation reached the original")
	}
	if cl.StateCodec != orig.StateCodec {
		t.Fatalf("StateCodec not copied: %q vs %q", cl.StateCodec, orig.StateCodec)
	}
	// A nil State stays nil.
	if (Checkpoint{}).clone().State != nil {
		t.Fatalf("clone of nil-State checkpoint produced non-nil State")
	}
}
