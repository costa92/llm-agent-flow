package flow

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// stateWriterNode writes a fixed string into State under the node's id and
// passes its input through, so it can sit in a fanout layer.
type stateWriterNode struct {
	inPort, outPort string
	write           string
	failWith        error
}

func (n *stateWriterNode) Inputs() []Port  { return []Port{{Name: n.inPort}} }
func (n *stateWriterNode) Outputs() []Port { return []Port{{Name: n.outPort}} }
func (n *stateWriterNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	if n.failWith != nil {
		return nil, n.failWith
	}
	if n.write != "" {
		SetState(ctx, n.write)
	}
	return map[string]any{n.outPort: in[n.inPort]}, nil
}

// stateReaderNode records the State it observes via StateFromContext.
type stateReaderNode struct {
	inPort, outPort string
	seen            *[]string
}

func (n *stateReaderNode) Inputs() []Port  { return []Port{{Name: n.inPort}} }
func (n *stateReaderNode) Outputs() []Port { return []Port{{Name: n.outPort}} }
func (n *stateReaderNode) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	if s, ok := StateFromContext[[]string](ctx); ok && n.seen != nil {
		*n.seen = append([]string(nil), s...)
	}
	return map[string]any{n.outPort: in[n.inPort]}, nil
}

// twoSiblingFlow: entry → {A, B} two siblings in the same layer.
func twoSiblingFlow() Flow {
	return Flow{
		ID: "two-sibling",
		Nodes: []Node{
			{ID: "entry", Type: "entry"},
			{ID: "A", Type: "A"},
			{ID: "B", Type: "B"},
		},
		Edges: []Edge{
			{Source: PortRef{"entry", "out"}, Target: PortRef{"A", "in"}},
			{Source: PortRef{"entry", "out"}, Target: PortRef{"B", "in"}},
		},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"entry", "in"}}},
		Outputs: []NamedPortRef{{Name: "ay", PortRef: PortRef{"A", "out"}}, {Name: "by", PortRef: PortRef{"B", "out"}}},
	}
}

// appendListReducer is a type-erased WithReducer-style reducer that appends
// each write (a string) to a []string committed value.
func appendListReducer(prev any, writes []any) (any, error) {
	out := append([]string(nil), prev.([]string)...)
	for _, w := range writes {
		out = append(out, w.(string))
	}
	return out, nil
}

// TestEngineState_TDet asserts that two siblings writing State in one layer,
// merged by a custom reducer, produce byte-identical committed State across
// 200 runs regardless of goroutine-finish order.
func TestEngineState_TDet(t *testing.T) {
	ctx := context.Background()
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", write: "b"},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	var want []string
	for i := 0; i < 200; i++ {
		cell := newStateCell([]string{}, appendListReducer)
		_, _, err := eng.runCore(ctx, map[string]any{"x": "v"}, nil, "", false, nil, cell)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		got := cell.committed.([]string)
		if i == 0 {
			want = got
			if !reflect.DeepEqual(want, []string{"a", "b"}) {
				t.Fatalf("committed = %v, want [a b] (nodeID-sorted)", want)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d committed = %v, want %v (nondeterministic)", i, got, want)
		}
	}
}

// TestEngineState_TErr asserts an erroring layer commits no State: the
// erroring sibling fails the run BEFORE the barrier reduce, so the other
// sibling's staged write is discarded (sibling-error-wins preserved).
func TestEngineState_TErr(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", failWith: boom},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cell := newStateCell([]string{"seed"}, appendListReducer)
	_, _, runErr := eng.runCore(ctx, map[string]any{"x": "v"}, nil, "", false, nil, cell)
	if !errors.Is(runErr, boom) {
		t.Fatalf("run err = %v, want boom", runErr)
	}
	got := cell.committed.([]string)
	if !reflect.DeepEqual(got, []string{"seed"}) {
		t.Fatalf("committed = %v, want [seed] (no commit on erroring layer)", got)
	}
}

// TestEngineState_CrossLayerVisible asserts a node writing State in layer N
// is visible to a node reading State in layer N+1.
func TestEngineState_CrossLayerVisible(t *testing.T) {
	ctx := context.Background()
	var seen []string
	f := Flow{
		ID: "chain",
		Nodes: []Node{
			{ID: "w", Type: "w"},
			{ID: "r", Type: "r"},
		},
		Edges:   []Edge{{Source: PortRef{"w", "out"}, Target: PortRef{"r", "in"}}},
		Inputs:  []NamedPortRef{{Name: "x", PortRef: PortRef{"w", "in"}}},
		Outputs: []NamedPortRef{{Name: "y", PortRef: PortRef{"r", "out"}}},
	}
	reg := registerMixed(map[string]NodeKind{
		"w": &stateWriterNode{inPort: "in", outPort: "out", write: "from-w"},
		"r": &stateReaderNode{inPort: "in", outPort: "out", seen: &seen},
	})
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cell := newStateCell([]string{}, appendListReducer)
	if _, _, err := eng.runCore(ctx, map[string]any{"x": "v"}, nil, "", false, nil, cell); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !reflect.DeepEqual(seen, []string{"from-w"}) {
		t.Fatalf("layer N+1 saw State %v, want [from-w]", seen)
	}
}

// TestEngineState_ConflictDefault asserts the engine surfaces
// ErrStateWriteConflict when two siblings write with no reducer supplied,
// and fires no edges / writes no checkpoint.
func TestEngineState_ConflictDefault(t *testing.T) {
	ctx := context.Background()
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", write: "b"},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cell := newStateCell([]string{}, nil) // no reducer → reject-on-collision
	_, _, runErr := eng.runCore(ctx, map[string]any{"x": "v"}, nil, "", false, nil, cell)
	if !errors.Is(runErr, ErrStateWriteConflict) {
		t.Fatalf("run err = %v, want ErrStateWriteConflict", runErr)
	}
}

// TestEngineState_LessRegression asserts a nil cell (State-less) run behaves
// exactly as before — same outputs, no error.
func TestEngineState_LessRegression(t *testing.T) {
	ctx := context.Background()
	reg := registerMixed(map[string]NodeKind{
		"entry": passthrough("in", "out"),
		"A":     &stateWriterNode{inPort: "in", outPort: "out", write: "a"},
		"B":     &stateWriterNode{inPort: "in", outPort: "out", write: "b"},
	})
	eng, err := Compile(twoSiblingFlow(), reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, _, err := eng.runCore(ctx, map[string]any{"x": "v"}, nil, "", false, nil, nil)
	if err != nil {
		t.Fatalf("State-less run: %v", err)
	}
	if out["ay"] != "v" || out["by"] != "v" {
		t.Fatalf("State-less outputs = %v, want ay=v by=v", out)
	}
}
