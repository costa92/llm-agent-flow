package flow

import (
	"context"
	"errors"
	"testing"
)

func TestMapping_InputFieldsAssembleNodeInput(t *testing.T) {
	capture := &lambdaNode{
		inPorts:  []Port{{Name: "req"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			req := in["req"].(map[string]any)
			return map[string]any{"out": req["user"].(string) + ":" + req["query"].(string)}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"N": capture})
	f := Flow{
		ID:    "input-map",
		Nodes: []Node{{ID: "N", Type: "N"}},
		Mappings: []Mapping{
			{
				Source: MappingSource{Input: "payload", Path: []string{"user"}},
				Target: MappingTarget{Node: "N", Port: "req", Path: []string{"user"}},
			},
			{
				Source: MappingSource{Input: "payload", Path: []string{"question"}},
				Target: MappingTarget{Node: "N", Port: "req", Path: []string{"query"}},
			},
		},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"N", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.Run(context.Background(), map[string]any{
		"payload": map[string]any{"user": "alice", "question": "status"},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["out"] != "alice:status" {
		t.Fatalf("out = %v, want alice:status", out["out"])
	}
}

func TestMapping_NodeOutputFieldActivatesTarget(t *testing.T) {
	producer := &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return map[string]any{"out": map[string]any{"title": "incident", "body": "sev2"}}, nil
		},
	}
	consumer := &lambdaNode{
		inPorts:  []Port{{Name: "doc"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			doc := in["doc"].(map[string]any)
			return map[string]any{"out": doc["headline"].(string)}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"P": producer, "C": consumer})
	f := Flow{
		ID:    "node-map",
		Nodes: []Node{{ID: "P", Type: "P"}, {ID: "C", Type: "C"}},
		Mappings: []Mapping{{
			Source: MappingSource{Node: "P", Port: "out", Path: []string{"title"}},
			Target: MappingTarget{Node: "C", Port: "doc", Path: []string{"headline"}},
		}},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"P", "in"}}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"C", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.Run(context.Background(), map[string]any{"in": "x"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["out"] != "incident" {
		t.Fatalf("out = %v, want incident", out["out"])
	}
}

func TestMapping_NodeOutputFieldsAssembleTargetPort(t *testing.T) {
	producer := &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return map[string]any{"out": map[string]any{"title": "incident", "body": "sev2"}}, nil
		},
	}
	consumer := &lambdaNode{
		inPorts:  []Port{{Name: "doc"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			doc := in["doc"].(map[string]any)
			return map[string]any{"out": doc["headline"].(string) + ":" + doc["summary"].(string)}, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"P": producer, "C": consumer})
	f := Flow{
		ID:    "node-map-assemble",
		Nodes: []Node{{ID: "P", Type: "P"}, {ID: "C", Type: "C"}},
		Mappings: []Mapping{
			{
				Source: MappingSource{Node: "P", Port: "out", Path: []string{"title"}},
				Target: MappingTarget{Node: "C", Port: "doc", Path: []string{"headline"}},
			},
			{
				Source: MappingSource{Node: "P", Port: "out", Path: []string{"body"}},
				Target: MappingTarget{Node: "C", Port: "doc", Path: []string{"summary"}},
			},
		},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"P", "in"}}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"C", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.Run(context.Background(), map[string]any{"in": "x"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out["out"] != "incident:sev2" {
		t.Fatalf("out = %v, want incident:sev2", out["out"])
	}
}

func TestMapping_MissingSourcePathErrors(t *testing.T) {
	reg := registerLambdas(map[string]*lambdaNode{"N": passthrough("in", "out")})
	f := Flow{
		ID:    "bad-path",
		Nodes: []Node{{ID: "N", Type: "N"}},
		Mappings: []Mapping{{
			Source: MappingSource{Input: "payload", Path: []string{"missing"}},
			Target: MappingTarget{Node: "N", Port: "in"},
		}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"N", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = eng.Run(context.Background(), map[string]any{"payload": map[string]any{"ok": "v"}})
	if err == nil {
		t.Fatal("run: want missing path error, got nil")
	}
}

func TestMapping_ValidateRejectsCycleThroughMapping(t *testing.T) {
	err := Validate(Flow{
		ID:    "cycle",
		Nodes: []Node{{ID: "A", Type: "A"}, {ID: "B", Type: "B"}},
		Edges: []Edge{{
			Source: PortRef{"A", "out"},
			Target: PortRef{"B", "in"},
		}},
		Mappings: []Mapping{{
			Source: MappingSource{Node: "B", Port: "out"},
			Target: MappingTarget{Node: "A", Port: "in"},
		}},
	})
	var verr *ValidateError
	if !errors.As(err, &verr) {
		t.Fatalf("Validate err = %v, want ValidateError", err)
	}
}

func TestMapping_ValidateRejectsEmptyPathSegments(t *testing.T) {
	err := Validate(Flow{
		ID:    "bad-path-segment",
		Nodes: []Node{{ID: "N", Type: "N"}},
		Mappings: []Mapping{
			{
				Source: MappingSource{Input: "payload", Path: []string{""}},
				Target: MappingTarget{Node: "N", Port: "in"},
			},
			{
				Source: MappingSource{Input: "payload"},
				Target: MappingTarget{Node: "N", Port: "in", Path: []string{"ok", ""}},
			},
		},
	})
	var verr *ValidateError
	if !errors.As(err, &verr) {
		t.Fatalf("Validate err = %v, want ValidateError", err)
	}
}

func TestMapping_ValidateRejectsMixedSource(t *testing.T) {
	err := Validate(Flow{
		ID:    "bad",
		Nodes: []Node{{ID: "N", Type: "N"}},
		Mappings: []Mapping{{
			Source: MappingSource{Input: "x", Node: "N", Port: "out"},
			Target: MappingTarget{Node: "N", Port: "in"},
		}},
	})
	var verr *ValidateError
	if !errors.As(err, &verr) {
		t.Fatalf("Validate err = %v, want ValidateError", err)
	}
}

func TestMapping_UnknownNodeSourcePortLeavesTargetInactive(t *testing.T) {
	producer := &lambdaNode{
		inPorts:  []Port{{Name: "in"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			return map[string]any{"out": "value"}, nil
		},
	}
	consumer := &lambdaNode{
		inPorts:  []Port{{Name: "doc"}},
		outPorts: []Port{{Name: "out"}},
		fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
			t.Fatal("consumer should stay inactive when mapping source port is absent")
			return nil, nil
		},
	}
	reg := registerLambdas(map[string]*lambdaNode{"P": producer, "C": consumer})
	f := Flow{
		ID:    "missing-source-port",
		Nodes: []Node{{ID: "P", Type: "P"}, {ID: "C", Type: "C"}},
		Mappings: []Mapping{{
			Source: MappingSource{Node: "P", Port: "missing"},
			Target: MappingTarget{Node: "C", Port: "doc"},
		}},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"P", "in"}}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"C", "out"}}},
	}
	eng, err := Compile(f, reg, Deps{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := eng.Run(context.Background(), map[string]any{"in": "x"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, ok := out["out"]; ok {
		t.Fatalf("out contains skipped target output: %v", out)
	}
}

func TestMapping_ResumeInjectsHumanInputToTarget(t *testing.T) {
	reg := registerMixed(map[string]NodeKind{
		"entry":   passthrough("in", "out"),
		"approve": newInterruptNode("in", "decision", InterruptRequest{Kind: "approval", Prompt: "approve?"}),
		"post": &lambdaNode{
			inPorts:  []Port{{Name: "req"}},
			outPorts: []Port{{Name: "out"}},
			fn: func(ctx context.Context, in map[string]any) (map[string]any, error) {
				req := in["req"].(map[string]any)
				return map[string]any{"out": req["decision"]}, nil
			},
		},
	})
	f := Flow{
		ID:    "resume-map",
		Nodes: []Node{{ID: "entry", Type: "entry"}, {ID: "approve", Type: "approve"}, {ID: "post", Type: "post"}},
		Edges: []Edge{{Source: PortRef{"entry", "out"}, Target: PortRef{"approve", "in"}}},
		Mappings: []Mapping{{
			Source: MappingSource{Node: "approve", Port: "decision"},
			Target: MappingTarget{Node: "post", Port: "req", Path: []string{"decision"}},
		}},
		Inputs:  []NamedPortRef{{Name: "in", PortRef: PortRef{"entry", "in"}}},
		Outputs: []NamedPortRef{{Name: "out", PortRef: PortRef{"post", "out"}}},
	}
	store := NewMemoryCheckpointStore()
	eng, err := Compile(f, reg, Deps{}, WithCheckpointStore(store))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runID := NewRunID()
	res, err := eng.RunResumable(context.Background(), runID, map[string]any{"in": "request"})
	if err != nil {
		t.Fatalf("RunResumable: %v", err)
	}
	if res.Suspended == nil {
		t.Fatal("RunResumable did not suspend")
	}
	res2, err := eng.Resume(context.Background(), runID, res.Suspended.ResumeToken, map[string]any{"decision": "approved"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res2.Outputs["out"] != "approved" {
		t.Fatalf("resumed out = %v, want approved", res2.Outputs["out"])
	}
}
