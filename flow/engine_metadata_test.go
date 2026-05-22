package flow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// metaNode is a hand-rolled NodeKind that also implements MetadataAware.
// It's the canonical test fixture for engine metadata propagation: it
// echoes its single "in" port to "out" and surfaces a fixed metadata
// map alongside. failWith makes it return an error while still
// publishing metadata (the D1 contract: error path keeps metadata).
type metaNode struct {
	meta     map[string]string
	failWith error
}

func (n *metaNode) Inputs() []Port  { return []Port{{Name: "in"}} }
func (n *metaNode) Outputs() []Port { return []Port{{Name: "out"}} }
func (n *metaNode) Run(ctx context.Context, in map[string]string) (map[string]string, error) {
	out, _, err := n.RunWithMetadata(ctx, in)
	return out, err
}
func (n *metaNode) RunWithMetadata(_ context.Context, in map[string]string) (map[string]string, map[string]string, error) {
	if n.failWith != nil {
		// D1: error path returns nil out but keeps metadata.
		return nil, n.meta, n.failWith
	}
	return map[string]string{"out": in["in"]}, n.meta, nil
}

// plainNode is a NodeKind without MetadataAware — confirms the engine
// fast-path leaves Metadata == nil for non-aware nodes.
type plainNode struct{}

func (plainNode) Inputs() []Port  { return []Port{{Name: "in"}} }
func (plainNode) Outputs() []Port { return []Port{{Name: "out"}} }
func (plainNode) Run(_ context.Context, in map[string]string) (map[string]string, error) {
	return map[string]string{"out": in["in"]}, nil
}

// metaFlowJSON is a single-node flow used by all three metadata
// engine tests. The node is type "meta" — tests register a factory
// that returns either metaNode or plainNode for each scenario.
const metaFlowJSON = `{
  "id": "meta_flow",
  "nodes": [
    { "id": "n", "type": "meta", "config": {} }
  ],
  "edges": [],
  "inputs":  [{ "name": "in",  "node": "n", "port": "in"  }],
  "outputs": [{ "name": "out", "node": "n", "port": "out" }]
}`

func compileMetaEngine(t *testing.T, kind NodeKind) *Engine {
	t.Helper()
	reg := NewNodeRegistry()
	if err := reg.Register("meta", func(_ json.RawMessage, _ Deps) (NodeKind, error) {
		return kind, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	eng, err := LoadCompile(strings.NewReader(metaFlowJSON), reg, Deps{})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	return eng
}

// TestEngineEmitsMetadataForMetadataAwareNode confirms the happy path:
// a MetadataAware node's RunWithMetadata return value is propagated to
// the NodeFinished event's Metadata field.
func TestEngineEmitsMetadataForMetadataAwareNode(t *testing.T) {
	eng := compileMetaEngine(t, &metaNode{
		meta: map[string]string{"http_status": "200", "tokens": "42"},
	})
	ch, err := eng.RunStream(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	var got map[string]string
	for ev := range ch {
		if ev.Kind == NodeFinished && ev.NodeID == "n" {
			got = ev.Metadata
		}
	}
	if got == nil {
		t.Fatalf("NodeFinished.Metadata = nil, want populated map")
	}
	if got["http_status"] != "200" || got["tokens"] != "42" {
		t.Fatalf("NodeFinished.Metadata = %v, want http_status=200 + tokens=42", got)
	}
}

// TestEngineEmitsNilMetadataForPlainNode pins the zero-behavior
// regression: a non-MetadataAware node MUST produce NodeFinished with
// Metadata == nil (not an empty map). v0.1.0-v0.1.2 callers that
// inspect FlowEvent.Metadata can rely on nil to mean "node didn't
// publish any".
func TestEngineEmitsNilMetadataForPlainNode(t *testing.T) {
	eng := compileMetaEngine(t, plainNode{})
	ch, err := eng.RunStream(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	for ev := range ch {
		if ev.Kind == NodeFinished && ev.NodeID == "n" {
			if ev.Metadata != nil {
				t.Fatalf("plain NodeFinished.Metadata = %v, want nil", ev.Metadata)
			}
		}
	}
}

// TestEngineKeepsMetadataOnErrorPath nails decision D1: when a
// MetadataAware node fails, the NodeFinished event still carries the
// metadata it managed to publish so traces / dashboards retain useful
// signal (e.g. HTTP 500 with the status code attached).
func TestEngineKeepsMetadataOnErrorPath(t *testing.T) {
	boom := errors.New("upstream 500")
	eng := compileMetaEngine(t, &metaNode{
		meta:     map[string]string{"http_status": "500"},
		failWith: boom,
	})
	ch, err := eng.RunStream(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	var (
		gotMeta map[string]string
		gotErr  error
	)
	for ev := range ch {
		if ev.Kind == NodeFinished && ev.NodeID == "n" {
			gotMeta = ev.Metadata
			gotErr = ev.Err
		}
	}
	if gotErr == nil || !errors.Is(gotErr, boom) {
		t.Fatalf("NodeFinished.Err = %v, want chain to %v", gotErr, boom)
	}
	if gotMeta == nil || gotMeta["http_status"] != "500" {
		t.Fatalf("NodeFinished.Metadata = %v, want http_status=500 (D1: error-path metadata preserved)", gotMeta)
	}
}
