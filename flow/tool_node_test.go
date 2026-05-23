package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// metaFakeTool is a Tool that also implements MetadataAwareTool. It
// returns a canned output / metadata / error tuple and records the
// last raw args it was called with, so tests can assert wiring.
type metaFakeTool struct {
	name string
	out  string
	meta map[string]string
	err  error

	gotArgs json.RawMessage
}

func (t *metaFakeTool) Name() string { return t.name }

// Execute satisfies flow.Tool — delegate to ExecuteWithMetadata to
// confirm the legacy path keeps working when the toolNode falls
// through to it.
func (t *metaFakeTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	out, _, err := t.ExecuteWithMetadata(ctx, args)
	return out, err
}

func (t *metaFakeTool) ExecuteWithMetadata(_ context.Context, args json.RawMessage) (string, map[string]string, error) {
	t.gotArgs = args
	return t.out, t.meta, t.err
}

// plainFakeTool implements only flow.Tool (no metadata capability).
// The toolNode should fall through to Execute and return nil metadata.
type plainFakeTool struct {
	name string
	out  string
	err  error
}

func (t *plainFakeTool) Name() string { return t.name }
func (t *plainFakeTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	return t.out, t.err
}

// newToolNode builds a toolNode from an inline Tool via the registered
// factory. This pins that the registered "tool" type is what we're
// exercising — not a hand-rolled internal struct.
func newToolNode(t *testing.T, tool Tool) NodeKind {
	t.Helper()
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	cfg, _ := json.Marshal(map[string]string{"tool": tool.Name()})
	deps := Deps{Tools: ToolMap{tool.Name(): tool}}
	kind, err := reg.Build(Node{ID: "n", Type: TypeTool, Config: cfg}, deps)
	if err != nil {
		t.Fatalf("registry.Build: %v", err)
	}
	return kind
}

// TestToolNode_RunWithMetadata_ForwardsMetadataFromAwareTool confirms
// the happy path: when the wrapped Tool implements MetadataAwareTool,
// the toolNode surfaces the metadata up through RunWithMetadata.
func TestToolNode_RunWithMetadata_ForwardsMetadataFromAwareTool(t *testing.T) {
	tool := &metaFakeTool{
		name: "fake",
		out:  "ok",
		meta: map[string]string{"exit_code": "0", "duration_ms": "7"},
	}
	kind := newToolNode(t, tool)
	ma, ok := kind.(MetadataAware)
	if !ok {
		t.Fatalf("toolNode does not implement MetadataAware")
	}
	out, meta, err := ma.RunWithMetadata(context.Background(), map[string]string{"input": "hello"})
	if err != nil {
		t.Fatalf("RunWithMetadata err = %v", err)
	}
	if out["output"] != "ok" {
		t.Fatalf("out = %v, want output=ok", out)
	}
	if meta == nil || meta["exit_code"] != "0" || meta["duration_ms"] != "7" {
		t.Fatalf("meta = %v, want exit_code=0 + duration_ms=7", meta)
	}
}

// TestToolNode_RunWithMetadata_ReturnsNilMetadataForPlainTool pins the
// fallback: a plain flow.Tool yields meta == nil.
func TestToolNode_RunWithMetadata_ReturnsNilMetadataForPlainTool(t *testing.T) {
	tool := &plainFakeTool{name: "plain", out: "ok"}
	kind := newToolNode(t, tool)
	ma, ok := kind.(MetadataAware)
	if !ok {
		t.Fatalf("toolNode does not implement MetadataAware")
	}
	out, meta, err := ma.RunWithMetadata(context.Background(), map[string]string{"input": "x"})
	if err != nil {
		t.Fatalf("RunWithMetadata err = %v", err)
	}
	if out["output"] != "ok" {
		t.Fatalf("out = %v, want output=ok", out)
	}
	if meta != nil {
		t.Fatalf("meta = %v, want nil (plain Tool path)", meta)
	}
}

// TestToolNode_RunWithMetadata_PreservesMetadataOnError mirrors D1:
// when an aware tool returns (out, meta, err), the toolNode surfaces
// (nil, meta, err) so engine traces keep their signal even on failure.
func TestToolNode_RunWithMetadata_PreservesMetadataOnError(t *testing.T) {
	boom := errors.New("upstream 500")
	tool := &metaFakeTool{
		name: "boom",
		out:  "",
		meta: map[string]string{"http_status": "500", "bytes": "12"},
		err:  boom,
	}
	kind := newToolNode(t, tool)
	ma, ok := kind.(MetadataAware)
	if !ok {
		t.Fatalf("toolNode does not implement MetadataAware")
	}
	out, meta, err := ma.RunWithMetadata(context.Background(), map[string]string{"input": "x"})
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want chain to %v", err, boom)
	}
	if out != nil {
		t.Fatalf("out = %v, want nil on error", out)
	}
	if meta == nil || meta["http_status"] != "500" || meta["bytes"] != "12" {
		t.Fatalf("meta = %v, want http_status=500 + bytes=12 (D1: error-path metadata preserved)", meta)
	}
}
