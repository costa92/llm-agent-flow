package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
	sqlitestore "github.com/costa92/llm-agent-flow/flow/store/sqlite"
)

// TestStreamPayloadIncludesMetadata pins decision D2: when a node
// emits a FlowEvent with a non-empty Metadata map, streamPayload
// surfaces it under the JSON key "metadata".
func TestStreamPayloadIncludesMetadata(t *testing.T) {
	ev := flow.FlowEvent{
		Kind:     flow.NodeFinished,
		NodeID:   "n",
		Metadata: map[string]string{"http_status": "200", "bytes": "1024"},
	}
	got := streamPayload(ev)
	raw, ok := got["metadata"]
	if !ok {
		t.Fatalf("streamPayload(ev).[metadata] missing; payload=%v", got)
	}
	m, ok := raw.(map[string]string)
	if !ok {
		t.Fatalf("metadata is %T, want map[string]string", raw)
	}
	if m["http_status"] != "200" || m["bytes"] != "1024" {
		t.Fatalf("metadata = %v, want http_status=200 + bytes=1024", m)
	}
}

// TestStreamPayloadOmitsNilMetadata locks back-compat: events with no
// metadata MUST NOT carry a "metadata" key (v0.1.0-v0.1.2 SSE / replay
// consumers see byte-identical payloads).
func TestStreamPayloadOmitsNilMetadata(t *testing.T) {
	ev := flow.FlowEvent{Kind: flow.NodeFinished, NodeID: "n"}
	got := streamPayload(ev)
	if _, ok := got["metadata"]; ok {
		t.Fatalf("streamPayload(ev) carries metadata=%v on nil-metadata event; want key absent", got["metadata"])
	}
}

// TestStreamPayloadOmitsEmptyMetadata extends the back-compat lock:
// an empty (non-nil) map is treated the same as nil — no key emitted.
func TestStreamPayloadOmitsEmptyMetadata(t *testing.T) {
	ev := flow.FlowEvent{
		Kind:     flow.NodeFinished,
		NodeID:   "n",
		Metadata: map[string]string{},
	}
	got := streamPayload(ev)
	if _, ok := got["metadata"]; ok {
		t.Fatalf("streamPayload(ev) carries metadata=%v on empty-map event; want key absent", got["metadata"])
	}
}

// metaServerNode is the test fixture for the replay round-trip test:
// it's a MetadataAware NodeKind that echoes "in" → "out" and emits
// a fixed metadata map. Kept inside the test so production code stays
// free of demo-only node implementations (D3 — toolNode sample
// MetadataAware deferred to a follow-up PR).
type metaServerNode struct{}

func (metaServerNode) Inputs() []flow.Port  { return []flow.Port{{Name: "in"}} }
func (metaServerNode) Outputs() []flow.Port { return []flow.Port{{Name: "out"}} }
func (n metaServerNode) Run(ctx context.Context, in map[string]string) (map[string]string, error) {
	out, _, err := n.RunWithMetadata(ctx, in)
	return out, err
}
func (metaServerNode) RunWithMetadata(_ context.Context, in map[string]string) (map[string]string, map[string]string, error) {
	return map[string]string{"out": in["in"]},
		map[string]string{"http_status": "200", "duration_ms": "12"},
		nil
}

const metaFlowBody = `{
  "id": "meta_flow",
  "nodes": [
    { "id": "n", "type": "meta", "config": {} }
  ],
  "edges": [],
  "inputs":  [{ "name": "in",  "node": "n", "port": "in"  }],
  "outputs": [{ "name": "out", "node": "n", "port": "out" }]
}`

// TestReplayRoundTripsMetadata is the end-to-end pin: a flow that
// invokes a MetadataAware node persists metadata into run_events such
// that GET /runs/{id}/events surfaces the JSON-encoded metadata map.
// Covers both the engine → store persistence path and the
// streamPayload → SSE/JSON encoding path.
func TestReplayRoundTripsMetadata(t *testing.T) {
	store, err := sqlitestore.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	reg := flow.NewNodeRegistry()
	if err := reg.Register("meta", func(_ json.RawMessage, _ flow.Deps) (flow.NodeKind, error) {
		return metaServerNode{}, nil
	}); err != nil {
		t.Fatalf("Register meta: %v", err)
	}

	srv, err := New(Config{Store: store, Registry: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// POST the flow + run it (sync path).
	if _, err := http.Post(ts.URL+"/flows", "application/json",
		strings.NewReader(`{"flow":`+metaFlowBody+`}`)); err != nil {
		t.Fatalf("POST /flows: %v", err)
	}
	resp, err := http.Post(ts.URL+"/flows/meta_flow/run", "application/json",
		strings.NewReader(`{"inputs":{"in":"hello"}}`))
	if err != nil {
		t.Fatalf("POST /flows/meta_flow/run: %v", err)
	}
	resp.Body.Close()
	runID := resp.Header.Get("X-Run-ID")
	if runID == "" {
		t.Fatalf("missing X-Run-ID; status=%d", resp.StatusCode)
	}

	// Pull persisted events back via the public store API.
	events, err := store.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	// Find the node_finished entry and confirm its payload carries
	// the "metadata" key with the values metaServerNode emitted.
	found := false
	for _, ev := range events {
		if string(ev.Kind) != "node_finished" {
			continue
		}
		var p struct {
			Metadata map[string]string `json:"metadata"`
		}
		if jerr := json.Unmarshal(ev.Payload, &p); jerr != nil {
			t.Fatalf("decode node_finished payload: %v (raw=%s)", jerr, ev.Payload)
		}
		if p.Metadata == nil {
			t.Fatalf("node_finished payload has no metadata; raw=%s", ev.Payload)
		}
		if p.Metadata["http_status"] != "200" || p.Metadata["duration_ms"] != "12" {
			t.Fatalf("metadata = %v, want http_status=200 + duration_ms=12", p.Metadata)
		}
		found = true
	}
	if !found {
		t.Fatalf("no node_finished event in %d events", len(events))
	}
}
