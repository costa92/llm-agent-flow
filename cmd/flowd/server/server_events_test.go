package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

// expectedKindsForEchoChain is the FlowEvent sequence the echo_chain
// flow produces under v0.0.6 semantics:
//   - flow_started
//   - node_started (upper) → node_finished (upper)
//   - node_started (reverse) → node_finished (reverse)
//   - flow_done
var expectedKindsForEchoChain = []flowstore.RunEventKind{
	flowstore.RunEventFlowStarted,
	flowstore.RunEventNodeStarted,
	flowstore.RunEventNodeFinished,
	flowstore.RunEventNodeStarted,
	flowstore.RunEventNodeFinished,
	flowstore.RunEventFlowDone,
}

func TestSyncRunPersistsEveryEvent(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	runID := resp.Header.Get("X-Run-ID")

	events, err := store.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(events) != len(expectedKindsForEchoChain) {
		t.Fatalf("events = %d, want %d (%+v)", len(events), len(expectedKindsForEchoChain), events)
	}
	for i, want := range expectedKindsForEchoChain {
		if events[i].Kind != want {
			t.Fatalf("event[%d].Kind = %q, want %q", i, events[i].Kind, want)
		}
		if events[i].Seq != i+1 {
			t.Fatalf("event[%d].Seq = %d, want %d", i, events[i].Seq, i+1)
		}
	}

	// FlowDone payload should carry the declared outputs.
	final := events[len(events)-1]
	var p struct {
		Outputs map[string]string `json:"outputs"`
	}
	if err := json.Unmarshal(final.Payload, &p); err != nil {
		t.Fatalf("FlowDone payload decode: %v", err)
	}
	if p.Outputs["out"] != "OLLEH" {
		t.Fatalf("flow_done payload outputs = %+v, want out=OLLEH", p)
	}
}

func TestStreamRunPersistsEveryEventAndStreamsToClient(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)

	req, _ := http.NewRequest("POST", srv.URL+"/flows/echo_chain/run/stream", strings.NewReader(`{"inputs":{"in":"hello"}}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST stream: %v", err)
	}
	defer resp.Body.Close()
	runID := resp.Header.Get("X-Run-ID")
	if runID == "" {
		t.Fatal("X-Run-ID missing on stream response")
	}

	// Consume SSE so the response is fully drained before we query
	// the store (otherwise the server may not have written the final
	// event yet).
	var sseFrames int
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "event: ") {
			sseFrames++
		}
	}
	if sseFrames != len(expectedKindsForEchoChain) {
		t.Fatalf("SSE frames = %d, want %d", sseFrames, len(expectedKindsForEchoChain))
	}

	events, err := store.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(events) != len(expectedKindsForEchoChain) {
		t.Fatalf("persisted events = %d, want %d", len(events), len(expectedKindsForEchoChain))
	}
}

func TestRunEventsHTTPEndpoint(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/events")
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (body=%s)", resp.StatusCode, raw)
	}
	var body struct {
		Events []flowstore.RunEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Events) != len(expectedKindsForEchoChain) {
		t.Fatalf("events len = %d, want %d", len(body.Events), len(expectedKindsForEchoChain))
	}
	for i, want := range expectedKindsForEchoChain {
		if body.Events[i].Kind != want {
			t.Fatalf("event[%d].Kind = %q, want %q", i, body.Events[i].Kind, want)
		}
	}
}

func TestRunEventsForFailedRunIncludesFlowErr(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	// Missing required input "in" → flow_err event after flow_started.
	resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{}}`)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	runID := resp.Header.Get("X-Run-ID")

	events, err := store.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("events = %d, want >= 2 (flow_started + flow_err)", len(events))
	}
	if events[0].Kind != flowstore.RunEventFlowStarted {
		t.Fatalf("first kind = %q, want flow_started", events[0].Kind)
	}
	last := events[len(events)-1]
	if last.Kind != flowstore.RunEventFlowErr {
		t.Fatalf("last kind = %q, want flow_err", last.Kind)
	}
	var p struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(last.Payload, &p)
	if !strings.Contains(p.Error, "missing required input") {
		t.Fatalf("flow_err payload error = %q, want missing-input", p.Error)
	}
}

func TestRunEventsRouterIncludesNodeSkipped(t *testing.T) {
	// The router flow's "other_path" should be skipped when input
	// matches "greet" — this is the smoke test for NodeSkipped event
	// persistence.
	const routerFlow = `{
		"id":"router",
		"nodes":[
			{"id":"classify","type":"tool","config":{"tool":"classify"}},
			{"id":"greet_path","type":"tool","config":{"tool":"make_greeting"}},
			{"id":"other_path","type":"tool","config":{"tool":"say_other"}}
		],
		"edges":[
			{"source":{"node":"classify","port":"output"},"target":{"node":"greet_path","port":"input"},"condition":"value == \"greet\""},
			{"source":{"node":"classify","port":"output"},"target":{"node":"other_path","port":"input"},"condition":"value != \"greet\""}
		],
		"inputs":[{"name":"in","node":"classify","port":"input"}],
		"outputs":[
			{"name":"greeting","node":"greet_path","port":"output"},
			{"name":"other","node":"other_path","port":"output"}
		]
	}`
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+routerFlow+`}`)
	resp := mustPOST(t, srv.URL+"/flows/router/run", `{"inputs":{"in":"hello world"}}`)
	defer resp.Body.Close()
	runID := resp.Header.Get("X-Run-ID")

	events, err := store.ListRunEvents(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	var sawSkipped bool
	for _, ev := range events {
		if ev.Kind == flowstore.RunEventNodeSkipped && ev.NodeID == "other_path" {
			sawSkipped = true
		}
	}
	if !sawSkipped {
		kinds := make([]flowstore.RunEventKind, len(events))
		for i, ev := range events {
			kinds[i] = ev.Kind
		}
		t.Fatalf("no node_skipped(other_path) in events: %+v", kinds)
	}
}

func TestRunEventsHTTPMissingRunReturnsEmpty(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/runs/missing/events")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	// ListRunEvents against an unknown run returns an empty slice,
	// not 404, so the endpoint stays idempotent for replay clients.
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Events []flowstore.RunEvent `json:"events"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if len(body.Events) != 0 {
		t.Fatalf("events = %d, want 0 for unknown run", len(body.Events))
	}
}
