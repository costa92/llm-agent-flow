package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/cmd/flowd/server"
	"github.com/costa92/llm-agent-flow/examples/echo_chain"
	"github.com/costa92/llm-agent-flow/flow"
	cond "github.com/costa92/llm-agent-flow/flow/cond/cel"
	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	sqlitestore "github.com/costa92/llm-agent-flow/flow/store/sqlite"
)

// echoChainFlow is reused across CRUD tests as the canonical valid
// flow body.
const echoChainFlow = `{
  "id": "echo_chain",
  "name": "echo chain",
  "nodes": [
    { "id": "upper",   "type": "tool", "config": { "tool": "upper" } },
    { "id": "reverse", "type": "tool", "config": { "tool": "reverse" } }
  ],
  "edges": [
    { "source": { "node": "upper",   "port": "output" },
      "target": { "node": "reverse", "port": "input"  } }
  ],
  "inputs":  [{ "name": "in",  "node": "upper",   "port": "input"  }],
  "outputs": [{ "name": "out", "node": "reverse", "port": "output" }]
}`

func newStoreServer(t *testing.T, opts ...serverOption) (*httptest.Server, flowstore.Store) {
	t.Helper()
	store, err := sqlitestore.Open(":memory:")
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := flow.FromAgentTools(echochain.Tools())
	celEval, err := cond.NewEvaluator()
	if err != nil {
		t.Fatalf("cel: %v", err)
	}
	cfg := server.Config{
		Store:    store,
		Registry: reg,
		Tools:    tools,
		Cond:     celEval,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(srv.Close)
	return httptest.NewServer(srv.Handler()), store
}

type serverOption func(*server.Config)

func withLegacyFlowID(id string) serverOption {
	return func(cfg *server.Config) { cfg.LegacyFlowID = id }
}

func mustPOST(t *testing.T, url string, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func TestCreateFlowHappyPath(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	body := `{"flow":` + echoChainFlow + `}`
	resp := mustPOST(t, srv.URL+"/flows", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body=%s)", resp.StatusCode, raw)
	}
	var rec flowstore.FlowRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.ID != "echo_chain" || rec.Name != "echo chain" {
		t.Fatalf("rec = %+v, want echo_chain", rec)
	}
}

func TestCreateFlowConflictIs409(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	body := `{"flow":` + echoChainFlow + `}`
	if r := mustPOST(t, srv.URL+"/flows", body); r.StatusCode != 201 {
		t.Fatalf("first create status = %d, want 201", r.StatusCode)
	}
	resp := mustPOST(t, srv.URL+"/flows", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second create status = %d, want 409", resp.StatusCode)
	}
}

func TestCreateFlowBadCompileIs400(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	const bad = `{"flow":{"id":"x","nodes":[{"id":"a","type":"unknown"}],"edges":[]}}`
	resp := mustPOST(t, srv.URL+"/flows", bad)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListFlows(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)

	resp, err := http.Get(srv.URL + "/flows")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Flows []flowstore.FlowMeta `json:"flows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Flows) != 1 || got.Flows[0].ID != "echo_chain" {
		t.Fatalf("flows = %+v, want one echo_chain", got.Flows)
	}
}

func TestGetFlow(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	resp, err := http.Get(srv.URL + "/flows/echo_chain")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestGetFlow404(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flows/missing")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestPutFlowReplacesAndInvalidatesCache(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)

	// Run once to seed the engine cache.
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runResp.Body.Close()

	// PUT a different flow body — same id, swap "reverse" for
	// "upper" so the output now uppercases twice.
	const swappedFlow = `{
		"id": "echo_chain",
		"name": "swapped",
		"nodes": [
			{ "id": "upper",   "type": "tool", "config": { "tool": "upper" } },
			{ "id": "reverse", "type": "tool", "config": { "tool": "upper" } }
		],
		"edges": [
			{ "source": {"node":"upper","port":"output"},
			  "target": {"node":"reverse","port":"input"} }
		],
		"inputs":  [{ "name": "in",  "node": "upper",   "port": "input"  }],
		"outputs": [{ "name": "out", "node": "reverse", "port": "output" }]
	}`
	req, _ := http.NewRequest("PUT", srv.URL+"/flows/echo_chain", strings.NewReader(`{"flow":`+swappedFlow+`}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	// Verify the new compile took effect (cache invalidated).
	rec, _ := store.GetFlow(context.Background(), "echo_chain")
	if !strings.Contains(string(rec.JSON), `"name": "swapped"`) {
		t.Fatalf("PUT did not replace flow JSON: %s", rec.JSON)
	}
	// Run with the new flow — both tools are now "upper" so output
	// is the doubly-uppercased input.
	runResp = mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	defer runResp.Body.Close()
	var rb struct {
		Outputs map[string]string `json:"outputs"`
	}
	_ = json.NewDecoder(runResp.Body).Decode(&rb)
	if rb.Outputs["out"] != "HELLO" {
		t.Fatalf("post-PUT out = %q, want HELLO (cache should have been evicted)", rb.Outputs["out"])
	}
}

func TestDeleteFlow(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)

	req, _ := http.NewRequest("DELETE", srv.URL+"/flows/echo_chain", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	getResp, err := http.Get(srv.URL + "/flows/echo_chain")
	if err != nil {
		t.Fatalf("post-delete GET: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != 404 {
		t.Fatalf("post-delete GET status = %d, want 404", getResp.StatusCode)
	}
}

func TestRunWithStoreRecordsRun(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (body=%s)", resp.StatusCode, raw)
	}
	runID := resp.Header.Get("X-Run-ID")
	if runID == "" {
		t.Fatal("X-Run-ID header missing")
	}

	rec, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if rec.Status != flowstore.RunStatusDone {
		t.Fatalf("status = %q, want done", rec.Status)
	}
	if rec.Outputs["out"] != "OLLEH" {
		t.Fatalf("outputs = %+v, want OLLEH", rec.Outputs)
	}
	if rec.FinishedAt == nil {
		t.Fatal("FinishedAt nil — run not finalized")
	}
}

func TestListRunsForFlow(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	for i := 0; i < 3; i++ {
		resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hi"}}`)
		resp.Body.Close()
	}
	resp, err := http.Get(srv.URL + "/flows/echo_chain/runs")
	if err != nil {
		t.Fatalf("GET runs: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Runs []flowstore.RunMeta `json:"runs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Runs) != 3 {
		t.Fatalf("runs len = %d, want 3", len(got.Runs))
	}
}

func TestGetRunRecord(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runID := resp.Header.Get("X-Run-ID")
	resp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var rec flowstore.RunRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.ID != runID || rec.Outputs["out"] != "OLLEH" {
		t.Fatalf("rec = %+v, want correct round-trip", rec)
	}
}

func TestRunMissingInputRecordsFailedRun(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)

	// Missing required input "in" → run fails.
	resp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{}}`)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	runID := resp.Header.Get("X-Run-ID")
	if runID == "" {
		t.Fatal("X-Run-ID header missing on failed run")
	}
	rec, _ := store.GetRun(context.Background(), runID)
	if rec.Status != flowstore.RunStatusFailed {
		t.Fatalf("status = %q, want failed", rec.Status)
	}
	if !strings.Contains(rec.Error, "missing required input") {
		t.Fatalf("error = %q, want missing-input message", rec.Error)
	}
}

func TestLegacyRunRoutesToSeededFlow(t *testing.T) {
	store, _ := sqlitestore.Open(":memory:")
	defer store.Close()
	// Seed before constructing the server.
	if _, err := store.PutFlow(context.Background(), "echo_chain", "echo chain", []byte(echoChainFlow), false); err != nil {
		t.Fatalf("seed: %v", err)
	}
	reg := flow.NewNodeRegistry()
	_ = flow.RegisterToolNode(reg)
	tools := flow.FromAgentTools(echochain.Tools())
	celEval, _ := cond.NewEvaluator()
	srv, _ := server.New(server.Config{
		Store:        store,
		Registry:     reg,
		Tools:        tools,
		Cond:         celEval,
		LegacyFlowID: "echo_chain",
	})
	defer srv.Close()
	hts := httptest.NewServer(srv.Handler())
	defer hts.Close()

	resp, err := http.Post(hts.URL+"/run", "application/json", bytes.NewReader([]byte(`{"inputs":{"in":"hello"}}`)))
	if err != nil {
		t.Fatalf("legacy POST /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (body=%s), want 200", resp.StatusCode, raw)
	}
}

func TestSilenceWithLegacyFlowID(t *testing.T) {
	// Without LegacyFlowID, /run should 404 since no route is
	// registered.
	srv, _ := newStoreServer(t, withLegacyFlowID(""))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/run", "application/json", strings.NewReader(`{"inputs":{}}`))
	if err != nil {
		t.Fatalf("POST /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404 (no legacy route)", resp.StatusCode)
	}
}
