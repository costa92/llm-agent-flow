package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/cmd/flowd/server"
	v2flow "github.com/costa92/llm-agent-flow/v2/flow"
	v2sqlite "github.com/costa92/llm-agent-flow/v2/flow/store/sqlite"
)

// hitlFlow is a v2 HITL flow: passthrough "pre" → approval "gate" →
// passthrough "post". The approval node interrupts; on resume the engine
// injects humanInput["decision"] as gate.decision, which the
// gate.decision → post.in edge wires into post.
const hitlFlow = `{
  "id": "hitl",
  "name": "hitl approval",
  "nodes": [
    { "id": "pre",  "type": "passthrough" },
    { "id": "gate", "type": "approval" },
    { "id": "post", "type": "passthrough" }
  ],
  "edges": [
    { "source": { "node": "pre",  "port": "out" },      "target": { "node": "gate", "port": "in" } },
    { "source": { "node": "gate", "port": "decision" }, "target": { "node": "post", "port": "in" } }
  ],
  "inputs":  [{ "name": "in",  "node": "pre",  "port": "in"  }],
  "outputs": [{ "name": "out", "node": "post", "port": "out" }]
}`

// --- test-local v2 node kinds (mirror cmd/flowd/v2nodes.go) ---

type tPassthrough struct{}

func (tPassthrough) Inputs() []v2flow.Port  { return []v2flow.Port{{Name: "in"}} }
func (tPassthrough) Outputs() []v2flow.Port { return []v2flow.Port{{Name: "out"}} }
func (tPassthrough) Run(_ context.Context, in map[string]any) (map[string]any, error) {
	return map[string]any{"out": in["in"]}, nil
}

type tApproval struct{}

func (tApproval) Inputs() []v2flow.Port  { return []v2flow.Port{{Name: "in"}} }
func (tApproval) Outputs() []v2flow.Port { return []v2flow.Port{{Name: "decision"}} }
func (tApproval) CanInterrupt() bool     { return true }
func (tApproval) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	return nil, v2flow.Interrupt(v2flow.InterruptRequest{Kind: "approval", Prompt: "approve?"})
}

// withV2 wires the resumable (HITL) path into the test server: a registry
// holding the passthrough/approval node kinds and a fresh sqlite checkpoint
// store. The checkpoint store uses its own :memory: DSN (a single-conn
// in-memory DB) — separate from the v0.1 runs/events store, which is fine
// since checkpoints live in their own table.
func withV2(t *testing.T) serverOption {
	t.Helper()
	reg := v2flow.NewNodeRegistry()
	if err := reg.Register("passthrough", func(json.RawMessage, v2flow.Deps) (v2flow.NodeKind, error) {
		return tPassthrough{}, nil
	}); err != nil {
		t.Fatalf("register passthrough: %v", err)
	}
	if err := reg.Register("approval", func(json.RawMessage, v2flow.Deps) (v2flow.NodeKind, error) {
		return tApproval{}, nil
	}); err != nil {
		t.Fatalf("register approval: %v", err)
	}
	cp, err := v2sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("open v2 checkpoint store: %v", err)
	}
	t.Cleanup(func() { _ = cp.Close() })
	return func(cfg *server.Config) {
		cfg.V2Registry = reg
		cfg.V2Checkpoints = cp
	}
}

func TestFlowd_SuspendedRunNotFinalized(t *testing.T) {
	srv, store := newStoreServer(t, withV2(t))
	defer srv.Close()
	// Seed the flow directly through the store: the HTTP POST /flows
	// path runs a v0.1 compile-probe that (correctly) rejects v2-only
	// node types like passthrough/approval. The store handle is exposed
	// by newStoreServer precisely for this kind of test seeding.
	if _, err := store.PutFlow(context.Background(), "hitl", "hitl approval", []byte(hitlFlow), false); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	resp := mustPOST(t, srv.URL+"/flows/hitl/run", `{"resumable":true,"inputs":{"in":"x"}}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	runID := resp.Header.Get("X-Run-ID")
	if runID == "" {
		t.Fatal("missing X-Run-ID")
	}

	var rr struct {
		Suspended *struct {
			ResumeToken string `json:"resume_token"`
			NodeID      string `json:"node"`
		} `json:"suspended"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rr.Suspended == nil {
		t.Fatal("response.suspended is nil, want set")
	}
	if rr.Suspended.ResumeToken != runID {
		t.Fatalf("resume_token = %q, want runID %q", rr.Suspended.ResumeToken, runID)
	}
	if rr.Suspended.NodeID != "gate" {
		t.Fatalf("suspended.node = %q, want gate", rr.Suspended.NodeID)
	}

	// GET /runs/{runID} → suspended, resume_token set, NOT finalized.
	getResp, err := http.Get(srv.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET run: %v", err)
	}
	defer getResp.Body.Close()
	var run struct {
		Status      string  `json:"status"`
		ResumeToken string  `json:"resume_token"`
		FinishedAt  *string `json:"finished_at"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if run.Status != "suspended" {
		t.Fatalf("status = %q, want suspended", run.Status)
	}
	if run.ResumeToken != runID {
		t.Fatalf("run.resume_token = %q, want %q", run.ResumeToken, runID)
	}
	if run.FinishedAt != nil {
		t.Fatalf("finished_at = %v, want nil (not finalized)", *run.FinishedAt)
	}
}

func TestFlowd_ResumeDrivesToDone(t *testing.T) {
	srv, store := newStoreServer(t, withV2(t))
	defer srv.Close()
	if _, err := store.PutFlow(context.Background(), "hitl", "hitl approval", []byte(hitlFlow), false); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	startResp := mustPOST(t, srv.URL+"/flows/hitl/run", `{"resumable":true,"inputs":{"in":"x"}}`)
	runID := startResp.Header.Get("X-Run-ID")
	startResp.Body.Close()
	if runID == "" {
		t.Fatal("missing X-Run-ID")
	}

	// Resume with the human decision.
	resumeResp := mustPOST(t, srv.URL+"/flows/hitl/runs/"+runID+"/resume", `{"decision":"approved"}`)
	defer resumeResp.Body.Close()
	if resumeResp.StatusCode != 200 {
		t.Fatalf("resume status = %d, want 200", resumeResp.StatusCode)
	}
	var rr struct {
		Outputs   map[string]string `json:"outputs"`
		Suspended *json.RawMessage  `json:"suspended"`
	}
	if err := json.NewDecoder(resumeResp.Body).Decode(&rr); err != nil {
		t.Fatalf("decode resume: %v", err)
	}
	if rr.Suspended != nil {
		t.Fatalf("resume.suspended = %s, want nil", *rr.Suspended)
	}
	if rr.Outputs["out"] != "approved" {
		t.Fatalf("outputs.out = %q, want approved", rr.Outputs["out"])
	}

	// GET /runs/{runID} → done, finished_at non-nil.
	getResp, err := http.Get(srv.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET run: %v", err)
	}
	defer getResp.Body.Close()
	var run struct {
		Status     string  `json:"status"`
		FinishedAt *string `json:"finished_at"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if run.Status != "done" {
		t.Fatalf("status = %q, want done", run.Status)
	}
	if run.FinishedAt == nil {
		t.Fatal("finished_at = nil, want non-nil")
	}

	// Replay → events in order flow_started, node_interrupted,
	// flow_suspended, flow_done; flow_suspended carries resume_token + request.
	replayResp, err := http.Post(srv.URL+"/runs/"+runID+"/replay", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer replayResp.Body.Close()
	if replayResp.StatusCode != 200 {
		t.Fatalf("replay status = %d, want 200", replayResp.StatusCode)
	}

	type frame struct {
		event string
		data  string
	}
	var frames []frame
	sc := bufio.NewScanner(replayResp.Body)
	var cur frame
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			cur.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if cur.event != "" {
				frames = append(frames, cur)
				cur = frame{}
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	wantOrder := []string{"flow_started", "node_interrupted", "flow_suspended", "flow_done"}
	if len(frames) != len(wantOrder) {
		t.Fatalf("frame count = %d, want %d (frames=%+v)", len(frames), len(wantOrder), frames)
	}
	for i, want := range wantOrder {
		if frames[i].event != want {
			t.Fatalf("frame[%d].event = %q, want %q (frames=%+v)", i, frames[i].event, want, frames)
		}
	}
	suspendedData := frames[2].data
	if !strings.Contains(suspendedData, "resume_token") {
		t.Fatalf("flow_suspended data missing resume_token: %s", suspendedData)
	}
	if !strings.Contains(suspendedData, "request") {
		t.Fatalf("flow_suspended data missing request: %s", suspendedData)
	}
}
