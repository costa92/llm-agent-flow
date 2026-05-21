package server_test

import (
	"bufio"
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
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	tools := flow.FromAgentTools(echochain.Tools())
	src := strings.NewReader(`{
		"id":"echo_chain","nodes":[
			{"id":"upper","type":"tool","config":{"tool":"upper"}},
			{"id":"reverse","type":"tool","config":{"tool":"reverse"}}
		],"edges":[
			{"source":{"node":"upper","port":"output"},"target":{"node":"reverse","port":"input"}}
		],
		"inputs":[{"name":"in","node":"upper","port":"input"}],
		"outputs":[{"name":"out","node":"reverse","port":"output"}]
	}`)
	engine, err := flow.LoadCompile(src, reg, flow.Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	return httptest.NewServer(server.NewMux(engine, nil))
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
}

func TestRunSync(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	body := []byte(`{"inputs":{"in":"hello"}}`)
	resp, err := http.Post(srv.URL+"/run", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s, want 200", resp.StatusCode, got)
	}
	var out struct {
		Outputs map[string]string `json:"outputs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Outputs["out"] != "OLLEH" {
		t.Fatalf("outputs.out = %q, want OLLEH", out.Outputs["out"])
	}
}

func TestRunMissingInputIs400(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/run", "application/json", strings.NewReader(`{"inputs":{}}`))
	if err != nil {
		t.Fatalf("POST /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&er)
	if !strings.Contains(er.Error, "missing required input") {
		t.Fatalf("error = %q, want missing-input message", er.Error)
	}
}

func TestRunBadJSONIs400(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/run", "application/json", strings.NewReader(`not json`))
	if err != nil {
		t.Fatalf("POST /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRunMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/run")
	if err != nil {
		t.Fatalf("GET /run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestRunStreamSSE(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/run/stream", strings.NewReader(`{"inputs":{"in":"hello"}}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /run/stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Parse SSE frames: blocks separated by blank lines. Each block
	// has `event: <kind>` and `data: <json>` lines.
	type frame struct {
		Event string
		Data  string
	}
	var frames []frame
	sc := bufio.NewScanner(resp.Body)
	var cur frame
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			cur.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.Data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if cur.Event != "" {
				frames = append(frames, cur)
				cur = frame{}
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Expect FlowStarted, NodeStarted/NodeFinished×2, FlowDone.
	wantOrder := []string{
		"flow_started",
		"node_started", "node_finished",
		"node_started", "node_finished",
		"flow_done",
	}
	if len(frames) != len(wantOrder) {
		t.Fatalf("frame count = %d, want %d (frames=%+v)", len(frames), len(wantOrder), frames)
	}
	for i, want := range wantOrder {
		if frames[i].Event != want {
			t.Fatalf("frame[%d].Event = %q, want %q (frames=%+v)", i, frames[i].Event, want, frames)
		}
	}
	// FlowDone should carry the declared outputs.
	final := frames[len(frames)-1]
	var payload map[string]any
	if err := json.Unmarshal([]byte(final.Data), &payload); err != nil {
		t.Fatalf("decode final data: %v", err)
	}
	outs, _ := payload["outputs"].(map[string]any)
	if outs["out"] != "OLLEH" {
		t.Fatalf("flow_done outputs = %+v, want out=OLLEH", outs)
	}
}
