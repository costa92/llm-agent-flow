package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

type debugRunNodeBody struct {
	ID          string          `json:"id"`
	Status      string          `json:"status"`
	StartedSeq  int             `json:"started_seq"`
	FinishedSeq int             `json:"finished_seq"`
	Input       json.RawMessage `json:"input"`
	Output      json.RawMessage `json:"output"`
	Error       string          `json:"error"`
}

func TestDebugFlowEndpointReturnsGraphAndRuns(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/flows/echo_chain/debug")
	if err != nil {
		t.Fatalf("GET debug: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got struct {
		Flow struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"flow"`
		Graph struct {
			Nodes []struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			} `json:"nodes"`
			Edges []struct {
				Source struct {
					Node string `json:"node"`
					Port string `json:"port"`
				} `json:"source"`
				Target struct {
					Node string `json:"node"`
					Port string `json:"port"`
				} `json:"target"`
			} `json:"edges"`
			Inputs []struct {
				Name string `json:"name"`
				Ref  struct {
					Node string `json:"node"`
					Port string `json:"port"`
				} `json:"ref"`
			} `json:"inputs"`
			Outputs []struct {
				Name string `json:"name"`
				Ref  struct {
					Node string `json:"node"`
					Port string `json:"port"`
				} `json:"ref"`
			} `json:"outputs"`
		} `json:"graph"`
		Runs []struct {
			ID     string `json:"id"`
			FlowID string `json:"flow_id"`
			Status string `json:"status"`
		} `json:"runs"`
		RawJSON json.RawMessage `json:"raw_json"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Flow.ID != "echo_chain" || got.Flow.Name != "echo chain" {
		t.Fatalf("flow = %+v, want echo_chain", got.Flow)
	}
	if len(got.Graph.Nodes) != 2 || got.Graph.Nodes[0].ID != "upper" || got.Graph.Nodes[1].ID != "reverse" {
		t.Fatalf("nodes = %+v, want upper/reverse", got.Graph.Nodes)
	}
	if len(got.Graph.Edges) != 1 || got.Graph.Edges[0].Source.Node != "upper" || got.Graph.Edges[0].Target.Node != "reverse" {
		t.Fatalf("edges = %+v, want upper -> reverse", got.Graph.Edges)
	}
	if len(got.Graph.Inputs) != 1 || got.Graph.Inputs[0].Name != "in" || got.Graph.Inputs[0].Ref.Node != "upper" {
		t.Fatalf("inputs = %+v, want in -> upper.input", got.Graph.Inputs)
	}
	if len(got.Graph.Outputs) != 1 || got.Graph.Outputs[0].Name != "out" || got.Graph.Outputs[0].Ref.Node != "reverse" {
		t.Fatalf("outputs = %+v, want out -> reverse.output", got.Graph.Outputs)
	}
	if len(got.Runs) != 1 || got.Runs[0].FlowID != "echo_chain" || got.Runs[0].Status != "done" {
		t.Fatalf("runs = %+v, want one done echo_chain run", got.Runs)
	}
	if len(got.RawJSON) == 0 {
		t.Fatal("raw_json is empty")
	}
}

func TestDebugFlowEndpointMissingFlow(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flows/missing/debug")
	if err != nil {
		t.Fatalf("GET debug: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDebugRunEndpointReturnsTraceSummary(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got struct {
		Run struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
		Events []struct {
			Seq  int    `json:"seq"`
			Kind string `json:"kind"`
		} `json:"events"`
		Nodes    []debugRunNodeBody `json:"nodes"`
		Timeline []struct {
			Seq    int    `json:"seq"`
			Kind   string `json:"kind"`
			NodeID string `json:"node_id"`
			Status string `json:"status"`
		} `json:"timeline"`
		Replay string `json:"replay"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Run.ID != runID || got.Run.Status != "done" {
		t.Fatalf("run = %+v, want done %s", got.Run, runID)
	}
	if len(got.Events) != len(expectedKindsForEchoChain) || len(got.Timeline) != len(expectedKindsForEchoChain) {
		t.Fatalf("events/timeline lens = %d/%d, want %d", len(got.Events), len(got.Timeline), len(expectedKindsForEchoChain))
	}
	if got.Replay != "/runs/"+runID+"/replay" {
		t.Fatalf("replay = %q, want /runs/%s/replay", got.Replay, runID)
	}
	nodes := debugNodesByID(got.Nodes)
	if nodes["upper"].Status != "done" || nodes["upper"].StartedSeq == 0 || nodes["upper"].FinishedSeq == 0 {
		t.Fatalf("upper node = %+v, want done with seqs", nodes["upper"])
	}
	if len(nodes["upper"].Input) == 0 || len(nodes["upper"].Output) == 0 {
		t.Fatalf("upper input/output empty: %+v", nodes["upper"])
	}
	if nodes["reverse"].Status != "done" {
		t.Fatalf("reverse node = %+v, want done", nodes["reverse"])
	}
	if got.Timeline[0].Kind != "flow_started" || got.Timeline[len(got.Timeline)-1].Status != "done" {
		t.Fatalf("timeline = %+v, want flow_started ... done", got.Timeline)
	}
}

func TestDebugRunEndpointFailedRun(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Run struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"run"`
		Timeline []struct {
			Kind   string `json:"kind"`
			Status string `json:"status"`
		} `json:"timeline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Run.Status != "failed" || !strings.Contains(got.Run.Error, "missing required input") {
		t.Fatalf("run = %+v, want failed missing-input", got.Run)
	}
	last := got.Timeline[len(got.Timeline)-1]
	if last.Kind != "flow_err" || last.Status != "failed" {
		t.Fatalf("last timeline = %+v, want failed flow_err", last)
	}
}

func TestDebugRunEndpointSkippedNode(t *testing.T) {
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
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+routerFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/router/run", `{"inputs":{"in":"hello world"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Nodes []debugRunNodeBody `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	nodes := debugNodesByID(got.Nodes)
	if nodes["other_path"].Status != "skipped" {
		t.Fatalf("other_path = %+v, want skipped", nodes["other_path"])
	}
}

func TestDebugRunEndpointSuspendedRun(t *testing.T) {
	srv, store := newStoreServer(t, withV2(t))
	defer srv.Close()
	if _, err := store.PutFlow(context.Background(), "hitl", "hitl approval", []byte(hitlFlow), false); err != nil {
		t.Fatalf("seed flow: %v", err)
	}
	runResp := mustPOST(t, srv.URL+"/flows/hitl/run", `{"resumable":true,"inputs":{"in":"x"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Run struct {
			Status string `json:"status"`
		} `json:"run"`
		Nodes     []debugRunNodeBody `json:"nodes"`
		Suspended *struct {
			ResumeToken   string          `json:"resume_token"`
			InterruptNode string          `json:"interrupt_node"`
			Payload       json.RawMessage `json:"payload"`
		} `json:"suspended"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Run.Status != "suspended" {
		t.Fatalf("run.status = %q, want suspended", got.Run.Status)
	}
	if got.Suspended == nil || got.Suspended.ResumeToken != runID || got.Suspended.InterruptNode != "gate" {
		t.Fatalf("suspended = %+v, want token %s gate", got.Suspended, runID)
	}
	if !strings.Contains(string(got.Suspended.Payload), "request") {
		t.Fatalf("suspended payload = %s, want request", got.Suspended.Payload)
	}
	nodes := debugNodesByID(got.Nodes)
	if nodes["gate"].Status != "suspended" {
		t.Fatalf("gate = %+v, want suspended", nodes["gate"])
	}
}

func TestDebugRunEndpointMissingRun(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/runs/missing/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDebugRunEndpointMalformedPayloadDoesNotFail(t *testing.T) {
	srv, store := newStoreServer(t)
	defer srv.Close()
	runID, err := store.StartRun(context.Background(), "manual", map[string]string{"in": "x"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := store.AppendRunEvent(context.Background(), runID, flowstore.RunEventNodeStarted, "bad", []byte(`{bad`)); err != nil {
		t.Fatalf("AppendRunEvent: %v", err)
	}

	resp, err := http.Get(srv.URL + "/runs/" + runID + "/debug")
	if err != nil {
		t.Fatalf("GET run debug: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Events []struct {
			DecodeError string `json:"decode_error"`
			RawPayload  string `json:"raw_payload"`
		} `json:"events"`
		Nodes    []debugRunNodeBody `json:"nodes"`
		Timeline []struct {
			DecodeError string `json:"decode_error"`
		} `json:"timeline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Events[0].DecodeError == "" || got.Timeline[0].DecodeError == "" {
		t.Fatalf("decode errors missing: events=%+v timeline=%+v", got.Events, got.Timeline)
	}
	if got.Events[0].RawPayload != "{bad" {
		t.Fatalf("raw_payload = %q, want {bad", got.Events[0].RawPayload)
	}
	nodes := debugNodesByID(got.Nodes)
	if nodes["bad"].Status != "running" || !strings.Contains(nodes["bad"].Error, "payload decode") {
		t.Fatalf("bad node = %+v, want running with decode error", nodes["bad"])
	}
}

func debugNodesByID(nodes []debugRunNodeBody) map[string]debugRunNodeBody {
	out := make(map[string]debugRunNodeBody, len(nodes))
	for _, n := range nodes {
		out[n.ID] = n
	}
	return out
}
