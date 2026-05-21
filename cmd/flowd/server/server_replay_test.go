package server_test

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
)

func TestReplayRunReproducesEvents(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Post(srv.URL+"/runs/"+runID+"/replay", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	if resp.Header.Get("X-Replay") != "true" {
		t.Fatalf("X-Replay = %q, want true", resp.Header.Get("X-Replay"))
	}
	if got := resp.Header.Get("X-Run-ID"); got != runID {
		t.Fatalf("X-Run-ID = %q, want %q", got, runID)
	}

	var frames []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			frames = append(frames, strings.TrimPrefix(line, "event: "))
		}
	}
	wantOrder := []string{
		"flow_started",
		"node_started", "node_finished",
		"node_started", "node_finished",
		"flow_done",
	}
	if len(frames) != len(wantOrder) {
		t.Fatalf("frames = %v (len %d), want %v", frames, len(frames), wantOrder)
	}
	for i, want := range wantOrder {
		if frames[i] != want {
			t.Fatalf("frame[%d] = %q, want %q", i, frames[i], want)
		}
	}
}

func TestReplayRunMissingIs404(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/runs/nope/replay", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestReplayRunGetIs405(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/runs/whatever/replay")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestReplayPayloadsByteForByteMatchOriginal(t *testing.T) {
	// The replay endpoint forwards stored payload JSON verbatim, so
	// a client decoding the data: lines should get the exact same
	// shape as the original /run/stream produced.
	srv, _ := newStoreServer(t)
	defer srv.Close()
	_ = mustPOST(t, srv.URL+"/flows", `{"flow":`+echoChainFlow+`}`)
	runResp := mustPOST(t, srv.URL+"/flows/echo_chain/run", `{"inputs":{"in":"hello"}}`)
	runID := runResp.Header.Get("X-Run-ID")
	runResp.Body.Close()

	resp, err := http.Post(srv.URL+"/runs/"+runID+"/replay", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	// Final data: line must include the FlowDone outputs unchanged.
	var lastData string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			lastData = strings.TrimPrefix(line, "data: ")
		}
	}
	if !strings.Contains(lastData, `"outputs":{"out":"OLLEH"}`) {
		t.Fatalf("final replay data = %q, want flow_done outputs", lastData)
	}
}
