// Package httptool_test exercises the http_tool demo end-to-end: load
// the bundled flow.json, point its named tools at a live httptest
// server via a generated manifest, and assert the round-tripped output.
//
// The test serves as both validation and as a copy-pasteable
// integration recipe — the same code can be lifted into a downstream
// project that wants to wire its own HTTP tools through a manifest.
package httptool_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
	"github.com/costa92/llm-agent-flow/flow/tools"
)

func newToolServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/upper", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"output":"`+strings.ToUpper(in.Input)+`"}`)
	})
	mux.HandleFunc("/reverse", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		r2 := []rune(in.Input)
		for i, j := 0, len(r2)-1; i < j; i, j = i+1, j-1 {
			r2[i], r2[j] = r2[j], r2[i]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"output":"`+string(r2)+`"}`)
	})
	return httptest.NewServer(mux)
}

func TestHTTPToolFlowEndToEnd(t *testing.T) {
	srv := newToolServer(t)
	defer srv.Close()

	// Build the manifest in-memory so it can point at the live
	// httptest URL. The committed tools.example.json shows the same
	// shape with localhost:8080 as a placeholder.
	manifest := `{
		"tools":[
			{"name":"upper",  "kind":"http","url":"` + srv.URL + `/upper"},
			{"name":"reverse","kind":"http","url":"` + srv.URL + `/reverse"}
		]}`
	built, err := tools.LoadAndBuild(strings.NewReader(manifest), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	tm := make(flow.ToolMap, len(built))
	for _, b := range built {
		tm[b.Name()] = b
	}

	flowJSON, err := os.ReadFile("flow.json")
	if err != nil {
		t.Fatalf("read flow.json: %v", err)
	}
	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	engine, err := flow.LoadCompile(strings.NewReader(string(flowJSON)), reg, flow.Deps{Tools: tm})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	out, err := engine.Run(context.Background(), map[string]string{"in": "hello"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, want := out["out"], "OLLEH"; got != want {
		t.Fatalf("out = %q, want %q", got, want)
	}
}
