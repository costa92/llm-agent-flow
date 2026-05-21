package tools_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow/tools"
)

func httpToolFromManifest(t *testing.T, url string) (flow_Tool, func()) {
	t.Helper()
	src := `{"tools":[{"name":"upper","kind":"http","url":"` + url + `"}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	if len(built) != 1 {
		t.Fatalf("len(built) = %d, want 1", len(built))
	}
	return built[0], func() {}
}

// flow_Tool is a local minimal alias of flow.Tool. The test files
// don't pull in flow just for this — toolspkg already exports the
// concrete type via LoadAndBuild's return.
type flow_Tool interface {
	Name() string
	Execute(ctx context.Context, args json.RawMessage) (string, error)
}

func TestHTTPToolOutputFieldDecoded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"output":"`+strings.ToUpper(in["input"])+`"}`)
	}))
	defer srv.Close()

	tool, cleanup := httpToolFromManifest(t, srv.URL)
	defer cleanup()
	out, err := tool.Execute(context.Background(), []byte(`{"input":"hello"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "HELLO" {
		t.Fatalf("out = %q, want HELLO", out)
	}
}

func TestHTTPToolRawBodyFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "plain body, not JSON")
	}))
	defer srv.Close()
	tool, _ := httpToolFromManifest(t, srv.URL)
	out, err := tool.Execute(context.Background(), []byte(`{"input":"x"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "plain body, not JSON" {
		t.Fatalf("out = %q, want raw body", out)
	}
}

func TestHTTPToolNon2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	}))
	defer srv.Close()
	tool, _ := httpToolFromManifest(t, srv.URL)
	_, err := tool.Execute(context.Background(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "status 500") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Execute = %v, want 500/boom error", err)
	}
}

func TestHTTPToolHeadersPassthrough(t *testing.T) {
	gotHeader := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Token")
		_, _ = io.WriteString(w, `{"output":"ok"}`)
	}))
	defer srv.Close()

	src := `{"tools":[{"name":"x","kind":"http","url":"` + srv.URL + `","headers":{"X-Token":"abc123"}}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	if _, err := built[0].Execute(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotHeader != "abc123" {
		t.Fatalf("X-Token = %q, want abc123", gotHeader)
	}
}
