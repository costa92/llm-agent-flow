package tools_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
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

// TestHTTPTool_EmitsStatusMetadata_On2xx pins D3 for the http kind:
// a successful call surfaces http_status, bytes, and duration_ms via
// the optional MetadataAwareTool capability.
func TestHTTPTool_EmitsStatusMetadata_On2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"output":"OK"}`)
	}))
	defer srv.Close()
	tool, _ := httpToolFromManifest(t, srv.URL)
	mat, ok := tool.(flow.MetadataAwareTool)
	if !ok {
		t.Fatalf("httpTool does not implement MetadataAwareTool")
	}
	out, meta, err := mat.ExecuteWithMetadata(context.Background(), []byte(`{"input":"x"}`))
	if err != nil {
		t.Fatalf("ExecuteWithMetadata: %v", err)
	}
	if out == "" {
		t.Fatalf("output empty; want decoded body")
	}
	if meta == nil {
		t.Fatalf("meta = nil, want populated")
	}
	if meta["http_status"] != "200" {
		t.Fatalf("meta[http_status] = %q, want 200", meta["http_status"])
	}
	if meta["bytes"] == "" {
		t.Fatalf("meta[bytes] = empty, want non-empty count")
	}
	if _, perr := strconv.Atoi(meta["bytes"]); perr != nil {
		t.Fatalf("meta[bytes] = %q, want integer string", meta["bytes"])
	}
	if meta["duration_ms"] == "" {
		t.Fatalf("meta[duration_ms] = empty, want numeric string")
	}
}

// TestHTTPTool_EmitsMetadataOnNon2xx pins D1 on the http kind:
// metadata MUST be non-nil even on the error path so trace consumers
// still see http_status=500 + bytes.
func TestHTTPTool_EmitsMetadataOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "upstream went bang")
	}))
	defer srv.Close()
	tool, _ := httpToolFromManifest(t, srv.URL)
	mat, ok := tool.(flow.MetadataAwareTool)
	if !ok {
		t.Fatalf("httpTool does not implement MetadataAwareTool")
	}
	out, meta, err := mat.ExecuteWithMetadata(context.Background(), []byte(`{}`))
	if err == nil {
		t.Fatalf("err = nil on 500; want error")
	}
	if out != "" {
		t.Fatalf("out = %q, want empty on error", out)
	}
	if meta == nil {
		t.Fatalf("meta = nil on error; D1 requires non-nil metadata on error path")
	}
	if meta["http_status"] != "500" {
		t.Fatalf("meta[http_status] = %q, want 500", meta["http_status"])
	}
	if meta["bytes"] == "" {
		t.Fatalf("meta[bytes] = empty, want byte count even on error")
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
