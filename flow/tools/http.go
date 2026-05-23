package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/costa92/llm-agent-flow/flow"
)

// httpSpec is the typed shape decoded from a manifest entry of kind
// "http". The full per-tool JSON object lives in Spec.Raw; httpSpec
// captures only the fields http kind reads.
type httpSpec struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	// TimeoutMs is the per-call request timeout. 0 => 30s default.
	TimeoutMs int `json:"timeout_ms,omitempty"`
}

// httpKindFactory is the built-in factory for kind == "http".
//
// Request: POST (or configured method) URL with body
//
//	{"input":"<stringified args.input>", ...args}
//
// when args is a JSON object, or the literal args bytes otherwise.
// Headers from the spec are applied verbatim; Content-Type defaults
// to application/json.
//
// Response handling: a 2xx body that parses as a JSON object with an
// "output" string field returns that field; otherwise the raw body
// becomes the output string. Non-2xx returns an error including the
// status code and a trimmed body snippet.
func httpKindFactory(spec Spec) (flow.Tool, error) {
	var cfg httpSpec
	if err := json.Unmarshal(spec.Raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode http spec: %w", err)
	}
	if cfg.URL == "" {
		return nil, errors.New("http tool: missing \"url\"")
	}
	if cfg.Method == "" {
		cfg.Method = http.MethodPost
	}
	if cfg.TimeoutMs == 0 {
		cfg.TimeoutMs = 30_000
	}
	return &httpTool{
		name:   spec.Name,
		cfg:    cfg,
		client: &http.Client{Timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond},
	}, nil
}

type httpTool struct {
	name   string
	cfg    httpSpec
	client *http.Client
}

func (t *httpTool) Name() string { return t.name }

func (t *httpTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	out, _, err := t.ExecuteWithMetadata(ctx, args)
	return out, err
}

// ExecuteWithMetadata satisfies flow.MetadataAwareTool. It emits:
//   - http_status: numeric HTTP status code as decimal string
//   - bytes:       size of the response body (capped read)
//   - duration_ms: wall-clock duration of the request in ms
//
// D1: on non-2xx the same three keys are still set so traces /
// dashboards retain signal on failed runs.
func (t *httpTool) ExecuteWithMetadata(ctx context.Context, args json.RawMessage) (string, map[string]string, error) {
	body := args
	if len(body) == 0 {
		body = []byte("{}")
	}
	req, err := http.NewRequestWithContext(ctx, t.cfg.Method, t.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("http tool %q: build request: %w", t.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range t.cfg.Headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := t.client.Do(req)
	if err != nil {
		// No status / bytes available — only duration. Keep meta
		// non-nil to honor D1 (something better than nothing).
		meta := map[string]string{
			"duration_ms": strconv.FormatInt(time.Since(start).Milliseconds(), 10),
		}
		return "", meta, fmt.Errorf("http tool %q: do: %w", t.name, err)
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	durationMs := strconv.FormatInt(time.Since(start).Milliseconds(), 10)
	meta := map[string]string{
		"http_status": strconv.Itoa(resp.StatusCode),
		"bytes":       strconv.Itoa(len(raw)),
		"duration_ms": durationMs,
	}
	if readErr != nil {
		return "", meta, fmt.Errorf("http tool %q: read body: %w", t.name, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", meta, fmt.Errorf("http tool %q: status %d: %s", t.name, resp.StatusCode, trimBody(raw))
	}
	// Try {"output":"..."} shape.
	var shaped struct {
		Output string `json:"output"`
	}
	if json.Unmarshal(raw, &shaped) == nil && shaped.Output != "" {
		return shaped.Output, meta, nil
	}
	return string(raw), meta, nil
}

// Compile-time pin: httpTool implements MetadataAwareTool (D3).
var _ flow.MetadataAwareTool = (*httpTool)(nil)

func trimBody(b []byte) string {
	const max = 256
	s := strings.TrimSpace(string(b))
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
