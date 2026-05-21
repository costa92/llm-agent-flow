package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/costa92/llm-agent-flow/flow"
)

// execSpec is the typed shape decoded from a manifest entry of kind
// "exec". The full per-tool JSON object lives in Spec.Raw.
type execSpec struct {
	Command   []string `json:"command"`
	TimeoutMs int      `json:"timeout_ms,omitempty"`
}

// execKindFactory is the built-in factory for kind == "exec".
//
// The tool's Execute(ctx, args):
//   - Spawns Command[0] with Command[1:] as argv.
//   - Writes args (raw JSON) on the child's stdin and closes it.
//   - Captures stdout up to a 1 MiB cap as the tool output.
//   - Captures stderr (up to a cap) into the error path on non-zero exit.
//   - Honors ctx cancellation and the per-call timeout (default 30 s).
//
// Security note: an exec tool is by definition arbitrary code on the
// host. Operators should treat the manifest as trusted input; the
// tool itself does no further sandboxing.
func execKindFactory(spec Spec) (flow.Tool, error) {
	var cfg execSpec
	if err := json.Unmarshal(spec.Raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode exec spec: %w", err)
	}
	if len(cfg.Command) == 0 {
		return nil, errors.New("exec tool: missing \"command\"")
	}
	if cfg.TimeoutMs == 0 {
		cfg.TimeoutMs = 30_000
	}
	return &execTool{name: spec.Name, cfg: cfg}, nil
}

type execTool struct {
	name string
	cfg  execSpec
}

func (t *execTool) Name() string { return t.name }

func (t *execTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	body := args
	if len(body) == 0 {
		body = []byte("{}")
	}
	callCtx, cancel := context.WithTimeout(ctx, time.Duration(t.cfg.TimeoutMs)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(callCtx, t.cfg.Command[0], t.cfg.Command[1:]...) //nolint:gosec // user-trusted manifest
	cmd.Stdin = bytes.NewReader(body)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitWriter{w: &stdout, max: 1 << 20}
	cmd.Stderr = &limitWriter{w: &stderr, max: 1 << 14}

	if err := cmd.Run(); err != nil {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("exec tool %q: timeout after %d ms", t.name, t.cfg.TimeoutMs)
		}
		return "", fmt.Errorf("exec tool %q: %w (stderr: %s)", t.name, err, trimBody(stderr.Bytes()))
	}
	return strings.TrimRight(stdout.String(), "\r\n"), nil
}

// limitWriter caps the bytes copied into the wrapped buffer. Excess
// writes succeed silently — caller sees a truncated buffer but no
// EPIPE upstream (which exec.Cmd treats as a fatal error).
type limitWriter struct {
	w   io.Writer
	max int
	n   int
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	if lw.n >= lw.max {
		return len(p), nil
	}
	allowed := lw.max - lw.n
	if allowed > len(p) {
		allowed = len(p)
	}
	wrote, err := lw.w.Write(p[:allowed])
	lw.n += wrote
	if err != nil {
		return wrote, err
	}
	return len(p), nil
}
