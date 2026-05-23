package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
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
	out, _, err := t.ExecuteWithMetadata(ctx, args)
	return out, err
}

// ExecuteWithMetadata satisfies flow.MetadataAwareTool. It emits:
//   - exit_code:   numeric exit status (decimal string) on clean exits.
//   - duration_ms: wall-clock command duration in ms.
//   - signal:      "timeout" on context-deadline cancellation (no
//     exit_code in that case — ProcessState may be nil or unexited).
//
// D1: on non-zero exit and on timeout the metadata map is non-nil so
// downstream traces / dashboards still see the failure signal
// alongside the wrapped error.
func (t *execTool) ExecuteWithMetadata(ctx context.Context, args json.RawMessage) (string, map[string]string, error) {
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

	start := time.Now()
	runErr := cmd.Run()
	durationMs := strconv.FormatInt(time.Since(start).Milliseconds(), 10)

	if runErr != nil {
		// Timeout / context-cancel path: ProcessState may be nil if the
		// child was killed before it could report exit, so guard the
		// deref. signal=timeout disambiguates from a normal non-zero
		// exit for trace consumers.
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return "", map[string]string{
				"signal":      "timeout",
				"duration_ms": durationMs,
			}, fmt.Errorf("exec tool %q: timeout after %d ms", t.name, t.cfg.TimeoutMs)
		}
		// Non-zero exit (clean child, bad return). Preserve exit_code.
		meta := map[string]string{"duration_ms": durationMs}
		if cmd.ProcessState != nil {
			meta["exit_code"] = strconv.Itoa(cmd.ProcessState.ExitCode())
		}
		return "", meta, fmt.Errorf("exec tool %q: %w (stderr: %s)", t.name, runErr, trimBody(stderr.Bytes()))
	}
	meta := map[string]string{
		"exit_code":   "0",
		"duration_ms": durationMs,
	}
	if cmd.ProcessState != nil {
		// Defensive: prefer the real ExitCode if available (always 0
		// here on the success branch, but the explicit form keeps the
		// meta map self-consistent for callers).
		meta["exit_code"] = strconv.Itoa(cmd.ProcessState.ExitCode())
	}
	return strings.TrimRight(stdout.String(), "\r\n"), meta, nil
}

// Compile-time pin: execTool implements MetadataAwareTool (D3).
var _ flow.MetadataAwareTool = (*execTool)(nil)

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
