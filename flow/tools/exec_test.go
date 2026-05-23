package tools_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
	"github.com/costa92/llm-agent-flow/flow/tools"
)

// execToolFromManifest builds a single exec-kind tool from a JSON
// manifest fragment. Mirrors the httpToolFromManifest helper.
func execToolFromManifest(t *testing.T, manifest string) flow.Tool {
	t.Helper()
	built, err := tools.LoadAndBuild(strings.NewReader(manifest), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	if len(built) != 1 {
		t.Fatalf("len(built) = %d, want 1", len(built))
	}
	return built[0]
}

func TestExecToolStdoutCaptured(t *testing.T) {
	// `cat` echoes stdin to stdout — sends our JSON args right back.
	src := `{"tools":[{"name":"echo","kind":"exec","command":["cat"]}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	out, err := built[0].Execute(context.Background(), []byte(`{"input":"hi"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != `{"input":"hi"}` {
		t.Fatalf("out = %q, want round-tripped JSON", out)
	}
}

func TestExecToolNonZeroExitErrors(t *testing.T) {
	// `false` exits 1 with no output.
	src := `{"tools":[{"name":"f","kind":"exec","command":["false"]}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	_, err = built[0].Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("Execute(false) = nil, want exit-1 error")
	}
	if !strings.Contains(err.Error(), "exec tool") {
		t.Fatalf("err = %v, want exec-tool error", err)
	}
}

func TestExecToolTimeout(t *testing.T) {
	// sleep 10 with a 50 ms timeout — must error promptly.
	src := `{"tools":[{"name":"slow","kind":"exec","command":["sleep","10"],"timeout_ms":50}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), tools.NewKindRegistry())
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	_, err = built[0].Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("Execute = %v, want timeout error", err)
	}
}

// TestExecTool_EmitsExitCodeOnSuccess pins D3 for the exec kind:
// a clean exit-0 surfaces exit_code=0 plus duration_ms.
func TestExecTool_EmitsExitCodeOnSuccess(t *testing.T) {
	tool := execToolFromManifest(t, `{"tools":[{"name":"true_cmd","kind":"exec","command":["sh","-c","exit 0"]}]}`)
	mat, ok := tool.(flow.MetadataAwareTool)
	if !ok {
		t.Fatalf("execTool does not implement MetadataAwareTool")
	}
	_, meta, err := mat.ExecuteWithMetadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("ExecuteWithMetadata: %v", err)
	}
	if meta == nil {
		t.Fatalf("meta = nil, want populated")
	}
	if meta["exit_code"] != "0" {
		t.Fatalf("meta[exit_code] = %q, want 0", meta["exit_code"])
	}
	if meta["duration_ms"] == "" {
		t.Fatalf("meta[duration_ms] = empty, want numeric string")
	}
	if _, perr := strconv.Atoi(meta["duration_ms"]); perr != nil {
		t.Fatalf("meta[duration_ms] = %q, want integer string", meta["duration_ms"])
	}
}

// TestExecTool_EmitsExitCodeOnFailure pins D1 for exec: exit_code
// metadata is preserved alongside the error so dashboards still see
// "exit 1" / "exit 2" signals on failed runs.
func TestExecTool_EmitsExitCodeOnFailure(t *testing.T) {
	tool := execToolFromManifest(t, `{"tools":[{"name":"false_cmd","kind":"exec","command":["sh","-c","exit 1"]}]}`)
	mat, ok := tool.(flow.MetadataAwareTool)
	if !ok {
		t.Fatalf("execTool does not implement MetadataAwareTool")
	}
	_, meta, err := mat.ExecuteWithMetadata(context.Background(), nil)
	if err == nil {
		t.Fatalf("err = nil, want exit-1 error")
	}
	if meta == nil {
		t.Fatalf("meta = nil on error; D1 requires non-nil metadata on error path")
	}
	if meta["exit_code"] != "1" {
		t.Fatalf("meta[exit_code] = %q, want 1", meta["exit_code"])
	}
}

// TestExecTool_EmitsSignalOnTimeout pins D1 on the timeout / cancel
// path: when ctx fires the deadline, metadata still carries a
// signal=timeout marker plus duration_ms.
func TestExecTool_EmitsSignalOnTimeout(t *testing.T) {
	tool := execToolFromManifest(t, `{"tools":[{"name":"slow","kind":"exec","command":["sleep","5"],"timeout_ms":50}]}`)
	mat, ok := tool.(flow.MetadataAwareTool)
	if !ok {
		t.Fatalf("execTool does not implement MetadataAwareTool")
	}
	_, meta, err := mat.ExecuteWithMetadata(context.Background(), nil)
	if err == nil {
		t.Fatalf("err = nil, want timeout error")
	}
	if meta == nil {
		t.Fatalf("meta = nil on timeout; D1 requires non-nil metadata")
	}
	if meta["signal"] != "timeout" {
		t.Fatalf("meta[signal] = %q, want timeout", meta["signal"])
	}
}
