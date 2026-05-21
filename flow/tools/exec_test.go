package tools_test

import (
	"context"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow/tools"
)

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
