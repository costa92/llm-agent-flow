package tools_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/flow"
	"github.com/costa92/llm-agent-flow/flow/tools"
)

const goodManifest = `{
  "tools": [
    {"name":"upper", "kind":"http", "url":"http://example.invalid/upper"},
    {"name":"wc",    "kind":"exec", "command":["wc", "-w"]}
  ]
}`

func TestLoadAndBuildHappyPath(t *testing.T) {
	reg := tools.NewKindRegistry()
	built, err := tools.LoadAndBuild(strings.NewReader(goodManifest), reg)
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	if len(built) != 2 {
		t.Fatalf("len(built) = %d, want 2", len(built))
	}
	names := map[string]bool{}
	for _, b := range built {
		names[b.Name()] = true
	}
	if !names["upper"] || !names["wc"] {
		t.Fatalf("names = %+v, want upper + wc", names)
	}
}

func TestLoadRejectsUnknownTopLevelField(t *testing.T) {
	const bad = `{"tools":[],"oops":1}`
	if _, err := tools.LoadManifest(strings.NewReader(bad)); err == nil {
		t.Fatal("LoadManifest(unknown field) = nil, want error")
	}
}

func TestBuildRejectsDuplicateName(t *testing.T) {
	const dup = `{
		"tools":[
			{"name":"x","kind":"exec","command":["true"]},
			{"name":"x","kind":"exec","command":["true"]}
		]}`
	_, err := tools.LoadAndBuild(strings.NewReader(dup), tools.NewKindRegistry())
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("LoadAndBuild(dup) = %v, want duplicate-name error", err)
	}
}

func TestBuildRejectsUnknownKind(t *testing.T) {
	const unk = `{"tools":[{"name":"x","kind":"nope"}]}`
	_, err := tools.LoadAndBuild(strings.NewReader(unk), tools.NewKindRegistry())
	if err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("LoadAndBuild(unknown kind) = %v, want unknown-kind error", err)
	}
}

func TestBuildRejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"empty name", `{"tools":[{"name":"","kind":"exec","command":["true"]}]}`, "empty name"},
		{"empty kind", `{"tools":[{"name":"x","kind":""}]}`, "empty kind"},
		{"http no url", `{"tools":[{"name":"x","kind":"http"}]}`, "missing \"url\""},
		{"exec no cmd", `{"tools":[{"name":"x","kind":"exec"}]}`, "missing \"command\""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := tools.LoadAndBuild(strings.NewReader(c.src), tools.NewKindRegistry())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("LoadAndBuild = %v, want %q in message", err, c.want)
			}
		})
	}
}

func TestRegisterCustomKind(t *testing.T) {
	reg := tools.NewKindRegistry()
	if err := reg.RegisterKind("stub", func(spec tools.Spec) (flow.Tool, error) {
		return &stubTool{name: spec.Name}, nil
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}
	const src = `{"tools":[{"name":"hi","kind":"stub"}]}`
	built, err := tools.LoadAndBuild(strings.NewReader(src), reg)
	if err != nil {
		t.Fatalf("LoadAndBuild: %v", err)
	}
	if len(built) != 1 || built[0].Name() != "hi" {
		t.Fatalf("built = %+v, want one stub named 'hi'", built)
	}
	out, err := built[0].Execute(context.Background(), nil)
	if err != nil || out != "hi" {
		t.Fatalf("Execute = (%q, %v), want (\"hi\", nil)", out, err)
	}
}

type stubTool struct{ name string }

func (s *stubTool) Name() string { return s.name }
func (s *stubTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	return s.name, nil
}
