package flow

import (
	"bytes"
	"strings"
	"testing"
)

const sampleJSON = `{
  "id": "echo_chain",
  "name": "echo chain",
  "nodes": [
    { "id": "upper",   "type": "tool", "config": { "tool": "upper" } },
    { "id": "reverse", "type": "tool", "config": { "tool": "reverse" } }
  ],
  "edges": [
    { "source": { "node": "upper", "port": "output" },
      "target": { "node": "reverse", "port": "input" } }
  ],
  "inputs":  [{ "name": "in",  "node": "upper",   "port": "input"  }],
  "outputs": [{ "name": "out", "node": "reverse", "port": "output" }]
}`

func TestLoadParsesSampleFlow(t *testing.T) {
	f, err := Load(strings.NewReader(sampleJSON))
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if f.ID != "echo_chain" {
		t.Fatalf("ID = %q, want echo_chain", f.ID)
	}
	if len(f.Nodes) != 2 || len(f.Edges) != 1 {
		t.Fatalf("Nodes=%d Edges=%d, want 2/1", len(f.Nodes), len(f.Edges))
	}
	if len(f.Inputs) != 1 || f.Inputs[0].Name != "in" {
		t.Fatalf("Inputs = %+v, want [{Name:in ...}]", f.Inputs)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	const bad = `{"id":"x","nodes":[{"id":"a","type":"tool"}],"edges":[],"extra_field":1}`
	if _, err := Load(strings.NewReader(bad)); err == nil {
		t.Fatal("Load(unknown field) = nil, want error")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	in, err := Load(strings.NewReader(sampleJSON))
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	raw, err := Marshal(in)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	out, err := Load(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Load(round-trip): %v", err)
	}
	if out.ID != in.ID || len(out.Nodes) != len(in.Nodes) || len(out.Edges) != len(in.Edges) {
		t.Fatalf("round-trip drift: in=%+v out=%+v", in, out)
	}
}
