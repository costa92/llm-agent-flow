package tools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/costa92/llm-agent-flow/flow"
)

// Manifest is the JSON document loaded from a --tools file. It is a
// flat list of named tool specifications resolved through a
// KindRegistry into concrete flow.Tool implementations.
type Manifest struct {
	Tools []Spec `json:"tools"`
}

// Spec is one tool's specification in the manifest. Required fields:
// Name (must be unique inside the manifest) and Kind (must resolve in
// the active KindRegistry). All remaining fields are passed through
// to the kind's factory as Spec.Config below.
//
// We keep the raw JSON object available as Raw so kinds can decode
// their own typed configuration without an intermediate map[string]any.
type Spec struct {
	Name string          `json:"name"`
	Kind string          `json:"kind"`
	Raw  json.RawMessage `json:"-"` // populated by Load — the full per-tool JSON object
}

// kindFactory builds one flow.Tool from a manifest entry.
type kindFactory func(spec Spec) (flow.Tool, error)

// KindRegistry maps a Spec.Kind value to a factory. Concurrency-safe;
// register all kinds at process startup, then Build many times.
type KindRegistry struct {
	mu        sync.RWMutex
	factories map[string]kindFactory
}

// NewKindRegistry returns a registry that knows the built-in kinds
// ("http", "exec"). Custom kinds add via RegisterKind.
func NewKindRegistry() *KindRegistry {
	r := &KindRegistry{factories: make(map[string]kindFactory)}
	_ = r.RegisterKind("http", httpKindFactory)
	_ = r.RegisterKind("exec", execKindFactory)
	return r
}

// RegisterKind adds a custom factory under name. Duplicate names
// return an error.
func (r *KindRegistry) RegisterKind(name string, factory func(Spec) (flow.Tool, error)) error {
	if name == "" {
		return errors.New("flow/tools: register kind: empty name")
	}
	if factory == nil {
		return errors.New("flow/tools: register kind: nil factory")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.factories[name]; dup {
		return fmt.Errorf("flow/tools: register kind: %q already registered", name)
	}
	r.factories[name] = factory
	return nil
}

// Build resolves every entry in m into a flow.Tool. Errors are
// aggregated so the caller sees every bad row in one pass.
func (r *KindRegistry) Build(m Manifest) ([]flow.Tool, error) {
	if len(m.Tools) == 0 {
		return nil, nil
	}
	out := make([]flow.Tool, 0, len(m.Tools))
	seenName := make(map[string]struct{}, len(m.Tools))
	var issues []string
	for i, spec := range m.Tools {
		if spec.Name == "" {
			issues = append(issues, fmt.Sprintf("tools[%d]: empty name", i))
			continue
		}
		if spec.Kind == "" {
			issues = append(issues, fmt.Sprintf("tools[%q]: empty kind", spec.Name))
			continue
		}
		if _, dup := seenName[spec.Name]; dup {
			issues = append(issues, fmt.Sprintf("tools[%q]: duplicate name", spec.Name))
			continue
		}
		r.mu.RLock()
		f, ok := r.factories[spec.Kind]
		r.mu.RUnlock()
		if !ok {
			issues = append(issues, fmt.Sprintf("tools[%q]: unknown kind %q", spec.Name, spec.Kind))
			continue
		}
		tool, err := f(spec)
		if err != nil {
			issues = append(issues, fmt.Sprintf("tools[%q]: %v", spec.Name, err))
			continue
		}
		seenName[spec.Name] = struct{}{}
		out = append(out, tool)
	}
	if len(issues) > 0 {
		return nil, fmt.Errorf("flow/tools: %d issue(s): %v", len(issues), issues)
	}
	return out, nil
}

// LoadManifest parses a Manifest from r (JSON). Unknown top-level
// fields are rejected; per-entry unknown fields fall through to the
// kind-specific decoder in Spec.Raw.
func LoadManifest(r io.Reader) (Manifest, error) {
	// Two-pass decode: first the outer shape with DisallowUnknownFields
	// so typos like "Tols" surface, then a raw pass to preserve each
	// entry's full object as Spec.Raw.
	raw, err := io.ReadAll(r)
	if err != nil {
		return Manifest{}, fmt.Errorf("flow/tools: load: %w", err)
	}
	var outer struct {
		Tools []json.RawMessage `json:"tools"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&outer); err != nil {
		return Manifest{}, fmt.Errorf("flow/tools: load: %w", err)
	}
	m := Manifest{Tools: make([]Spec, 0, len(outer.Tools))}
	for i, rawEntry := range outer.Tools {
		var head Spec
		if err := json.Unmarshal(rawEntry, &head); err != nil {
			return Manifest{}, fmt.Errorf("flow/tools: load: tools[%d]: %w", i, err)
		}
		head.Raw = rawEntry
		m.Tools = append(m.Tools, head)
	}
	return m, nil
}

// LoadAndBuild is the common load-then-build path: a single call that
// reads the manifest and resolves every entry into a flow.Tool.
func LoadAndBuild(r io.Reader, reg *KindRegistry) ([]flow.Tool, error) {
	m, err := LoadManifest(r)
	if err != nil {
		return nil, err
	}
	return reg.Build(m)
}

