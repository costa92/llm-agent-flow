package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// TypeTool is the registered key for tool-backed nodes. Register it
// against a NodeRegistry with RegisterToolNode.
const TypeTool = "tool"

// RegisterToolNode wires the bundled "tool" node type into a
// NodeRegistry. The factory looks up the named Tool via the engine's
// Deps.Tools at run time. Returns an error if "tool" is already
// registered.
func RegisterToolNode(r *NodeRegistry) error {
	return r.Register(TypeTool, toolNodeFactory)
}

type toolNodeConfig struct {
	// Tool is the name the engine's ToolLookup resolves at Run time.
	Tool string `json:"tool"`
	// Static, optional JSON args to pass to Tool.Execute. The
	// upstream node's "output" port value (if any) is merged into the
	// "input" field of this JSON object when the port is wired.
	Args json.RawMessage `json:"args,omitempty"`
}

func toolNodeFactory(cfg json.RawMessage, deps Deps) (NodeKind, error) {
	var c toolNodeConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &c); err != nil {
			return nil, fmt.Errorf("tool node config: %w", err)
		}
	}
	if c.Tool == "" {
		return nil, errors.New("tool node config: missing \"tool\"")
	}
	if deps.Tools == nil {
		return nil, errors.New("tool node: engine Deps.Tools is nil")
	}
	t, ok := deps.Tools.Lookup(c.Tool)
	if !ok {
		return nil, fmt.Errorf("tool node: unknown tool %q", c.Tool)
	}
	return &toolNode{tool: t, args: c.Args}, nil
}

type toolNode struct {
	tool Tool
	args json.RawMessage
}

func (n *toolNode) Inputs() []Port  { return []Port{{Name: "input", Type: "string"}} }
func (n *toolNode) Outputs() []Port { return []Port{{Name: "output", Type: "string"}} }

func (n *toolNode) Run(ctx context.Context, in map[string]string) (map[string]string, error) {
	out, _, err := n.RunWithMetadata(ctx, in)
	return out, err
}

// RunWithMetadata satisfies MetadataAware. It type-asserts the wrapped
// Tool against MetadataAwareTool: capable tools surface their meta map
// up to the engine; plain tools fall through to Tool.Execute and
// produce nil metadata (pre-v0.2 behavior).
//
// D1: metadata returned by an aware tool on the error path is
// preserved — the engine forwards it onto NodeFinished so traces /
// dashboards keep their signal (e.g. http_status=500).
func (n *toolNode) RunWithMetadata(ctx context.Context, in map[string]string) (map[string]string, map[string]string, error) {
	// Build the args payload for the underlying Tool. Order:
	//   1. start from the static Args blob in the node config (or {}),
	//   2. overlay {"input": <"input" port value>} when the port is wired.
	merged := map[string]any{}
	if len(n.args) > 0 {
		if err := json.Unmarshal(n.args, &merged); err != nil {
			return nil, nil, fmt.Errorf("tool node %q: decode args: %w", n.tool.Name(), err)
		}
	}
	if v, ok := in["input"]; ok {
		merged["input"] = v
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil, nil, fmt.Errorf("tool node %q: encode args: %w", n.tool.Name(), err)
	}
	if mat, ok := n.tool.(MetadataAwareTool); ok {
		out, meta, execErr := mat.ExecuteWithMetadata(ctx, raw)
		if execErr != nil {
			// D1: preserve metadata on error.
			return nil, meta, fmt.Errorf("tool node %q: execute: %w", n.tool.Name(), execErr)
		}
		return map[string]string{"output": out}, meta, nil
	}
	out, err := n.tool.Execute(ctx, raw)
	if err != nil {
		return nil, nil, fmt.Errorf("tool node %q: execute: %w", n.tool.Name(), err)
	}
	return map[string]string{"output": out}, nil, nil
}

// Compile-time pin: toolNode must implement MetadataAware so the
// engine's type-assertion path picks it up. The tool inside may or
// may not be MetadataAwareTool — RunWithMetadata handles both.
var _ MetadataAware = (*toolNode)(nil)

// ToolMap is a minimal in-memory ToolLookup. Useful for tests and
// for callers who hold an `agents.Tool` directly without an Agent
// registry.
type ToolMap map[string]Tool

// Lookup implements ToolLookup.
func (m ToolMap) Lookup(name string) (Tool, bool) {
	t, ok := m[name]
	return t, ok
}
