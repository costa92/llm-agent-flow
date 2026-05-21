package flow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// sleepyTool blocks for delay then returns the input unchanged (or
// fails when failWith != nil). active counts the goroutines currently
// inside Execute; tests assert it exceeds 1 to prove parallelism.
type sleepyTool struct {
	name     string
	delay    time.Duration
	failWith error
	active   *atomic.Int32
	peakObs  *atomic.Int32
}

func (t *sleepyTool) Name() string { return t.name }
func (t *sleepyTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	if t.active != nil {
		n := t.active.Add(1)
		defer t.active.Add(-1)
		for {
			cur := t.peakObs.Load()
			if n <= cur || t.peakObs.CompareAndSwap(cur, n) {
				break
			}
		}
	}
	select {
	case <-time.After(t.delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if t.failWith != nil {
		return "", t.failWith
	}
	var p struct {
		Input string `json:"input"`
	}
	_ = json.Unmarshal(args, &p)
	return p.Input + "@" + t.name, nil
}

// fanoutFlow is the canonical diamond used by parallelism tests:
//
//	   in
//	  /  \
//	left right     <- layer 1 (both siblings, runs in parallel)
//	  \  /
//	  join         <- layer 2 (depends on both)
//
// "join" pulls "input" from left.output (right.output is unused at v0;
// the topology is what we're testing, not value merging).
const diamondJSON = `{
  "id": "diamond",
  "nodes": [
    { "id": "in",    "type": "tool", "config": { "tool": "passthrough" } },
    { "id": "left",  "type": "tool", "config": { "tool": "left" } },
    { "id": "right", "type": "tool", "config": { "tool": "right" } },
    { "id": "join",  "type": "tool", "config": { "tool": "passthrough" } }
  ],
  "edges": [
    { "source": { "node": "in",    "port": "output" }, "target": { "node": "left",  "port": "input" } },
    { "source": { "node": "in",    "port": "output" }, "target": { "node": "right", "port": "input" } },
    { "source": { "node": "left",  "port": "output" }, "target": { "node": "join",  "port": "input" } }
  ],
  "inputs":  [{ "name": "in",  "node": "in",   "port": "input"  }],
  "outputs": [{ "name": "out", "node": "join", "port": "output" }]
}`

func TestEngineRunsLayerSiblingsInParallel(t *testing.T) {
	var active, peak atomic.Int32
	const delay = 80 * time.Millisecond
	tools := ToolMap{
		"passthrough": &sleepyTool{name: "pass", delay: 1 * time.Millisecond},
		"left":        &sleepyTool{name: "left", delay: delay, active: &active, peakObs: &peak},
		"right":       &sleepyTool{name: "right", delay: delay, active: &active, peakObs: &peak},
	}
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	eng, err := LoadCompile(strings.NewReader(diamondJSON), reg, Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	start := time.Now()
	if _, err := eng.Run(context.Background(), map[string]string{"in": "hi"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)

	// Parallelism: both siblings should have been active at the same
	// time at some point.
	if got := peak.Load(); got < 2 {
		t.Fatalf("peak concurrent siblings = %d, want >= 2", got)
	}
	// Wallclock should be closer to one delay than two. Allow 1.5x
	// margin for CI jitter.
	if budget := time.Duration(float64(delay) * 1.5); elapsed > budget {
		t.Fatalf("elapsed = %s, want < %s (siblings did not overlap)", elapsed, budget)
	}
}

func TestEngineSerialModeWhenMaxConcurrencyOne(t *testing.T) {
	var active, peak atomic.Int32
	tools := ToolMap{
		"passthrough": &sleepyTool{name: "pass", delay: 1 * time.Millisecond},
		"left":        &sleepyTool{name: "left", delay: 30 * time.Millisecond, active: &active, peakObs: &peak},
		"right":       &sleepyTool{name: "right", delay: 30 * time.Millisecond, active: &active, peakObs: &peak},
	}
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	eng, err := LoadCompile(strings.NewReader(diamondJSON), reg, Deps{Tools: tools}, WithMaxNodeConcurrency(1))
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	if _, err := eng.Run(context.Background(), map[string]string{"in": "hi"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrent siblings = %d, want 1 (serial mode)", got)
	}
}

func TestEngineFailFastCancelsPeers(t *testing.T) {
	var active atomic.Int32
	var peak atomic.Int32
	var slowCanceled atomic.Bool
	wantErr := errors.New("boom")

	slow := &cancelObserverTool{
		name:    "slow",
		delay:   500 * time.Millisecond,
		flagged: &slowCanceled,
	}
	tools := ToolMap{
		"passthrough": &sleepyTool{name: "pass", delay: 1 * time.Millisecond},
		// left fails quickly
		"left": &sleepyTool{name: "left", delay: 10 * time.Millisecond, failWith: wantErr, active: &active, peakObs: &peak},
		// right would take ~500ms but should be cancelled
		"right": slow,
	}
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	eng, err := LoadCompile(strings.NewReader(diamondJSON), reg, Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	start := time.Now()
	_, err = eng.Run(context.Background(), map[string]string{"in": "hi"})
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Run() err = %v, want boom", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("elapsed = %s, want < 200ms (peer was not cancelled)", elapsed)
	}
	if !slowCanceled.Load() {
		t.Fatal("slow sibling did not observe ctx cancel")
	}
}

func TestEngineStreamEventsKeyedByNode(t *testing.T) {
	tools := ToolMap{
		"passthrough": &sleepyTool{name: "pass", delay: 1 * time.Millisecond},
		"left":        &sleepyTool{name: "left", delay: 20 * time.Millisecond},
		"right":       &sleepyTool{name: "right", delay: 20 * time.Millisecond},
	}
	reg := NewNodeRegistry()
	if err := RegisterToolNode(reg); err != nil {
		t.Fatalf("RegisterToolNode: %v", err)
	}
	eng, err := LoadCompile(strings.NewReader(diamondJSON), reg, Deps{Tools: tools})
	if err != nil {
		t.Fatalf("LoadCompile: %v", err)
	}
	ch, err := eng.RunStream(context.Background(), map[string]string{"in": "hi"})
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	// Per-node we expect exactly one NodeStarted then exactly one
	// NodeFinished. Across nodes in the same layer, events may
	// interleave. Verify the per-node ordering plus the global
	// FlowStarted-first / FlowDone-last invariants.
	starts := map[string]bool{}
	finishes := map[string]bool{}
	var first, last FlowEventKind = 255, 255
	count := 0
	for ev := range ch {
		count++
		if count == 1 {
			first = ev.Kind
		}
		last = ev.Kind
		switch ev.Kind {
		case NodeStarted:
			if starts[ev.NodeID] {
				t.Fatalf("duplicate NodeStarted for %q", ev.NodeID)
			}
			starts[ev.NodeID] = true
		case NodeFinished:
			if !starts[ev.NodeID] {
				t.Fatalf("NodeFinished without NodeStarted for %q", ev.NodeID)
			}
			finishes[ev.NodeID] = true
		case FlowErr:
			t.Fatalf("unexpected FlowErr: %v", ev.Err)
		}
	}
	if first != FlowStarted {
		t.Fatalf("first event = %d, want FlowStarted", first)
	}
	if last != FlowDone {
		t.Fatalf("last event = %d, want FlowDone", last)
	}
	for _, id := range []string{"in", "left", "right", "join"} {
		if !starts[id] || !finishes[id] {
			t.Fatalf("missing events for node %q (start=%v finish=%v)", id, starts[id], finishes[id])
		}
	}
}

// cancelObserverTool returns ctx.Err() as soon as Done fires; flagged
// is set when the cancel was observed (the test gate).
type cancelObserverTool struct {
	name    string
	delay   time.Duration
	flagged *atomic.Bool
}

func (t *cancelObserverTool) Name() string { return t.name }
func (t *cancelObserverTool) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	select {
	case <-time.After(t.delay):
		return "ok", nil
	case <-ctx.Done():
		t.flagged.Store(true)
		return "", ctx.Err()
	}
}
