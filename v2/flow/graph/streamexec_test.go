package graph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/costa92/llm-agent-contract/llm"
	"github.com/costa92/llm-agent-contract/prompt"
)

// noFolderType is reflect.Type of noFolder (declared in stream_test.go), a type
// with no registered Concatenator — used to drive a tail-fold lookup failure.
var noFolderType = reflect.TypeFor[noFolder]()

// TestLiveCarrier_CloseIfLive_ClosesHeldStream is the unit test for the shared
// cleanup helper: a held live-stream carrier is Closed exactly once by
// closeIfLive, and a value carrier (or a cleared slot) is a no-op.
func TestLiveCarrier_CloseIfLive_ClosesHeldStream(t *testing.T) {
	// Held live stream → closed once.
	up := &trackingAnyStream{}
	var live liveCarrier
	live.set(streamCarrier{isStream: true, stream: up})
	live.closeIfLive()
	if up.closeCount != 1 {
		t.Fatalf("held live stream closeCount = %d, want 1", up.closeCount)
	}
	// Second call after the slot was zeroed: no-op.
	live.closeIfLive()
	if up.closeCount != 1 {
		t.Fatalf("closeCount after re-close = %d, want still 1", up.closeCount)
	}

	// Cleared (handed off) live stream → NOT closed by closeIfLive.
	up2 := &trackingAnyStream{}
	var live2 liveCarrier
	live2.set(streamCarrier{isStream: true, stream: up2})
	live2.clear()
	live2.closeIfLive()
	if up2.closeCount != 0 {
		t.Fatalf("handed-off stream closeCount = %d, want 0 (clear() released the claim)", up2.closeCount)
	}

	// Value carrier → no-op (no stream to close).
	var live3 liveCarrier
	live3.set(streamCarrier{value: 42})
	live3.closeIfLive() // must not panic
}

// errModel is an llm.ChatModel whose Stream succeeds (returning a spyStream)
// but whose post-stream consumer errors — used to drive runLinearStream's
// error path while a stream has been produced, proving the live-carrier
// cleanup releases it with no goroutine leak.
//
// We model the error at the TAIL-fold stage by making the graph output a type
// with no registered Concatenator: the tail folder lookup fails AFTER the
// chatmodel produced a live stream. That is the one reachable error-return in
// the current linear shape that occurs with a stream in flight, and it must
// not leak.
func TestRunLinearStream_ErrorPath_NoLeak(t *testing.T) {
	tmpl, _ := prompt.New(prompt.Spec{System: "s", User: "{q}"})
	req := tmpl.(prompt.Requester)
	spy := newSpyStream("a", "b", "c")
	model := &spyModel{sr: spy}

	// Graph: tmpl -> chatmodel, with graph output O = noFolder (no folder).
	// chatmodel is the exit, emits a stream; the tail must fold to noFolder but
	// no Concatenator exists, so runLinearStream errors at the tail with the
	// stream already live. (Compile permits this only because we make the exit
	// type-check via an `any`-typed seam; here we instead assert the runtime
	// cleanup directly through a constructed pipeline below.)
	//
	// Simplest reachable shape: build the linear pipeline manually so the tail
	// fold target lacks a folder, then call runLinearStream and assert the
	// underlying stream is Closed and no goroutine leaks.
	g := NewGraph[prompt.Vars, llm.Response]()
	tNode, _ := AddTemplateNode(g, "tmpl", req)
	cmNode, _ := AddChatModelNode(g, "chat", model)
	if err := g.AddEdge(tNode, cmNode); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Entry(tNode); err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if err := g.Exit(cmNode); err != nil {
		t.Fatalf("Exit: %v", err)
	}
	r, err := g.Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// Override the tail fold target to a type with no folder so the tail-fold
	// lookup fails with the chatmodel stream already in flight. outT drives the
	// tail lookupConcat in runLinearStream.
	r.outT = noFolderType

	base := settleGoroutines()
	_, serr := r.runLinearStream(context.Background(), prompt.Vars{"q": "hi"})
	if serr == nil {
		t.Fatalf("expected tail-fold error (no Concatenator for noFolder), got nil")
	}
	if errors.Is(serr, io.EOF) {
		t.Fatalf("unexpected EOF error: %v", serr)
	}
	if !spy.wasClosed() {
		t.Fatalf("underlying llm stream was NOT closed on the error path (leak)")
	}
	if n := settleGoroutines(); n > base {
		t.Fatalf("goroutine leak on error path: base=%d now=%d", base, n)
	}
}
