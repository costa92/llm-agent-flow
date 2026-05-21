package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// FlowMeta is the lightweight per-flow record returned by ListFlows.
type FlowMeta struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FlowRecord pairs the metadata with the serialized flow JSON.
type FlowRecord struct {
	FlowMeta
	// JSON is the canonical Flow IR as bytes. Engines re-parse from
	// this on demand; the store does not interpret the contents.
	JSON []byte `json:"json"`
}

// RunStatus tracks a run's lifecycle.
type RunStatus string

const (
	RunStatusRunning RunStatus = "running"
	RunStatusDone    RunStatus = "done"
	RunStatusFailed  RunStatus = "failed"
)

// RunMeta is the lightweight per-run record returned by ListRuns.
type RunMeta struct {
	ID         string     `json:"id"`
	FlowID     string     `json:"flow_id"`
	Status     RunStatus  `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// RunRecord is the full row including inputs / outputs / error. Nil
// FinishedAt is the in-flight signal.
type RunRecord struct {
	RunMeta
	Inputs  map[string]string `json:"inputs,omitempty"`
	Outputs map[string]string `json:"outputs,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// RunEventKind enumerates the persisted run-event variants. The
// string values match the SSE event: names emitted by cmd/flowd so a
// replay-from-history endpoint can reuse the same client decoder.
type RunEventKind string

const (
	RunEventFlowStarted  RunEventKind = "flow_started"
	RunEventNodeStarted  RunEventKind = "node_started"
	RunEventNodeFinished RunEventKind = "node_finished"
	RunEventNodeSkipped  RunEventKind = "node_skipped"
	RunEventFlowDone     RunEventKind = "flow_done"
	RunEventFlowErr      RunEventKind = "flow_err"
)

// RunEvent is one row in a run's event history. Seq is 1-indexed and
// monotonic within a single run; ordering across two different runs
// is not comparable.
type RunEvent struct {
	Seq       int             `json:"seq"`
	Kind      RunEventKind    `json:"kind"`
	NodeID    string          `json:"node_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"ts"`
}

// RunEventBatchItem is the input shape for store implementations that
// support bulk-insertion of events (see e.g. sqlite.Store.AppendRunEvents).
// The Store interface itself does NOT include AppendRunEvents in v0.1
// — implementations expose it as an optional capability that callers
// detect via a type assertion. Adding it to the Store interface would
// break downstream custom implementations and must wait for v0.2.
//
// Seq and Timestamp are NOT carried here; they remain server-assigned
// by the store, matching the single-event AppendRunEvent semantics.
type RunEventBatchItem struct {
	Kind    RunEventKind
	NodeID  string
	Payload []byte
}

// ErrNotFound is returned by GetFlow / GetRun when no row matches.
// Tests should use errors.Is.
var ErrNotFound = errors.New("flow/store: not found")

// ErrAlreadyExists is returned by PutFlow when called with create=true
// against an existing id. PUT-style overwrites use create=false.
var ErrAlreadyExists = errors.New("flow/store: already exists")

// Store is the persistence contract for flowd. Implementations must
// be safe for concurrent use.
//
// The split between Put with create=true / create=false lets HTTP
// handlers map cleanly: POST /flows = create=true (409 on conflict),
// PUT /flows/{id} = create=false (replace or insert).
type Store interface {
	// PutFlow inserts or updates a flow. With create=true an existing
	// id returns ErrAlreadyExists. With create=false the row is
	// inserted-or-updated (PUT semantics). The returned record's
	// CreatedAt / UpdatedAt are populated by the store.
	PutFlow(ctx context.Context, id, name string, json []byte, create bool) (FlowRecord, error)

	GetFlow(ctx context.Context, id string) (FlowRecord, error)
	ListFlows(ctx context.Context, limit int) ([]FlowMeta, error)
	DeleteFlow(ctx context.Context, id string) error

	// StartRun creates a row in RunStatusRunning and returns the
	// generated run id. The store is responsible for id allocation.
	StartRun(ctx context.Context, flowID string, inputs map[string]string) (string, error)

	// FinishRun transitions a run to "done" (errMsg == "") or
	// "failed" (errMsg != ""). Calling FinishRun on a row already in
	// a terminal state is a no-op so retries are safe.
	FinishRun(ctx context.Context, runID string, outputs map[string]string, errMsg string) error

	GetRun(ctx context.Context, runID string) (RunRecord, error)
	ListRuns(ctx context.Context, flowID string, limit int) ([]RunMeta, error)

	// AppendRunEvent appends one event to a run's history. The store
	// assigns the seq (next monotonic value) and ts (server-side
	// "now"). Payload may be nil; when non-nil it is stored verbatim
	// — callers typically serialize their FlowEvent shape to JSON.
	//
	// AppendRunEvent is safe to call on a finished run (replay /
	// late-arrival writes). Calls against an unknown runID return
	// ErrNotFound.
	AppendRunEvent(ctx context.Context, runID string, kind RunEventKind, nodeID string, payload []byte) error

	// ListRunEvents returns every event for runID in seq order
	// (oldest first). limit <= 0 means "all".
	ListRunEvents(ctx context.Context, runID string, limit int) ([]RunEvent, error)

	// Close releases any underlying handles. Idempotent.
	Close() error
}
