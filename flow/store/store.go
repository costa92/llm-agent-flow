package store

import (
	"context"
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

	// Close releases any underlying handles. Idempotent.
	Close() error
}
