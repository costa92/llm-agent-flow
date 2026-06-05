package flow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// EdgeFireState records the terminal disposition of one flow edge for the
// resume cursor (RV-1). Fresh runs start every edge EdgePending; the
// edge-firing stage transitions each as the layer completes.
type EdgeFireState uint8

const (
	EdgePending     EdgeFireState = iota // source layer not yet processed; recompute on resume
	EdgeFired                            // setPort + activate already done — never re-fire
	EdgeDeadFalse                        // conditional edge evaluated false — never fires this run
	EdgeDeadNoPort                       // source ran but did not emit the source port
	EdgeDeadSrcSkip                      // source was not activated (skipped)
	EdgeDeferred                         // interrupt node's out-edge; fired on resume after humanInput
)

// Checkpoint is the serializable snapshot captured at a suspend point.
type Checkpoint struct {
	Version  int    `json:"version"`
	RunID    string `json:"run_id"`
	FlowID   string `json:"flow_id"`
	FlowHash string `json:"flow_hash"` // sha256 of canonical v2 Flow JSON

	PortValues   map[string]map[string]json.RawMessage `json:"port_values"`   // node -> port -> type-tagged JSON
	Activated    map[string]bool                       `json:"activated"`     // authoritative resume cursor
	EdgeStates   []EdgeFireState                        `json:"edge_states"`   // len == len(flow.Edges), stable index
	SuspendLayer int                                   `json:"suspend_layer"` // layer index where the interrupt occurred

	InterruptNodeID string                     `json:"interrupt_node_id"`
	InterruptReq    InterruptRequest           `json:"interrupt_req"`
	OriginalInputs  map[string]json.RawMessage `json:"original_inputs"`
	CreatedAt       time.Time                  `json:"created_at"`
}

// clone returns a deep copy of cp's reference-type fields (Activated, the
// two-level PortValues, EdgeStates, OriginalInputs) so a caller mutating a
// loaded Checkpoint cannot corrupt a store's retained copy, and vice versa.
// json.RawMessage values are treated as immutable, so copying the map/slice
// structure (not the underlying bytes) suffices. Scalars + InterruptReq copy
// by value.
func (cp Checkpoint) clone() Checkpoint {
	if cp.Activated != nil {
		act := make(map[string]bool, len(cp.Activated))
		for k, v := range cp.Activated {
			act[k] = v
		}
		cp.Activated = act
	}
	if cp.PortValues != nil {
		pv := make(map[string]map[string]json.RawMessage, len(cp.PortValues))
		for node, ports := range cp.PortValues {
			if ports == nil {
				pv[node] = nil
				continue
			}
			inner := make(map[string]json.RawMessage, len(ports))
			for port, raw := range ports {
				inner[port] = raw
			}
			pv[node] = inner
		}
		cp.PortValues = pv
	}
	if cp.EdgeStates != nil {
		es := make([]EdgeFireState, len(cp.EdgeStates))
		copy(es, cp.EdgeStates)
		cp.EdgeStates = es
	}
	if cp.OriginalInputs != nil {
		oi := make(map[string]json.RawMessage, len(cp.OriginalInputs))
		for k, raw := range cp.OriginalInputs {
			oi[k] = raw
		}
		cp.OriginalInputs = oi
	}
	return cp
}

// CheckpointMeta is the lightweight listing projection of a Checkpoint.
type CheckpointMeta struct {
	RunID, FlowID, InterruptNodeID string
	CreatedAt                      time.Time
}

// CheckpointStore persists and retrieves checkpoints by token (== RunID).
type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, cp Checkpoint) error
	LoadCheckpoint(ctx context.Context, token string) (Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, token string) error
	ListCheckpoints(ctx context.Context, flowID string, limit int) ([]CheckpointMeta, error)
}

// RunResult is the outcome of a resumable run leg: either it completed
// (Outputs set, Suspended nil) or it suspended awaiting human input
// (Suspended set, Outputs nil).
type RunResult struct {
	Outputs   map[string]any
	Suspended *Suspension
}

// Suspension describes a paused run awaiting human input.
type Suspension struct {
	ResumeToken string
	NodeID      string
	Request     InterruptRequest
}

var (
	ErrNoCheckpointStore  = errors.New("flow: interrupt requested but no CheckpointStore configured")
	ErrCheckpointNotFound = errors.New("flow: resume: checkpoint not found")
	ErrFlowChanged        = errors.New("flow: resume: flow definition changed since checkpoint")
	ErrNotCheckpointable  = errors.New("flow: graph not checkpointable (non-serializable port or in-flight stream)")
	ErrMultipleInterrupts = errors.New("flow: multiple pending interrupts in one layer")
	ErrInvalidHumanInput  = errors.New("flow: resume: humanInput key not a declared output port of the interrupt node")
)

// WithCheckpointStore enables the resumable engine path.
func WithCheckpointStore(cs CheckpointStore) EngineOption {
	return func(c *engineConfig) { c.checkpointStore = cs }
}

// NewRunID mints a fresh run identity for the library-only path (no flowd /
// sqlite store). Caller-owned identity per FC-3 option b.
func NewRunID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "run_" + hex.EncodeToString(b[:])
}

// MemoryCheckpointStore is an in-memory CheckpointStore for tests / embedding.
type MemoryCheckpointStore struct {
	mu sync.Mutex
	m  map[string]Checkpoint
}

// NewMemoryCheckpointStore returns an empty in-memory store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{m: make(map[string]Checkpoint)}
}

func (s *MemoryCheckpointStore) SaveCheckpoint(ctx context.Context, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[cp.RunID] = cp.clone()
	return nil
}

func (s *MemoryCheckpointStore) LoadCheckpoint(ctx context.Context, token string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.m[token]
	if !ok {
		return Checkpoint{}, ErrCheckpointNotFound
	}
	return cp.clone(), nil
}

func (s *MemoryCheckpointStore) DeleteCheckpoint(ctx context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, token)
	return nil
}

func (s *MemoryCheckpointStore) ListCheckpoints(ctx context.Context, flowID string, limit int) ([]CheckpointMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CheckpointMeta, 0, len(s.m))
	for _, cp := range s.m {
		if flowID != "" && cp.FlowID != flowID {
			continue
		}
		out = append(out, CheckpointMeta{
			RunID:           cp.RunID,
			FlowID:          cp.FlowID,
			InterruptNodeID: cp.InterruptNodeID,
			CreatedAt:       cp.CreatedAt,
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}
