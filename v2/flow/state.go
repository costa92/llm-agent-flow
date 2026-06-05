package flow

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// ErrStateWriteConflict is returned by the default reducer when more than
// one node in a single layer staged a State write and no WithReducer was
// supplied. The default reducer is reject-on-collision: silently clobbering
// a sibling's write would pass a byte-identity test while corrupting the
// marquee "running summary across nodes" case, so a collision is a hard
// error. Supply WithReducer to define an explicit merge.
var ErrStateWriteConflict = errors.New("flow: state: two nodes wrote State in one layer with no WithReducer")

// stateCell carries the whole-graph State across a single runCore call.
// The committed value is an arbitrary user State S (held as any). Per-node
// writes are buffered in staged (keyed by nodeID, last write per node wins)
// and folded into committed at each layer barrier by reduce, on the single
// engine goroutine. All access to committed/staged is guarded by mu — a
// node that leaks a goroutine outliving Run could otherwise race the
// barrier reduce on staged.
type stateCell struct {
	mu        sync.Mutex
	committed any
	staged    map[string]any
	// reducer is the type-erased form of a user WithReducer. nil means
	// the default reject-on-collision reducer applies.
	reducer func(prev any, writes []any) (any, error)
}

// newStateCell builds a cell seeded with the initial committed State and an
// optional type-erased reducer (nil → default reject-on-collision).
func newStateCell(initial any, reducer func(prev any, writes []any) (any, error)) *stateCell {
	return &stateCell{
		committed: initial,
		staged:    make(map[string]any),
		reducer:   reducer,
	}
}

// stage records nodeID's State write, overwriting any prior write from the
// same node (last-write-per-node).
func (c *stateCell) stage(nodeID string, s any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.staged[nodeID] = s
}

// snapshot returns the committed State under the lock so a node reads a
// consistent value even if a sibling's leaked goroutine is mid-stage.
func (c *stateCell) snapshot() any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

// reduce folds the staged writes into committed and clears staged for the
// next layer. It MUST run on the single engine goroutine at the layer
// barrier (no live node goroutines). Writes are ordered by nodeID — never
// goroutine-finish order — so the result is a pure function of which nodes
// wrote what, preserving byte-identical fresh Invoke.
//
// With no reducer (default), >1 writer is ErrStateWriteConflict; exactly
// one (or zero) writers applies with no error. With a reducer, it is always
// called with (prev, writes-sorted-by-nodeID) and never errors on collision.
func (c *stateCell) reduce() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.staged) == 0 {
		return nil
	}

	ids := make([]string, 0, len(c.staged))
	for id := range c.staged {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	if c.reducer == nil {
		// Default: reject-on-collision.
		if len(ids) > 1 {
			return ErrStateWriteConflict
		}
		c.committed = c.staged[ids[0]]
		c.staged = make(map[string]any)
		return nil
	}

	writes := make([]any, len(ids))
	for i, id := range ids {
		writes[i] = c.staged[id]
	}
	next, err := c.reducer(c.committed, writes)
	if err != nil {
		return err
	}
	c.committed = next
	c.staged = make(map[string]any)
	return nil
}

// stateCtxKey binds a stateCell + the current nodeID into a context.
type stateCtxKey struct{}

type stateCtxVal struct {
	cell   *stateCell
	nodeID string
}

// withStateNode binds BOTH the cell and the current nodeID into ctx so
// SetState can stage under the right node. A nil cell makes the helpers
// inert (hasState=false): State-less flows pay nothing and behave
// byte-identically.
func withStateNode(ctx context.Context, cell *stateCell, nodeID string) context.Context {
	if cell == nil {
		return ctx
	}
	return context.WithValue(ctx, stateCtxKey{}, stateCtxVal{cell: cell, nodeID: nodeID})
}

func stateFromCtx(ctx context.Context) (stateCtxVal, bool) {
	v, ok := ctx.Value(stateCtxKey{}).(stateCtxVal)
	return v, ok && v.cell != nil
}

// StateFromContext returns a COPY of the committed State snapshot. Value
// semantics provide the isolation: if S is a struct, the returned value is
// independent of the cell. NOTE: the copy is shallow — pointer/map/slice
// FIELDS inside S still alias the committed sub-structures, so the caller
// MUST NOT mutate them in place (that would break the determinism contract).
// Returns (zero, false) if no State cell is bound into ctx.
func StateFromContext[S any](ctx context.Context) (S, bool) {
	var zero S
	v, ok := stateFromCtx(ctx)
	if !ok {
		return zero, false
	}
	committed := v.cell.snapshot()
	s, ok := committed.(S)
	if !ok {
		return zero, false
	}
	return s, true
}

// SetState stages a State write under the current nodeID (last SetState
// wins per node, applied synchronously at the layer barrier before Run
// returns). It is a silent no-op if no State cell is bound into ctx, so
// State-less flows stay inert. Writes from a goroutine outliving the node's
// Run are unsupported (only synchronous writes are honored).
func SetState[S any](ctx context.Context, s S) {
	v, ok := stateFromCtx(ctx)
	if !ok {
		return
	}
	v.cell.stage(v.nodeID, s)
}

// StateOption configures the State machinery for a single run. Options are
// passed to the *WithState entry points; the bare Run/RunResumable/Resume
// take none and run with no cell (inert).
type StateOption func(*stateConfig)

// stateConfig accumulates StateOptions before a run builds its cell.
type stateConfig struct {
	hasInitial bool
	initial    any
	reducer    func(prev any, writes []any) (any, error)
}

// WithInitialState seeds the committed State the run starts from.
func WithInitialState[S any](s S) StateOption {
	return func(c *stateConfig) {
		c.hasInitial = true
		c.initial = s
	}
}

// WithReducer supplies the explicit merge applied at each layer barrier
// when one or more nodes staged a State write. It receives the prior
// committed State and the layer's writes sorted by nodeID (deterministic),
// and returns the new committed State. Supplying a reducer turns off the
// default reject-on-collision behavior.
func WithReducer[S any](fn func(prev S, writes []S) S) StateOption {
	return func(c *stateConfig) {
		c.reducer = func(prev any, writes []any) (any, error) {
			ws := make([]S, len(writes))
			for i, w := range writes {
				ws[i], _ = w.(S)
			}
			p, _ := prev.(S)
			return fn(p, ws), nil
		}
	}
}

// buildStateCell returns a cell for the run, or nil when State is unused (no
// initial state AND no reducer). A nil cell makes withStateNode/the helpers
// inert — zero behavior change for existing flows.
func buildStateCell(opts []StateOption) *stateCell {
	if len(opts) == 0 {
		return nil
	}
	var cfg stateConfig
	for _, o := range opts {
		o(&cfg)
	}
	if !cfg.hasInitial && cfg.reducer == nil {
		return nil
	}
	return newStateCell(cfg.initial, cfg.reducer)
}
