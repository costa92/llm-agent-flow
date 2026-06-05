package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// RunResumable executes a fresh run that may suspend on a node interrupt.
// On suspend, RunResult.Suspended is set and RunResult.Outputs is nil;
// otherwise Outputs is set. runID is the caller-owned run identity (see
// NewRunID) under which any checkpoint is stored.
func (e *Engine) RunResumable(ctx context.Context, runID string, inputs map[string]any) (RunResult, error) {
	out, susp, err := e.runCore(ctx, inputs, nil, runID, true, nil, nil)
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{Outputs: out, Suspended: susp}, nil
}

// RunResumableWithState is RunResumable with optional graph State. With no
// options it is byte-identical to RunResumable (no cell, helpers inert).
func (e *Engine) RunResumableWithState(ctx context.Context, runID string, inputs map[string]any, opts ...StateOption) (RunResult, error) {
	out, susp, err := e.runCore(ctx, inputs, nil, runID, true, nil, buildStateCell(opts))
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{Outputs: out, Suspended: susp}, nil
}

// Resume continues a suspended run identified by token, injecting
// humanInput as the interrupt node's output ports, firing the deferred
// out-edges of the interrupt node, then resuming the layer loop from the
// layer after the suspend point.
func (e *Engine) Resume(ctx context.Context, runID, token string, humanInput map[string]any) (RunResult, error) {
	return e.resumeWithCell(ctx, runID, token, humanInput, nil)
}

// ResumeWithState is Resume with optional graph State for the resumed
// layers. With no options it is byte-identical to Resume.
//
// When the checkpoint carries durable State (Checkpoint.State) AND the
// caller supplies State opts, the restored State OVERRIDES the supplied
// WithInitialState seed (durable State wins) and the State-type fingerprint
// is guarded against the checkpoint's StateCodec (mismatch → ErrFlowChanged).
// O3: if the caller supplies NO State opts but the checkpoint has State, the
// restored State is dropped (seed-fresh) — no phantom cell is built. See
// resumeWithCell.
func (e *Engine) ResumeWithState(ctx context.Context, runID, token string, humanInput map[string]any, opts ...StateOption) (RunResult, error) {
	return e.resumeWithCell(ctx, runID, token, humanInput, buildStateCell(opts))
}

func (e *Engine) resumeWithCell(ctx context.Context, runID, token string, humanInput map[string]any, cell *stateCell) (RunResult, error) {
	if e.checkpointStore == nil {
		return RunResult{}, ErrNoCheckpointStore
	}
	cp, err := e.checkpointStore.LoadCheckpoint(ctx, token)
	if err != nil {
		if errors.Is(err, ErrCheckpointNotFound) {
			return RunResult{}, err
		}
		// A genuine store/IO failure is NOT a not-found — wrap with context
		// but do NOT mislabel it under ErrCheckpointNotFound, or callers'
		// errors.Is(err, ErrCheckpointNotFound) would treat a transient
		// failure as a permanent missing checkpoint.
		return RunResult{}, fmt.Errorf("flow: resume: load checkpoint: %w", err)
	}
	switch {
	case cp.FlowHash == e.flowHash:
		// identical flow — fast path, allow.
	case cp.StructHash != "":
		// v2 checkpoint: allow iff the STRUCTURE is unchanged (only Config
		// differs). A struct-hash mismatch means ports/edges/topology changed.
		if cp.StructHash != e.structHash {
			return RunResult{}, ErrFlowChanged
		}
	default:
		// v1 checkpoint (no StructHash) or otherwise: strict full-hash compare.
		return RunResult{}, ErrFlowChanged
	}

	// State restore + type-change guard. Only when the resume supplies a cell
	// (State opts). O3: a State-less resume (cell==nil) of a State-bearing
	// checkpoint silently drops the restored State (seed-fresh) — we do NOT
	// fabricate a phantom cell.
	if cell != nil && len(cp.State) > 0 {
		// MAJOR-4 guard: the resume run's State fingerprint must match the
		// checkpoint's. The cell's committed value carries the resume run's S
		// (from WithInitialState); compare its fingerprint to cp.StateCodec.
		// A different tag (different S) or a same-tag/different-shape S both
		// surface here as a mismatch → ErrFlowChanged, before we Unmarshal
		// into a mismatched shape (which would corrupt silently). For the
		// pure-engine route S is a run option, so tag stability across a
		// shape change is the caller's responsibility; the fingerprint still
		// catches the same-tag/different-shape case.
		if cp.StateCodec != "" {
			seed := cell.snapshot()
			if seed != nil {
				tag, terr := stateCodecTag(reflect.TypeOf(seed))
				if terr != nil {
					return RunResult{}, fmt.Errorf("flow: resume: state: %w", terr)
				}
				if tag != cp.StateCodec {
					return RunResult{}, ErrFlowChanged
				}
			}
		}
		// Decode the durable State and override the cell's committed seed —
		// durable State wins over WithInitialState.
		restored, derr := decodePortValue(cp.State)
		if derr != nil {
			return RunResult{}, fmt.Errorf("flow: resume: state: %w", derr)
		}
		cell.setCommitted(restored)
	}

	// Restore activated cursor.
	activated := make(map[string]bool, len(cp.Activated))
	for k, v := range cp.Activated {
		activated[k] = v
	}

	// Decode port values into fresh, mutable inner maps.
	portValues := make(map[string]map[string]any, len(cp.PortValues))
	for nodeID, ports := range cp.PortValues {
		inner := make(map[string]any, len(ports))
		for port, raw := range ports {
			v, derr := decodePortValue(raw)
			if derr != nil {
				return RunResult{}, fmt.Errorf("flow: resume: node %q port %q: %w", nodeID, port, derr)
			}
			inner[port] = v
		}
		portValues[nodeID] = inner
	}

	edgeStates := make([]EdgeFireState, len(cp.EdgeStates))
	copy(edgeStates, cp.EdgeStates)

	// FC-NEW4: validate humanInput keys ∈ interrupt node's declared output ports.
	node := e.nodes[cp.InterruptNodeID]
	if node == nil {
		return RunResult{}, fmt.Errorf("flow: resume: interrupt node %q not found in flow", cp.InterruptNodeID)
	}
	declared := make(map[string]bool, len(node.Outputs()))
	for _, p := range node.Outputs() {
		declared[p.Name] = true
	}
	for k := range humanInput {
		if !declared[k] {
			return RunResult{}, fmt.Errorf("%w: %q", ErrInvalidHumanInput, k)
		}
	}

	// Inject humanInput as the interrupt node's output ports.
	if portValues[cp.InterruptNodeID] == nil {
		portValues[cp.InterruptNodeID] = make(map[string]any)
	}
	for k, v := range humanInput {
		portValues[cp.InterruptNodeID][k] = v
	}
	activated[cp.InterruptNodeID] = true

	// Fire the deferred edges (interrupt node's out-edges) now.
	for i, edge := range e.flow.Edges {
		if edgeStates[i] != EdgeDeferred {
			continue
		}
		srcPorts := portValues[edge.Source.Node]
		value, ok := srcPorts[edge.Source.Port]
		if !ok {
			// humanInput didn't supply this port → edge stays unfired.
			continue
		}
		cond := e.edgeCond[i]
		if cond != nil {
			sval, ok := value.(string)
			if !ok {
				return RunResult{}, fmt.Errorf("flow: resume: edge[%d] (%s.%s → %s.%s) condition: source value is %T, not string — conditional (CEL) edges route on string values only",
					i, edge.Source.Node, edge.Source.Port, edge.Target.Node, edge.Target.Port, value)
			}
			fire, evalErr := cond.Evaluate(ctx, CondEnv{Value: sval})
			if evalErr != nil {
				return RunResult{}, fmt.Errorf("flow: resume: edge[%d] (%s.%s → %s.%s) condition: %w",
					i, edge.Source.Node, edge.Source.Port, edge.Target.Node, edge.Target.Port, evalErr)
			}
			if !fire {
				edgeStates[i] = EdgeDeadFalse
				continue
			}
		}
		if portValues[edge.Target.Node] == nil {
			portValues[edge.Target.Node] = make(map[string]any)
		}
		portValues[edge.Target.Node][edge.Target.Port] = value
		activated[edge.Target.Node] = true
		edgeStates[i] = EdgeFired
	}

	out, susp, err := e.runCore(ctx, nil, nil, runID, true, &resumeSeed{
		activated:  activated,
		portValues: portValues,
		edgeStates: edgeStates,
		startLayer: cp.SuspendLayer + 1,
	}, cell)
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{Outputs: out, Suspended: susp}, nil
}
