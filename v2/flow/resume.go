package flow

import (
	"context"
	"errors"
	"fmt"
)

// RunResumable executes a fresh run that may suspend on a node interrupt.
// On suspend, RunResult.Suspended is set and RunResult.Outputs is nil;
// otherwise Outputs is set. runID is the caller-owned run identity (see
// NewRunID) under which any checkpoint is stored.
func (e *Engine) RunResumable(ctx context.Context, runID string, inputs map[string]any) (RunResult, error) {
	out, susp, err := e.runCore(ctx, inputs, nil, runID, true, nil)
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
	if cp.FlowHash != e.flowHash {
		return RunResult{}, ErrFlowChanged
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
	})
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{Outputs: out, Suspended: susp}, nil
}
