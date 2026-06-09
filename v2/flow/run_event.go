package flow

import contractagents "github.com/costa92/llm-agent-contract/agents"

// RunEventFromFlowEvent converts a flow Event into the shared Agent runtime
// event envelope. The flow Runner.RunStream contract remains unchanged.
func RunEventFromFlowEvent(ev Event) contractagents.RunEvent {
	out := contractagents.RunEvent{
		Kind:        contractagents.RunEventFlowStarted,
		FlowID:      ev.FlowID,
		NodeID:      ev.NodeID,
		Input:       cloneAnyMap(ev.Input),
		Output:      cloneAnyMap(ev.Output),
		Outputs:     cloneAnyMap(ev.Outputs),
		Metadata:    cloneStringMap(ev.Metadata),
		ResumeToken: ev.ResumeToken,
		Err:         ev.Err,
	}
	switch ev.Kind {
	case FlowStarted:
		out.Kind = contractagents.RunEventFlowStarted
	case NodeStarted:
		out.Kind = contractagents.RunEventFlowNodeStarted
	case NodeFinished:
		out.Kind = contractagents.RunEventFlowNodeDone
	case NodeSkipped:
		out.Kind = contractagents.RunEventFlowNodeSkipped
	case FlowDone:
		out.Kind = contractagents.RunEventFlowDone
	case FlowErr:
		out.Kind = contractagents.RunEventFlowError
	case NodeInterrupted:
		out.Kind = contractagents.RunEventInterrupt
		if ev.Request != nil {
			req := *ev.Request
			out.Payload = req
		}
	case FlowSuspended:
		out.Kind = contractagents.RunEventSuspended
		if ev.Request != nil {
			req := *ev.Request
			out.Payload = req
		}
	default:
		out.Kind = contractagents.RunEventFlowStarted
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
