package server

import (
	"encoding/json"
	"fmt"

	"github.com/costa92/llm-agent-flow/flow"
	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

type debugFlowView struct {
	Flow    flowstore.FlowMeta  `json:"flow"`
	Graph   debugGraph          `json:"graph"`
	Runs    []flowstore.RunMeta `json:"runs"`
	RawJSON json.RawMessage     `json:"raw_json,omitempty"`
}

type debugGraph struct {
	Nodes   []debugNode      `json:"nodes"`
	Edges   []debugEdge      `json:"edges"`
	Inputs  []debugNamedPort `json:"inputs,omitempty"`
	Outputs []debugNamedPort `json:"outputs,omitempty"`
}

type debugNode struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Config json.RawMessage `json:"config,omitempty"`
}

type debugEdge struct {
	Source    debugPortRef `json:"source"`
	Target    debugPortRef `json:"target"`
	Condition string       `json:"condition,omitempty"`
}

type debugNamedPort struct {
	Name string       `json:"name,omitempty"`
	Ref  debugPortRef `json:"ref"`
}

type debugPortRef struct {
	Node string `json:"node"`
	Port string `json:"port"`
}

type debugRunView struct {
	Run       flowstore.RunRecord    `json:"run"`
	Events    []debugRunEvent        `json:"events"`
	Nodes     []debugRunNode         `json:"nodes,omitempty"`
	Timeline  []debugRunTimelineItem `json:"timeline"`
	Replay    string                 `json:"replay"`
	Suspended *debugSuspendedRun     `json:"suspended,omitempty"`
}

type debugRunEvent struct {
	Seq         int                    `json:"seq"`
	Kind        flowstore.RunEventKind `json:"kind"`
	NodeID      string                 `json:"node_id,omitempty"`
	Payload     json.RawMessage        `json:"payload,omitempty"`
	RawPayload  string                 `json:"raw_payload,omitempty"`
	Timestamp   string                 `json:"ts"`
	DecodeError string                 `json:"decode_error,omitempty"`
}

type debugRunNode struct {
	ID          string          `json:"id"`
	Status      string          `json:"status"`
	StartedSeq  int             `json:"started_seq,omitempty"`
	FinishedSeq int             `json:"finished_seq,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type debugRunTimelineItem struct {
	Seq         int                    `json:"seq"`
	Kind        flowstore.RunEventKind `json:"kind"`
	NodeID      string                 `json:"node_id,omitempty"`
	Status      string                 `json:"status,omitempty"`
	DecodeError string                 `json:"decode_error,omitempty"`
}

type debugSuspendedRun struct {
	ResumeToken   string          `json:"resume_token,omitempty"`
	InterruptNode string          `json:"interrupt_node,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	RawPayload    string          `json:"raw_payload,omitempty"`
	DecodeError   string          `json:"decode_error,omitempty"`
}

func debugViewFromFlow(rec flowstore.FlowRecord, f flow.Flow, runs []flowstore.RunMeta) debugFlowView {
	view := debugFlowView{
		Flow:    rec.FlowMeta,
		Runs:    runs,
		RawJSON: append(json.RawMessage(nil), rec.JSON...),
	}
	view.Graph.Nodes = make([]debugNode, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		view.Graph.Nodes = append(view.Graph.Nodes, debugNode{
			ID:     n.ID,
			Type:   n.Type,
			Config: append(json.RawMessage(nil), n.Config...),
		})
	}
	view.Graph.Edges = make([]debugEdge, 0, len(f.Edges))
	for _, e := range f.Edges {
		view.Graph.Edges = append(view.Graph.Edges, debugEdge{
			Source:    debugRef(e.Source),
			Target:    debugRef(e.Target),
			Condition: e.Condition,
		})
	}
	view.Graph.Inputs = make([]debugNamedPort, 0, len(f.Inputs))
	for _, in := range f.Inputs {
		view.Graph.Inputs = append(view.Graph.Inputs, debugNamedPort{
			Name: in.Name,
			Ref:  debugRef(in.PortRef),
		})
	}
	view.Graph.Outputs = make([]debugNamedPort, 0, len(f.Outputs))
	for _, out := range f.Outputs {
		view.Graph.Outputs = append(view.Graph.Outputs, debugNamedPort{
			Name: out.Name,
			Ref:  debugRef(out.PortRef),
		})
	}
	return view
}

func debugRef(ref flow.PortRef) debugPortRef {
	return debugPortRef{Node: ref.Node, Port: ref.Port}
}

func debugViewFromRun(run flowstore.RunRecord, events []flowstore.RunEvent) debugRunView {
	view := debugRunView{
		Run:      run,
		Events:   make([]debugRunEvent, 0, len(events)),
		Timeline: make([]debugRunTimelineItem, 0, len(events)),
		Replay:   "/runs/" + run.ID + "/replay",
	}
	nodes := map[string]*debugRunNode{}
	nodeOrder := make([]string, 0)
	for _, ev := range events {
		payload, decodeErr := decodeDebugPayload(ev.Payload)
		view.Events = append(view.Events, debugRunEvent{
			Seq:         ev.Seq,
			Kind:        ev.Kind,
			NodeID:      ev.NodeID,
			Payload:     debugPayloadForJSON(ev.Payload),
			RawPayload:  debugRawPayloadForJSON(ev.Payload),
			Timestamp:   ev.Timestamp.Format("2006-01-02T15:04:05.999999999Z07:00"),
			DecodeError: decodeErrString(decodeErr),
		})
		item := debugRunTimelineItem{
			Seq:         ev.Seq,
			Kind:        ev.Kind,
			NodeID:      ev.NodeID,
			DecodeError: decodeErrString(decodeErr),
		}
		if ev.NodeID != "" {
			node := debugNodeFor(nodes, &nodeOrder, ev.NodeID)
			applyDebugNodeEvent(node, ev, payload, decodeErr)
			item.Status = node.Status
		} else if ev.Kind == flowstore.RunEventFlowErr {
			item.Status = "failed"
		} else if ev.Kind == flowstore.RunEventFlowDone {
			item.Status = "done"
		}
		view.Timeline = append(view.Timeline, item)
		if ev.Kind == flowstore.RunEventFlowSuspended {
			view.Suspended = &debugSuspendedRun{
				ResumeToken:   run.ResumeToken,
				InterruptNode: run.InterruptNode,
				Payload:       debugPayloadForJSON(ev.Payload),
				RawPayload:    debugRawPayloadForJSON(ev.Payload),
				DecodeError:   decodeErrString(decodeErr),
			}
			if tok := payloadString(payload, "resume_token"); tok != "" {
				view.Suspended.ResumeToken = tok
			}
			if node := payloadString(payload, "node"); node != "" {
				view.Suspended.InterruptNode = node
			}
		}
	}
	if view.Suspended == nil && run.Status == flowstore.RunStatusSuspended {
		view.Suspended = &debugSuspendedRun{
			ResumeToken:   run.ResumeToken,
			InterruptNode: run.InterruptNode,
		}
	}
	view.Nodes = make([]debugRunNode, 0, len(nodeOrder))
	for _, id := range nodeOrder {
		view.Nodes = append(view.Nodes, *nodes[id])
	}
	return view
}

func debugNodeFor(nodes map[string]*debugRunNode, order *[]string, id string) *debugRunNode {
	if n, ok := nodes[id]; ok {
		return n
	}
	n := &debugRunNode{ID: id, Status: "observed"}
	nodes[id] = n
	*order = append(*order, id)
	return n
}

func applyDebugNodeEvent(node *debugRunNode, ev flowstore.RunEvent, payload map[string]json.RawMessage, decodeErr error) {
	switch ev.Kind {
	case flowstore.RunEventNodeStarted:
		node.Status = "running"
		if node.StartedSeq == 0 {
			node.StartedSeq = ev.Seq
		}
		if decodeErr == nil {
			node.Input = copyRaw(payload["input"])
		}
	case flowstore.RunEventNodeFinished:
		node.Status = "done"
		node.FinishedSeq = ev.Seq
		if node.StartedSeq == 0 {
			node.StartedSeq = ev.Seq
		}
		if decodeErr == nil {
			node.Output = copyRaw(payload["output"])
		}
	case flowstore.RunEventNodeSkipped:
		node.Status = "skipped"
		node.FinishedSeq = ev.Seq
	case flowstore.RunEventNodeInterrupted:
		node.Status = "interrupted"
		node.FinishedSeq = ev.Seq
	case flowstore.RunEventFlowSuspended:
		node.Status = "suspended"
		node.FinishedSeq = ev.Seq
	case flowstore.RunEventFlowErr:
		node.Status = "failed"
		node.FinishedSeq = ev.Seq
		if decodeErr == nil {
			node.Error = payloadString(payload, "error")
		}
	}
	if decodeErr != nil && node.Error == "" {
		node.Error = fmt.Sprintf("payload decode: %v", decodeErr)
	}
}

func decodeDebugPayload(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func payloadString(payload map[string]json.RawMessage, key string) string {
	if len(payload[key]) == 0 {
		return ""
	}
	var out string
	if err := json.Unmarshal(payload[key], &out); err == nil {
		return out
	}
	return ""
}

func copyRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func debugPayloadForJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	return copyRaw(raw)
}

func debugRawPayloadForJSON(raw json.RawMessage) string {
	if len(raw) == 0 || json.Valid(raw) {
		return ""
	}
	return string(raw)
}

func decodeErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
