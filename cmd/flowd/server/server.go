// Package server is the HTTP layer for cmd/flowd. It is split into its
// own package so it can be exercised by httptest without booting the
// full flowd binary.
//
// v0.0.2 surface — one flow per server instance:
//
//	GET  /healthz       → 200 "ok"
//	POST /run           → sync; body {"inputs": {...}} → {"outputs": {...}}
//	POST /run/stream    → SSE stream of FlowEvent JSON payloads
//
// A future phase will introduce flow CRUD (/flows, /flows/{id}, etc.)
// once the run-history store lands; for now this layer wraps one
// engine and accepts a fresh inputs map per request.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/costa92/llm-agent-flow/flow"
)

// NewMux wires the v0.0.2 endpoints against a compiled Engine. The
// supplied logger receives one line per request error.
func NewMux(engine *flow.Engine, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/run", runHandler(engine, logger))
	mux.HandleFunc("/run/stream", streamHandler(engine, logger))
	return mux
}

type runRequest struct {
	Inputs map[string]string `json:"inputs"`
}

type runResponse struct {
	Outputs map[string]string `json:"outputs"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func runHandler(engine *flow.Engine, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
			return
		}
		var req runRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
			return
		}
		outputs, err := engine.Run(r.Context(), req.Inputs)
		if err != nil {
			logger.Printf("flowd: /run: %v", err)
			writeError(w, statusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, runResponse{Outputs: outputs})
	}
}

func streamHandler(engine *flow.Engine, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
			return
		}
		var req runRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, errors.New("response writer does not support streaming"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ch, err := engine.RunStream(r.Context(), req.Inputs)
		if err != nil {
			// rare — RunStream only errors on configuration. Encode it
			// as one terminal event so the client can detect it.
			writeSSE(w, "flow_err", map[string]string{"error": err.Error()})
			flusher.Flush()
			return
		}
		for ev := range ch {
			payload := streamPayload(ev)
			writeSSE(w, eventKindString(ev.Kind), payload)
			flusher.Flush()
			if ev.Kind == flow.FlowErr {
				logger.Printf("flowd: /run/stream: %v", ev.Err)
				return
			}
		}
	}
}

func decodeJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

// writeSSE writes a single Server-Sent Events frame:
//
//	event: <kind>
//	data: <json>
//
// followed by the required blank line. Caller must Flush() between
// frames if it wants real streaming.
func writeSSE(w io.Writer, kind string, data any) {
	raw, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, raw)
}

func eventKindString(k flow.FlowEventKind) string {
	switch k {
	case flow.FlowStarted:
		return "flow_started"
	case flow.NodeStarted:
		return "node_started"
	case flow.NodeFinished:
		return "node_finished"
	case flow.NodeSkipped:
		return "node_skipped"
	case flow.FlowDone:
		return "flow_done"
	case flow.FlowErr:
		return "flow_err"
	default:
		return "unknown"
	}
}

// streamPayload reshapes a FlowEvent into the JSON shape sent in SSE
// data frames. We omit the Kind field (it's the SSE event: name) and
// the always-empty *Map fields for that variant.
func streamPayload(ev flow.FlowEvent) map[string]any {
	m := map[string]any{}
	if ev.FlowID != "" {
		m["flow"] = ev.FlowID
	}
	if ev.NodeID != "" {
		m["node"] = ev.NodeID
	}
	if ev.Input != nil {
		m["input"] = ev.Input
	}
	if ev.Output != nil {
		m["output"] = ev.Output
	}
	if ev.Outputs != nil {
		m["outputs"] = ev.Outputs
	}
	if ev.Err != nil {
		m["error"] = ev.Err.Error()
	}
	return m
}

// statusForError maps the few error shapes /run can return into HTTP
// status codes. Validation / missing-input errors → 400; everything
// else → 500.
func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var ve *flow.ValidateError
	if errors.As(err, &ve) {
		return http.StatusBadRequest
	}
	if errors.Is(err, flow.ErrEmptyFlow) {
		return http.StatusBadRequest
	}
	// missing input / port-not-emitted errors come back as plain fmt-
	// wrapped errors from engine.run. They are caller-faults.
	msg := err.Error()
	for _, hint := range []string{"missing required input", "awaits port", "awaits"} {
		if contains(msg, hint) {
			return http.StatusBadRequest
		}
	}
	return http.StatusInternalServerError
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
