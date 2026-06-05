// Package server is the HTTP layer for cmd/flowd. It is split into
// its own package so it can be exercised by httptest without booting
// the full flowd binary.
//
// Two entry points:
//
//   - NewMux(engine, logger) — legacy, single-engine. Wires only
//     /healthz, /run, /run/stream against the provided Engine. No
//     CRUD, no persistence. Retained for existing callers.
//   - New(cfg) (*Server, error) — v0.0.5 surface with a backing
//     flow/store.Store, lazy engine cache, and the full REST shape:
//     /flows CRUD, /flows/{id}/run + /run/stream, /flows/{id}/runs,
//     /runs/{id}. When Config.LegacyFlowID is set, /run and
//     /run/stream also route to that id so v0.0.4 clients keep
//     working.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/costa92/llm-agent-flow/flow"
	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	v2flow "github.com/costa92/llm-agent-flow/v2/flow"
)

// Ensure stdlib sync stays used (runWithStore still uses sync.Mutex
// for emit serialization in some call paths). Removing the engine
// cache's sync.Map dropped one user but emitMu in run handlers
// still pulls it in transitively via the runtime. This import-
// pinning comment is a hedge against a future cleanup that removes
// the last sync usage and leaves the line dangling.
var _ sync.Mutex

// NewMux is the legacy single-engine wrapper. The returned handler
// exposes /healthz + /run + /run/stream against the supplied Engine.
// It does not persist anything and does not expose CRUD endpoints —
// use New(cfg) for the full v0.0.5 surface.
func NewMux(engine *flow.Engine, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/run", runHandler(engine, logger))
	mux.HandleFunc("/run/stream", streamHandler(engine, logger))
	return mux
}

// Config is the input bundle for New. Required: Store, Registry.
type Config struct {
	Store              flowstore.Store         // persistence
	Registry           *flow.NodeRegistry      // shared across compiled engines
	Tools              flow.ToolMap            // tool catalog wired into every engine
	Cond               flow.ConditionEvaluator // optional — required only for flows with conditions
	MaxNodeConcurrency int                     // per-layer cap; 0 = unlimited
	Logger             *log.Logger             // request-error log; defaults to log.Default

	// LegacyFlowID, when non-empty, makes /run and /run/stream route
	// to this flow id. Bootstraps the v0.0.4 single-flow workflow on
	// top of the v0.0.5 store-backed server.
	LegacyFlowID string

	// Authenticator, when non-nil, gates every endpoint except
	// /healthz. The default (nil) leaves the API open — backward
	// compatible with v0.0.7 callers. Use BearerTokenAuthenticator
	// for the bundled static-token implementation, or supply a custom
	// implementation for JWT / OAuth / mTLS.
	Authenticator Authenticator

	// EngineCacheSize caps the number of compiled flow engines kept
	// in memory. A non-positive value disables bounding — every
	// flow_id compiled stays cached indefinitely (the v0.1.0
	// behavior). A positive value enables LRU eviction once the
	// cache reaches that size; entries are touched on every run.
	EngineCacheSize int

	// V2Registry / V2Checkpoints enable the additive flowd v2 resumable
	// (HITL) path. When BOTH are non-nil, the run handler honors a
	// {"resumable":true} request opt-in and the resume route is
	// registered. When either is nil the server behaves exactly as
	// v0.1 — the v2 path is fully gated.
	V2Registry    *v2flow.NodeRegistry
	V2Checkpoints v2flow.CheckpointStore
}

// Server is the v0.0.5 store-backed HTTP layer. Concurrent-safe;
// construct once and serve forever.
type Server struct {
	cfg       Config
	engines   *engineCache // LRU-bounded compiled-flow cache (v0.1)
	enginesV2 *engineCache // separate cache for compiled v2 engines
}

// New returns a Server wired with the supplied Config.
func New(cfg Config) (*Server, error) {
	if cfg.Store == nil {
		return nil, errors.New("flowd/server: Config.Store is required")
	}
	if cfg.Registry == nil {
		return nil, errors.New("flowd/server: Config.Registry is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &Server{
		cfg:       cfg,
		engines:   newEngineCache(cfg.EngineCacheSize),
		enginesV2: newEngineCache(cfg.EngineCacheSize),
	}, nil
}

// Handler returns the HTTP handler this Server serves.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)

	mux.HandleFunc("POST /flows", s.handleCreateFlow)
	mux.HandleFunc("GET /flows", s.handleListFlows)
	mux.HandleFunc("GET /flows/{id}", s.handleGetFlow)
	mux.HandleFunc("PUT /flows/{id}", s.handlePutFlow)
	mux.HandleFunc("DELETE /flows/{id}", s.handleDeleteFlow)

	mux.HandleFunc("POST /flows/{id}/run", s.handleRun)
	mux.HandleFunc("POST /flows/{id}/run/stream", s.handleRunStream)
	mux.HandleFunc("GET /flows/{id}/runs", s.handleListRunsForFlow)

	mux.HandleFunc("GET /runs/{id}", s.handleGetRun)
	mux.HandleFunc("GET /runs/{id}/events", s.handleListRunEvents)
	mux.HandleFunc("POST /runs/{id}/replay", s.handleReplayRun)

	// Additive v2 resumable (HITL) resume route — registered only when
	// the v2 engine path is fully wired.
	if s.cfg.V2Registry != nil && s.cfg.V2Checkpoints != nil {
		mux.HandleFunc("POST /flows/{id}/runs/{runID}/resume", s.handleResumeRun)
	}

	// Legacy single-flow routes for v0.0.4 backward compat.
	if s.cfg.LegacyFlowID != "" {
		mux.HandleFunc("POST /run", s.handleLegacyRun)
		mux.HandleFunc("POST /run/stream", s.handleLegacyRunStream)
	}

	return withAuth(s.cfg.Authenticator, mux)
}

// Close releases the engine cache. Underlying Store ownership stays
// with the caller.
func (s *Server) Close() {
	s.engines = newEngineCache(s.cfg.EngineCacheSize)
	s.enginesV2 = newEngineCache(s.cfg.EngineCacheSize)
}

// engineFor returns the compiled Engine for id, lazily compiling +
// caching on miss. The engineCache uses LRU eviction when full —
// PUT and DELETE handlers still call engineEvict for immediate
// invalidation. The (*flowstore.FlowRecord, ...) return shape is
// preserved for v0.1 API compatibility; the record is currently nil
// on cache hit (callers do not consume it on hit paths).
func (s *Server) engineFor(id string) (*flow.Engine, *flowstore.FlowRecord, error) {
	if v, ok := s.engines.Get(id); ok {
		return v.(*flow.Engine), nil, nil
	}
	rec, err := s.cfg.Store.GetFlow(serverCtxFor(s), id)
	if err != nil {
		return nil, nil, err
	}
	opts := []flow.EngineOption{flow.WithMaxNodeConcurrency(s.cfg.MaxNodeConcurrency)}
	if s.cfg.Cond != nil {
		opts = append(opts, flow.WithConditionEvaluator(s.cfg.Cond))
	}
	eng, err := flow.LoadCompile(bytesReader(rec.JSON), s.cfg.Registry, flow.Deps{Tools: s.cfg.Tools}, opts...)
	if err != nil {
		return nil, nil, err
	}
	s.engines.Set(id, eng)
	return eng, &rec, nil
}

func (s *Server) engineEvict(id string) {
	s.engines.Delete(id)
	if s.enginesV2 != nil {
		s.enginesV2.Delete(id)
	}
}

// engineForV2 mirrors engineFor but builds a v2 engine (any-carrier,
// checkpoint-enabled) and caches it in the separate enginesV2 LRU. The
// stored flow JSON is the v0.1 IR, which the v2 loader accepts unchanged
// (v2 JSON is a superset). Returns flowstore.ErrNotFound for unknown ids.
func (s *Server) engineForV2(id string) (*v2flow.Engine, error) {
	if v, ok := s.enginesV2.Get(id); ok {
		return v.(*v2flow.Engine), nil
	}
	rec, err := s.cfg.Store.GetFlow(serverCtxFor(s), id)
	if err != nil {
		return nil, err
	}
	eng, err := v2flow.LoadCompile(
		bytesReader(rec.JSON),
		s.cfg.V2Registry,
		v2flow.Deps{},
		v2flow.WithCheckpointStore(s.cfg.V2Checkpoints),
	)
	if err != nil {
		return nil, err
	}
	s.enginesV2.Set(id, eng)
	return eng, nil
}

// healthHandler is shared between legacy NewMux and New.Handler.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok")
}

// ----------------------------------------------------------------------
// Legacy single-engine handlers (NewMux path)
// ----------------------------------------------------------------------

type runRequest struct {
	Inputs map[string]string `json:"inputs"`
	// Resumable opts a run into the additive v2 resumable (HITL) engine
	// path. Honored only when the server is configured with a V2Registry
	// + V2Checkpoints; otherwise ignored and the v0.1 path runs.
	Resumable bool `json:"resumable,omitempty"`
}

// suspendedInfo is the resume handle surfaced to the client when a v2 run
// pauses awaiting human input.
type suspendedInfo struct {
	ResumeToken string                  `json:"resume_token"`
	NodeID      string                  `json:"node"`
	Request     v2flow.InterruptRequest `json:"request"`
}

type runResponse struct {
	Outputs   map[string]string `json:"outputs"`
	RunID     string            `json:"run_id,omitempty"`
	Suspended *suspendedInfo    `json:"suspended,omitempty"`
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
		runSSE(w, r, engine, req.Inputs, "", nil, logger)
	}
}

// ----------------------------------------------------------------------
// v0.0.5 store-backed handlers (New(cfg).Handler() path)
// ----------------------------------------------------------------------

type createFlowRequest struct {
	ID   string          `json:"id"`
	Name string          `json:"name,omitempty"`
	Flow json.RawMessage `json:"flow"`
}

func (s *Server) handleCreateFlow(w http.ResponseWriter, r *http.Request) {
	var req createFlowRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
		return
	}
	id, name, body, ferr := flowHeaderFromBody(req.ID, req.Name, req.Flow)
	if ferr != nil {
		writeError(w, http.StatusBadRequest, ferr)
		return
	}
	if cerr := s.compileProbe(body); cerr != nil {
		writeError(w, http.StatusBadRequest, cerr)
		return
	}
	rec, err := s.cfg.Store.PutFlow(r.Context(), id, name, body, true)
	if errors.Is(err, flowstore.ErrAlreadyExists) {
		writeError(w, http.StatusConflict, fmt.Errorf("flow %q already exists", id))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: POST /flows: %v", err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, rec)
}

func (s *Server) handlePutFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req createFlowRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
		return
	}
	// PUT URL is the source of truth; if a body id is supplied and
	// it disagrees, reject so the caller doesn't silently overwrite
	// the wrong row.
	if req.ID != "" && req.ID != id {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body id %q does not match URL id %q", req.ID, id))
		return
	}
	if len(req.Flow) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("flow field is required"))
		return
	}
	if cerr := s.compileProbe(req.Flow); cerr != nil {
		writeError(w, http.StatusBadRequest, cerr)
		return
	}
	name := req.Name
	if name == "" {
		_, name, _, _ = flowHeaderFromBody(id, "", req.Flow)
	}
	rec, err := s.cfg.Store.PutFlow(r.Context(), id, name, req.Flow, false)
	if err != nil {
		s.cfg.Logger.Printf("flowd: PUT /flows/%s: %v", id, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	s.engineEvict(id)
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleListFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := s.cfg.Store.ListFlows(r.Context(), 0)
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET /flows: %v", err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flows})
}

func (s *Server) handleGetFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.cfg.Store.GetFlow(r.Context(), id)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("flow %q not found", id))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET /flows/%s: %v", id, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleDeleteFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.cfg.Store.DeleteFlow(r.Context(), id); err != nil {
		if errors.Is(err, flowstore.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("flow %q not found", id))
			return
		}
		s.cfg.Logger.Printf("flowd: DELETE /flows/%s: %v", id, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	s.engineEvict(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Peek at the request to honor the {"resumable":true} opt-in. The
	// body is buffered + restored so the v0.1 sync path (runWithStore)
	// re-decodes exactly as before.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
		return
	}
	var req runRequest
	if derr := decodeJSON(bytesReader(body), &req); derr != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", derr))
		return
	}
	if req.Resumable && s.cfg.V2Registry != nil {
		s.runResumableV2(w, r, id, req)
		return
	}
	r.Body = io.NopCloser(bytesReader(body))
	s.runWithStore(w, r, id, false)
}

func (s *Server) handleRunStream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.runWithStore(w, r, id, true)
}

func (s *Server) handleLegacyRun(w http.ResponseWriter, r *http.Request) {
	s.runWithStore(w, r, s.cfg.LegacyFlowID, false)
}

func (s *Server) handleLegacyRunStream(w http.ResponseWriter, r *http.Request) {
	s.runWithStore(w, r, s.cfg.LegacyFlowID, true)
}

// runWithStore is the unified entry for /flows/{id}/run and
// /flows/{id}/run/stream (plus the legacy aliases). Regardless of
// the `stream` bool, the engine is driven through RunStream so every
// FlowEvent can be persisted to flow/store; the bool only controls
// whether those events are also forwarded to the HTTP client as SSE
// frames.
func (s *Server) runWithStore(w http.ResponseWriter, r *http.Request, flowID string, stream bool) {
	var req runRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
		return
	}
	eng, _, err := s.engineFor(flowID)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("flow %q not found", flowID))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: engineFor %q: %v", flowID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	runID, serr := s.cfg.Store.StartRun(r.Context(), flowID, req.Inputs)
	if serr != nil {
		s.cfg.Logger.Printf("flowd: StartRun: %v", serr)
		writeError(w, http.StatusInternalServerError, serr)
		return
	}
	w.Header().Set("X-Run-ID", runID)

	var flusher http.Flusher
	if stream {
		var ok bool
		flusher, ok = w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, errors.New("response writer does not support streaming"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
	}

	ch, err := eng.RunStream(r.Context(), req.Inputs)
	if err != nil {
		// Configuration-level error before the engine started. Record
		// it as the lone event so the run history is coherent, mark
		// the run failed, and surface to the client appropriately.
		_ = s.cfg.Store.AppendRunEvent(serverCtxFor(s), runID,
			flowstore.RunEventFlowErr, "", mustJSON(map[string]string{"error": err.Error()}))
		s.persistFinish(runID, nil, err)
		if stream {
			writeSSE(w, "flow_err", map[string]string{"error": err.Error()})
			flusher.Flush()
			return
		}
		writeError(w, statusForError(err), err)
		return
	}

	var (
		finalOutputs map[string]string
		finalErr     error
		// syncBatch collects events for the sync path so they can be
		// flushed via the optional AppendRunEvents bulk-insert at the
		// end of the loop. Stream path bypasses the batch and writes
		// per-event so a dropped client still leaves a complete audit
		// trail (the v0.0.6 durability guarantee).
		syncBatch []flowstore.RunEventBatchItem
	)
	batcher, canBatch := s.cfg.Store.(interface {
		AppendRunEvents(ctx context.Context, runID string, items []flowstore.RunEventBatchItem) error
	})
	for ev := range ch {
		payload := streamPayload(ev)
		payloadRaw := mustJSON(payload)
		kind := flowstore.RunEventKind(eventKindString(ev.Kind))

		if stream {
			// Stream path: persist FIRST so events outlive a client
			// that drops the connection mid-stream, then forward.
			if perr := s.cfg.Store.AppendRunEvent(serverCtxFor(s), runID, kind, ev.NodeID, payloadRaw); perr != nil {
				s.cfg.Logger.Printf("flowd: AppendRunEvent %s: %v", runID, perr)
			}
			writeSSE(w, eventKindString(ev.Kind), payload)
			flusher.Flush()
		} else {
			// Sync path: collect for batched insert. No client is
			// reading mid-run, so durability only matters at
			// FinishRun time.
			syncBatch = append(syncBatch, flowstore.RunEventBatchItem{
				Kind:    kind,
				NodeID:  ev.NodeID,
				Payload: payloadRaw,
			})
		}

		switch ev.Kind {
		case flow.FlowDone:
			finalOutputs = ev.Outputs
		case flow.FlowErr:
			finalErr = ev.Err
			s.cfg.Logger.Printf("flowd: /flows/%s/run: %v", flowID, ev.Err)
		}
	}

	// Flush the sync batch. If the store supports AppendRunEvents,
	// one transaction; otherwise fall back to per-event inserts.
	if !stream && len(syncBatch) > 0 {
		var perr error
		if canBatch {
			perr = batcher.AppendRunEvents(serverCtxFor(s), runID, syncBatch)
		} else {
			for _, item := range syncBatch {
				if e := s.cfg.Store.AppendRunEvent(serverCtxFor(s), runID, item.Kind, item.NodeID, item.Payload); e != nil {
					perr = e
					break
				}
			}
		}
		if perr != nil {
			s.cfg.Logger.Printf("flowd: AppendRunEvents %s: %v", runID, perr)
		}
	}

	s.persistFinish(runID, finalOutputs, finalErr)

	if stream {
		return // SSE body already complete
	}
	if finalErr != nil {
		writeError(w, statusForError(finalErr), finalErr)
		return
	}
	writeJSON(w, http.StatusOK, runResponse{Outputs: finalOutputs, RunID: runID})
}

// mustJSON marshals v and returns the bytes. Errors are unreachable
// for the types we feed it (map[string]any built from streamPayload),
// but if json.Marshal does fail we fall back to a literal "null" so
// the caller never sees a nil payload that the store would reject.
func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 {
		return []byte("null")
	}
	return raw
}

func (s *Server) persistFinish(runID string, outputs map[string]string, runErr error) {
	msg := ""
	if runErr != nil {
		msg = runErr.Error()
		outputs = nil
	}
	if err := s.cfg.Store.FinishRun(serverCtxFor(s), runID, outputs, msg); err != nil {
		s.cfg.Logger.Printf("flowd: FinishRun %s: %v", runID, err)
	}
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.cfg.Store.GetRun(r.Context(), id)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("run %q not found", id))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET /runs/%s: %v", id, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// handleReplayRun re-streams a run's persisted events as a fresh SSE
// session. No new engine run is started; the response is a literal
// replay of run_events for the given runID. Useful for clients that
// connected too late to the original /run/stream, for debugging
// reproductions, or for UIs that "scrub through" a recorded run.
//
// Unknown run id → 404. Empty event log → 200 with no SSE frames
// (idempotent for just-created or never-event'd runs).
func (s *Server) handleReplayRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, err := s.cfg.Store.GetRun(r.Context(), runID); err != nil {
		if errors.Is(err, flowstore.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("run %q not found", runID))
			return
		}
		s.cfg.Logger.Printf("flowd: POST /runs/%s/replay: %v", runID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	events, err := s.cfg.Store.ListRunEvents(r.Context(), runID, 0)
	if err != nil {
		s.cfg.Logger.Printf("flowd: replay list events %s: %v", runID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("response writer does not support streaming"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Run-ID", runID)
	w.Header().Set("X-Replay", "true")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for _, ev := range events {
		writeSSERaw(w, string(ev.Kind), ev.Payload)
		flusher.Flush()
		if err := r.Context().Err(); err != nil {
			return
		}
	}
}

func (s *Server) handleListRunEvents(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	events, err := s.cfg.Store.ListRunEvents(r.Context(), runID, 0)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("run %q not found", runID))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET /runs/%s/events: %v", runID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) handleListRunsForFlow(w http.ResponseWriter, r *http.Request) {
	flowID := r.PathValue("id")
	runs, err := s.cfg.Store.ListRuns(r.Context(), flowID, 0)
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET /flows/%s/runs: %v", flowID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// ----------------------------------------------------------------------
// v2 resumable (HITL) path — additive, gated behind {"resumable":true}
// + a configured V2Registry/V2Checkpoints. The v2 engine's
// RunResumable/Resume take NO event channel and DISCARD suspensions on
// RunStream, so the persisted/SSE events are SYNTHESIZED here from the
// returned RunResult rather than flowing through the v0.1 RunStream loop.
// ----------------------------------------------------------------------

// suspender is the optional store capability for recording a HITL pause.
// Type-asserted (mirroring the AppendRunEvents pattern) so the Store
// interface stays unchanged.
type suspender interface {
	SuspendRun(ctx context.Context, runID, resumeToken, interruptNodeID string) error
}

// runResumableV2 drives a fresh resumable run. On suspend it records the
// HITL events + marks the run suspended (NOT finalized) and returns the
// resume handle; on completion it synthesizes flow_done + finalizes.
func (s *Server) runResumableV2(w http.ResponseWriter, r *http.Request, flowID string, req runRequest) {
	eng, err := s.engineForV2(flowID)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("flow %q not found", flowID))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: engineForV2 %q: %v", flowID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	runID, serr := s.cfg.Store.StartRun(r.Context(), flowID, req.Inputs)
	if serr != nil {
		s.cfg.Logger.Printf("flowd: StartRun: %v", serr)
		writeError(w, http.StatusInternalServerError, serr)
		return
	}
	w.Header().Set("X-Run-ID", runID)

	// Synthesized flow_started so the replay history is coherent.
	s.appendV2Event(runID, flowstore.RunEventFlowStarted, "",
		streamPayloadV2(v2flow.Event{Kind: v2flow.FlowStarted, FlowID: flowID}))

	inputsAny := make(map[string]any, len(req.Inputs))
	for k, v := range req.Inputs {
		inputsAny[k] = v
	}

	res, rerr := eng.RunResumable(r.Context(), runID, inputsAny)
	if rerr != nil {
		s.appendV2Event(runID, flowstore.RunEventFlowErr, "",
			map[string]any{"error": rerr.Error()})
		s.persistFinish(runID, nil, rerr)
		s.cfg.Logger.Printf("flowd: RunResumable %s: %v", runID, rerr)
		writeError(w, http.StatusInternalServerError, rerr)
		return
	}

	if res.Suspended != nil {
		s.persistSuspension(runID, res.Suspended)
		writeJSON(w, http.StatusOK, runResponse{
			RunID:     runID,
			Suspended: suspendedInfoFrom(res.Suspended),
		})
		return
	}

	outputs := stringifyOutputs(res.Outputs)
	s.appendV2Event(runID, flowstore.RunEventFlowDone, "",
		map[string]any{"outputs": outputs})
	s.persistFinish(runID, outputs, nil)
	writeJSON(w, http.StatusOK, runResponse{Outputs: outputs, RunID: runID})
}

// handleResumeRun continues a suspended run with the supplied human input.
func (s *Server) handleResumeRun(w http.ResponseWriter, r *http.Request) {
	var humanInput map[string]any
	if err := decodeJSON(r.Body, &humanInput); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
		return
	}

	runID := r.PathValue("runID")
	rec, err := s.cfg.Store.GetRun(r.Context(), runID)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("run %q not found", runID))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: GET run %s for resume: %v", runID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	eng, err := s.engineForV2(rec.FlowID)
	if errors.Is(err, flowstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("flow %q not found", rec.FlowID))
		return
	}
	if err != nil {
		s.cfg.Logger.Printf("flowd: engineForV2 %q: %v", rec.FlowID, err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	res, rerr := eng.Resume(r.Context(), runID, rec.ResumeToken, humanInput)
	if rerr != nil {
		writeError(w, statusForResumeError(rerr), rerr)
		return
	}

	if res.Suspended != nil {
		s.persistSuspension(runID, res.Suspended)
		writeJSON(w, http.StatusOK, runResponse{
			RunID:     runID,
			Suspended: suspendedInfoFrom(res.Suspended),
		})
		return
	}

	outputs := stringifyOutputs(res.Outputs)
	s.appendV2Event(runID, flowstore.RunEventFlowDone, "",
		map[string]any{"outputs": outputs})
	s.persistFinish(runID, outputs, nil)
	writeJSON(w, http.StatusOK, runResponse{Outputs: outputs, RunID: runID})
}

// persistSuspension records the node_interrupted + flow_suspended events
// then marks the run suspended via the optional SuspendRun capability.
// Falling back to persistFinish would finalize the run, which is wrong —
// so without the capability we log and leave the run running.
func (s *Server) persistSuspension(runID string, susp *v2flow.Suspension) {
	s.appendV2Event(runID, flowstore.RunEventNodeInterrupted, susp.NodeID,
		map[string]any{"node": susp.NodeID, "request": susp.Request})
	s.appendV2Event(runID, flowstore.RunEventFlowSuspended, susp.NodeID,
		map[string]any{"node": susp.NodeID, "resume_token": susp.ResumeToken, "request": susp.Request})
	if sus, ok := s.cfg.Store.(suspender); ok {
		if err := sus.SuspendRun(serverCtxFor(s), runID, susp.ResumeToken, susp.NodeID); err != nil {
			s.cfg.Logger.Printf("flowd: SuspendRun %s: %v", runID, err)
		}
		return
	}
	s.cfg.Logger.Printf("flowd: store does not support SuspendRun; run %s left un-suspended", runID)
}

// appendV2Event persists one synthesized v2 event, JSON-encoding payload.
func (s *Server) appendV2Event(runID string, kind flowstore.RunEventKind, nodeID string, payload map[string]any) {
	if err := s.cfg.Store.AppendRunEvent(serverCtxFor(s), runID, kind, nodeID, mustJSON(payload)); err != nil {
		s.cfg.Logger.Printf("flowd: AppendRunEvent %s (%s): %v", runID, kind, err)
	}
}

func suspendedInfoFrom(susp *v2flow.Suspension) *suspendedInfo {
	return &suspendedInfo{
		ResumeToken: susp.ResumeToken,
		NodeID:      susp.NodeID,
		Request:     susp.Request,
	}
}

// stringifyOutputs projects a v2 any-carrier output map to the v0.1
// map[string]string shape the store + run response use, JSON-stringifying
// any non-string value.
func stringifyOutputs(out map[string]any) map[string]string {
	if out == nil {
		return nil
	}
	res := make(map[string]string, len(out))
	for k, v := range out {
		if sv, ok := v.(string); ok {
			res[k] = sv
			continue
		}
		raw, _ := json.Marshal(v)
		res[k] = string(raw)
	}
	return res
}

// statusForResumeError maps the engine's resume sentinels to HTTP codes.
func statusForResumeError(err error) int {
	switch {
	case errors.Is(err, v2flow.ErrInvalidHumanInput):
		return http.StatusBadRequest
	case errors.Is(err, v2flow.ErrCheckpointNotFound):
		return http.StatusNotFound
	case errors.Is(err, v2flow.ErrFlowChanged):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}


// streamPayloadV2 renders a v2 Event into the map payload shape persisted
// + emitted as SSE. Mirrors the v0.1 streamPayload but covers the HITL
// kinds (resume_token / request).
func streamPayloadV2(ev v2flow.Event) map[string]any {
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
	if ev.ResumeToken != "" {
		m["resume_token"] = ev.ResumeToken
	}
	if ev.Request != nil {
		m["request"] = ev.Request
	}
	if len(ev.Metadata) > 0 {
		m["metadata"] = ev.Metadata
	}
	if ev.Err != nil {
		m["error"] = ev.Err.Error()
	}
	return m
}

// ----------------------------------------------------------------------
// Shared helpers
// ----------------------------------------------------------------------

// runSSE runs an engine in streaming mode and forwards FlowEvents to
// the client as SSE frames. The caller is responsible for decoding
// the request body; inputs is passed through to engine.RunStream.
// onFinish (optional) is called once after the terminal event with
// the final outputs / error so the store can persist the outcome.
func runSSE(
	w http.ResponseWriter,
	r *http.Request,
	engine *flow.Engine,
	inputs map[string]string,
	runID string,
	onFinish func(outputs map[string]string, runErr error),
	logger *log.Logger,
) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("response writer does not support streaming"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if runID != "" {
		w.Header().Set("X-Run-ID", runID)
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, err := engine.RunStream(r.Context(), inputs)
	if err != nil {
		writeSSE(w, "flow_err", map[string]string{"error": err.Error()})
		flusher.Flush()
		if onFinish != nil {
			onFinish(nil, err)
		}
		return
	}
	var (
		finalOutputs map[string]string
		finalErr     error
	)
	for ev := range ch {
		payload := streamPayload(ev)
		writeSSE(w, eventKindString(ev.Kind), payload)
		flusher.Flush()
		switch ev.Kind {
		case flow.FlowDone:
			finalOutputs = ev.Outputs
		case flow.FlowErr:
			finalErr = ev.Err
			logger.Printf("flowd: /run/stream: %v", ev.Err)
		}
	}
	if onFinish != nil {
		onFinish(finalOutputs, finalErr)
	}
}

// flowHeaderFromBody picks the canonical id/name (URL id wins; URL
// name wins if non-empty; otherwise both fall back to the flow body's
// own fields) and ensures the flow JSON body's id agrees with the
// chosen value.
func flowHeaderFromBody(reqID, reqName string, body json.RawMessage) (string, string, json.RawMessage, error) {
	if len(body) == 0 {
		return "", "", nil, errors.New("flow field is required")
	}
	var head struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return "", "", nil, fmt.Errorf("flow head: %w", err)
	}
	id := reqID
	if id == "" {
		id = head.ID
	}
	if id == "" {
		return "", "", nil, errors.New("missing flow id (set top-level \"id\" or include in body)")
	}
	if head.ID != "" && head.ID != id {
		return "", "", nil, fmt.Errorf("body's flow.id %q does not match outer id %q", head.ID, id)
	}
	name := reqName
	if name == "" {
		name = head.Name
	}
	return id, name, body, nil
}

// compileProbe parses + validates + compiles a flow body to surface
// configuration errors at POST/PUT time rather than at run time.
func (s *Server) compileProbe(body []byte) error {
	opts := []flow.EngineOption{flow.WithMaxNodeConcurrency(s.cfg.MaxNodeConcurrency)}
	if s.cfg.Cond != nil {
		opts = append(opts, flow.WithConditionEvaluator(s.cfg.Cond))
	}
	if _, err := flow.LoadCompile(bytesReader(body), s.cfg.Registry, flow.Deps{Tools: s.cfg.Tools}, opts...); err != nil {
		return fmt.Errorf("flow compile: %w", err)
	}
	return nil
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

func writeSSE(w io.Writer, kind string, data any) {
	raw, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, raw)
}

// writeSSERaw emits an SSE frame with an already-encoded JSON payload.
// Used by the replay endpoint to forward stored payloads byte-for-byte.
func writeSSERaw(w io.Writer, kind string, data []byte) {
	if len(data) == 0 {
		data = []byte("null")
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, data)
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
	// Metadata is emitted only when populated, mirroring the other
	// optional fields above. v0.1.0-v0.1.2 SSE consumers see no
	// "metadata" key on events that don't carry any — fully
	// back-compatible. D2: key name aligns with otelflow's
	// span-attribute convention.
	if len(ev.Metadata) > 0 {
		m["metadata"] = ev.Metadata
	}
	return m
}

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
