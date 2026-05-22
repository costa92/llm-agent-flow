// Command flowd is the long-running HTTP variant of cmd/flow.
//
// v0.0.5 adds a persistence layer: flows are stored in SQLite,
// runs are recorded with status / outputs / error, and the full
// REST surface (flow CRUD + per-flow runs + run-history) is wired
// against any number of flows.
//
//	flowd --addr :7861 \
//	      --db   /var/lib/flowd/flow.db   # use ":memory:" for ephemeral runs
//	      --flow examples/echo_chain/flow.json   # optional boot-time seed + legacy /run alias
//	      --tools mytools.json                   # optional tool manifest
//
// All flags are optional. --db defaults to ":memory:" so the binary
// runs out-of-box. --flow, when supplied, seeds the database at boot
// (if the flow id is not already present) AND enables the legacy
// /run + /run/stream endpoints for v0.0.4-compatible clients.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	agents "github.com/costa92/llm-agent"
	"github.com/costa92/llm-agent-flow/cmd/flowd/server"
	"github.com/costa92/llm-agent-flow/examples/echo_chain"
	"github.com/costa92/llm-agent-flow/examples/router"
	"github.com/costa92/llm-agent-flow/flow"
	cond "github.com/costa92/llm-agent-flow/flow/cond/cel"
	flowstore "github.com/costa92/llm-agent-flow/flow/store"
	sqlitestore "github.com/costa92/llm-agent-flow/flow/store/sqlite"
	toolspkg "github.com/costa92/llm-agent-flow/flow/tools"
)

func main() {
	addr := flag.String("addr", ":7861", "HTTP listen address.")
	dbPath := flag.String("db", ":memory:", "SQLite DSN for flow + run-history storage. \":memory:\" for ephemeral runs.")
	flowPath := flag.String("flow", "", "Optional path to a flow JSON. Seeds the database at boot AND enables /run + /run/stream legacy aliases for that flow id.")
	toolsPath := flag.String("tools", "", "Path to a tool-manifest JSON (see flow/tools). When unset, the built-in echo_chain + router demo tools are used.")
	readTimeout := flag.Duration("read-timeout", 5*time.Second, "HTTP server read timeout.")
	writeTimeout := flag.Duration("write-timeout", 0, "HTTP server write timeout (0 disables — required for SSE).")
	maxNodeConcurrency := flag.Int("max-node-concurrency", 0, "Cap on goroutines per topological layer. 0 = unlimited.")
	token := flag.String("token", "", "Static bearer token; when set, every endpoint except /healthz requires \"Authorization: Bearer <token>\". The FLOWD_TOKEN env var sets the same field; the flag wins when both are provided.")
	flag.Parse()

	tok := *token
	if tok == "" {
		tok = os.Getenv("FLOWD_TOKEN")
	}

	store, err := sqlitestore.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd: open db:", err)
		os.Exit(1)
	}
	defer store.Close()
	if *dbPath != ":memory:" {
		log.Printf("flowd: sqlite WAL mode active for %s (expect %s-wal and %s-shm sidecar files; include them in backups)", *dbPath, *dbPath, *dbPath)
	}

	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		fmt.Fprintln(os.Stderr, "flowd: register tool node:", err)
		os.Exit(1)
	}
	tools, err := loadTools(*toolsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd:", err)
		os.Exit(1)
	}
	celEval, err := cond.NewEvaluator()
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd: cel evaluator:", err)
		os.Exit(1)
	}

	legacyID := ""
	if *flowPath != "" {
		id, err := seedFlow(store, *flowPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "flowd: seed flow:", err)
			os.Exit(1)
		}
		legacyID = id
		log.Printf("flowd: seeded flow %q from %s", id, *flowPath)
	}

	var auth server.Authenticator
	if tok != "" {
		auth = server.BearerTokenAuthenticator{Token: tok}
		log.Printf("flowd: bearer-token authentication enabled")
	}

	srvCfg := server.Config{
		Store:              store,
		Registry:           reg,
		Tools:              tools,
		Cond:               celEval,
		MaxNodeConcurrency: *maxNodeConcurrency,
		Logger:             log.Default(),
		LegacyFlowID:       legacyID,
		Authenticator:      auth,
	}
	flowdServer, err := server.New(srvCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd:", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Addr:         *addr,
		Handler:      flowdServer.Handler(),
		ReadTimeout:  *readTimeout,
		WriteTimeout: *writeTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("flowd: listening on %s (db=%s)", *addr, *dbPath)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("flowd: server: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("flowd: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("flowd: shutdown: %v", err)
	}
}

// loadTools resolves the manifest at path into a flow.ToolMap. With
// an empty path, fall back to the union of every bundled example's
// demo tools so any `examples/*/flow.json` boots out-of-box.
func loadTools(path string) (flow.ToolMap, error) {
	if path == "" {
		demo := []agents.Tool{}
		demo = append(demo, echochain.Tools()...)
		demo = append(demo, router.Tools()...)
		return flow.FromAgentTools(demo), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open tools manifest: %w", err)
	}
	defer f.Close()
	built, err := toolspkg.LoadAndBuild(f, toolspkg.NewKindRegistry())
	if err != nil {
		return nil, err
	}
	out := make(flow.ToolMap, len(built))
	for _, t := range built {
		out[t.Name()] = t
	}
	return out, nil
}

// seedFlow reads path, parses the JSON to extract the flow id, and
// inserts (or PUT-updates) the row in store. Returns the flow id so
// the caller can wire it as LegacyFlowID.
func seedFlow(store flowstore.Store, path string) (string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	f, err := flow.Load(bytesReader(body))
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if f.ID == "" {
		return "", fmt.Errorf("flow file %s has no id", path)
	}
	if _, err := store.PutFlow(context.Background(), f.ID, f.Name, body, false); err != nil {
		return "", fmt.Errorf("store: %w", err)
	}
	return f.ID, nil
}
