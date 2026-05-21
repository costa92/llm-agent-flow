// Command flowd is the long-running HTTP variant of cmd/flow.
//
// Boot mode: load + validate + compile a single flow.json at startup;
// expose /run and /run/stream so callers can invoke it many times
// without paying the load/validate/compile cost on every request.
//
// This is the v0.0.x narrow shape — one flow per boot, no CRUD, no
// persistence. A flow registry and run-history store are tracked in
// the project's research SUMMARY as later-phase deliverables.
//
//	flowd --addr :7861 \
//	      --flow  examples/echo_chain/flow.json \
//	      --tools mytools.json
//
// --tools is optional. When unset the bundled echo_chain demo tools
// are used (suitable for examples/echo_chain/flow.json).
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
	toolspkg "github.com/costa92/llm-agent-flow/flow/tools"
)

func main() {
	addr := flag.String("addr", ":7861", "HTTP listen address.")
	flowPath := flag.String("flow", "", "Path to the flow JSON to serve. Required.")
	toolsPath := flag.String("tools", "", "Path to a tool-manifest JSON (see flow/tools). When unset, the built-in echo_chain demo tools are used.")
	readTimeout := flag.Duration("read-timeout", 5*time.Second, "HTTP server read timeout.")
	writeTimeout := flag.Duration("write-timeout", 0, "HTTP server write timeout (0 disables — required for SSE).")
	maxNodeConcurrency := flag.Int("max-node-concurrency", 0, "Cap on goroutines per topological layer. 0 = unlimited.")
	flag.Parse()

	if *flowPath == "" {
		fmt.Fprintln(os.Stderr, "flowd: --flow is required")
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(*flowPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd: open flow:", err)
		os.Exit(1)
	}
	defer f.Close()

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
	engine, err := flow.LoadCompile(f, reg, flow.Deps{Tools: tools},
		flow.WithMaxNodeConcurrency(*maxNodeConcurrency),
		flow.WithConditionEvaluator(celEval))
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowd:", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:         *addr,
		Handler:      server.NewMux(engine, log.Default()),
		ReadTimeout:  *readTimeout,
		WriteTimeout: *writeTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("flowd: listening on %s (flow=%s)", *addr, *flowPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("flowd: server: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("flowd: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("flowd: shutdown: %v", err)
	}
}

// loadTools resolves the manifest at path into a flow.ToolMap. With
// an empty path, fall back to the union of every bundled example's
// demo tools so any `examples/*/flow.json` boots out-of-box. Real
// deployments are expected to pass --tools.
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

