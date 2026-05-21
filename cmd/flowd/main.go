// Command flowd is the long-running HTTP variant of cmd/flow.
//
// Boot mode: load + validate + compile a single flow.json at startup;
// expose /run and /run/stream so callers can invoke it many times
// without paying the load/validate/compile cost on every request.
//
// This is the v0.0.2 narrow shape — one flow per boot, no CRUD, no
// persistence. A flow registry and run-history store are tracked in
// the project's research SUMMARY as later-phase deliverables.
//
//	flowd --addr :7861 --flow examples/echo_chain/flow.json
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

	"github.com/costa92/llm-agent-flow/cmd/flowd/server"
	"github.com/costa92/llm-agent-flow/examples/echo_chain"
	"github.com/costa92/llm-agent-flow/flow"
)

func main() {
	addr := flag.String("addr", ":7861", "HTTP listen address.")
	flowPath := flag.String("flow", "", "Path to the flow JSON to serve. Required.")
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
	tools := flow.FromAgentTools(echochain.Tools())

	engine, err := flow.LoadCompile(f, reg, flow.Deps{Tools: tools}, flow.WithMaxNodeConcurrency(*maxNodeConcurrency))
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

