// Command flow is the v0.0.x CLI entry point for llm-agent-flow.
//
// Subcommands:
//
//	flow run <file.json> --input <key>=<value> [--input ...] [--stream]
//
// Tool resolution at v0.0.x is hard-coded to the bundled echo_chain
// demo tools. A future release will accept a manifest naming the Go
// package whose tools should be registered, or a plugin-driver
// interface — for now the CLI is intentionally narrow so it can be
// the integration-test driver.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/costa92/llm-agent-flow/examples/echo_chain"
	"github.com/costa92/llm-agent-flow/flow"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "flow:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		printUsage(os.Stdout)
		return nil
	}
	switch args[0] {
	case "run":
		return cmdRun(args[1:])
	default:
		printUsage(os.Stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: flow run <file.json> [--input key=value ...] [--stream]")
}

type inputList []string

func (l *inputList) String() string     { return strings.Join(*l, ",") }
func (l *inputList) Set(s string) error { *l = append(*l, s); return nil }

func cmdRun(args []string) error {
	if len(args) < 1 {
		printUsage(os.Stderr)
		return errors.New("flow run: <file.json> required")
	}
	// Path is the first positional argument so the more intuitive
	// `flow run <file.json> --input ...` order works under Go's stock
	// flag parser (which otherwise stops at the first non-flag arg).
	path := args[0]
	if strings.HasPrefix(path, "-") {
		printUsage(os.Stderr)
		return errors.New("flow run: <file.json> must come before flags")
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var ins inputList
	fs.Var(&ins, "input", `Flow input as "key=value". Repeatable.`)
	stream := fs.Bool("stream", false, "Print FlowEvents as they happen instead of just the final outputs.")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		printUsage(os.Stderr)
		return errors.New("flow run: unexpected trailing arguments")
	}

	inputs := map[string]string{}
	for _, s := range ins {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return fmt.Errorf("flow run: bad --input %q (want key=value)", s)
		}
		inputs[k] = v
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("flow run: open: %w", err)
	}
	defer f.Close()

	reg := flow.NewNodeRegistry()
	if err := flow.RegisterToolNode(reg); err != nil {
		return fmt.Errorf("flow run: register tool node: %w", err)
	}
	tools := flow.FromAgentTools(echochain.Tools())
	engine, err := flow.LoadCompile(f, reg, flow.Deps{Tools: tools})
	if err != nil {
		return err
	}

	ctx := context.Background()
	if *stream {
		ch, err := engine.RunStream(ctx, inputs)
		if err != nil {
			return err
		}
		for ev := range ch {
			line, _ := json.Marshal(map[string]any{
				"kind":    eventKindString(ev.Kind),
				"flow":    ev.FlowID,
				"node":    ev.NodeID,
				"input":   ev.Input,
				"output":  ev.Output,
				"outputs": ev.Outputs,
				"err":     errString(ev.Err),
			})
			fmt.Println(string(line))
			if ev.Kind == flow.FlowErr {
				return ev.Err
			}
		}
		return nil
	}

	outs, err := engine.Run(ctx, inputs)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(outs)
}

func errString(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}

func eventKindString(k flow.FlowEventKind) string {
	switch k {
	case flow.FlowStarted:
		return "flow_started"
	case flow.NodeStarted:
		return "node_started"
	case flow.NodeFinished:
		return "node_finished"
	case flow.FlowDone:
		return "flow_done"
	case flow.FlowErr:
		return "flow_err"
	default:
		return "unknown"
	}
}
