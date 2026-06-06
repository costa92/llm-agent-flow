# llm-agent-flow/v2

The v2 flow engine: an `any`-carrier, layered DAG executor that ports the
v0.1 scheduler onto a structured value carrier and closes the eino-gap
features the string-only v0.1 engine could not express:

- **Typed `Graph[I, O]` front-end** — a statically typed, program-only DAG
  builder (`flow/graph`) with reflect-based type checking and an
  `Invoke(I) -> O` / data-level `Stream`. It lowers, through the engine's
  exported API only, into an in-process `flow.Engine`.
- **Data streaming** — a linear (single entry→exit) chain streams a
  `ChatModel` end-to-end; branches degrade `Stream` to `box(Invoke)`.
- **Human-in-the-loop checkpoint / interrupt / resume** — a node may
  suspend a run for human input; the engine checkpoints the run cursor and
  resumes it later with the human decision.

The JSON IR is a superset of v0.1: the typed front-end's `Port.GoType`
metadata is not serialized, so every v0.1 flow JSON loads unchanged.

This is a nested Go module (`github.com/costa92/llm-agent-flow/v2`). Run
its go commands from this directory with `GOWORK=off`.

## Typed Invoke

```go
g := graph.NewGraph[prompt.Vars, string]()
tNode, _ := graph.AddTemplateNode(g, "tmpl", requester)
cmNode, _ := graph.AddChatModelNode(g, "chat", model)
exNode, _ := graph.AddLambdaNode(g, "extract",
    func(_ context.Context, r llm.Response) (json.RawMessage, error) {
        return r.ToolCalls[0].Arguments, nil
    })
toolNode, _ := graph.AddToolNode(g, "tool", tool)

_ = g.AddEdge(tNode, cmNode)
_ = g.AddEdge(cmNode, exNode)
_ = g.AddEdge(exNode, toolNode)
_ = g.Entry(tNode)
_ = g.Exit(toolNode)

run, _ := g.Compile(ctx)        // reflect type-check + lower to engine
out, _ := run.Invoke(ctx, prompt.Vars{"expr": "2+3"}) // typed result
```

## HITL resume

```go
engine, _ := flow.Compile(f, reg, flow.Deps{},
    flow.WithCheckpointStore(flow.NewMemoryCheckpointStore()))

runID := flow.NewRunID()
res, _ := engine.RunResumable(ctx, runID, inputs)
// res.Suspended != nil: the gate node interrupted for human input.

res2, _ := engine.Resume(ctx, runID, res.Suspended.ResumeToken,
    map[string]any{"decision": "approved"})
// res2.Outputs holds the completed result.
```

## Examples

Runnable, test-asserted demos live under `examples/`:

- [`examples/typed_graph`](examples/typed_graph) — the headline
  template → chatmodel → tool typed chain.
- [`examples/hitl`](examples/hitl) — checkpoint / interrupt / resume.

See [`flow/doc.go`](flow/doc.go) for the package-level overview.
