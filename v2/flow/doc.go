// Package flow is the v2 flow engine: an any-carrier, layered DAG
// executor that ports the v0.1 scheduler onto a structured value carrier
// and adds the eino-gap capabilities (typed graphs, data streaming, and
// human-in-the-loop checkpoint/resume).
//
// # Engine
//
// Compile resolves a Flow IR against a NodeRegistry + Deps once, computes
// a topological layer order, and returns an immutable, concurrency-safe
// Engine. The engine threads values between nodes as an any port carrier
// (map[string]any), so a port may hold a structured Go value rather than
// only a string as in v0.1. Run executes synchronously and returns the
// declared outputs; RunStream executes asynchronously and emits Events
// describing each node's lifecycle. The per-node activation, layered
// fanout, and post-fanout edge-firing semantics are preserved verbatim
// from v0.1.
//
// # Typed graph front-end
//
// The graph subpackage (Graph[I, O]) is a statically typed, program-only
// DAG builder. It accepts Lambda/Passthrough/Branch nodes plus component
// nodes (template/chatmodel/tool), runs reflect-based assignability
// checks at AddEdge/Entry/Exit/Compile time, and lowers — through this
// package's exported API only — into an in-process Engine. Compile
// returns a Runnable[I, O] with Invoke(I) -> O and a data-level Stream.
//
// # Checkpoint / interrupt / resume (HITL)
//
// An Engine compiled WithCheckpointStore satisfies ResumableRunner. A
// NodeKind that also implements InterruptCapable may return Interrupt(...)
// from Run to suspend the run; RunResumable then returns a Suspension
// instead of outputs, and Resume(humanInput) injects the human decision
// as the interrupt node's output ports and drives the run to completion.
// Checkpoints (the serializable run cursor: port values, activation set,
// per-edge fire state, and suspend layer) persist through a
// CheckpointStore — NewMemoryCheckpointStore for embedding/tests, or the
// sqlite store under flow/store/sqlite.
//
// # Boundaries
//
// Data streaming is linear-only: a graph that is a single entry→exit
// chain streams end-to-end; any branch/fan-out degrades Stream to
// box(Invoke). Only serializable graphs (no Go closure / injected
// dependency) lower back to JSON IR and are checkpointable — see
// Runnable.Serializable / Checkpointable and the serializable
// passthrough/component lowering.
//
// The JSON IR is a superset of v0.1: the optional Port.GoType metadata
// the typed front-end attaches is not serialized, so every v0.1 flow JSON
// loads here unchanged.
package flow
