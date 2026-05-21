// Package store declares the persistence contract for flows and run
// history. The core flow library uses this only via the interface —
// the SQLite-backed implementation (and any future Postgres / KV /
// in-memory variants) live in sub-packages so the core stays
// stdlib-only at the source level.
//
// Two concerns are bundled into one interface for v0.0.x:
//
//   - Flows: the persisted IR (FlowRecord = metadata + JSON bytes).
//     CRUD-style; last write wins (no versioning yet).
//   - Runs: lifecycle of one execution. StartRun creates a row in
//     state "running"; FinishRun transitions it to "done" or "failed"
//     with the final outputs / error captured.
//
// Run events (per-node start/finish) are NOT persisted at v0.0.5;
// they remain client-stream-only via cmd/flowd's /run/stream SSE.
// A future phase may persist them for replay / debugging.
package store
