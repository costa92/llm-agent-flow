// Package sqlite provides a modernc.org/sqlite-backed, CGo-free
// implementation of v2/flow.CheckpointStore. It persists suspend-point
// checkpoints so a resumable run survives a process restart: the resume
// token (== run id) is the primary key, and the full flow.Checkpoint is
// stored as JSON in the data_json column.
//
// On-disk DSNs run in WAL journal mode with synchronous=NORMAL, mirroring
// the v0.1 flow/store/sqlite store; in-memory (":memory:") DSNs skip those
// pragmas.
package sqlite
