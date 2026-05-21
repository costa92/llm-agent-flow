// Package sqlite is the modernc.org/sqlite-backed implementation of
// flow/store.Store.
//
// Importing this package pulls in modernc.org/sqlite and its
// transitive deps (a CGO-free SQLite port). The core flow library
// imports none of it; applications opt in by importing this
// sub-package explicitly:
//
//	import (
//	    "github.com/costa92/llm-agent-flow/flow/store"
//	    sqlitestore "github.com/costa92/llm-agent-flow/flow/store/sqlite"
//	)
//
//	s, err := sqlitestore.Open("flow.db")
//	defer s.Close()
//
//	// s satisfies flow/store.Store
//	flows, _ := s.ListFlows(ctx, 100)
//
// The DSN ":memory:" is supported for tests. The implementation uses
// modernc.org/sqlite, a pure-Go port — no CGO required and clean
// cross-compile.
//
// Schema (see open.go):
//
//   - flows(id PRIMARY KEY, name, json, created_at, updated_at)
//   - runs(id PRIMARY KEY, flow_id, status, started_at, finished_at,
//          inputs_json, outputs_json, error_msg)
//
// runs.flow_id has no FK to flows.id — that lets us keep historical
// runs after a flow is deleted, which is desirable for audit.
package sqlite
