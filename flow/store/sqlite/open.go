package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the modernc.org/sqlite-backed implementation of
// flow/store.Store.
//
// The zero value is not usable — construct with Open.
type Store struct {
	db *sql.DB

	// idMu guards the run-id RNG so concurrent StartRun calls don't
	// share state on the underlying *rand.Rand. crypto/rand is used
	// instead for the actual entropy; this is only an iteration mu.
	idMu sync.Mutex
}

// Open returns a Store backed by the SQLite database at dsn. The DSN
// is passed to modernc.org/sqlite verbatim; use ":memory:" for tests.
//
// The schema is created if the database is empty (idempotent), so
// callers do NOT need to run migrations separately.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("flow/store/sqlite: open: %w", err)
	}
	// In-memory DBs do not survive concurrent connections; cap to 1.
	if dsn == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	s := &Store{db: db}
	if err := s.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// ensureSchema creates tables if they do not already exist. Safe to
// call multiple times.
func (s *Store) ensureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS flows (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL DEFAULT '',
  json        TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id            TEXT PRIMARY KEY,
  flow_id       TEXT NOT NULL,
  status        TEXT NOT NULL,
  started_at    INTEGER NOT NULL,
  finished_at   INTEGER,
  inputs_json   TEXT,
  outputs_json  TEXT,
  error_msg     TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_flow_id_started_at
  ON runs(flow_id, started_at DESC);

CREATE TABLE IF NOT EXISTS run_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id       TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  kind         TEXT NOT NULL,
  node_id      TEXT,
  payload_json TEXT,
  ts           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_events_run_id_seq
  ON run_events(run_id, seq);
`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("flow/store/sqlite: ensure schema: %w", err)
	}
	return nil
}

// Close closes the underlying *sql.DB. Idempotent.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func nowUnix() int64 { return time.Now().UnixMicro() }

func unixToTime(us int64) time.Time { return time.UnixMicro(us).UTC() }

// errIs unwraps the modernc-driver constraint error into our public
// sentinels. The driver does not export typed errors so we string-match
// for PRIMARY-KEY violations.
func errIs(err error, hint string) bool {
	return err != nil && (errors.Is(err, sql.ErrNoRows) || (hint != "" && containsCI(err.Error(), hint)))
}

// containsCI is case-insensitive substring search using stdlib only.
func containsCI(haystack, needle string) bool {
	if len(needle) == 0 || len(needle) > len(haystack) {
		return needle == ""
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			a := haystack[i+j]
			b := needle[j]
			if a >= 'A' && a <= 'Z' {
				a += 32
			}
			if b >= 'A' && b <= 'Z' {
				b += 32
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
