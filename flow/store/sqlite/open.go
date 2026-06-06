package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
	// busy_timeout is a PER-CONNECTION pragma: setting it via ExecContext in
	// configurePragmas only covers the connection that happens to run it, so
	// new pooled connections would still fail with SQLITE_BUSY under
	// concurrent writes. For on-disk DSNs we therefore also inject it into
	// the driver DSN (modernc honors _pragma=busy_timeout on every connection
	// open). configurePragmas keeps the explicit PRAGMA too, for callers that
	// pass a pre-built DSN of their own.
	openDSN := withBusyTimeoutPragma(dsn)
	db, err := sql.Open("sqlite", openDSN)
	if err != nil {
		return nil, fmt.Errorf("flow/store/sqlite: open: %w", err)
	}
	// In-memory DBs do not survive concurrent connections; cap to 1.
	if dsn == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	s := &Store{db: db}
	if err := s.configurePragmas(context.Background(), dsn); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) configurePragmas(ctx context.Context, dsn string) error {
	if isMemoryDSN(dsn) {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("flow/store/sqlite: enable WAL: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA synchronous=NORMAL`); err != nil {
		return fmt.Errorf("flow/store/sqlite: set synchronous=NORMAL: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		return fmt.Errorf("flow/store/sqlite: set busy_timeout: %w", err)
	}
	return nil
}

// withBusyTimeoutPragma appends modernc's _pragma=busy_timeout(5000) query
// param to an on-disk DSN so EVERY pooled connection (not just the one that
// runs configurePragmas) waits up to 5s on a held write lock instead of
// failing with SQLITE_BUSY. In-memory DSNs are left untouched. If the DSN
// already requests a busy_timeout pragma it is left as-is.
func withBusyTimeoutPragma(dsn string) string {
	if isMemoryDSN(dsn) {
		return dsn
	}
	if strings.Contains(strings.ToLower(dsn), "busy_timeout") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "_pragma=busy_timeout(5000)"
}

func isMemoryDSN(dsn string) bool {
	if dsn == ":memory:" {
		return true
	}
	return strings.Contains(strings.ToLower(dsn), "mode=memory")
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
  error_msg     TEXT,
  resume_token   TEXT NOT NULL DEFAULT '',
  interrupt_node TEXT NOT NULL DEFAULT ''
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
	// Migration for pre-existing on-disk DBs: CREATE TABLE IF NOT EXISTS
	// leaves an already-present `runs` table untouched, so the HITL
	// columns must be added via ALTER. SQLite has no ADD COLUMN IF NOT
	// EXISTS, so we run each ALTER and swallow the "duplicate column"
	// error that fires on a DB created with the columns already in place.
	for _, col := range []string{
		`ALTER TABLE runs ADD COLUMN resume_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN interrupt_node TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := s.db.ExecContext(ctx, col); err != nil {
			if containsCI(err.Error(), "duplicate column name") {
				continue
			}
			return fmt.Errorf("flow/store/sqlite: migrate runs columns: %w", err)
		}
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
