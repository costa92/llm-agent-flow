package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// Store is the modernc.org/sqlite-backed implementation of
// v2/flow.CheckpointStore.
//
// The zero value is not usable — construct with Open.
type Store struct {
	db *sql.DB
}

// Open returns a Store backed by the SQLite database at dsn. The DSN is
// passed to modernc.org/sqlite verbatim; use ":memory:" for tests.
//
// For on-disk DSNs the WAL journal mode + synchronous=NORMAL pragmas are
// enabled (mirroring the v0.1 store); in-memory DBs skip them since they
// cannot carry a -wal sidecar. The schema is created if absent
// (idempotent).
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("v2/flow/store/sqlite: open: %w", err)
	}
	// In-memory DBs do not survive concurrent connections; cap to 1.
	if isMemoryDSN(dsn) {
		db.SetMaxOpenConns(1)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("v2/flow/store/sqlite: ping: %w", err)
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
		return fmt.Errorf("v2/flow/store/sqlite: enable WAL: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA synchronous=NORMAL`); err != nil {
		return fmt.Errorf("v2/flow/store/sqlite: set synchronous=NORMAL: %w", err)
	}
	return nil
}

func isMemoryDSN(dsn string) bool {
	if dsn == ":memory:" {
		return true
	}
	return strings.Contains(strings.ToLower(dsn), "mode=memory")
}

// ensureSchema creates the checkpoints table + index if absent. Safe to
// call multiple times.
func (s *Store) ensureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS checkpoints (
  token          TEXT PRIMARY KEY,   -- resume token == run_id
  run_id         TEXT NOT NULL,
  flow_id        TEXT NOT NULL,
  interrupt_node TEXT NOT NULL DEFAULT '',
  created_at     INTEGER NOT NULL,   -- unix nanos
  data_json      TEXT NOT NULL       -- full flow.Checkpoint JSON
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_flow_id_created_at
  ON checkpoints(flow_id, created_at DESC);
`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("v2/flow/store/sqlite: ensure schema: %w", err)
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
