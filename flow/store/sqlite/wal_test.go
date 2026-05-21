package sqlite

// White-box tests for the WAL + synchronous=NORMAL pragma wiring in
// Open(). White-box because we need to read s.db directly to query
// PRAGMA values; the modernc driver does not surface them through any
// exported API.

import (
	"path/filepath"
	"strings"
	"testing"
)

func queryPragma(t *testing.T, s *Store, name string) string {
	t.Helper()
	var v string
	if err := s.db.QueryRow("PRAGMA " + name).Scan(&v); err != nil {
		t.Fatalf("PRAGMA %s: %v", name, err)
	}
	return v
}

// TestOpen_OnDiskEnablesWAL — when Open is called with an on-disk DSN
// it must enable WAL journaling and synchronous=NORMAL so on-disk
// users get the ~5x single-write speedup without an explicit pragma
// call. WAL files (.db-wal / .db-shm) being present is the ops-side
// observable; here we just verify the pragma values themselves.
func TestOpen_OnDiskEnablesWAL(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "flow.db")
	s, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	jm := queryPragma(t, s, "journal_mode")
	if !strings.EqualFold(jm, "wal") {
		t.Fatalf("journal_mode = %q, want wal", jm)
	}
	// synchronous=NORMAL corresponds to value "1" (FULL=2, OFF=0). The
	// modernc driver echoes the numeric form; accept both the number
	// and the word in case driver behaviour changes.
	sync := queryPragma(t, s, "synchronous")
	if sync != "1" && !strings.EqualFold(sync, "normal") {
		t.Fatalf("synchronous = %q, want 1 (NORMAL)", sync)
	}
}

// TestOpen_MemoryDSNDoesNotEnableWAL — in-memory DBs cannot survive
// WAL semantics (no on-disk -wal sidecar), so Open must leave the
// journal mode at the SQLite default ("memory") and skip the WAL
// pragma. Validated by asserting journal_mode is NOT "wal".
func TestOpen_MemoryDSNDoesNotEnableWAL(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	jm := queryPragma(t, s, "journal_mode")
	if strings.EqualFold(jm, "wal") {
		t.Fatalf("journal_mode = %q, want NOT wal for :memory: DSN", jm)
	}
}

// TestOpen_PragmaFailureDoesNotBreakOpen — Open must return an error
// (not panic, not silently succeed) when the DSN points at an
// un-openable path. /proc/cannot/... is not writable on Linux so the
// pragma or initial ExecContext path fails predictably.
func TestOpen_PragmaFailureDoesNotBreakOpen(t *testing.T) {
	s, err := Open("/proc/cannot/test.db")
	if err == nil {
		// Don't leak the handle on the unexpected-success branch.
		_ = s.Close()
		t.Fatalf("Open(/proc/cannot/test.db) succeeded; want error")
	}
}
