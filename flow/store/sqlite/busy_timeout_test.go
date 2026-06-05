package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// TestConcurrentWritesNoBusyError — concurrent writers against one
// on-disk DB must wait (PRAGMA busy_timeout) rather than fail with
// SQLITE_BUSY. Without the busy_timeout pragma this flakes/fails as the
// writers contend on the single SQLite write lock. With it, every
// StartRun succeeds.
func TestConcurrentWritesNoBusyError(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "flow.db")
	s, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, e := s.StartRun(context.Background(), "flow-x", map[string]string{"k": "v"})
			errs[idx] = e
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("StartRun #%d failed: %v", i, e)
		}
	}
}
