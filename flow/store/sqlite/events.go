package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

// AppendRunEvent implements flow/store.Store.
//
// The seq column is assigned server-side as `max(existing seq for
// this run) + 1`. Doing the read+write in a single statement under
// SQLite's WAL is race-free; under the default journal mode our
// SetMaxOpenConns(1) for :memory: keeps it serialized. On-disk users
// who hammer the same run from multiple goroutines should rely on
// SQLite's row-level lock + this single-statement upsert.
func (s *Store) AppendRunEvent(ctx context.Context, runID string, kind flowstore.RunEventKind, nodeID string, payload []byte) error {
	if runID == "" {
		return errors.New("flow/store/sqlite: empty run_id")
	}
	// Confirm the run exists so callers get a clean ErrNotFound for
	// typos instead of a silently-orphaned event row.
	var dummy string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM runs WHERE id = ?`, runID).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flowstore.ErrNotFound
		}
		return fmt.Errorf("flow/store/sqlite: append event probe: %w", err)
	}
	var payloadCol any
	if len(payload) > 0 {
		payloadCol = string(payload)
	}
	var nodeCol any
	if nodeID != "" {
		nodeCol = nodeID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO run_events (run_id, seq, kind, node_id, payload_json, ts)
		 VALUES (
		   ?,
		   COALESCE((SELECT MAX(seq) FROM run_events WHERE run_id = ?), 0) + 1,
		   ?, ?, ?, ?
		 )`,
		runID, runID, string(kind), nodeCol, payloadCol, nowUnix(),
	)
	if err != nil {
		return fmt.Errorf("flow/store/sqlite: append event: %w", err)
	}
	return nil
}

// ListRunEvents implements flow/store.Store. Events are returned in
// seq-ascending order. limit <= 0 returns every event.
func (s *Store) ListRunEvents(ctx context.Context, runID string, limit int) ([]flowstore.RunEvent, error) {
	if runID == "" {
		return nil, errors.New("flow/store/sqlite: empty run_id")
	}
	query := `SELECT seq, kind, node_id, payload_json, ts
	            FROM run_events
	           WHERE run_id = ?
	        ORDER BY seq ASC`
	args := []any{runID}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("flow/store/sqlite: list events: %w", err)
	}
	defer rows.Close()
	var out []flowstore.RunEvent
	for rows.Next() {
		var (
			ev      flowstore.RunEvent
			kind    string
			nodeID  sql.NullString
			payload sql.NullString
			ts      int64
		)
		if err := rows.Scan(&ev.Seq, &kind, &nodeID, &payload, &ts); err != nil {
			return nil, fmt.Errorf("flow/store/sqlite: scan event: %w", err)
		}
		ev.Kind = flowstore.RunEventKind(kind)
		if nodeID.Valid {
			ev.NodeID = nodeID.String
		}
		if payload.Valid && payload.String != "" {
			ev.Payload = []byte(payload.String)
		}
		ev.Timestamp = unixToTime(ts)
		out = append(out, ev)
	}
	return out, rows.Err()
}
