package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/costa92/llm-agent-flow/v2/flow"
)

var _ flow.CheckpointStore = (*Store)(nil)

// SaveCheckpoint persists cp under token == cp.RunID. INSERT OR REPLACE
// makes re-suspending the same run idempotent.
func (s *Store) SaveCheckpoint(ctx context.Context, cp flow.Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("v2/flow/store/sqlite: marshal checkpoint: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO checkpoints
		   (token, run_id, flow_id, interrupt_node, created_at, data_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		cp.RunID, cp.RunID, cp.FlowID, cp.InterruptNodeID, cp.CreatedAt.UnixNano(), string(data),
	); err != nil {
		return fmt.Errorf("v2/flow/store/sqlite: save checkpoint: %w", err)
	}
	return nil
}

// LoadCheckpoint returns the checkpoint stored under token, or
// flow.ErrCheckpointNotFound if none.
func (s *Store) LoadCheckpoint(ctx context.Context, token string) (flow.Checkpoint, error) {
	var data string
	err := s.db.QueryRowContext(ctx,
		`SELECT data_json FROM checkpoints WHERE token = ?`, token).Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flow.Checkpoint{}, flow.ErrCheckpointNotFound
		}
		return flow.Checkpoint{}, fmt.Errorf("v2/flow/store/sqlite: load checkpoint: %w", err)
	}
	var cp flow.Checkpoint
	if err := json.Unmarshal([]byte(data), &cp); err != nil {
		return flow.Checkpoint{}, fmt.Errorf("v2/flow/store/sqlite: unmarshal checkpoint: %w", err)
	}
	return cp, nil
}

// DeleteCheckpoint removes the checkpoint stored under token. Deleting a
// non-existent token is not an error (mirrors MemoryCheckpointStore).
func (s *Store) DeleteCheckpoint(ctx context.Context, token string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM checkpoints WHERE token = ?`, token); err != nil {
		return fmt.Errorf("v2/flow/store/sqlite: delete checkpoint: %w", err)
	}
	return nil
}

// ListCheckpoints returns the metadata projection for flowID, newest
// first. limit <= 0 means "all".
func (s *Store) ListCheckpoints(ctx context.Context, flowID string, limit int) ([]flow.CheckpointMeta, error) {
	query := `SELECT run_id, flow_id, interrupt_node, created_at
		   FROM checkpoints
		  WHERE flow_id = ?
		  ORDER BY created_at DESC`
	args := []any{flowID}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("v2/flow/store/sqlite: list checkpoints: %w", err)
	}
	defer rows.Close()
	var out []flow.CheckpointMeta
	for rows.Next() {
		var (
			m     flow.CheckpointMeta
			nanos int64
		)
		if err := rows.Scan(&m.RunID, &m.FlowID, &m.InterruptNodeID, &nanos); err != nil {
			return nil, fmt.Errorf("v2/flow/store/sqlite: scan checkpoint: %w", err)
		}
		m.CreatedAt = time.Unix(0, nanos)
		out = append(out, m)
	}
	return out, rows.Err()
}
