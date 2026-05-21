package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

// StartRun implements flow/store.Store.
func (s *Store) StartRun(ctx context.Context, flowID string, inputs map[string]string) (string, error) {
	if flowID == "" {
		return "", errors.New("flow/store/sqlite: empty flow_id")
	}
	runID, err := newRunID()
	if err != nil {
		return "", err
	}
	inputsJSON, _ := json.Marshal(inputs)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (id, flow_id, status, started_at, inputs_json)
		 VALUES (?, ?, ?, ?, ?)`,
		runID, flowID, string(flowstore.RunStatusRunning), nowUnix(), string(inputsJSON),
	); err != nil {
		return "", fmt.Errorf("flow/store/sqlite: start run: %w", err)
	}
	return runID, nil
}

// FinishRun implements flow/store.Store. Idempotent — calling against
// a row that is already in a terminal state is a no-op.
func (s *Store) FinishRun(ctx context.Context, runID string, outputs map[string]string, errMsg string) error {
	outputsJSON, _ := json.Marshal(outputs)
	status := flowstore.RunStatusDone
	if errMsg != "" {
		status = flowstore.RunStatusFailed
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs
		   SET status = ?, finished_at = ?, outputs_json = ?, error_msg = ?
		 WHERE id = ? AND status = ?`,
		string(status), nowUnix(), string(outputsJSON), errMsg, runID, string(flowstore.RunStatusRunning),
	)
	if err != nil {
		return fmt.Errorf("flow/store/sqlite: finish run: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Already terminal OR unknown id. Distinguish for the caller.
		var dummy string
		if err := s.db.QueryRowContext(ctx, `SELECT id FROM runs WHERE id = ?`, runID).Scan(&dummy); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return flowstore.ErrNotFound
			}
			return fmt.Errorf("flow/store/sqlite: finish run probe: %w", err)
		}
		// Already terminal — idempotent no-op.
	}
	return nil
}

// GetRun implements flow/store.Store.
func (s *Store) GetRun(ctx context.Context, runID string) (flowstore.RunRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, flow_id, status, started_at, finished_at, inputs_json, outputs_json, error_msg
		   FROM runs WHERE id = ?`, runID)
	var (
		rec         flowstore.RunRecord
		status      string
		startedAt   int64
		finishedAt  sql.NullInt64
		inputsJSON  sql.NullString
		outputsJSON sql.NullString
		errMsg      sql.NullString
	)
	if err := row.Scan(&rec.ID, &rec.FlowID, &status, &startedAt, &finishedAt, &inputsJSON, &outputsJSON, &errMsg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flowstore.RunRecord{}, flowstore.ErrNotFound
		}
		return flowstore.RunRecord{}, fmt.Errorf("flow/store/sqlite: get run: %w", err)
	}
	rec.Status = flowstore.RunStatus(status)
	rec.StartedAt = unixToTime(startedAt)
	if finishedAt.Valid {
		t := unixToTime(finishedAt.Int64)
		rec.FinishedAt = &t
	}
	if inputsJSON.Valid && inputsJSON.String != "" {
		_ = json.Unmarshal([]byte(inputsJSON.String), &rec.Inputs)
	}
	if outputsJSON.Valid && outputsJSON.String != "" {
		_ = json.Unmarshal([]byte(outputsJSON.String), &rec.Outputs)
	}
	if errMsg.Valid {
		rec.Error = errMsg.String
	}
	return rec, nil
}

// ListRuns implements flow/store.Store.
func (s *Store) ListRuns(ctx context.Context, flowID string, limit int) ([]flowstore.RunMeta, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, flow_id, status, started_at, finished_at
		   FROM runs
		  WHERE flow_id = ?
		  ORDER BY started_at DESC, id
		  LIMIT ?`, flowID, limit)
	if err != nil {
		return nil, fmt.Errorf("flow/store/sqlite: list runs: %w", err)
	}
	defer rows.Close()
	out := make([]flowstore.RunMeta, 0, limit)
	for rows.Next() {
		var (
			m          flowstore.RunMeta
			status     string
			startedAt  int64
			finishedAt sql.NullInt64
		)
		if err := rows.Scan(&m.ID, &m.FlowID, &status, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("flow/store/sqlite: scan run: %w", err)
		}
		m.Status = flowstore.RunStatus(status)
		m.StartedAt = unixToTime(startedAt)
		if finishedAt.Valid {
			t := unixToTime(finishedAt.Int64)
			m.FinishedAt = &t
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// newRunID produces a 16-char hex string (8 bytes of crypto/rand).
// Collision probability at 1e6 runs is ~3e-11 — adequate for v0.0.5
// where this is a demo store. Future revisions can switch to UUIDv7.
func newRunID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("flow/store/sqlite: id rand: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
