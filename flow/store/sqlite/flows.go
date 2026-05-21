package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	flowstore "github.com/costa92/llm-agent-flow/flow/store"
)

// PutFlow implements flow/store.Store.
func (s *Store) PutFlow(ctx context.Context, id, name string, jsonBytes []byte, create bool) (flowstore.FlowRecord, error) {
	if id == "" {
		return flowstore.FlowRecord{}, errors.New("flow/store/sqlite: empty id")
	}
	if len(jsonBytes) == 0 {
		return flowstore.FlowRecord{}, errors.New("flow/store/sqlite: empty json")
	}

	now := nowUnix()

	if create {
		// INSERT only — duplicate id → ErrAlreadyExists.
		res, err := s.db.ExecContext(ctx,
			`INSERT OR IGNORE INTO flows (id, name, json, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?)`,
			id, name, string(jsonBytes), now, now)
		if err != nil {
			return flowstore.FlowRecord{}, fmt.Errorf("flow/store/sqlite: insert flow: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return flowstore.FlowRecord{}, flowstore.ErrAlreadyExists
		}
		return s.GetFlow(ctx, id)
	}

	// UPSERT — replace json + name, bump updated_at; preserve
	// created_at on existing rows.
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO flows (id, name, json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   name = excluded.name,
		   json = excluded.json,
		   updated_at = excluded.updated_at`,
		id, name, string(jsonBytes), now, now)
	if err != nil {
		return flowstore.FlowRecord{}, fmt.Errorf("flow/store/sqlite: upsert flow: %w", err)
	}
	return s.GetFlow(ctx, id)
}

// GetFlow implements flow/store.Store.
func (s *Store) GetFlow(ctx context.Context, id string) (flowstore.FlowRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, json, created_at, updated_at FROM flows WHERE id = ?`, id)
	var (
		rec        flowstore.FlowRecord
		jsonText   string
		createdAt  int64
		updatedAt  int64
	)
	if err := row.Scan(&rec.ID, &rec.Name, &jsonText, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flowstore.FlowRecord{}, flowstore.ErrNotFound
		}
		return flowstore.FlowRecord{}, fmt.Errorf("flow/store/sqlite: get flow: %w", err)
	}
	rec.JSON = []byte(jsonText)
	rec.CreatedAt = unixToTime(createdAt)
	rec.UpdatedAt = unixToTime(updatedAt)
	return rec, nil
}

// ListFlows implements flow/store.Store.
func (s *Store) ListFlows(ctx context.Context, limit int) ([]flowstore.FlowMeta, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, created_at, updated_at
		 FROM flows
		 ORDER BY updated_at DESC, id
		 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("flow/store/sqlite: list flows: %w", err)
	}
	defer rows.Close()
	out := make([]flowstore.FlowMeta, 0, limit)
	for rows.Next() {
		var (
			m         flowstore.FlowMeta
			createdAt int64
			updatedAt int64
		)
		if err := rows.Scan(&m.ID, &m.Name, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("flow/store/sqlite: scan flow: %w", err)
		}
		m.CreatedAt = unixToTime(createdAt)
		m.UpdatedAt = unixToTime(updatedAt)
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteFlow implements flow/store.Store. Returns ErrNotFound when
// the row does not exist so handlers can map cleanly to 404.
func (s *Store) DeleteFlow(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM flows WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("flow/store/sqlite: delete flow: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return flowstore.ErrNotFound
	}
	return nil
}
