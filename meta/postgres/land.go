package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/majorcontext/motte/meta"
)

type logJSON struct {
	I     int      `json:"i"`
	Old   string   `json:"old"`
	New   string   `json:"new"`
	Paths []string `json:"paths"`
}

type doneJSON struct {
	ID     string `json:"id"`
	Result string `json:"result"`
	Landed bool   `json:"landed"`
}

func (s *Store) EnqueueLand(ctx context.Context, r meta.LandRequest, now time.Time) (meta.LandItem, error) {
	item := meta.LandItem{LandRequest: meta.LandRequest{ID: r.ID, Repo: r.Repo}}
	var state string
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO land_queue (repo, id, ref, base, head, kind, expect_tree, enqueued_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (repo, id) DO UPDATE SET id = land_queue.id
		RETURNING seq, ref, base, head, kind, expect_tree, state, result, COALESCE(generation, 0), acked`, r.Repo, r.ID, r.Ref, r.Base, r.Head, r.Kind, r.ExpectTree, now).
		Scan(&item.Seq, &item.Ref, &item.Base, &item.Head, &item.Kind, &item.ExpectTree, &state, &item.Result, &item.Generation, &item.Acked)
	if err != nil {
		return meta.LandItem{}, fmt.Errorf("postgres: enqueue land: %w", err)
	}
	item.State = meta.LandState(state)
	return item, nil
}

func (s *Store) LandItem(ctx context.Context, repo, id string) (meta.LandItem, error) {
	item := meta.LandItem{LandRequest: meta.LandRequest{ID: id, Repo: repo}}
	var state string
	err := s.Pool.QueryRow(ctx, `SELECT seq, ref, base, head, kind, expect_tree, state, result, COALESCE(generation, 0), acked FROM land_queue WHERE repo = $1 AND id = $2`, repo, id).
		Scan(&item.Seq, &item.Ref, &item.Base, &item.Head, &item.Kind, &item.ExpectTree, &state, &item.Result, &item.Generation, &item.Acked)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.LandItem{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.LandItem{}, fmt.Errorf("postgres: read land: %w", err)
	}
	item.State = meta.LandState(state)
	return item, nil
}

func (s *Store) LandPending(ctx context.Context) ([]meta.LandKey, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT repo, ref FROM land_queue WHERE state <> 'done'`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list pending lands: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.LandKey])
	if err != nil {
		return nil, fmt.Errorf("postgres: list pending lands: %w", err)
	}
	return out, nil
}

func (s *Store) AcquireLandLease(ctx context.Context, repo, ref, owner string, now, until time.Time) (time.Time, error) {
	tag, err := s.Pool.Exec(ctx, `
		INSERT INTO land_leases (repo, ref, owner, lease_until) VALUES ($1, $2, $3, $5)
		ON CONFLICT (repo, ref) DO UPDATE SET owner = $3, lease_until = $5
		WHERE land_leases.owner = $3 OR land_leases.lease_until <= $4`, repo, ref, owner, now, until)
	if err != nil {
		return time.Time{}, fmt.Errorf("postgres: acquire land lease: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return until, nil
	}
	var end time.Time
	err = s.Pool.QueryRow(ctx, `SELECT lease_until FROM land_leases WHERE repo = $1 AND ref = $2`, repo, ref).Scan(&end)
	if errors.Is(err, pgx.ErrNoRows) {
		return now, meta.ErrLeaseHeld
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("postgres: read land lease end: %w", err)
	}
	return end, meta.ErrLeaseHeld
}

func (s *Store) ReleaseLandLease(ctx context.Context, repo, ref, owner string) error {
	if _, err := s.Pool.Exec(ctx, `UPDATE land_leases SET lease_until = 'epoch' WHERE repo = $1 AND ref = $2 AND owner = $3`, repo, ref, owner); err != nil {
		return fmt.Errorf("postgres: release land lease: %w", err)
	}
	return nil
}

func (s *Store) landLeaseHeld(ctx context.Context, repo, ref, owner string, now time.Time) error {
	var held bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM land_leases WHERE repo = $1 AND ref = $2 AND owner = $3 AND lease_until > $4)`, repo, ref, owner, now).Scan(&held)
	if err != nil {
		return fmt.Errorf("postgres: read land lease: %w", err)
	}
	if !held {
		return meta.ErrLeaseHeld
	}
	return nil
}

func (s *Store) ClaimLands(ctx context.Context, repo, ref, owner string, now time.Time, limit int) ([]meta.LandItem, error) {
	rows, err := s.Pool.Query(ctx, `
		WITH lease AS (SELECT 1 FROM land_leases WHERE repo = $1 AND ref = $2 AND owner = $3 AND lease_until > $4),
		picked AS (
			SELECT id FROM land_queue WHERE repo = $1 AND ref = $2 AND state <> 'done' AND EXISTS (SELECT 1 FROM lease)
			ORDER BY seq LIMIT $5)
		UPDATE land_queue q SET state = 'claimed', owner = $3 FROM picked WHERE q.repo = $1 AND q.id = picked.id AND q.state <> 'done'
		RETURNING q.id, q.seq, q.base, q.head, q.kind, q.expect_tree`, repo, ref, owner, now, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim lands: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (meta.LandItem, error) {
		it := meta.LandItem{LandRequest: meta.LandRequest{Repo: repo, Ref: ref}, State: meta.LandStateClaimed}
		err := r.Scan(&it.ID, &it.Seq, &it.Base, &it.Head, &it.Kind, &it.ExpectTree)
		return it, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: claim lands: %w", err)
	}
	if len(out) == 0 {
		return nil, s.landLeaseHeld(ctx, repo, ref, owner, now)
	}
	slices.SortFunc(out, func(a, b meta.LandItem) int { return int(a.Seq - b.Seq) })
	return out, nil
}

func doneRows(results []meta.LandDone) []byte {
	rows := make([]doneJSON, len(results))
	for i, r := range results {
		rows[i] = doneJSON{ID: r.ID, Result: r.Result, Landed: r.Landed}
	}
	raw, _ := json.Marshal(rows)
	return raw
}

func (s *Store) LandRef(ctx context.Context, m meta.LandMove, owner string, now time.Time) (meta.Ref, error) {
	logRows := make([]logJSON, len(m.Rows))
	for i, r := range m.Rows {
		logRows[i] = logJSON{I: i + 1, Old: r.Old, New: r.New, Paths: r.Paths}
	}
	rawRows, _ := json.Marshal(logRows)
	part := meta.Partition(m.Repo, m.Ref)
	var generation int64
	wrote, err := s.withEvent(ctx, part, func(tx pgx.Tx, seq int64) (bool, error) {
		err := tx.QueryRow(ctx, `
			WITH lease AS (SELECT 1 FROM land_leases WHERE repo = $1 AND ref = $2 AND owner = $3 AND lease_until > $4 FOR SHARE),
			cur AS (
				SELECT commit_sha FROM refs WHERE repo = $1 AND name = $2 AND commit_sha = $5 AND EXISTS (SELECT 1 FROM lease) FOR UPDATE),
			moved AS (
				UPDATE refs SET commit_sha = $6, generation = nextval('ref_generation'), updated_at = now()
				FROM cur WHERE refs.repo = $1 AND refs.name = $2 RETURNING refs.generation),
			ev AS (
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $7::int, $8::bigint, $1::text, $2::text, $5::text, $6::text, moved.generation, $9::text FROM moved RETURNING 1),
			head AS (
				INSERT INTO land_heads (repo, ref, n) SELECT $1, $2, $10::bigint FROM moved
				ON CONFLICT (repo, ref) DO UPDATE SET n = land_heads.n + EXCLUDED.n RETURNING n),
			logged AS (
				INSERT INTO land_log (repo, ref, n, old_tip, new_tip, paths)
				SELECT $1, $2, head.n - $10::bigint + r.i, r.old, r.new, NULLIF(r.paths, 'null'::jsonb)
				FROM head, jsonb_to_recordset($11::jsonb) AS r(i bigint, old text, new text, paths jsonb) RETURNING 1),
			pruned AS (
				DELETE FROM land_log WHERE repo = $1 AND ref = $2 AND n <= (SELECT n FROM head) - $12::bigint RETURNING 1),
			ended AS (
				UPDATE land_queue q SET state = 'done', result = r.result, done_at = $4, generation = CASE WHEN r.landed THEN (SELECT generation FROM moved) END
				FROM jsonb_to_recordset($13::jsonb) AS r(id text, result text, landed boolean)
				WHERE q.repo = $1 AND q.id = r.id AND q.state <> 'done' AND EXISTS (SELECT 1 FROM moved) RETURNING 1)
			SELECT generation FROM moved`,
			m.Repo, m.Ref, owner, now, m.Old, m.New, part, seq, m.Kind, int64(len(m.Rows)), string(rawRows), m.Retain(), string(doneRows(m.Results))).Scan(&generation)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		return meta.Ref{}, fmt.Errorf("postgres: land ref: %w", err)
	}
	if wrote {
		return meta.Ref{Name: m.Ref, Commit: m.New, Generation: generation}, nil
	}
	if err := s.landLeaseHeld(ctx, m.Repo, m.Ref, owner, now); err != nil {
		return meta.Ref{}, err
	}
	cur, err := s.Ref(ctx, m.Repo, m.Ref)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return meta.Ref{}, &meta.RefConflict{}
	case err != nil:
		return meta.Ref{}, err
	}
	return meta.Ref{}, &meta.RefConflict{Current: &cur}
}

func (s *Store) FinishLands(ctx context.Context, repo, ref, owner string, now time.Time, results []meta.LandDone) error {
	var held bool
	err := s.Pool.QueryRow(ctx, `
		WITH lease AS (SELECT 1 FROM land_leases WHERE repo = $1 AND ref = $2 AND owner = $3 AND lease_until > $4 FOR SHARE),
		ended AS (
			UPDATE land_queue q SET state = 'done', result = r.result, done_at = $4
			FROM jsonb_to_recordset($5::jsonb) AS r(id text, result text, landed boolean)
			WHERE q.repo = $1 AND q.id = r.id AND q.state <> 'done' AND EXISTS (SELECT 1 FROM lease) RETURNING 1)
		SELECT EXISTS (SELECT 1 FROM lease)`, repo, ref, owner, now, string(doneRows(results))).Scan(&held)
	if err != nil {
		return fmt.Errorf("postgres: finish lands: %w", err)
	}
	if !held {
		return meta.ErrLeaseHeld
	}
	return nil
}

func (s *Store) LandLogSince(ctx context.Context, repo, ref, base string, limit int) ([]meta.LandLogRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT old_tip, new_tip, paths FROM land_log WHERE repo = $1 AND ref = $2 AND n >= GREATEST(
			(SELECT max(n) + 1 FROM land_log WHERE repo = $1 AND ref = $2 AND new_tip = $3),
			(SELECT max(n) FROM land_log WHERE repo = $1 AND ref = $2 AND old_tip = $3))
		ORDER BY n LIMIT $4`, repo, ref, base, limit+1)
	if err != nil {
		return nil, fmt.Errorf("postgres: read land log: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (meta.LandLogRow, error) {
		var row meta.LandLogRow
		var raw []byte
		if err := r.Scan(&row.Old, &row.New, &raw); err != nil {
			return row, err
		}
		if raw != nil {
			err := json.Unmarshal(raw, &row.Paths)
			return row, err
		}
		return row, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: read land log: %w", err)
	}
	if len(out) > limit {
		return nil, nil
	}
	return out, nil
}

func (s *Store) AckLand(ctx context.Context, repo, id string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE land_queue SET acked = true, result = '' WHERE repo = $1 AND id = $2 AND state = 'done'`, repo, id)
	if err != nil {
		return fmt.Errorf("postgres: ack land: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	if _, err := s.LandItem(ctx, repo, id); err != nil {
		return err
	}
	return meta.ErrLandPending
}

func (s *Store) PruneLands(ctx context.Context, before time.Time) error {
	if _, err := s.Pool.Exec(ctx, `DELETE FROM land_queue WHERE state = 'done' AND done_at < $1`, before); err != nil {
		return fmt.Errorf("postgres: prune lands: %w", err)
	}
	return nil
}
