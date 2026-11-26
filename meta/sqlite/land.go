package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/majorcontext/motte/internal/meta/sqlcore"
	"github.com/majorcontext/motte/meta"
)

func (s *Store) EnqueueLand(ctx context.Context, r meta.LandRequest, now time.Time) (meta.LandItem, error) {
	item := meta.LandItem{LandRequest: meta.LandRequest{ID: r.ID, Repo: r.Repo}}
	var state string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO land_queue (repo, id, ref, base, head, kind, expect_tree, enqueued_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (repo, id) DO UPDATE SET id = land_queue.id
		RETURNING seq, ref, base, head, kind, expect_tree, state, result, COALESCE(generation, 0), acked`, r.Repo, r.ID, r.Ref, r.Base, r.Head, r.Kind, r.ExpectTree, now.UnixMicro()).
		Scan(&item.Seq, &item.Ref, &item.Base, &item.Head, &item.Kind, &item.ExpectTree, &state, &item.Result, &item.Generation, &item.Acked)
	if err != nil {
		return meta.LandItem{}, fmt.Errorf("sqlite: enqueue land: %w", err)
	}
	item.State = meta.LandState(state)
	return item, nil
}

func (s *Store) LandItem(ctx context.Context, repo, id string) (meta.LandItem, error) {
	item := meta.LandItem{LandRequest: meta.LandRequest{ID: id, Repo: repo}}
	var state string
	err := s.db.QueryRowContext(ctx, `SELECT seq, ref, base, head, kind, expect_tree, state, result, COALESCE(generation, 0), acked FROM land_queue WHERE repo = ? AND id = ?`, repo, id).
		Scan(&item.Seq, &item.Ref, &item.Base, &item.Head, &item.Kind, &item.ExpectTree, &state, &item.Result, &item.Generation, &item.Acked)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.LandItem{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.LandItem{}, fmt.Errorf("sqlite: read land: %w", err)
	}
	item.State = meta.LandState(state)
	return item, nil
}

func (s *Store) LandPending(ctx context.Context) ([]meta.LandKey, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT repo, ref FROM land_queue WHERE state <> 'done'`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list pending lands: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.LandKey
	for rows.Next() {
		var k meta.LandKey
		if err := rows.Scan(&k.Repo, &k.Ref); err != nil {
			return nil, fmt.Errorf("sqlite: list pending lands: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *Store) AcquireLandLease(ctx context.Context, repo, ref, owner string, now, until time.Time) (time.Time, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO land_leases (repo, ref, owner, lease_until) VALUES (?1, ?2, ?3, ?5)
		ON CONFLICT (repo, ref) DO UPDATE SET owner = ?3, lease_until = ?5
		WHERE land_leases.owner = ?3 OR land_leases.lease_until <= ?4`, repo, ref, owner, now.UnixMicro(), until.UnixMicro())
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlite: acquire land lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, err
	}
	if n > 0 {
		return until, nil
	}
	var end int64
	err = s.db.QueryRowContext(ctx, `SELECT lease_until FROM land_leases WHERE repo = ? AND ref = ?`, repo, ref).Scan(&end)
	if errors.Is(err, sql.ErrNoRows) {
		return now, meta.ErrLeaseHeld
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlite: read land lease end: %w", err)
	}
	return time.UnixMicro(end), meta.ErrLeaseHeld
}

func (s *Store) ReleaseLandLease(ctx context.Context, repo, ref, owner string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE land_leases SET lease_until = 0 WHERE repo = ? AND ref = ? AND owner = ?`, repo, ref, owner); err != nil {
		return fmt.Errorf("sqlite: release land lease: %w", err)
	}
	return nil
}

const leaseHeld = `EXISTS (SELECT 1 FROM land_leases WHERE repo = ?1 AND ref = ?2 AND owner = ?3 AND lease_until > ?4)`

func (s *Store) ClaimLands(ctx context.Context, repo, ref, owner string, now time.Time, limit int) ([]meta.LandItem, error) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE land_queue SET state = 'claimed', owner = ?3
		WHERE repo = ?1 AND `+leaseHeld+` AND id IN (
			SELECT id FROM land_queue WHERE repo = ?1 AND ref = ?2 AND state <> 'done' ORDER BY seq LIMIT ?5)
		RETURNING id, seq, base, head, kind, expect_tree`, repo, ref, owner, now.UnixMicro(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: claim lands: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.LandItem
	for rows.Next() {
		it := meta.LandItem{LandRequest: meta.LandRequest{Repo: repo, Ref: ref}, State: meta.LandStateClaimed}
		if err := rows.Scan(&it.ID, &it.Seq, &it.Base, &it.Head, &it.Kind, &it.ExpectTree); err != nil {
			return nil, fmt.Errorf("sqlite: claim lands: %w", err)
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: claim lands: %w", err)
	}
	_ = rows.Close()
	if len(out) == 0 {
		return nil, sqlcore.LandLeaseLost(ctx, s.probe(), repo, ref, owner, now)
	}
	slices.SortFunc(out, func(a, b meta.LandItem) int { return int(a.Seq - b.Seq) })
	return out, nil
}

func (s *Store) FinishLands(ctx context.Context, repo, ref, owner string, now time.Time, results []meta.LandDone) error {
	var held bool
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `UPDATE land_queue SET state = 'done', result = ?5, done_at = ?4 WHERE repo = ?1 AND id = ?6 AND state <> 'done' AND `+leaseHeld)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for _, r := range results {
			if _, err := stmt.ExecContext(ctx, repo, ref, owner, now.UnixMicro(), r.Result, r.ID); err != nil {
				return err
			}
		}
		return tx.QueryRowContext(ctx, `SELECT `+leaseHeld, repo, ref, owner, now.UnixMicro()).Scan(&held)
	})
	if err != nil {
		return fmt.Errorf("sqlite: finish lands: %w", err)
	}
	if !held {
		return meta.ErrLeaseHeld
	}
	return nil
}

// leaseOK is leaseHeld for the statements of LandRef, which number repo, ref, owner and now as ?2, ?3, ?4 and ?5.
const leaseOK = `EXISTS (SELECT 1 FROM land_leases WHERE repo = ?2 AND ref = ?3 AND owner = ?4 AND lease_until > ?5)`

// LandRef is one transaction of conditional statements. Each carries the one condition applied: the event insert took
// its number in this transaction. No statement reads a result of the transaction into Go.
func (s *Store) LandRef(ctx context.Context, m meta.LandMove, owner string, now time.Time) (meta.Ref, error) {
	part := meta.Partition(m.Repo, m.Ref)
	args := []any{part, m.Repo, m.Ref, owner, now.UnixMicro(), m.Old, m.New, m.Kind, int64(len(m.Rows)), m.Retain()}
	const applied = `EXISTS (SELECT 1 FROM events WHERE partition = ?1 AND seq = (SELECT seq FROM event_partitions WHERE partition = ?1) AND generation = ` + nextGeneration + `)`
	var ok bool
	var generation int64
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			bumpGeneration,
			`INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at)
				SELECT ?1, ` + nextSeq("?1") + `, ?2, ?3, ?6, ?7, ` + nextGeneration + `, ?8, ?5 FROM refs
				WHERE repo = ?2 AND name = ?3 AND commit_sha = ?6 AND ` + leaseOK,
			bumpSeq,
			`INSERT INTO land_heads (repo, ref, n) SELECT ?2, ?3, ?9 WHERE ` + applied + `
				ON CONFLICT (repo, ref) DO UPDATE SET n = n + excluded.n`,
			`DELETE FROM land_log WHERE repo = ?2 AND ref = ?3 AND n <= (SELECT n FROM land_heads WHERE repo = ?2 AND ref = ?3) - ?10 AND ` + applied,
		} {
			stmtArgs := args
			if stmt == bumpSeq {
				stmtArgs = args[:1]
			}
			if _, err := tx.ExecContext(ctx, stmt, stmtArgs...); err != nil {
				return err
			}
		}
		if err := landRows(ctx, tx, m, part, now.UnixMicro(), applied); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE refs SET commit_sha = ?7, generation = `+nextGeneration+`, updated_at = ?5 WHERE repo = ?2 AND name = ?3 AND commit_sha = ?6 AND `+applied, args...); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT `+applied+`, `+nextGeneration, args[:1]...).Scan(&ok, &generation)
	})
	if err != nil {
		return meta.Ref{}, fmt.Errorf("sqlite: land ref: %w", err)
	}
	if ok {
		return meta.Ref{Name: m.Ref, Commit: m.New, Generation: generation}, nil
	}
	if err := sqlcore.LandLeaseLost(ctx, s.probe(), m.Repo, m.Ref, owner, now); err != nil {
		return meta.Ref{}, err
	}
	return meta.Ref{}, sqlcore.RefConflict(ctx, s.probe(), m.Repo, m.Ref)
}

// landRows appends the log rows and ends the items of m, one statement each, while applied holds. applied reads the
// partition as ?1.
func landRows(ctx context.Context, tx *sql.Tx, m meta.LandMove, part int, now int64, applied string) error {
	rowStmt, err := tx.PrepareContext(ctx, `INSERT INTO land_log (repo, ref, n, old_tip, new_tip, paths)
		SELECT ?2, ?3, (SELECT n FROM land_heads WHERE repo = ?2 AND ref = ?3) - ?4 + ?5, ?6, ?7, ?8 WHERE `+applied)
	if err != nil {
		return err
	}
	defer func() { _ = rowStmt.Close() }()
	for i, r := range m.Rows {
		var paths any
		if r.Paths != nil {
			raw, _ := json.Marshal(r.Paths)
			paths = string(raw)
		}
		if _, err := rowStmt.ExecContext(ctx, part, m.Repo, m.Ref, int64(len(m.Rows)), int64(i+1), r.Old, r.New, paths); err != nil {
			return err
		}
	}
	doneStmt, err := tx.PrepareContext(ctx, `UPDATE land_queue SET state = 'done', result = ?4, done_at = ?5, generation = CASE WHEN ?6 THEN `+nextGeneration+` END
		WHERE repo = ?2 AND id = ?3 AND state <> 'done' AND `+applied)
	if err != nil {
		return err
	}
	defer func() { _ = doneStmt.Close() }()
	for _, r := range m.Results {
		if _, err := doneStmt.ExecContext(ctx, part, m.Repo, r.ID, r.Result, now, r.Landed); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LandLogSince(ctx context.Context, repo, ref, base string, limit int) ([]meta.LandLogRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT old_tip, new_tip, paths FROM land_log WHERE repo = ?1 AND ref = ?2 AND n >= COALESCE(max(
			(SELECT max(n) + 1 FROM land_log WHERE repo = ?1 AND ref = ?2 AND new_tip = ?3),
			(SELECT max(n) FROM land_log WHERE repo = ?1 AND ref = ?2 AND old_tip = ?3)), (SELECT max(n) + 1 FROM land_log WHERE repo = ?1 AND ref = ?2 AND new_tip = ?3), (SELECT max(n) FROM land_log WHERE repo = ?1 AND ref = ?2 AND old_tip = ?3))
		ORDER BY n LIMIT ?4`, repo, ref, base, limit+1)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read land log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.LandLogRow
	for rows.Next() {
		var row meta.LandLogRow
		var raw sql.NullString
		if err := rows.Scan(&row.Old, &row.New, &raw); err != nil {
			return nil, fmt.Errorf("sqlite: read land log: %w", err)
		}
		if raw.Valid {
			if err := json.Unmarshal([]byte(raw.String), &row.Paths); err != nil {
				return nil, fmt.Errorf("sqlite: read land log: %w", err)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: read land log: %w", err)
	}
	if len(out) > limit {
		return nil, nil
	}
	return out, nil
}

func (s *Store) AckLand(ctx context.Context, repo, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE land_queue SET acked = 1, result = '' WHERE repo = ? AND id = ? AND state = 'done'`, repo, id)
	if err != nil {
		return fmt.Errorf("sqlite: ack land: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n > 0 {
		return err
	}
	if _, err := s.LandItem(ctx, repo, id); err != nil {
		return err
	}
	return meta.ErrLandPending
}

func (s *Store) PruneLands(ctx context.Context, before time.Time) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM land_queue WHERE state = 'done' AND done_at < ?`, before.UnixMicro()); err != nil {
		return fmt.Errorf("sqlite: prune lands: %w", err)
	}
	return nil
}
