package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/majorcontext/motte/internal/meta/sqlcore"
	"github.com/majorcontext/motte/meta"
)

func (s *Store) ProtectedRefs(ctx context.Context, repo string) ([]string, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT patterns FROM protected_refs WHERE repo = ?`, repo).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: read protected refs: %w", err)
	}
	var patterns []string
	if err := json.Unmarshal([]byte(raw), &patterns); err != nil {
		return nil, fmt.Errorf("sqlite: read protected refs: %w", err)
	}
	return patterns, nil
}

func (s *Store) SetProtectedRefs(ctx context.Context, repo string, patterns []string) error {
	raw, err := json.Marshal(append([]string{}, patterns...))
	if err != nil {
		return fmt.Errorf("sqlite: set protected refs: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO protected_refs (repo, patterns) VALUES (?, ?)
		ON CONFLICT (repo) DO UPDATE SET patterns = excluded.patterns`, repo, string(raw)); err != nil {
		return fmt.Errorf("sqlite: set protected refs: %w", err)
	}
	return nil
}

// Events reads the rows first and the floor second, so a prune between the reads gives ErrEventsPruned and never a
// silent gap.
func (s *Store) Events(ctx context.Context, partition int, after int64, limit int) ([]meta.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT partition, seq, repo, ref, old_commit, new_commit, generation, kind, at
		FROM events WHERE partition = ? AND seq > ? ORDER BY seq LIMIT ?`, partition, after, limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Event
	for rows.Next() {
		var e meta.Event
		var at int64
		if err := rows.Scan(&e.Partition, &e.Seq, &e.Repo, &e.Ref, &e.Old, &e.New, &e.Generation, &e.Kind, &at); err != nil {
			return nil, fmt.Errorf("sqlite: list events: %w", err)
		}
		e.At = time.UnixMicro(at).UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list events: %w", err)
	}
	_ = rows.Close()
	var floor int64
	if err := s.db.QueryRowContext(ctx, `SELECT floor FROM event_partitions WHERE partition = ?`, partition).Scan(&floor); err != nil {
		return nil, fmt.Errorf("sqlite: read events floor: %w", err)
	}
	if after < floor {
		return nil, meta.ErrEventsPruned
	}
	return out, nil
}

func (s *Store) EventHeads(ctx context.Context) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq FROM event_partitions ORDER BY partition`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read event heads: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var heads []int64
	for rows.Next() {
		var h int64
		if err := rows.Scan(&h); err != nil {
			return nil, fmt.Errorf("sqlite: read event heads: %w", err)
		}
		heads = append(heads, h)
	}
	return heads, rows.Err()
}

func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_partitions SET floor = max(floor, coalesce((SELECT max(seq) FROM events e WHERE e.partition = event_partitions.partition AND e.at < ?1), 0))`,
			before.UnixMicro()); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM events WHERE at < ?`, before.UnixMicro())
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlite: prune events: %w", err)
	}
	return nil
}

func (s *Store) CreateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	var created time.Time
	var cursors []int64
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		var micro int64
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO subscriptions (id, url, secret, repo, created_at) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (id) DO NOTHING RETURNING created_at`, sub.ID, sub.URL, sub.Secret, sub.Repo, micros()).Scan(&micro); err != nil {
			return err
		}
		created = time.UnixMicro(micro).UTC()
		if _, err := tx.ExecContext(ctx, `INSERT INTO subscription_cursors (sub_id, partition, cursor) SELECT ?, partition, seq FROM event_partitions`, sub.ID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT cursor FROM subscription_cursors WHERE sub_id = ? ORDER BY partition`, sub.ID)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var c int64
			if err := rows.Scan(&c); err != nil {
				return err
			}
			cursors = append(cursors, c)
		}
		return rows.Err()
	})
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Subscription{}, meta.ErrExists
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("sqlite: create subscription: %w", err)
	}
	sub.CreatedAt, sub.Cursors = created, cursors
	return sub, nil
}

func (s *Store) UpdateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE subscriptions SET url = ?, secret = ?, repo = ? WHERE id = ?`, sub.URL, sub.Secret, sub.Repo, sub.ID)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("sqlite: update subscription: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return meta.Subscription{}, meta.ErrNotFound
	}
	all, err := s.Subscriptions(ctx)
	if err != nil {
		return meta.Subscription{}, err
	}
	for _, got := range all {
		if got.ID == sub.ID {
			return got, nil
		}
	}
	return meta.Subscription{}, meta.ErrNotFound
}

func (s *Store) Subscriptions(ctx context.Context) ([]meta.Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT s.id, s.url, s.secret, s.repo, s.created_at, c.cursor
		FROM subscriptions s JOIN subscription_cursors c ON c.sub_id = s.id ORDER BY s.created_at, s.id, c.partition`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list subscriptions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Subscription
	for rows.Next() {
		var sub meta.Subscription
		var created, cursor int64
		if err := rows.Scan(&sub.ID, &sub.URL, &sub.Secret, &sub.Repo, &created, &cursor); err != nil {
			return nil, fmt.Errorf("sqlite: list subscriptions: %w", err)
		}
		if n := len(out); n > 0 && out[n-1].ID == sub.ID {
			out[n-1].Cursors = append(out[n-1].Cursors, cursor)
			continue
		}
		sub.CreatedAt, sub.Cursors = time.UnixMicro(created).UTC(), []int64{cursor}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list subscriptions: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteSubscription(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM subscriptions WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("sqlite: delete subscription: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		if err != nil {
			return err
		}
		return meta.ErrNotFound
	}
	return nil
}

func (s *Store) AcquireLease(ctx context.Context, id string, partition int, owner string, now, until time.Time) (meta.Lease, error) {
	l := meta.Lease{ID: id, Partition: partition}
	err := s.db.QueryRowContext(ctx, `
		UPDATE subscription_cursors SET lease_owner = ?3, lease_until = ?5
		WHERE sub_id = ?1 AND partition = ?2 AND (lease_owner = ?3 OR lease_until <= ?4)
		RETURNING cursor, (SELECT url FROM subscriptions WHERE id = ?1), (SELECT secret FROM subscriptions WHERE id = ?1), (SELECT repo FROM subscriptions WHERE id = ?1)`,
		id, partition, owner, now.UnixMicro(), until.UnixMicro()).Scan(&l.Cursor, &l.URL, &l.Secret, &l.Repo)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Lease{}, sqlcore.LeaseLost(ctx, s.probe(), id, partition)
	}
	if err != nil {
		return meta.Lease{}, fmt.Errorf("sqlite: acquire lease: %w", err)
	}
	return l, nil
}

func (s *Store) AdvanceCursor(ctx context.Context, id string, partition int, owner string, cursor int64) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE subscription_cursors SET cursor = max(cursor, ?4) WHERE sub_id = ?1 AND partition = ?2 AND lease_owner = ?3`, id, partition, owner, cursor)
	if err != nil {
		return fmt.Errorf("sqlite: advance cursor: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		if err != nil {
			return err
		}
		return sqlcore.LeaseLost(ctx, s.probe(), id, partition)
	}
	return nil
}
