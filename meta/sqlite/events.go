package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
func (s *Store) Events(ctx context.Context, after int64, limit int) ([]meta.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT seq, repo, ref, old_commit, new_commit, generation, kind, at FROM events WHERE seq > ? ORDER BY seq LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Event
	for rows.Next() {
		var e meta.Event
		var at int64
		if err := rows.Scan(&e.Seq, &e.Repo, &e.Ref, &e.Old, &e.New, &e.Generation, &e.Kind, &at); err != nil {
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
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM counters WHERE name = 'events_floor'`).Scan(&floor); err != nil {
		return nil, fmt.Errorf("sqlite: read events floor: %w", err)
	}
	if after < floor {
		return nil, meta.ErrEventsPruned
	}
	return out, nil
}

func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE counters SET value = max(value, coalesce((SELECT max(seq) FROM events WHERE at < ?1), 0))
			WHERE name = 'events_floor'`, before.UnixMicro()); err != nil {
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

const subscriptionColumns = `id, url, secret, repo, cursor, created_at, lease_owner, lease_until`

type scanner interface{ Scan(dest ...any) error }

func scanSubscription(row scanner) (meta.Subscription, error) {
	var sub meta.Subscription
	var created, until int64
	if err := row.Scan(&sub.ID, &sub.URL, &sub.Secret, &sub.Repo, &sub.Cursor, &created, &sub.LeaseOwner, &until); err != nil {
		return meta.Subscription{}, err
	}
	sub.CreatedAt, sub.LeaseUntil = time.UnixMicro(created).UTC(), time.UnixMicro(until).UTC()
	return sub, nil
}

func (s *Store) CreateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	created, err := scanSubscription(s.db.QueryRowContext(ctx, `
		INSERT INTO subscriptions (id, url, secret, repo, cursor, created_at)
		VALUES (?, ?, ?, ?, (SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)), ?)
		ON CONFLICT (id) DO NOTHING RETURNING `+subscriptionColumns, sub.ID, sub.URL, sub.Secret, sub.Repo, micros()))
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Subscription{}, meta.ErrExists
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("sqlite: create subscription: %w", err)
	}
	return created, nil
}

func (s *Store) Subscriptions(ctx context.Context) ([]meta.Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+subscriptionColumns+` FROM subscriptions ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list subscriptions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Subscription
	for rows.Next() {
		sub, err := scanSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list subscriptions: %w", err)
		}
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

func (s *Store) leaseLost(ctx context.Context, id string) error {
	var found bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM subscriptions WHERE id = ?)`, id).Scan(&found); err != nil {
		return fmt.Errorf("sqlite: read subscription: %w", err)
	}
	if !found {
		return meta.ErrNotFound
	}
	return meta.ErrLeaseHeld
}

func (s *Store) AcquireLease(ctx context.Context, id, owner string, now, until time.Time) (meta.Subscription, error) {
	sub, err := scanSubscription(s.db.QueryRowContext(ctx, `
		UPDATE subscriptions SET lease_owner = ?2, lease_until = ?4
		WHERE id = ?1 AND (lease_owner = ?2 OR lease_until <= ?3)
		RETURNING `+subscriptionColumns, id, owner, now.UnixMicro(), until.UnixMicro()))
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Subscription{}, s.leaseLost(ctx, id)
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("sqlite: acquire lease: %w", err)
	}
	return sub, nil
}

func (s *Store) AdvanceCursor(ctx context.Context, id, owner string, cursor int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE subscriptions SET cursor = max(cursor, ?3) WHERE id = ?1 AND lease_owner = ?2`, id, owner, cursor)
	if err != nil {
		return fmt.Errorf("sqlite: advance cursor: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		if err != nil {
			return err
		}
		return s.leaseLost(ctx, id)
	}
	return nil
}
