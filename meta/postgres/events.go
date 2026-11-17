package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/majorcontext/motte/meta"
)

func (s *Store) ProtectedRefs(ctx context.Context, repo string) ([]string, error) {
	var patterns []string
	err := s.Pool.QueryRow(ctx, `SELECT patterns FROM protected_refs WHERE repo = $1`, repo).Scan(&patterns)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read protected refs: %w", err)
	}
	return patterns, nil
}

func (s *Store) SetProtectedRefs(ctx context.Context, repo string, patterns []string) error {
	if patterns == nil {
		patterns = []string{}
	}
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO protected_refs (repo, patterns) VALUES ($1, $2)
		ON CONFLICT (repo) DO UPDATE SET patterns = EXCLUDED.patterns`, repo, patterns); err != nil {
		return fmt.Errorf("postgres: set protected refs: %w", err)
	}
	return nil
}

// Events reads the rows first and the floor second, so a prune between the reads gives ErrEventsPruned and never a
// silent gap.
func (s *Store) Events(ctx context.Context, after int64, limit int) ([]meta.Event, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT seq, repo, ref, old_commit, new_commit, generation, kind, at FROM events WHERE seq > $1 ORDER BY seq LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list events: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Event])
	if err != nil {
		return nil, fmt.Errorf("postgres: list events: %w", err)
	}
	var floor int64
	if err := s.Pool.QueryRow(ctx, `SELECT value FROM counters WHERE name = 'events_floor'`).Scan(&floor); err != nil {
		return nil, fmt.Errorf("postgres: read events floor: %w", err)
	}
	if after < floor {
		return nil, meta.ErrEventsPruned
	}
	return out, nil
}

func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	if _, err := s.Pool.Exec(ctx, `
		WITH gone AS (DELETE FROM events WHERE at < $1 RETURNING seq)
		UPDATE counters SET value = GREATEST(value, (SELECT max(seq) FROM gone))
		WHERE name = 'events_floor' AND EXISTS (SELECT 1 FROM gone)`, before); err != nil {
		return fmt.Errorf("postgres: prune events: %w", err)
	}
	return nil
}

const subscriptionColumns = `id, url, secret, repo, cursor, created_at, lease_owner, lease_until`

func scanSubscription(row pgx.CollectableRow) (meta.Subscription, error) {
	return pgx.RowToStructByPos[meta.Subscription](row)
}

func (s *Store) CreateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	rows, err := s.Pool.Query(ctx, `
		INSERT INTO subscriptions (id, url, secret, repo, cursor)
		VALUES ($1, $2, $3, $4, (SELECT value FROM counters WHERE name = 'event_seq'))
		ON CONFLICT (id) DO NOTHING RETURNING `+subscriptionColumns, sub.ID, sub.URL, sub.Secret, sub.Repo)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: create subscription: %w", err)
	}
	created, err := pgx.CollectOneRow(rows, scanSubscription)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Subscription{}, meta.ErrExists
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: create subscription: %w", err)
	}
	return created, nil
}

func (s *Store) Subscriptions(ctx context.Context) ([]meta.Subscription, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+subscriptionColumns+` FROM subscriptions ORDER BY created_at, id COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list subscriptions: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanSubscription)
	if err != nil {
		return nil, fmt.Errorf("postgres: list subscriptions: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteSubscription(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("postgres: delete subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return meta.ErrNotFound
	}
	return nil
}

// leaseLost picks the error of a lease write that changed no row.
func (s *Store) leaseLost(ctx context.Context, id string) error {
	var found bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM subscriptions WHERE id = $1)`, id).Scan(&found); err != nil {
		return fmt.Errorf("postgres: read subscription: %w", err)
	}
	if !found {
		return meta.ErrNotFound
	}
	return meta.ErrLeaseHeld
}

func (s *Store) AcquireLease(ctx context.Context, id, owner string, now, until time.Time) (meta.Subscription, error) {
	rows, err := s.Pool.Query(ctx, `
		UPDATE subscriptions SET lease_owner = $2, lease_until = $4
		WHERE id = $1 AND (lease_owner = $2 OR lease_until <= $3)
		RETURNING `+subscriptionColumns, id, owner, now, until)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: acquire lease: %w", err)
	}
	sub, err := pgx.CollectOneRow(rows, scanSubscription)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Subscription{}, s.leaseLost(ctx, id)
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: acquire lease: %w", err)
	}
	return sub, nil
}

func (s *Store) AdvanceCursor(ctx context.Context, id, owner string, cursor int64) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE subscriptions SET cursor = GREATEST(cursor, $3) WHERE id = $1 AND lease_owner = $2`, id, owner, cursor)
	if err != nil {
		return fmt.Errorf("postgres: advance cursor: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.leaseLost(ctx, id)
	}
	return nil
}
