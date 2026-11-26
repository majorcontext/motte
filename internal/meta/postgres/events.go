package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/majorcontext/motte/internal/meta/sqlcore"
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
func (s *Store) Events(ctx context.Context, partition int, after int64, limit int) ([]meta.Event, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT partition, seq, repo, ref, old_commit, new_commit, generation, kind, at
		FROM events WHERE partition = $1 AND seq > $2 ORDER BY seq LIMIT $3`, partition, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list events: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Event])
	if err != nil {
		return nil, fmt.Errorf("postgres: list events: %w", err)
	}
	var floor int64
	if err := s.Pool.QueryRow(ctx, `SELECT floor FROM event_partitions WHERE partition = $1`, partition).Scan(&floor); err != nil {
		return nil, fmt.Errorf("postgres: read events floor: %w", err)
	}
	if after < floor {
		return nil, meta.ErrEventsPruned
	}
	return out, nil
}

func (s *Store) EventHeads(ctx context.Context) ([]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT seq FROM event_partitions ORDER BY partition`)
	if err != nil {
		return nil, fmt.Errorf("postgres: read event heads: %w", err)
	}
	heads, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("postgres: read event heads: %w", err)
	}
	return heads, nil
}

func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	if _, err := s.Pool.Exec(ctx, `
		WITH gone AS (DELETE FROM events WHERE at < $1 RETURNING partition, seq),
		top AS (SELECT partition, max(seq) AS seq FROM gone GROUP BY partition)
		UPDATE event_partitions p SET floor = GREATEST(p.floor, top.seq) FROM top WHERE p.partition = top.partition`, before); err != nil {
		return fmt.Errorf("postgres: prune events: %w", err)
	}
	return nil
}

const subscriptionSelect = `
	SELECT s.id, s.url, s.secret, s.repo, s.created_at,
		(SELECT array_agg(c.cursor ORDER BY c.partition) FROM subscription_cursors c WHERE c.sub_id = s.id)
	FROM subscriptions s`

func (s *Store) CreateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	rows, err := s.Pool.Query(ctx, `
		WITH made AS (
			INSERT INTO subscriptions (id, url, secret, repo) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING
			RETURNING id, url, secret, repo, created_at),
		cursors AS (
			INSERT INTO subscription_cursors (sub_id, partition, cursor)
			SELECT made.id, p.partition, p.seq FROM made, event_partitions p RETURNING 1)
		SELECT made.id, made.url, made.secret, made.repo, made.created_at, (SELECT array_agg(seq ORDER BY partition) FROM event_partitions)
		FROM made`, sub.ID, sub.URL, sub.Secret, sub.Repo)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: create subscription: %w", err)
	}
	created, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[meta.Subscription])
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Subscription{}, meta.ErrExists
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: create subscription: %w", err)
	}
	return created, nil
}

func (s *Store) UpdateSubscription(ctx context.Context, sub meta.Subscription) (meta.Subscription, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE subscriptions SET url = $2, secret = $3, repo = $4 WHERE id = $1`, sub.ID, sub.URL, sub.Secret, sub.Repo)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: update subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return meta.Subscription{}, meta.ErrNotFound
	}
	rows, err := s.Pool.Query(ctx, subscriptionSelect+` WHERE s.id = $1`, sub.ID)
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: update subscription: %w", err)
	}
	out, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[meta.Subscription])
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Subscription{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Subscription{}, fmt.Errorf("postgres: update subscription: %w", err)
	}
	return out, nil
}

func (s *Store) Subscriptions(ctx context.Context) ([]meta.Subscription, error) {
	rows, err := s.Pool.Query(ctx, subscriptionSelect+` ORDER BY s.created_at, s.id COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list subscriptions: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Subscription])
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

func (s *Store) AcquireLease(ctx context.Context, id string, partition int, owner string, now, until time.Time) (meta.Lease, error) {
	l := meta.Lease{ID: id, Partition: partition}
	err := s.Pool.QueryRow(ctx, `
		WITH taken AS (
			UPDATE subscription_cursors SET lease_owner = $3, lease_until = $5
			WHERE sub_id = $1 AND partition = $2 AND (lease_owner = $3 OR lease_until <= $4) RETURNING cursor)
		SELECT s.url, s.secret, s.repo, taken.cursor FROM taken, subscriptions s WHERE s.id = $1`,
		id, partition, owner, now, until).Scan(&l.URL, &l.Secret, &l.Repo, &l.Cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Lease{}, sqlcore.LeaseLost(ctx, s.probe(), id, partition)
	}
	if err != nil {
		return meta.Lease{}, fmt.Errorf("postgres: acquire lease: %w", err)
	}
	return l, nil
}

func (s *Store) AdvanceCursor(ctx context.Context, id string, partition int, owner string, cursor int64) error {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE subscription_cursors SET cursor = GREATEST(cursor, $4) WHERE sub_id = $1 AND partition = $2 AND lease_owner = $3`,
		id, partition, owner, cursor)
	if err != nil {
		return fmt.Errorf("postgres: advance cursor: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return sqlcore.LeaseLost(ctx, s.probe(), id, partition)
	}
	return nil
}
