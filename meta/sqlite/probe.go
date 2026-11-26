package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/majorcontext/motte/internal/meta/sqlcore"
	"github.com/majorcontext/motte/meta"
)

type probe struct{ s *Store }

func (s *Store) probe() sqlcore.Probe { return probe{s} }

func (p probe) Fence(ctx context.Context, ws string) (meta.Fence, error) { return p.s.fence(ctx, ws) }

func (p probe) OpHeads(ctx context.Context, ws string) (meta.OpHeads, error) {
	return p.s.OpHeads(ctx, ws)
}

func (p probe) Ref(ctx context.Context, repo, name string) (meta.Ref, error) {
	return p.s.Ref(ctx, repo, name)
}

func (p probe) CursorExists(ctx context.Context, id string, partition int) (bool, error) {
	var found bool
	if err := p.s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM subscription_cursors WHERE sub_id = ? AND partition = ?)`, id, partition).Scan(&found); err != nil {
		return false, fmt.Errorf("sqlite: read subscription: %w", err)
	}
	return found, nil
}

func (p probe) LandLeaseHeld(ctx context.Context, repo, ref, owner string, now time.Time) (bool, error) {
	var held bool
	if err := p.s.db.QueryRowContext(ctx, `SELECT `+leaseHeld, repo, ref, owner, now.UnixMicro()).Scan(&held); err != nil {
		return false, fmt.Errorf("sqlite: read land lease: %w", err)
	}
	return held, nil
}

func (p probe) LandLeaseEnd(ctx context.Context, repo, ref string) (time.Time, error) {
	var end int64
	err := p.s.db.QueryRowContext(ctx, `SELECT lease_until FROM land_leases WHERE repo = ? AND ref = ?`, repo, ref).Scan(&end)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, meta.ErrNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlite: read land lease end: %w", err)
	}
	return time.UnixMicro(end), nil
}

func (p probe) LandItem(ctx context.Context, repo, id string) (meta.LandItem, error) {
	return p.s.LandItem(ctx, repo, id)
}
