package postgres

import (
	"context"
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
	if err := p.s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM subscription_cursors WHERE sub_id = $1 AND partition = $2)`, id, partition).Scan(&found); err != nil {
		return false, fmt.Errorf("postgres: read subscription: %w", err)
	}
	return found, nil
}

func (p probe) LandLeaseHeld(ctx context.Context, repo, ref, owner string, now time.Time) (bool, error) {
	var held bool
	err := p.s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM land_leases WHERE repo = $1 AND ref = $2 AND owner = $3 AND lease_until > $4)`, repo, ref, owner, now).Scan(&held)
	if err != nil {
		return false, fmt.Errorf("postgres: read land lease: %w", err)
	}
	return held, nil
}
