// Package sqlcore holds the checks that the SQL backends of meta.Store run after a write changed no row: they read
// the cause and pick the error. A backend keeps its SQL and gives these functions a Probe.
package sqlcore

import (
	"context"
	"errors"
	"time"

	"github.com/majorcontext/motte/meta"
)

// Probe reads the rows that the checks need. Each method wraps a database error with the name of its backend.
type Probe interface {
	// Fence returns meta.ErrNotFound for a workspace that does not exist.
	Fence(ctx context.Context, ws string) (meta.Fence, error)
	// OpHeads returns meta.ErrNotFound for a workspace with no op heads.
	OpHeads(ctx context.Context, ws string) (meta.OpHeads, error)
	// Ref returns meta.ErrNotFound for a missing ref.
	Ref(ctx context.Context, repo, name string) (meta.Ref, error)
	// CursorExists reports whether the subscription has a cursor row for the partition.
	CursorExists(ctx context.Context, id string, partition int) (bool, error)
	// LandLeaseHeld reports whether owner holds the land lease of the ref at now.
	LandLeaseHeld(ctx context.Context, repo, ref, owner string, now time.Time) (bool, error)
	// LandLeaseEnd returns when the land lease of the ref ends, or meta.ErrNotFound when it has no row.
	LandLeaseEnd(ctx context.Context, repo, ref string) (time.Time, error)
	// LandItem returns meta.ErrNotFound for a land that does not exist.
	LandItem(ctx context.Context, repo, id string) (meta.LandItem, error)
}

// FenceLost reports a failed fenced write as ErrClaimLost when the fence no longer holds. It returns nil when the
// fence holds, so the write lost on its own condition.
func FenceLost(ctx context.Context, p Probe, ws string, epoch uint64, repo string) error {
	f, err := p.Fence(ctx, ws)
	if errors.Is(err, meta.ErrNotFound) {
		return meta.ErrClaimLost
	}
	if err != nil {
		return err
	}
	return f.WriteLost(epoch, repo)
}

// FlushLost is the error of a flush that changed no row.
func FlushLost(ctx context.Context, p Probe, ws string, epoch uint64) error {
	f, err := p.Fence(ctx, ws)
	if err != nil {
		return err
	}
	return f.FlushLost(epoch)
}

// RefLost is the error of a ref write that changed no row: the claim of a node writer that was lost, or a conflict
// with the current ref.
func RefLost(ctx context.Context, p Probe, node bool, ws string, epoch uint64, repo, name string) error {
	if node {
		if err := FenceLost(ctx, p, ws, epoch, repo); err != nil {
			return err
		}
	}
	return RefConflict(ctx, p, repo, name)
}

// RefConflict is the conflict of a ref write that lost on its own condition: the current ref, or none when it is missing.
func RefConflict(ctx context.Context, p Probe, repo, name string) error {
	cur, err := p.Ref(ctx, repo, name)
	if errors.Is(err, meta.ErrNotFound) {
		return &meta.RefConflict{}
	}
	if err != nil {
		return err
	}
	return &meta.RefConflict{Current: &cur}
}

// OpHeadsLost is the error of an op heads swap that changed no row.
func OpHeadsLost(ctx context.Context, p Probe, ws string, epoch uint64, repo string) error {
	if err := FenceLost(ctx, p, ws, epoch, repo); err != nil {
		return err
	}
	cur, err := p.OpHeads(ctx, ws)
	if errors.Is(err, meta.ErrNotFound) {
		return &meta.OpHeadsConflict{}
	}
	if err != nil {
		return err
	}
	if cur.Epoch > epoch {
		return meta.ErrClaimLost
	}
	return &meta.OpHeadsConflict{Current: &cur}
}

// LeaseLost picks the error of an event lease write that changed no row.
func LeaseLost(ctx context.Context, p Probe, id string, partition int) error {
	found, err := p.CursorExists(ctx, id, partition)
	if err != nil {
		return err
	}
	if !found {
		return meta.ErrNotFound
	}
	return meta.ErrLeaseHeld
}

// LandLeaseLost returns meta.ErrLeaseHeld when owner no longer holds the land lease of the ref, and nil when it does,
// so the write lost on its own condition.
func LandLeaseLost(ctx context.Context, p Probe, repo, ref, owner string, now time.Time) error {
	held, err := p.LandLeaseHeld(ctx, repo, ref, owner, now)
	if err != nil {
		return err
	}
	if !held {
		return meta.ErrLeaseHeld
	}
	return nil
}

// ClaimLost is the error of a claim that changed no row: ErrGone for a deleted workspace and ErrWorkspaceConflict for
// one that another owner or repo holds.
func ClaimLost(ctx context.Context, p Probe, id string) error {
	f, err := p.Fence(ctx, id)
	if err != nil {
		return err
	}
	if f.Deleted {
		return meta.ErrGone
	}
	return meta.ErrWorkspaceConflict
}

// AckLandLost is the error of an ack that changed no row: ErrNotFound for a missing land, and ErrLandPending for a
// land that is not done.
func AckLandLost(ctx context.Context, p Probe, repo, id string) error {
	if _, err := p.LandItem(ctx, repo, id); err != nil {
		return err
	}
	return meta.ErrLandPending
}

// LandLeaseBusy is the answer to an acquire that changed no row: ErrLeaseHeld, and the time that the holder's lease
// ends, or now when the row is gone.
func LandLeaseBusy(ctx context.Context, p Probe, repo, ref string, now time.Time) (time.Time, error) {
	end, err := p.LandLeaseEnd(ctx, repo, ref)
	if errors.Is(err, meta.ErrNotFound) {
		return now, meta.ErrLeaseHeld
	}
	if err != nil {
		return time.Time{}, err
	}
	return end, meta.ErrLeaseHeld
}
