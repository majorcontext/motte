package meta

import (
	"context"
	"time"
)

// LandLogRetention is the number of land log rows that a ref keeps. A batch removes the older rows in the unit that
// adds its own.
const LandLogRetention = 10000

// LandRequest is a land that a replica accepted. ID names it for the repo, so a repeat of an accepted request finds
// the first one. Kind is the writer kind of the grant, WriterWrite or WriterMerge.
type LandRequest struct {
	ID, Repo, Ref, Base, Head, Kind string
}

// LandItem is a queued land. Seq orders the queue of a repo. Result is set once the item is done, and is opaque to the
// store. Generation is the generation of the ref move that landed the item, and 0 for an item that did not land.
type LandItem struct {
	LandRequest
	Seq        int64
	Done       bool
	Result     string
	Generation int64
}

// LandKey names a ref that has queued lands.
type LandKey struct{ Repo, Ref string }

// LandDone is the result of one item, written in the unit that ends the item. Landed marks an item that the ref move
// of the unit holds: the store records the generation of that move on it.
type LandDone struct {
	ID, Result string
	Landed     bool
}

// LandLogRow is one landed change of a ref: the tip before it, the tip after it, and the paths that it changed. Paths
// is nil when the change touched too many paths to keep.
type LandLogRow struct {
	Old, New string
	Paths    []string
}

// LandMove is the ref move of one batch: the ref from Old to New, the log rows of the changes that it holds in land
// order, and the results of every item that the batch ended, whether it landed or not. Kind is the writer kind of the
// event. Keep is the number of log rows that the ref keeps, and LandLogRetention when it is zero.
type LandMove struct {
	Repo, Ref, Old, New, Kind string
	Keep                      int
	Rows                      []LandLogRow
	Results                   []LandDone
}

// Retain is the number of log rows that the unit of m keeps.
func (m LandMove) Retain() int64 {
	if m.Keep > 0 {
		return int64(m.Keep)
	}
	return LandLogRetention
}

// LandStore is the queue, the leader lease and the land log of the batched land. See Backends for the contract.
type LandStore interface {
	// EnqueueLand queues a land, or returns the item that has the same repo and id.
	EnqueueLand(ctx context.Context, r LandRequest, now time.Time) (LandItem, error)
	// LandItem returns the item of a repo and id, or ErrNotFound.
	LandItem(ctx context.Context, repo, id string) (LandItem, error)
	// LandPending lists the refs that have an item that is not done.
	LandPending(ctx context.Context) ([]LandKey, error)
	// AcquireLandLease gives owner the lease of a ref until the time until, when no owner holds it at now or owner holds
	// it. ErrLeaseHeld otherwise.
	AcquireLandLease(ctx context.Context, repo, ref, owner string, now, until time.Time) error
	// ReleaseLandLease ends the lease of owner at once. A lease of another owner stays.
	ReleaseLandLease(ctx context.Context, repo, ref, owner string) error
	// ClaimLands marks as claimed by owner, and returns, the oldest items of a ref that are not done, at most limit,
	// while owner holds the lease at now. An item that an earlier owner claimed and did not end comes again. ErrLeaseHeld.
	ClaimLands(ctx context.Context, repo, ref, owner string, now time.Time, limit int) ([]LandItem, error)
	// LandRef moves the ref of m from m.Old to m.New, appends the log rows and one event, and ends the items of
	// m.Results, in one unit. It applies only while the ref is at m.Old and owner holds the lease at now. ErrLeaseHeld
	// when the lease fails, else *RefConflict.
	LandRef(ctx context.Context, m LandMove, owner string, now time.Time) (Ref, error)
	// FinishLands ends the items of results with no ref move, while owner holds the lease at now. ErrLeaseHeld.
	FinishLands(ctx context.Context, repo, ref, owner string, now time.Time, results []LandDone) error
	// LandLogSince returns the log rows from the latest one that starts or ends at base, in order, at most limit. It
	// returns none when base is in no row or the rows after it are more than limit.
	LandLogSince(ctx context.Context, repo, ref, base string, limit int) ([]LandLogRow, error)
	// PruneLands removes the items that ended before the time.
	PruneLands(ctx context.Context, before time.Time) error
}
