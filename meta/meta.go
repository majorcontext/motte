// Package meta is the metadata seam of the store service: repositories,
// workspaces and their epoch fence, op heads, refs and the pack listing. A
// Store keeps small mutable records, never object data. Implementations:
// PostgreSQL (meta/postgres) and SQLite (meta/sqlite). The conformance suite
// is meta/metatest. docs/architecture.md, section Backends, is the contract
// of every method.
package meta

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is a read of a record that does not exist.
	ErrNotFound = errors.New("meta: not found")
	// ErrExists is a create of a record that exists.
	ErrExists = errors.New("meta: already exists")
	// ErrGone is a workspace that was deleted.
	ErrGone = errors.New("meta: workspace deleted")
	// ErrClaimLost is a write or claim whose epoch is not the stored one.
	ErrClaimLost = errors.New("meta: claim lost")
	// ErrWorkspaceConflict is a claim whose owner or repo differs from the stored one.
	ErrWorkspaceConflict = errors.New("meta: workspace owner or repo differs")
	// ErrWrongRepo is a grant whose repo is not the repo of the workspace.
	ErrWrongRepo = errors.New("meta: workspace is on another repo")
	// ErrEventsPruned is a read of events from a sequence that retention removed.
	ErrEventsPruned = errors.New("meta: events pruned")
	// ErrLeaseHeld is a lease or cursor write by an owner that does not hold the lease.
	ErrLeaseHeld = errors.New("meta: lease held by another owner")
)

// The kinds of writer that an Event records. Each is the scope of the grant
// that wrote the ref.
const (
	WriterNode     = "node"
	WriterMerge    = "merge"
	WriterOperator = "operator"
)

// RefConflict is a lost ref compare-and-swap. Current is nil when the ref is absent.
type RefConflict struct{ Current *Ref }

func (*RefConflict) Error() string { return "meta: ref generation conflict" }

// OpHeadsConflict is a lost op-heads compare-and-swap. Current is nil when no row exists.
type OpHeadsConflict struct{ Current *OpHeads }

func (*OpHeadsConflict) Error() string { return "meta: op heads generation conflict" }

// Repo is a repository that the store created. CreatedAt is set by the backend.
type Repo struct {
	Path, Name, Owner string
	CreatedAt         time.Time
}

// Workspace is the state that a claim returns.
type Workspace struct {
	Epoch    uint64
	Op       string
	Snapshot string
}

// Ref is one ref of a repository.
type Ref struct {
	Name       string
	Commit     string
	Generation int64
}

// OpHeads is the head set of the op log of a workspace.
type OpHeads struct {
	Epoch      uint64
	Generation int64
	Heads      []string
}

// Event is one change of a ref. Seq is unique and ordered in commit order across
// the store. Old is empty for a create, and New is empty for a delete. For a
// delete Generation is the generation that the ref had. Kind is a Writer constant.
type Event struct {
	Seq        int64
	Repo, Ref  string
	Old, New   string
	Generation int64
	Kind       string
	At         time.Time
}

// Subscription is a registered receiver of events. Cursor is the Seq of the
// last event that the receiver acknowledged. LeaseOwner holds the right to
// deliver until LeaseUntil. Repo is empty for every repo.
type Subscription struct {
	ID, URL, Secret, Repo string
	Cursor                int64
	CreatedAt             time.Time
	LeaseOwner            string
	LeaseUntil            time.Time
}

// Pack is a pack that a repository holds.
type Pack struct {
	ID    string
	Bytes int64
}

// Store is the metadata of the service. Every method that writes is one
// atomic unit, and none needs a lock or a transaction that spans calls. When
// a conditional write changes nothing, the backend reads the state once to
// pick the error. Names, repos and ids are opaque text, compared by byte.
type Store interface {
	// Ping checks that the backend answers.
	Ping(ctx context.Context) error

	// CreateRepo stores r and its ref main at commit, both or neither. It
	// returns ErrExists when r.Path or the ref exists. repoText is the text
	// form of the repo that the refs and packs name.
	CreateRepo(ctx context.Context, r Repo, repoText, commit string) error
	// Repo returns the repo at path, or ErrNotFound.
	Repo(ctx context.Context, path string) (Repo, error)
	// Repos lists every repo, newest first, ties by path descending.
	Repos(ctx context.Context) ([]Repo, error)

	// Claim raises the epoch of workspace id to epoch, or creates the
	// workspace. The returned epoch may be higher: the caller compares it
	// with its own. owner and repo are set at the first claim. ErrGone,
	// ErrWorkspaceConflict.
	Claim(ctx context.Context, id string, epoch uint64, owner, repo string) (Workspace, error)
	// Live checks that workspace id is on repo at epoch and not deleted.
	// ErrNotFound, ErrGone, ErrWrongRepo, ErrClaimLost, in that order.
	Live(ctx context.Context, id string, epoch uint64, repo string) error
	// Delete marks a workspace deleted. It is idempotent, and an unknown
	// workspace is not an error.
	Delete(ctx context.Context, id string) error
	// Report stores the op and snapshot of a node report under the fence.
	// generation 0 writes only while the stored op is prev. A lower
	// generation than the stored one is dropped without an error; an equal
	// one writes again. ErrClaimLost when the fence fails.
	Report(ctx context.Context, ws string, epoch uint64, repo string, generation int64, prev, op, snapshot string) error
	// Flush is Report for the control plane. The repo is the stored one.
	// ErrNotFound, ErrGone, ErrClaimLost.
	Flush(ctx context.Context, ws string, epoch uint64, generation int64, prev, op, snapshot string) error

	// OpHeads returns the head set of ws, or ErrNotFound.
	OpHeads(ctx context.Context, ws string) (OpHeads, error)
	// SwapOpHeads replaces the head set when the stored generation is expect
	// (0 for the first write) and the fence holds. A lost swap is
	// *OpHeadsConflict. ErrClaimLost wins over it.
	SwapOpHeads(ctx context.Context, ws string, epoch uint64, repo string, expect int64, heads []string) (OpHeads, error)

	// Ref returns a ref, or ErrNotFound.
	Ref(ctx context.Context, repo, name string) (Ref, error)
	// Refs lists the refs of a repo by name.
	Refs(ctx context.Context, repo string) ([]Ref, error)
	// SwapRef moves a ref to sha when its stored generation is expect (0
	// creates the ref) and the fence holds. A lost swap is *RefConflict.
	// ErrClaimLost wins over it.
	SwapRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64, sha string) (Ref, error)
	// DeleteRef removes a ref when its stored generation is expect and the
	// fence holds, with the errors of SwapRef.
	DeleteRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64) error

	// UpdateRef is the ref write of a direct push, with no workspace and no
	// fence. It moves ref name from commit old to commit new. An empty old
	// creates the ref and an empty new deletes it. A ref whose commit is not
	// old is *RefConflict. Every write takes a new generation.
	UpdateRef(ctx context.Context, repo, name, old, new string) (Ref, error)

	// MergeRef is SwapRef without a workspace fence: it needs only the
	// stored generation to be expect. The event kind is WriterMerge.
	MergeRef(ctx context.Context, repo, name string, expect int64, sha string) (Ref, error)
	// MergeDeleteRef is DeleteRef without a workspace fence.
	MergeDeleteRef(ctx context.Context, repo, name string, expect int64) error

	// ProtectedRefs returns the protected ref patterns of repo, empty when none.
	ProtectedRefs(ctx context.Context, repo string) ([]string, error)
	// SetProtectedRefs replaces the protected ref patterns of repo.
	SetProtectedRefs(ctx context.Context, repo string, patterns []string) error

	// Events lists at most limit events with Seq above after, oldest first.
	// ErrEventsPruned when after is below the last pruned Seq.
	Events(ctx context.Context, after int64, limit int) ([]Event, error)
	// PruneEvents removes the events written before the time and records the
	// highest removed Seq.
	PruneEvents(ctx context.Context, before time.Time) error

	// CreateSubscription stores s with its cursor at the newest event.
	// ErrExists when the id exists. The returned value has Cursor and CreatedAt.
	CreateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	// Subscriptions lists every subscription, oldest first, ties by id.
	Subscriptions(ctx context.Context) ([]Subscription, error)
	// DeleteSubscription removes a subscription. ErrNotFound when it is absent.
	DeleteSubscription(ctx context.Context, id string) error
	// AcquireLease gives owner the lease of subscription id until the
	// time until, when no owner holds it at now or owner holds it. It returns
	// the subscription. ErrNotFound, ErrLeaseHeld.
	AcquireLease(ctx context.Context, id, owner string, now, until time.Time) (Subscription, error)
	// AdvanceCursor raises the cursor of subscription id to cursor while
	// owner holds the lease. A lower cursor changes nothing. ErrNotFound, ErrLeaseHeld.
	AdvanceCursor(ctx context.Context, id, owner string, cursor int64) error

	// AddPack records a pack once. A repeat is a no-op.
	AddPack(ctx context.Context, repo, packID string, bytes int64) error
	// Packs lists the packs of a repo, oldest first, ties by id.
	Packs(ctx context.Context, repo string) ([]Pack, error)
}

// Fence is the state of a workspace row that every fenced write checks. A
// backend reads it after a conditional write changed nothing, and these
// methods pick the error, so every backend gives the same precedence.
type Fence struct {
	Epoch   uint64
	Repo    string
	Deleted bool
}

// Live is the answer of Store.Live.
func (f Fence) Live(epoch uint64, repo string) error {
	switch {
	case f.Deleted:
		return ErrGone
	case f.Repo != repo:
		return ErrWrongRepo
	case f.Epoch != epoch:
		return ErrClaimLost
	}
	return nil
}

// WriteLost is the error of a write that changed nothing: ErrClaimLost when
// the fence no longer holds, and nil when the write lost on its own
// condition. A missing workspace also fails the fence.
func (f Fence) WriteLost(epoch uint64, repo string) error {
	if f.Live(epoch, repo) != nil {
		return ErrClaimLost
	}
	return nil
}

// FlushLost is the error of a flush that changed nothing.
func (f Fence) FlushLost(epoch uint64) error {
	switch {
	case f.Deleted:
		return ErrGone
	case f.Epoch != epoch:
		return ErrClaimLost
	}
	return nil
}
