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
	"hash/fnv"
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
	// ErrManifestStale is a manifest write whose expected generation is not the stored one.
	ErrManifestStale = errors.New("meta: manifest generation conflict")
	// ErrLeaseHeld is a lease or cursor write by an owner that does not hold the lease.
	ErrLeaseHeld = errors.New("meta: lease held by another owner")
)

// The kinds of writer that an Event records. Each is the scope of the grant
// that wrote the ref.
const (
	WriterNode     = "node"
	WriterMerge    = "merge"
	WriterWrite    = "write"
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

// Partitions is the number of event partitions of a store. The partition of a
// ref is fixed by its repo and name, so every event of one ref is in one
// partition and in order. Changing the number is a migration.
const Partitions = 64

// Partition returns the event partition of a ref: the FNV-1a hash of the repo,
// a zero byte and the name, modulo Partitions.
func Partition(repo, ref string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(repo + "\x00" + ref))
	return int(h.Sum32() % Partitions)
}

// Event is one change of a ref. Its identity is Partition and Seq. Seq starts
// at 1 in each partition and has no gap there, and it follows commit order
// within the partition. No order holds across partitions. Old is empty for a
// create, and New is empty for a delete. For a delete Generation is the
// generation that the ref had. Kind is a Writer constant.
type Event struct {
	Partition  int
	Seq        int64
	Repo, Ref  string
	Old, New   string
	Generation int64
	Kind       string
	At         time.Time
}

// Subscription is a registered receiver. Cursors holds one cursor for each
// partition: the Seq of the last event of that partition that the receiver
// acknowledged. Repo is empty for every repo.
type Subscription struct {
	ID, URL, Secret, Repo string
	CreatedAt             time.Time
	Cursors               []int64
}

// Lease is the right to deliver one partition of a subscription, with the cursor of that partition.
type Lease struct {
	ID, URL, Secret, Repo string
	Partition             int
	Cursor                int64
}

// Pack is a pack that a repository holds.
type Pack struct {
	ID    string
	Bytes int64
}

// Retired is a file of a repo that no manifest needs. Key is relative to the prefix of the repo.
type Retired struct {
	Key string
	At  time.Time
	// Deleting is set by BeginDelete: a collector has taken the file and is deleting it.
	Deleting   bool
	DeletingAt time.Time
}

// Manifest is the stored pack manifest of a repo. The store keeps Body as opaque bytes. Generation starts at 1.
type Manifest struct {
	Generation int64
	Body       []byte
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
	// old is *RefConflict. Every write takes a new generation and appends one
	// event of the writer kind, WriterWrite or WriterMerge.
	UpdateRef(ctx context.Context, repo, name, old, new, kind string) (Ref, error)

	// MergeRef is SwapRef without a workspace fence: it needs only the
	// stored generation to be expect. The event kind is WriterMerge.
	MergeRef(ctx context.Context, repo, name string, expect int64, sha string) (Ref, error)
	// MergeDeleteRef is DeleteRef without a workspace fence.
	MergeDeleteRef(ctx context.Context, repo, name string, expect int64) error

	// ProtectedRefs returns the protected ref patterns of repo, empty when none.
	ProtectedRefs(ctx context.Context, repo string) ([]string, error)
	// SetProtectedRefs replaces the protected ref patterns of repo.
	SetProtectedRefs(ctx context.Context, repo string, patterns []string) error

	// Events lists at most limit events of one partition with Seq above
	// after, oldest first. ErrEventsPruned when after is below the last
	// pruned Seq of the partition.
	Events(ctx context.Context, partition int, after int64, limit int) ([]Event, error)
	// EventHeads returns the newest Seq of each partition, 0 for none.
	EventHeads(ctx context.Context) ([]int64, error)
	// PruneEvents removes the events written before the time and records the
	// highest removed Seq of each partition.
	PruneEvents(ctx context.Context, before time.Time) error

	// CreateSubscription stores s with each cursor at the newest Seq of its
	// partition. ErrExists when the id exists. The returned value has
	// Cursors and CreatedAt.
	CreateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	// UpdateSubscription replaces the URL, the secret and the repo of subscription id and keeps its cursors and
	// CreatedAt. ErrNotFound when it is absent.
	UpdateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	// Subscriptions lists every subscription, oldest first, ties by id.
	Subscriptions(ctx context.Context) ([]Subscription, error)
	// DeleteSubscription removes a subscription and its cursors. ErrNotFound when it is absent.
	DeleteSubscription(ctx context.Context, id string) error
	// AcquireLease gives owner the lease of one partition of subscription id
	// until the time until, when no owner holds it at now or owner holds
	// it. It returns the lease with the cursor. ErrNotFound, ErrLeaseHeld.
	AcquireLease(ctx context.Context, id string, partition int, owner string, now, until time.Time) (Lease, error)
	// AdvanceCursor raises the cursor of one partition of subscription id to
	// cursor while owner holds its lease. A lower cursor changes nothing.
	// ErrNotFound, ErrLeaseHeld.
	AdvanceCursor(ctx context.Context, id string, partition int, owner string, cursor int64) error

	LandStore
	UserTokenStore

	// AddPack records a pack once. A repeat is a no-op.
	AddPack(ctx context.Context, repo, packID string, bytes int64) error
	// Packs lists the packs of a repo, oldest first, ties by id.
	Packs(ctx context.Context, repo string) ([]Pack, error)
	// RemovePack forgets a pack of a repo. A missing pack is not an error.
	RemovePack(ctx context.Context, repo, packID string) error
	// Manifest returns the manifest of a repo, or ErrNotFound.
	Manifest(ctx context.Context, repo string) (Manifest, error)
	// SwapManifest stores body as the manifest of repo when the stored generation is expect (0 when none is stored),
	// and returns the new generation. A lost swap is ErrManifestStale.
	SwapManifest(ctx context.Context, repo string, expect int64, body []byte) (int64, error)
	// RetireFiles records keys as retired at at. A key that is already recorded and not deleting takes the new time.
	RetireFiles(ctx context.Context, repo string, keys []string, at time.Time) error
	// RetiredFiles lists the retired files of a repo, oldest first, then by key.
	RetiredFiles(ctx context.Context, repo string) ([]Retired, error)
	// BeginDelete marks each of keys as deleting at now, and returns the keys that it marked. It skips a key that is
	// deleting since after staleBefore, which another collector owns, and a key retired after retiredBefore, which a writer used and retired again. Of concurrent callers, exactly one marks a key.
	BeginDelete(ctx context.Context, repo string, keys []string, now, staleBefore, retiredBefore time.Time) ([]string, error)
	// ClaimRetired removes each of keys that is retired and not deleting, so no collector takes it, and reports whether
	// any of keys is deleting. A writer that stores a file again calls it first, and waits while it reports true.
	ClaimRetired(ctx context.Context, repo string, keys []string) (deleting bool, err error)
	// ForgetRetired removes keys from the retired files of a repo. A missing key is not an error.
	ForgetRetired(ctx context.Context, repo string, keys []string) error
	// PackRepos lists the text of every repo that holds a pack, in byte order.
	PackRepos(ctx context.Context) ([]string, error)
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
