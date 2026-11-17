package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/store"
)

var (
	ErrNotFound = errors.New("pg: not found")
	// ErrGone is a workspace that was deleted.
	ErrGone = errors.New("pg: workspace deleted")
	// ErrClaimLost is a write or claim whose epoch is not the stored one.
	ErrClaimLost = errors.New("pg: claim lost")
	// ErrWorkspaceConflict is a claim whose owner or repo differs from the stored one.
	ErrWorkspaceConflict = errors.New("pg: workspace owner or repo differs")
	// ErrWrongRepo is a grant whose repo is not the repo of the workspace.
	ErrWrongRepo = errors.New("pg: workspace is on another repo")
)

// RefConflict is a lost ref compare-and-swap. Current is nil when the ref is absent.
type RefConflict struct{ Current *api.Ref }

func (*RefConflict) Error() string { return "pg: ref generation conflict" }

// OpHeadsConflict is a lost op-heads compare-and-swap. Current is nil when no row exists.
type OpHeadsConflict struct{ Current *api.OpHeads }

func (*OpHeadsConflict) Error() string { return "pg: op heads generation conflict" }

type DB struct{ Pool *pgxpool.Pool }

type Repo struct {
	Path, Name, Owner string
	CreatedAt         time.Time
}

type Workspace struct {
	Epoch    uint64
	Op       string
	Snapshot string
}

// CreateRepo stores r and its ref main at commit in one transaction.
func (d *DB) CreateRepo(ctx context.Context, r Repo, repoText, commit string) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: create repo: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO repos (path, name, owner) VALUES ($1, $2, $3)`, r.Path, r.Name, r.Owner); err != nil {
		return fmt.Errorf("pg: create repo: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO refs (repo, name, commit_sha, generation) VALUES ($1, 'main', $2, nextval('ref_generation'))`, repoText, commit); err != nil {
		return fmt.Errorf("pg: create repo ref: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: create repo: %w", err)
	}
	return nil
}

func (d *DB) Repo(ctx context.Context, path string) (Repo, error) {
	var r Repo
	err := d.Pool.QueryRow(ctx, `SELECT path, name, owner, created_at FROM repos WHERE path = $1`, path).
		Scan(&r.Path, &r.Name, &r.Owner, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Repo{}, ErrNotFound
	}
	if err != nil {
		return Repo{}, fmt.Errorf("pg: read repo: %w", err)
	}
	return r, nil
}

// Repos lists every repository, newest first.
func (d *DB) Repos(ctx context.Context) ([]Repo, error) {
	rows, err := d.Pool.Query(ctx, `SELECT path, name, owner, created_at FROM repos ORDER BY created_at DESC, path DESC`)
	if err != nil {
		return nil, fmt.Errorf("pg: list repos: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Repo])
	if err != nil {
		return nil, fmt.Errorf("pg: list repos: %w", err)
	}
	return out, nil
}

// Claim raises the epoch of workspace id to epoch, or creates it. The stored
// epoch may be higher: the caller compares it with its own.
func (d *DB) Claim(ctx context.Context, id string, epoch uint64, owner, repo string) (Workspace, error) {
	var w Workspace
	var stored int64
	err := d.Pool.QueryRow(ctx, `
		INSERT INTO workspaces (id, repo, owner, epoch) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET epoch = GREATEST(workspaces.epoch, EXCLUDED.epoch)
		WHERE workspaces.deleted_at IS NULL AND workspaces.owner = EXCLUDED.owner AND workspaces.repo = EXCLUDED.repo
		RETURNING epoch, op, snapshot`, id, repo, owner, int64(epoch)).Scan(&stored, &w.Op, &w.Snapshot)
	if err == nil {
		w.Epoch = uint64(stored)
		return w, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Workspace{}, fmt.Errorf("pg: claim: %w", err)
	}
	var deleted bool
	if err := d.Pool.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM workspaces WHERE id = $1`, id).Scan(&deleted); err != nil {
		return Workspace{}, fmt.Errorf("pg: claim: %w", err)
	}
	if deleted {
		return Workspace{}, ErrGone
	}
	return Workspace{}, ErrWorkspaceConflict
}

// Live checks that workspace id is on repo at epoch and not deleted.
func (d *DB) Live(ctx context.Context, id string, epoch uint64, repo string) error {
	var stored int64
	var curRepo string
	var deleted bool
	err := d.Pool.QueryRow(ctx, `SELECT epoch, repo, deleted_at IS NOT NULL FROM workspaces WHERE id = $1`, id).Scan(&stored, &curRepo, &deleted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotFound
	case err != nil:
		return fmt.Errorf("pg: read workspace: %w", err)
	case deleted:
		return ErrGone
	case curRepo != repo:
		return ErrWrongRepo
	case uint64(stored) != epoch:
		return ErrClaimLost
	}
	return nil
}

// lost reports a failed fenced write as ErrClaimLost when the fence no longer holds. It returns nil when the fence
// holds, so the write lost on its own condition.
func (d *DB) lost(ctx context.Context, ws string, epoch uint64, repo string) error {
	err := d.Live(ctx, ws, epoch, repo)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrGone), errors.Is(err, ErrWrongRepo), errors.Is(err, ErrClaimLost):
		return ErrClaimLost
	}
	return err
}

// Delete marks a workspace deleted. It is idempotent, and a missing workspace is not an error.
func (d *DB) Delete(ctx context.Context, id string) error {
	if _, err := d.Pool.Exec(ctx, `UPDATE workspaces SET deleted_at = COALESCE(deleted_at, now()) WHERE id = $1`, id); err != nil {
		return fmt.Errorf("pg: delete workspace: %w", err)
	}
	return nil
}

// guard is the fence of every write. $ws, $epoch and $repo are the parameters at the given positions.
const guard = `SELECT id FROM workspaces WHERE id = $%d AND epoch = $%d AND repo = $%d AND deleted_at IS NULL FOR SHARE`

// Report stores the op and snapshot of a workspace report or flush. generation 0 writes only while the stored op is
// prev. A lower generation than the stored one is dropped without an error.
func (d *DB) Report(ctx context.Context, ws string, epoch uint64, repo string, generation int64, prev, op, snapshot string) error {
	tag, err := d.Pool.Exec(ctx, fmt.Sprintf(`
		WITH guard AS (`+guard+`)
		UPDATE workspaces w SET op = $4, snapshot = $5,
			report_generation = COALESCE(NULLIF($6::bigint, 0), w.report_generation)
		FROM guard
		WHERE w.id = guard.id AND CASE WHEN $6::bigint > 0
			THEN (w.report_generation IS NULL OR w.report_generation <= $6::bigint)
			ELSE w.op = $7 END`, 1, 2, 3), ws, int64(epoch), repo, op, snapshot, generation, prev)
	if err != nil {
		return fmt.Errorf("pg: report workspace: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return d.lost(ctx, ws, epoch, repo)
}

// Flush is Report for the control plane: the repo comes from the workspace row, which never changes once set.
func (d *DB) Flush(ctx context.Context, ws string, epoch uint64, generation int64, prev, op, snapshot string) error {
	var repo string
	var deleted bool
	err := d.Pool.QueryRow(ctx, `SELECT repo, deleted_at IS NOT NULL FROM workspaces WHERE id = $1`, ws).Scan(&repo, &deleted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotFound
	case err != nil:
		return fmt.Errorf("pg: flush workspace: %w", err)
	case deleted:
		return ErrGone
	}
	return d.Report(ctx, ws, epoch, repo, generation, prev, op, snapshot)
}

func (d *DB) OpHeads(ctx context.Context, ws string) (api.OpHeads, error) {
	var h api.OpHeads
	var epoch int64
	err := d.Pool.QueryRow(ctx, `SELECT epoch, generation, heads FROM op_heads WHERE workspace_id = $1`, ws).Scan(&epoch, &h.Generation, &h.Heads)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.OpHeads{}, ErrNotFound
	}
	if err != nil {
		return api.OpHeads{}, fmt.Errorf("pg: read op heads: %w", err)
	}
	h.Epoch = uint64(epoch)
	return h, nil
}

// SwapOpHeads replaces the head set when the stored generation is expect (0
// for the first write) and the fence holds. A lost swap returns
// *OpHeadsConflict, or ErrClaimLost when the fence fails first.
func (d *DB) SwapOpHeads(ctx context.Context, ws string, epoch uint64, repo string, expect int64, heads []string) (api.OpHeads, error) {
	var generation int64
	var err error
	if expect == 0 {
		err = d.Pool.QueryRow(ctx, `
			INSERT INTO op_heads (workspace_id, epoch, generation, heads)
			SELECT id, $2, 1, $4 FROM workspaces WHERE id = $1 AND epoch = $2 AND repo = $3 AND deleted_at IS NULL FOR SHARE
			ON CONFLICT (workspace_id) DO NOTHING
			RETURNING generation`, ws, int64(epoch), repo, heads).Scan(&generation)
	} else {
		err = d.Pool.QueryRow(ctx, `
			WITH guard AS (`+fmt.Sprintf(guard, 1, 2, 3)+`)
			UPDATE op_heads h SET epoch = $2, generation = h.generation + 1, heads = $5, updated_at = now()
			FROM guard
			WHERE h.workspace_id = guard.id AND h.generation = $4 AND h.epoch <= $2
			RETURNING h.generation`, ws, int64(epoch), repo, expect, heads).Scan(&generation)
	}
	if err == nil {
		return api.OpHeads{Epoch: epoch, Generation: generation, Heads: heads}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return api.OpHeads{}, fmt.Errorf("pg: swap op heads: %w", err)
	}
	if err := d.lost(ctx, ws, epoch, repo); err != nil {
		return api.OpHeads{}, err
	}
	cur, err := d.OpHeads(ctx, ws)
	if errors.Is(err, ErrNotFound) {
		return api.OpHeads{}, &OpHeadsConflict{}
	}
	if err != nil {
		return api.OpHeads{}, err
	}
	if cur.Epoch > epoch {
		return api.OpHeads{}, ErrClaimLost
	}
	return api.OpHeads{}, &OpHeadsConflict{Current: &cur}
}

// AddPack records a pack once. A repeat is a no-op.
func (d *DB) AddPack(ctx context.Context, repo store.Repo, packID string, bytes int64) error {
	if _, err := d.Pool.Exec(ctx, `INSERT INTO packs (repo, pack_id, bytes) VALUES ($1, $2, $3) ON CONFLICT (repo, pack_id) DO NOTHING`, repo.String(), packID, bytes); err != nil {
		return fmt.Errorf("pg: add pack: %w", err)
	}
	return nil
}

// Packs lists the packs of a repo, oldest first.
func (d *DB) Packs(ctx context.Context, repo store.Repo) ([]store.PackRef, error) {
	rows, err := d.Pool.Query(ctx, `SELECT pack_id, bytes FROM packs WHERE repo = $1 ORDER BY created_at, pack_id`, repo.String())
	if err != nil {
		return nil, fmt.Errorf("pg: list packs: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[store.PackRef])
	if err != nil {
		return nil, fmt.Errorf("pg: list packs: %w", err)
	}
	return out, nil
}

func (d *DB) Ref(ctx context.Context, repo, name string) (api.Ref, error) {
	r := api.Ref{Name: name}
	err := d.Pool.QueryRow(ctx, `SELECT commit_sha, generation FROM refs WHERE repo = $1 AND name = $2`, repo, name).Scan(&r.Commit, &r.Generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.Ref{}, ErrNotFound
	}
	if err != nil {
		return api.Ref{}, fmt.Errorf("pg: read ref: %w", err)
	}
	return r, nil
}

// Refs lists the refs of a repo by name.
func (d *DB) Refs(ctx context.Context, repo string) ([]api.Ref, error) {
	rows, err := d.Pool.Query(ctx, `SELECT name, commit_sha, generation FROM refs WHERE repo = $1 ORDER BY name`, repo)
	if err != nil {
		return nil, fmt.Errorf("pg: list refs: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[api.Ref])
	if err != nil {
		return nil, fmt.Errorf("pg: list refs: %w", err)
	}
	return out, nil
}

// SwapRef moves a ref to sha when its stored generation is expect (0 creates
// the ref) and the fence holds. Every create and update takes a new
// generation from one sequence, so a deleted and recreated ref never repeats
// one. ErrClaimLost wins over *RefConflict.
func (d *DB) SwapRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64, sha string) (api.Ref, error) {
	var generation int64
	var err error
	if expect == 0 {
		err = d.Pool.QueryRow(ctx, `
			INSERT INTO refs (repo, name, commit_sha, generation)
			SELECT $3, $4, $5, nextval('ref_generation') FROM workspaces
			WHERE id = $1 AND epoch = $2 AND repo = $3 AND deleted_at IS NULL FOR SHARE
			ON CONFLICT (repo, name) DO NOTHING
			RETURNING generation`, ws, int64(epoch), repo, name, sha).Scan(&generation)
	} else {
		err = d.Pool.QueryRow(ctx, `
			WITH guard AS (`+fmt.Sprintf(guard, 1, 2, 3)+`)
			UPDATE refs SET commit_sha = $5, generation = nextval('ref_generation'), updated_at = now()
			FROM guard
			WHERE refs.repo = $3 AND refs.name = $4 AND refs.generation = $6
			RETURNING generation`, ws, int64(epoch), repo, name, sha, expect).Scan(&generation)
	}
	if err == nil {
		return api.Ref{Name: name, Commit: sha, Generation: generation}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return api.Ref{}, fmt.Errorf("pg: swap ref: %w", err)
	}
	return api.Ref{}, d.refLost(ctx, ws, epoch, repo, name)
}

// DeleteRef removes a ref when its stored generation is expect and the fence holds.
func (d *DB) DeleteRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64) error {
	tag, err := d.Pool.Exec(ctx, `
		WITH guard AS (`+fmt.Sprintf(guard, 1, 2, 3)+`)
		DELETE FROM refs USING guard
		WHERE refs.repo = $3 AND refs.name = $4 AND refs.generation = $5`, ws, int64(epoch), repo, name, expect)
	if err != nil {
		return fmt.Errorf("pg: delete ref: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return d.refLost(ctx, ws, epoch, repo, name)
}

func (d *DB) refLost(ctx context.Context, ws string, epoch uint64, repo, name string) error {
	if err := d.lost(ctx, ws, epoch, repo); err != nil {
		return err
	}
	cur, err := d.Ref(ctx, repo, name)
	if errors.Is(err, ErrNotFound) {
		return &RefConflict{}
	}
	if err != nil {
		return err
	}
	return &RefConflict{Current: &cur}
}
