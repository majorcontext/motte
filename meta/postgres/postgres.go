// Package postgres is the PostgreSQL backend of meta.Store.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/meta"
)

// Store keeps the metadata in the schema of Pool.
type Store struct{ Pool *pgxpool.Pool }

var _ meta.Store = (*Store)(nil)

// Open connects to the database at dsn and applies the migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// Ping checks that the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *Store) CreateRepo(ctx context.Context, r meta.Repo, repoText, commit string) error {
	wrote, err := s.withEvent(ctx, meta.Partition(repoText, "main"), func(tx pgx.Tx, seq int64) (bool, error) {
		tag, err := tx.Exec(ctx, `
			WITH created AS (
				INSERT INTO repos (path, name, owner) VALUES ($1, $2, $3) ON CONFLICT (path) DO NOTHING RETURNING path),
			made AS (
				INSERT INTO refs (repo, name, commit_sha, generation)
				SELECT $4::text, 'main', $5::text, nextval('ref_generation') FROM created RETURNING generation)
			INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
			SELECT $8::int, $6::bigint, $4::text, 'main', '', $5::text, made.generation, $7::text FROM made`,
			r.Path, r.Name, r.Owner, repoText, commit, seq, meta.WriterOperator, meta.Partition(repoText, "main"))
		return tag.RowsAffected() == 1, err
	})
	if uniqueViolation(err) || err == nil && !wrote {
		return meta.ErrExists
	}
	if err != nil {
		return fmt.Errorf("postgres: create repo: %w", err)
	}
	return nil
}

func (s *Store) Repo(ctx context.Context, path string) (meta.Repo, error) {
	var r meta.Repo
	err := s.Pool.QueryRow(ctx, `SELECT path, name, owner, created_at FROM repos WHERE path = $1`, path).
		Scan(&r.Path, &r.Name, &r.Owner, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Repo{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Repo{}, fmt.Errorf("postgres: read repo: %w", err)
	}
	return r, nil
}

func (s *Store) Repos(ctx context.Context) ([]meta.Repo, error) {
	rows, err := s.Pool.Query(ctx, `SELECT path, name, owner, created_at FROM repos ORDER BY created_at DESC, path COLLATE "C" DESC`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list repos: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Repo])
	if err != nil {
		return nil, fmt.Errorf("postgres: list repos: %w", err)
	}
	return out, nil
}

func (s *Store) Claim(ctx context.Context, id string, epoch uint64, owner, repo string) (meta.Workspace, error) {
	var w meta.Workspace
	var stored int64
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO workspaces (id, repo, owner, epoch) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET epoch = GREATEST(workspaces.epoch, EXCLUDED.epoch)
		WHERE workspaces.deleted_at IS NULL AND workspaces.owner = EXCLUDED.owner AND workspaces.repo = EXCLUDED.repo
		RETURNING epoch, op, snapshot`, id, repo, owner, int64(epoch)).Scan(&stored, &w.Op, &w.Snapshot)
	if err == nil {
		w.Epoch = uint64(stored)
		return w, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return meta.Workspace{}, fmt.Errorf("postgres: claim: %w", err)
	}
	f, err := s.fence(ctx, id)
	if err != nil {
		return meta.Workspace{}, err
	}
	if f.Deleted {
		return meta.Workspace{}, meta.ErrGone
	}
	return meta.Workspace{}, meta.ErrWorkspaceConflict
}

func (s *Store) fence(ctx context.Context, id string) (meta.Fence, error) {
	var f meta.Fence
	var epoch int64
	err := s.Pool.QueryRow(ctx, `SELECT epoch, repo, deleted_at IS NOT NULL FROM workspaces WHERE id = $1`, id).Scan(&epoch, &f.Repo, &f.Deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Fence{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Fence{}, fmt.Errorf("postgres: read workspace: %w", err)
	}
	f.Epoch = uint64(epoch)
	return f, nil
}

func (s *Store) Live(ctx context.Context, id string, epoch uint64, repo string) error {
	f, err := s.fence(ctx, id)
	if err != nil {
		return err
	}
	return f.Live(epoch, repo)
}

// lost reports a failed fenced write as ErrClaimLost when the fence no longer holds. It returns nil when the fence
// holds, so the write lost on its own condition.
func (s *Store) lost(ctx context.Context, ws string, epoch uint64, repo string) error {
	f, err := s.fence(ctx, ws)
	if errors.Is(err, meta.ErrNotFound) {
		return meta.ErrClaimLost
	}
	if err != nil {
		return err
	}
	return f.WriteLost(epoch, repo)
}

func (s *Store) Delete(ctx context.Context, id string) error {
	if _, err := s.Pool.Exec(ctx, `UPDATE workspaces SET deleted_at = COALESCE(deleted_at, now()) WHERE id = $1`, id); err != nil {
		return fmt.Errorf("postgres: delete workspace: %w", err)
	}
	return nil
}

// guard is the fence of every write. $ws, $epoch and $repo are the parameters at the given positions.
const guard = `SELECT id FROM workspaces WHERE id = $%d AND epoch = $%d AND repo = $%d AND deleted_at IS NULL FOR SHARE`

// report is one UPDATE that takes the row lock itself: a FOR SHARE guard here would deadlock two concurrent reports.
// A NULL repo matches the stored one.
func (s *Store) report(ctx context.Context, ws string, epoch uint64, repo any, generation int64, prev, op, snapshot string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE workspaces SET op = $4, snapshot = $5,
			report_generation = COALESCE(NULLIF($6::bigint, 0), report_generation)
		WHERE id = $1 AND epoch = $2 AND ($3::text IS NULL OR repo = $3) AND deleted_at IS NULL AND CASE WHEN $6::bigint > 0
			THEN (report_generation IS NULL OR report_generation <= $6::bigint)
			ELSE op = $7 END`, ws, int64(epoch), repo, op, snapshot, generation, prev)
	if err != nil {
		return false, fmt.Errorf("postgres: report workspace: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) Report(ctx context.Context, ws string, epoch uint64, repo string, generation int64, prev, op, snapshot string) error {
	if wrote, err := s.report(ctx, ws, epoch, repo, generation, prev, op, snapshot); wrote || err != nil {
		return err
	}
	return s.lost(ctx, ws, epoch, repo)
}

func (s *Store) Flush(ctx context.Context, ws string, epoch uint64, generation int64, prev, op, snapshot string) error {
	if wrote, err := s.report(ctx, ws, epoch, nil, generation, prev, op, snapshot); wrote || err != nil {
		return err
	}
	f, err := s.fence(ctx, ws)
	if err != nil {
		return err
	}
	return f.FlushLost(epoch)
}

func (s *Store) OpHeads(ctx context.Context, ws string) (meta.OpHeads, error) {
	var h meta.OpHeads
	var epoch int64
	err := s.Pool.QueryRow(ctx, `SELECT epoch, generation, heads FROM op_heads WHERE workspace_id = $1`, ws).Scan(&epoch, &h.Generation, &h.Heads)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.OpHeads{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.OpHeads{}, fmt.Errorf("postgres: read op heads: %w", err)
	}
	h.Epoch = uint64(epoch)
	return h, nil
}

func (s *Store) SwapOpHeads(ctx context.Context, ws string, epoch uint64, repo string, expect int64, heads []string) (meta.OpHeads, error) {
	var generation int64
	var err error
	if expect == 0 {
		err = s.Pool.QueryRow(ctx, `
			INSERT INTO op_heads (workspace_id, epoch, generation, heads)
			SELECT id, $2, 1, $4 FROM workspaces WHERE id = $1 AND epoch = $2 AND repo = $3 AND deleted_at IS NULL FOR SHARE
			ON CONFLICT (workspace_id) DO NOTHING
			RETURNING generation`, ws, int64(epoch), repo, heads).Scan(&generation)
	} else {
		err = s.Pool.QueryRow(ctx, `
			WITH guard AS (`+fmt.Sprintf(guard, 1, 2, 3)+`)
			UPDATE op_heads h SET epoch = $2, generation = h.generation + 1, heads = $5, updated_at = now()
			FROM guard
			WHERE h.workspace_id = guard.id AND h.generation = $4 AND h.epoch <= $2
			RETURNING h.generation`, ws, int64(epoch), repo, expect, heads).Scan(&generation)
	}
	if err == nil {
		return meta.OpHeads{Epoch: epoch, Generation: generation, Heads: heads}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return meta.OpHeads{}, fmt.Errorf("postgres: swap op heads: %w", err)
	}
	if err := s.lost(ctx, ws, epoch, repo); err != nil {
		return meta.OpHeads{}, err
	}
	cur, err := s.OpHeads(ctx, ws)
	if errors.Is(err, meta.ErrNotFound) {
		return meta.OpHeads{}, &meta.OpHeadsConflict{}
	}
	if err != nil {
		return meta.OpHeads{}, err
	}
	if cur.Epoch > epoch {
		return meta.OpHeads{}, meta.ErrClaimLost
	}
	return meta.OpHeads{}, &meta.OpHeadsConflict{Current: &cur}
}

func (s *Store) AddPack(ctx context.Context, repo, packID string, bytes int64) error {
	if _, err := s.Pool.Exec(ctx, `INSERT INTO packs (repo, pack_id, bytes) VALUES ($1, $2, $3) ON CONFLICT (repo, pack_id) DO NOTHING`, repo, packID, bytes); err != nil {
		return fmt.Errorf("postgres: add pack: %w", err)
	}
	return nil
}

func (s *Store) Packs(ctx context.Context, repo string) ([]meta.Pack, error) {
	rows, err := s.Pool.Query(ctx, `SELECT pack_id, bytes FROM packs WHERE repo = $1 ORDER BY created_at, pack_id COLLATE "C"`, repo)
	if err != nil {
		return nil, fmt.Errorf("postgres: list packs: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Pack])
	if err != nil {
		return nil, fmt.Errorf("postgres: list packs: %w", err)
	}
	return out, nil
}

func (s *Store) RemovePack(ctx context.Context, repo, packID string) error {
	if _, err := s.Pool.Exec(ctx, `DELETE FROM packs WHERE repo = $1 AND pack_id = $2`, repo, packID); err != nil {
		return fmt.Errorf("postgres: remove pack: %w", err)
	}
	return nil
}

func (s *Store) Manifest(ctx context.Context, repo string) (meta.Manifest, error) {
	var m meta.Manifest
	err := s.Pool.QueryRow(ctx, `SELECT generation, body FROM pack_manifests WHERE repo = $1`, repo).Scan(&m.Generation, &m.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Manifest{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Manifest{}, fmt.Errorf("postgres: read manifest: %w", err)
	}
	return m, nil
}

func (s *Store) SwapManifest(ctx context.Context, repo string, expect int64, body []byte) (int64, error) {
	var tag pgconn.CommandTag
	var err error
	if expect == 0 {
		tag, err = s.Pool.Exec(ctx, `INSERT INTO pack_manifests (repo, generation, body) VALUES ($1, 1, $2) ON CONFLICT (repo) DO NOTHING`, repo, body)
	} else {
		tag, err = s.Pool.Exec(ctx, `UPDATE pack_manifests SET generation = generation + 1, body = $3, updated_at = now() WHERE repo = $1 AND generation = $2`, repo, expect, body)
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: swap manifest: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, meta.ErrManifestStale
	}
	return expect + 1, nil
}

func (s *Store) RetireFiles(ctx context.Context, repo string, keys []string, at time.Time) error {
	if _, err := s.Pool.Exec(ctx, `INSERT INTO pack_retired (repo, key, retired_at) SELECT $1, k, $3 FROM (SELECT DISTINCT k FROM unnest($2::text[]) AS k) AS d ON CONFLICT (repo, key) DO UPDATE SET retired_at = EXCLUDED.retired_at WHERE pack_retired.deleting_at IS NULL`, repo, keys, at); err != nil {
		return fmt.Errorf("postgres: retire files: %w", err)
	}
	return nil
}

func (s *Store) RetiredFiles(ctx context.Context, repo string) ([]meta.Retired, error) {
	rows, err := s.Pool.Query(ctx, `SELECT key, retired_at, deleting_at FROM pack_retired WHERE repo = $1 ORDER BY retired_at, key COLLATE "C"`, repo)
	if err != nil {
		return nil, fmt.Errorf("postgres: list retired files: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (meta.Retired, error) {
		var x meta.Retired
		var at *time.Time
		if err := r.Scan(&x.Key, &x.At, &at); err != nil {
			return x, err
		}
		if at != nil {
			x.Deleting, x.DeletingAt = true, *at
		}
		return x, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: list retired files: %w", err)
	}
	return out, nil
}

func (s *Store) BeginDelete(ctx context.Context, repo string, keys []string, now, staleBefore, retiredBefore time.Time) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `UPDATE pack_retired SET deleting_at = $3 WHERE repo = $1 AND key = ANY($2) AND retired_at <= $5 AND (deleting_at IS NULL OR deleting_at <= $4) RETURNING key`, repo, keys, now, staleBefore, retiredBefore)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin delete: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("postgres: begin delete: %w", err)
	}
	return out, nil
}

// ClaimRetired locks the rows first, so a mark that is not yet committed is seen: the lock waits for it, and the
// statements after it read the row as it is.
func (s *Store) ClaimRetired(ctx context.Context, repo string, keys []string) (bool, error) {
	var deleting bool
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT deleting_at IS NOT NULL FROM pack_retired WHERE repo = $1 AND key = ANY($2) FOR UPDATE`, repo, keys)
		if err != nil {
			return err
		}
		marks, err := pgx.CollectRows(rows, pgx.RowTo[bool])
		if err != nil {
			return err
		}
		for _, m := range marks {
			deleting = deleting || m
		}
		_, err = tx.Exec(ctx, `DELETE FROM pack_retired WHERE repo = $1 AND key = ANY($2) AND deleting_at IS NULL`, repo, keys)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("postgres: claim retired: %w", err)
	}
	return deleting, nil
}

func (s *Store) ForgetRetired(ctx context.Context, repo string, keys []string) error {
	if _, err := s.Pool.Exec(ctx, `DELETE FROM pack_retired WHERE repo = $1 AND key = ANY($2)`, repo, keys); err != nil {
		return fmt.Errorf("postgres: forget retired files: %w", err)
	}
	return nil
}

func (s *Store) PackRepos(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT repo FROM packs GROUP BY repo ORDER BY repo COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list pack repos: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("postgres: list pack repos: %w", err)
	}
	return out, nil
}

func (s *Store) Ref(ctx context.Context, repo, name string) (meta.Ref, error) {
	r := meta.Ref{Name: name}
	err := s.Pool.QueryRow(ctx, `SELECT commit_sha, generation FROM refs WHERE repo = $1 AND name = $2`, repo, name).Scan(&r.Commit, &r.Generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Ref{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Ref{}, fmt.Errorf("postgres: read ref: %w", err)
	}
	return r, nil
}

func (s *Store) Refs(ctx context.Context, repo string) ([]meta.Ref, error) {
	rows, err := s.Pool.Query(ctx, `SELECT name, commit_sha, generation FROM refs WHERE repo = $1 ORDER BY name COLLATE "C"`, repo)
	if err != nil {
		return nil, fmt.Errorf("postgres: list refs: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meta.Ref])
	if err != nil {
		return nil, fmt.Errorf("postgres: list refs: %w", err)
	}
	return out, nil
}

// writer is the fence and the event kind of one ref write. A merge write has no workspace, and its guard row keeps
// the parameters $1 and $2 in use.
type writer struct {
	guard string
	kind  string
}

var (
	nodeWriter  = writer{guard: fmt.Sprintf(guard, 1, 2, 3), kind: meta.WriterNode}
	mergeWriter = writer{guard: `SELECT $1::text AS id WHERE $2::bigint >= 0 AND $3::text <> ''`, kind: meta.WriterMerge}
)

func (s *Store) SwapRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64, sha string) (meta.Ref, error) {
	return s.swapRef(ctx, nodeWriter, ws, epoch, repo, name, expect, sha)
}

func (s *Store) MergeRef(ctx context.Context, repo, name string, expect int64, sha string) (meta.Ref, error) {
	return s.swapRef(ctx, mergeWriter, "", 0, repo, name, expect, sha)
}

func (s *Store) DeleteRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64) error {
	return s.deleteRef(ctx, nodeWriter, ws, epoch, repo, name, expect)
}

func (s *Store) MergeDeleteRef(ctx context.Context, repo, name string, expect int64) error {
	return s.deleteRef(ctx, mergeWriter, "", 0, repo, name, expect)
}

// withEvent runs fn in a transaction that first takes the next event number of the partition from its row in
// event_partitions. The row lock lasts to the commit, so Seq order is commit order within the partition and a reader
// never sees a lower Seq appear after a higher one. Writes to refs of other partitions do not wait for it. The
// number is taken before any other lock, so a write in flight never holds a workspace lock while it waits for it. The
// transaction commits only when fn reports a write, so a failed condition leaves no gap.
func (s *Store) withEvent(ctx context.Context, partition int, fn func(tx pgx.Tx, seq int64) (bool, error)) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var seq int64
	if err := tx.QueryRow(ctx, `UPDATE event_partitions SET seq = seq + 1 WHERE partition = $1 RETURNING seq`, partition).Scan(&seq); err != nil {
		return false, err
	}
	wrote, err := fn(tx, seq)
	if err != nil || !wrote {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// swapRef takes every generation from one sequence, so a deleted and recreated ref never repeats one. The ref write
// and its event commit together.
func (s *Store) swapRef(ctx context.Context, w writer, ws string, epoch uint64, repo, name string, expect int64, sha string) (meta.Ref, error) {
	var generation int64
	wrote, err := s.withEvent(ctx, meta.Partition(repo, name), func(tx pgx.Tx, seq int64) (bool, error) {
		var err error
		if expect == 0 {
			err = tx.QueryRow(ctx, `
				WITH guard AS (`+w.guard+`),
				moved AS (
					INSERT INTO refs (repo, name, commit_sha, generation)
					SELECT $3::text, $4::text, $5::text, nextval('ref_generation') FROM guard
					ON CONFLICT (repo, name) DO NOTHING RETURNING generation)
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $8::int, $7::bigint, $3::text, $4::text, '', $5::text, moved.generation, $6::text FROM moved
				RETURNING generation`, ws, int64(epoch), repo, name, sha, w.kind, seq, meta.Partition(repo, name)).Scan(&generation)
		} else {
			err = tx.QueryRow(ctx, `
				WITH guard AS (`+w.guard+`),
				cur AS (
					SELECT commit_sha FROM refs WHERE repo = $3 AND name = $4 AND generation = $8 AND EXISTS (SELECT 1 FROM guard) FOR UPDATE),
				moved AS (
					UPDATE refs SET commit_sha = $5, generation = nextval('ref_generation'), updated_at = now()
					FROM cur WHERE refs.repo = $3 AND refs.name = $4 AND refs.generation = $8
					RETURNING refs.generation, cur.commit_sha AS old_commit)
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $9::int, $7::bigint, $3::text, $4::text, moved.old_commit, $5::text, moved.generation, $6::text FROM moved
				RETURNING generation`, ws, int64(epoch), repo, name, sha, w.kind, seq, expect, meta.Partition(repo, name)).Scan(&generation)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		return meta.Ref{}, fmt.Errorf("postgres: swap ref: %w", err)
	}
	if wrote {
		return meta.Ref{Name: name, Commit: sha, Generation: generation}, nil
	}
	return meta.Ref{}, s.refLost(ctx, w, ws, epoch, repo, name)
}

func (s *Store) deleteRef(ctx context.Context, w writer, ws string, epoch uint64, repo, name string, expect int64) error {
	wrote, err := s.withEvent(ctx, meta.Partition(repo, name), func(tx pgx.Tx, seq int64) (bool, error) {
		tag, err := tx.Exec(ctx, `
			WITH guard AS (`+w.guard+`),
			cur AS (
				SELECT commit_sha FROM refs WHERE repo = $3 AND name = $4 AND generation = $5 AND EXISTS (SELECT 1 FROM guard) FOR UPDATE),
			gone AS (
				DELETE FROM refs USING cur WHERE refs.repo = $3 AND refs.name = $4 AND refs.generation = $5
				RETURNING cur.commit_sha AS old_commit)
			INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
			SELECT $8::int, $7::bigint, $3::text, $4::text, gone.old_commit, '', $5::bigint, $6::text FROM gone`,
			ws, int64(epoch), repo, name, expect, w.kind, seq, meta.Partition(repo, name))
		if err != nil || tag.RowsAffected() != 1 {
			return false, err
		}
		return true, dropLandMeta(ctx, tx, repo, name)
	})
	if err != nil {
		return fmt.Errorf("postgres: delete ref: %w", err)
	}
	if wrote {
		return nil
	}
	return s.refLost(ctx, w, ws, epoch, repo, name)
}

// dropLandMeta removes the land log, head and lease of a deleted ref in the transaction of the delete.
func dropLandMeta(ctx context.Context, tx pgx.Tx, repo, name string) error {
	for _, table := range []string{"land_log", "land_heads", "land_leases"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE repo = $1 AND ref = $2`, repo, name); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRef compares on the commit of the ref instead of its generation. The write and its event commit together.
func (s *Store) UpdateRef(ctx context.Context, repo, name, old, new, kind string) (meta.Ref, error) {
	var generation int64
	wrote, err := s.withEvent(ctx, meta.Partition(repo, name), func(tx pgx.Tx, seq int64) (bool, error) {
		part := meta.Partition(repo, name)
		var err error
		switch {
		case new == "":
			err = tx.QueryRow(ctx, `
				WITH gone AS (DELETE FROM refs WHERE repo = $1 AND name = $2 AND commit_sha = $3 RETURNING generation)
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $6::int, $5::bigint, $1::text, $2::text, $3::text, '', gone.generation, $4::text FROM gone
				RETURNING generation`, repo, name, old, kind, seq, part).Scan(&generation)
		case old == "":
			err = tx.QueryRow(ctx, `
				WITH moved AS (
					INSERT INTO refs (repo, name, commit_sha, generation) VALUES ($1, $2, $3, nextval('ref_generation'))
					ON CONFLICT (repo, name) DO NOTHING RETURNING generation)
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $6::int, $5::bigint, $1::text, $2::text, '', $3::text, moved.generation, $4::text FROM moved
				RETURNING generation`, repo, name, new, kind, seq, part).Scan(&generation)
		default:
			err = tx.QueryRow(ctx, `
				WITH moved AS (
					UPDATE refs SET commit_sha = $4, generation = nextval('ref_generation'), updated_at = now()
					WHERE repo = $1 AND name = $2 AND commit_sha = $3 RETURNING generation)
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind)
				SELECT $7::int, $6::bigint, $1::text, $2::text, $3::text, $4::text, moved.generation, $5::text FROM moved
				RETURNING generation`, repo, name, old, new, kind, seq, part).Scan(&generation)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if new == "" {
			err = dropLandMeta(ctx, tx, repo, name)
		}
		return err == nil, err
	})
	if err != nil {
		return meta.Ref{}, fmt.Errorf("postgres: update ref: %w", err)
	}
	if wrote {
		return meta.Ref{Name: name, Commit: new, Generation: generation}, nil
	}
	cur, err := s.Ref(ctx, repo, name)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return meta.Ref{}, &meta.RefConflict{}
	case err != nil:
		return meta.Ref{}, err
	}
	return meta.Ref{}, &meta.RefConflict{Current: &cur}
}

func (s *Store) refLost(ctx context.Context, w writer, ws string, epoch uint64, repo, name string) error {
	if w.kind == meta.WriterNode {
		if err := s.lost(ctx, ws, epoch, repo); err != nil {
			return err
		}
	}
	cur, err := s.Ref(ctx, repo, name)
	if errors.Is(err, meta.ErrNotFound) {
		return &meta.RefConflict{}
	}
	if err != nil {
		return err
	}
	return &meta.RefConflict{Current: &cur}
}
