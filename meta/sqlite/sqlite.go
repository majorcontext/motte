// Package sqlite is the SQLite backend of meta.Store, for tests and for a
// service that runs with no other process. It uses a pure Go driver, so a
// binary that imports it builds with CGO disabled. One process uses one
// database file.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/majorcontext/motte/meta"
)

// Store keeps the metadata in one SQLite database.
type Store struct{ db *sql.DB }

var _ meta.Store = (*Store)(nil)

// Memory is the path of a database that lives as long as its Store.
const Memory = ":memory:"

// Open opens the database file at path, or creates it, and applies the migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	escaped := strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23").Replace(path)
	db, err := sql.Open("sqlite", "file:"+escaped+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	// One connection orders every write, and keeps an in-memory database alive.
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func inTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func micros() int64 { return time.Now().UnixMicro() }

func uniqueViolation(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	c := se.Code()
	return c == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY || c == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

const bumpGeneration = `UPDATE counters SET value = value + 1 WHERE name = 'ref_generation'`

// nextSeq is the number of the next event of the partition in the parameter at param.
func nextSeq(param string) string {
	return `(SELECT seq + 1 FROM event_partitions WHERE partition = ` + param + `)`
}

// bumpSeq takes the number that the event insert just used, and only when that insert changed a row, so a failed write
// leaves no gap even in a batch that does not roll back.
const bumpSeq = `UPDATE event_partitions SET seq = seq + 1 WHERE partition = ?1 AND EXISTS (SELECT 1 FROM events WHERE partition = ?1 AND seq = event_partitions.seq + 1)`

const nextGeneration = `(SELECT value FROM counters WHERE name = 'ref_generation')`

// fenceHolds is the fence of every write. ?1, ?2 and ?3 are the workspace, the epoch and the repo.
const fenceHolds = `EXISTS (SELECT 1 FROM workspaces WHERE id = ?1 AND epoch = ?2 AND repo = ?3 AND deleted_at IS NULL)`

func (s *Store) CreateRepo(ctx context.Context, r meta.Repo, repoText, commit string) error {
	now := micros()
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO repos (path, name, owner, created_at) VALUES (?, ?, ?, ?)`, r.Path, r.Name, r.Owner, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, bumpGeneration); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO refs (repo, name, commit_sha, generation, updated_at) VALUES (?, 'main', ?, `+nextGeneration+`, ?)`, repoText, commit, now); err != nil {
			return err
		}
		part := meta.Partition(repoText, "main")
		if _, err := tx.ExecContext(ctx, `INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at) VALUES (?1, `+nextSeq("?1")+`, ?2, 'main', '', ?3, `+nextGeneration+`, ?4, ?5)`, part, repoText, commit, meta.WriterOperator, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, bumpSeq, part)
		return err
	})
	if uniqueViolation(err) {
		return meta.ErrExists
	}
	if err != nil {
		return fmt.Errorf("sqlite: create repo: %w", err)
	}
	return nil
}

func (s *Store) Repo(ctx context.Context, path string) (meta.Repo, error) {
	var r meta.Repo
	var created int64
	err := s.db.QueryRowContext(ctx, `SELECT path, name, owner, created_at FROM repos WHERE path = ?`, path).Scan(&r.Path, &r.Name, &r.Owner, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Repo{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Repo{}, fmt.Errorf("sqlite: read repo: %w", err)
	}
	r.CreatedAt = time.UnixMicro(created).UTC()
	return r, nil
}

func (s *Store) Repos(ctx context.Context) ([]meta.Repo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT path, name, owner, created_at FROM repos ORDER BY created_at DESC, path DESC`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list repos: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Repo
	for rows.Next() {
		var r meta.Repo
		var created int64
		if err := rows.Scan(&r.Path, &r.Name, &r.Owner, &created); err != nil {
			return nil, fmt.Errorf("sqlite: list repos: %w", err)
		}
		r.CreatedAt = time.UnixMicro(created).UTC()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list repos: %w", err)
	}
	return out, nil
}

func (s *Store) Claim(ctx context.Context, id string, epoch uint64, owner, repo string) (meta.Workspace, error) {
	var w meta.Workspace
	var stored int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO workspaces (id, repo, owner, epoch, created_at) VALUES (?1, ?2, ?3, ?4, ?5)
		ON CONFLICT (id) DO UPDATE SET epoch = max(workspaces.epoch, excluded.epoch)
		WHERE workspaces.deleted_at IS NULL AND workspaces.owner = excluded.owner AND workspaces.repo = excluded.repo
		RETURNING epoch, op, snapshot`, id, repo, owner, int64(epoch), micros()).Scan(&stored, &w.Op, &w.Snapshot)
	if err == nil {
		w.Epoch = uint64(stored)
		return w, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return meta.Workspace{}, fmt.Errorf("sqlite: claim: %w", err)
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
	err := s.db.QueryRowContext(ctx, `SELECT epoch, repo, deleted_at IS NOT NULL FROM workspaces WHERE id = ?`, id).Scan(&epoch, &f.Repo, &f.Deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Fence{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Fence{}, fmt.Errorf("sqlite: read workspace: %w", err)
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
	if _, err := s.db.ExecContext(ctx, `UPDATE workspaces SET deleted_at = COALESCE(deleted_at, ?) WHERE id = ?`, micros(), id); err != nil {
		return fmt.Errorf("sqlite: delete workspace: %w", err)
	}
	return nil
}

// report is one UPDATE. A NULL repo matches the stored one.
func (s *Store) report(ctx context.Context, ws string, epoch uint64, repo any, generation int64, prev, op, snapshot string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE workspaces SET op = ?4, snapshot = ?5,
			report_generation = COALESCE(NULLIF(?6, 0), report_generation)
		WHERE id = ?1 AND epoch = ?2 AND (?3 IS NULL OR repo = ?3) AND deleted_at IS NULL AND CASE WHEN ?6 > 0
			THEN (report_generation IS NULL OR report_generation <= ?6)
			ELSE op = ?7 END`, ws, int64(epoch), repo, op, snapshot, generation, prev)
	if err != nil {
		return false, fmt.Errorf("sqlite: report workspace: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
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
	var heads string
	err := s.db.QueryRowContext(ctx, `SELECT epoch, generation, heads FROM op_heads WHERE workspace_id = ?`, ws).Scan(&epoch, &h.Generation, &heads)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.OpHeads{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.OpHeads{}, fmt.Errorf("sqlite: read op heads: %w", err)
	}
	if err := json.Unmarshal([]byte(heads), &h.Heads); err != nil {
		return meta.OpHeads{}, fmt.Errorf("sqlite: read op heads: %w", err)
	}
	h.Epoch = uint64(epoch)
	return h, nil
}

func (s *Store) SwapOpHeads(ctx context.Context, ws string, epoch uint64, repo string, expect int64, heads []string) (meta.OpHeads, error) {
	raw, err := json.Marshal(heads)
	if err != nil {
		return meta.OpHeads{}, fmt.Errorf("sqlite: swap op heads: %w", err)
	}
	var generation int64
	if expect == 0 {
		err = s.db.QueryRowContext(ctx, `
			INSERT INTO op_heads (workspace_id, epoch, generation, heads, updated_at)
			SELECT ?1, ?2, 1, ?4, ?5 WHERE `+fenceHolds+`
			ON CONFLICT (workspace_id) DO NOTHING
			RETURNING generation`, ws, int64(epoch), repo, string(raw), micros()).Scan(&generation)
	} else {
		err = s.db.QueryRowContext(ctx, `
			UPDATE op_heads SET epoch = ?2, generation = generation + 1, heads = ?5, updated_at = ?6
			WHERE workspace_id = ?1 AND generation = ?4 AND epoch <= ?2 AND `+fenceHolds+`
			RETURNING generation`, ws, int64(epoch), repo, expect, string(raw), micros()).Scan(&generation)
	}
	if err == nil {
		return meta.OpHeads{Epoch: epoch, Generation: generation, Heads: heads}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return meta.OpHeads{}, fmt.Errorf("sqlite: swap op heads: %w", err)
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
	if _, err := s.db.ExecContext(ctx, `INSERT INTO packs (repo, pack_id, bytes, created_at) VALUES (?, ?, ?, ?) ON CONFLICT (repo, pack_id) DO NOTHING`, repo, packID, bytes, micros()); err != nil {
		return fmt.Errorf("sqlite: add pack: %w", err)
	}
	return nil
}

func (s *Store) Packs(ctx context.Context, repo string) ([]meta.Pack, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pack_id, bytes FROM packs WHERE repo = ? ORDER BY created_at, pack_id`, repo)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list packs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Pack
	for rows.Next() {
		var p meta.Pack
		if err := rows.Scan(&p.ID, &p.Bytes); err != nil {
			return nil, fmt.Errorf("sqlite: list packs: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list packs: %w", err)
	}
	return out, nil
}

func (s *Store) RemovePack(ctx context.Context, repo, packID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM packs WHERE repo = ? AND pack_id = ?`, repo, packID); err != nil {
		return fmt.Errorf("sqlite: remove pack: %w", err)
	}
	return nil
}

func (s *Store) Manifest(ctx context.Context, repo string) (meta.Manifest, error) {
	var m meta.Manifest
	err := s.db.QueryRowContext(ctx, `SELECT generation, body FROM pack_manifests WHERE repo = ?`, repo).Scan(&m.Generation, &m.Body)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Manifest{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Manifest{}, fmt.Errorf("sqlite: read manifest: %w", err)
	}
	return m, nil
}

func (s *Store) SwapManifest(ctx context.Context, repo string, expect int64, body []byte) (int64, error) {
	var res sql.Result
	var err error
	if expect == 0 {
		res, err = s.db.ExecContext(ctx, `INSERT INTO pack_manifests (repo, generation, body, updated_at) VALUES (?, 1, ?, ?) ON CONFLICT (repo) DO NOTHING`, repo, body, micros())
	} else {
		res, err = s.db.ExecContext(ctx, `UPDATE pack_manifests SET generation = generation + 1, body = ?, updated_at = ? WHERE repo = ? AND generation = ?`, body, micros(), repo, expect)
	}
	if err != nil {
		return 0, fmt.Errorf("sqlite: swap manifest: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, meta.ErrManifestStale
	}
	return expect + 1, nil
}

func (s *Store) RetireFiles(ctx context.Context, repo string, keys []string, at time.Time) error {
	for _, k := range keys {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO pack_retired (repo, key, retired_at) VALUES (?, ?, ?) ON CONFLICT (repo, key) DO UPDATE SET retired_at = excluded.retired_at WHERE deleting_at IS NULL`, repo, k, at.UnixMicro()); err != nil {
			return fmt.Errorf("sqlite: retire files: %w", err)
		}
	}
	return nil
}

func (s *Store) RetiredFiles(ctx context.Context, repo string) ([]meta.Retired, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, retired_at, deleting_at FROM pack_retired WHERE repo = ? ORDER BY retired_at, key`, repo)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list retired files: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Retired
	for rows.Next() {
		var r meta.Retired
		var at int64
		var del sql.NullInt64
		if err := rows.Scan(&r.Key, &at, &del); err != nil {
			return nil, fmt.Errorf("sqlite: list retired files: %w", err)
		}
		r.At = time.UnixMicro(at).UTC()
		if del.Valid {
			r.Deleting, r.DeletingAt = true, time.UnixMicro(del.Int64).UTC()
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list retired files: %w", err)
	}
	return out, nil
}

func (s *Store) BeginDelete(ctx context.Context, repo string, keys []string, now, staleBefore time.Time) ([]string, error) {
	var out []string
	for _, k := range keys {
		res, err := s.db.ExecContext(ctx, `UPDATE pack_retired SET deleting_at = ? WHERE repo = ? AND key = ? AND (deleting_at IS NULL OR deleting_at <= ?)`, now.UnixMicro(), repo, k, staleBefore.UnixMicro())
		if err != nil {
			return nil, fmt.Errorf("sqlite: begin delete: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			out = append(out, k)
		}
	}
	return out, nil
}

func (s *Store) ClaimRetired(ctx context.Context, repo string, keys []string) (bool, error) {
	deleting := false
	for _, k := range keys {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM pack_retired WHERE repo = ? AND key = ? AND deleting_at IS NULL`, repo, k); err != nil {
			return false, fmt.Errorf("sqlite: claim retired: %w", err)
		}
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pack_retired WHERE repo = ? AND key = ?`, repo, k).Scan(&n); err != nil {
			return false, fmt.Errorf("sqlite: claim retired: %w", err)
		}
		deleting = deleting || n > 0
	}
	return deleting, nil
}

func (s *Store) ForgetRetired(ctx context.Context, repo string, keys []string) error {
	for _, k := range keys {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM pack_retired WHERE repo = ? AND key = ?`, repo, k); err != nil {
			return fmt.Errorf("sqlite: forget retired files: %w", err)
		}
	}
	return nil
}

func (s *Store) PackRepos(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT repo FROM packs ORDER BY repo`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list pack repos: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, fmt.Errorf("sqlite: list pack repos: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list pack repos: %w", err)
	}
	return out, nil
}

func (s *Store) Ref(ctx context.Context, repo, name string) (meta.Ref, error) {
	r := meta.Ref{Name: name}
	err := s.db.QueryRowContext(ctx, `SELECT commit_sha, generation FROM refs WHERE repo = ? AND name = ?`, repo, name).Scan(&r.Commit, &r.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return meta.Ref{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.Ref{}, fmt.Errorf("sqlite: read ref: %w", err)
	}
	return r, nil
}

func (s *Store) Refs(ctx context.Context, repo string) ([]meta.Ref, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, commit_sha, generation FROM refs WHERE repo = ? ORDER BY name`, repo)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list refs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.Ref
	for rows.Next() {
		var r meta.Ref
		if err := rows.Scan(&r.Name, &r.Commit, &r.Generation); err != nil {
			return nil, fmt.Errorf("sqlite: list refs: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list refs: %w", err)
	}
	return out, nil
}

// writer is the guard condition of a ref write and the kind of its event. A merge write has no workspace.
type writer struct{ fence, kind string }

var (
	nodeWriter  = writer{fence: fenceHolds, kind: meta.WriterNode}
	mergeWriter = writer{fence: `1`, kind: meta.WriterMerge}
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

// swapRef takes every generation from the row 'ref_generation' of counters, which the same transaction increments, so
// a deleted and recreated ref never repeats one. The event insert has the condition of the ref write and runs first;
// a ref write that changes no row rolls the transaction back, and the event with it.
func (s *Store) swapRef(ctx context.Context, w writer, ws string, epoch uint64, repo, name string, expect int64, sha string) (meta.Ref, error) {
	var generation int64
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		now := micros()
		args := []any{ws, int64(epoch), repo, name, sha, expect, now, w.kind, meta.Partition(repo, name)}
		if _, err := tx.ExecContext(ctx, bumpGeneration); err != nil {
			return err
		}
		if expect == 0 {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at)
				SELECT ?9, `+nextSeq("?9")+`, ?3, ?4, '', ?5, `+nextGeneration+`, ?8, ?7
				WHERE `+w.fence+` AND NOT EXISTS (SELECT 1 FROM refs WHERE repo = ?3 AND name = ?4)`, args...); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, bumpSeq, args[8]); err != nil {
				return err
			}
			return tx.QueryRowContext(ctx, `
				INSERT INTO refs (repo, name, commit_sha, generation, updated_at)
				SELECT ?3, ?4, ?5, `+nextGeneration+`, ?7 WHERE `+w.fence+`
				ON CONFLICT (repo, name) DO NOTHING
				RETURNING generation`, args...).Scan(&generation)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at)
			SELECT ?9, `+nextSeq("?9")+`, ?3, ?4, commit_sha, ?5, `+nextGeneration+`, ?8, ?7
			FROM refs WHERE repo = ?3 AND name = ?4 AND generation = ?6 AND `+w.fence, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, bumpSeq, args[8]); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `
			UPDATE refs SET commit_sha = ?5, generation = `+nextGeneration+`, updated_at = ?7
			WHERE repo = ?3 AND name = ?4 AND generation = ?6 AND `+w.fence+`
			RETURNING generation`, args...).Scan(&generation)
	})
	if err == nil {
		return meta.Ref{Name: name, Commit: sha, Generation: generation}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return meta.Ref{}, fmt.Errorf("sqlite: swap ref: %w", err)
	}
	return meta.Ref{}, s.refLost(ctx, w, ws, epoch, repo, name)
}

func (s *Store) deleteRef(ctx context.Context, w writer, ws string, epoch uint64, repo, name string, expect int64) error {
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		args := []any{ws, int64(epoch), repo, name, expect, micros(), w.kind, meta.Partition(repo, name)}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at)
			SELECT ?8, `+nextSeq("?8")+`, ?3, ?4, commit_sha, '', ?5, ?7, ?6
			FROM refs WHERE repo = ?3 AND name = ?4 AND generation = ?5 AND `+w.fence, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, bumpSeq, args[7]); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE repo = ?3 AND name = ?4 AND generation = ?5 AND `+w.fence, ws, int64(epoch), repo, name, expect)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			if err != nil {
				return err
			}
			return sql.ErrNoRows
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: delete ref: %w", err)
	}
	return s.refLost(ctx, w, ws, epoch, repo, name)
}

// UpdateRef compares on the commit of the ref instead of its generation. Its event insert has the condition of the
// write and runs first, as in swapRef.
func (s *Store) UpdateRef(ctx context.Context, repo, name, old, new, kind string) (meta.Ref, error) {
	var generation int64
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		part := meta.Partition(repo, name)
		args := []any{repo, name, old, new, micros(), kind, part}
		if _, err := tx.ExecContext(ctx, bumpGeneration); err != nil {
			return err
		}
		const insert = `INSERT INTO events (partition, seq, repo, ref, old_commit, new_commit, generation, kind, at) `
		seq := nextSeq("?7")
		var event, write string
		switch {
		case new == "":
			event = insert + `SELECT ?7, ` + seq + `, ?1, ?2, ?3, '', generation, ?6, ?5 FROM refs WHERE repo = ?1 AND name = ?2 AND commit_sha = ?3`
			write = `DELETE FROM refs WHERE repo = ?1 AND name = ?2 AND commit_sha = ?3 RETURNING generation`
		case old == "":
			event = insert + `SELECT ?7, ` + seq + `, ?1, ?2, '', ?4, ` + nextGeneration + `, ?6, ?5 WHERE NOT EXISTS (SELECT 1 FROM refs WHERE repo = ?1 AND name = ?2)`
			write = `INSERT INTO refs (repo, name, commit_sha, generation, updated_at) VALUES (?1, ?2, ?4, ` + nextGeneration + `, ?5)
				ON CONFLICT (repo, name) DO NOTHING RETURNING generation`
		default:
			event = insert + `SELECT ?7, ` + seq + `, ?1, ?2, ?3, ?4, ` + nextGeneration + `, ?6, ?5 FROM refs WHERE repo = ?1 AND name = ?2 AND commit_sha = ?3`
			write = `UPDATE refs SET commit_sha = ?4, generation = ` + nextGeneration + `, updated_at = ?5
				WHERE repo = ?1 AND name = ?2 AND commit_sha = ?3 RETURNING generation`
		}
		if _, err := tx.ExecContext(ctx, event, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, bumpSeq, part); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, write, args...).Scan(&generation)
	})
	if err == nil {
		return meta.Ref{Name: name, Commit: new, Generation: generation}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return meta.Ref{}, fmt.Errorf("sqlite: update ref: %w", err)
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
