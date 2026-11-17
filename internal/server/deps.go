package server

import (
	"context"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/store"
)

// index is the PostgreSQL state that the handlers read and write.
type index interface {
	Ping(ctx context.Context) error
	CreateRepo(ctx context.Context, r pg.Repo, repoText, commit string) error
	Repo(ctx context.Context, path string) (pg.Repo, error)
	Repos(ctx context.Context) ([]pg.Repo, error)
	Claim(ctx context.Context, id string, epoch uint64, owner, repo string) (pg.Workspace, error)
	Live(ctx context.Context, id string, epoch uint64, repo string) error
	Delete(ctx context.Context, id string) error
	Report(ctx context.Context, ws string, epoch uint64, repo string, generation int64, prev, op, snapshot string) error
	Flush(ctx context.Context, ws string, epoch uint64, generation int64, prev, op, snapshot string) error
	OpHeads(ctx context.Context, ws string) (pg.OpHeads, error)
	SwapOpHeads(ctx context.Context, ws string, epoch uint64, repo string, expect int64, heads []string) (pg.OpHeads, error)
	Ref(ctx context.Context, repo, name string) (pg.Ref, error)
	Refs(ctx context.Context, repo string) ([]pg.Ref, error)
	SwapRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64, sha string) (pg.Ref, error)
	DeleteRef(ctx context.Context, ws string, epoch uint64, repo, name string, expect int64) error
}

// objects is the object service that the handlers call.
type objects interface {
	Reserve(ctx context.Context, bytes int64) (release func(), err error)
	Commit(ctx context.Context, repo store.Repo, sha gitpack.SHA) (gitpack.SHA, time.Time, error)
	TreeBulk(ctx context.Context, repo store.Repo, sha gitpack.SHA, maxEntries int, grow func(treeBytes int64) error) ([]store.Tree, error)
	Blobs(ctx context.Context, repo store.Repo, shas []gitpack.SHA) ([][]byte, error)
	Has(ctx context.Context, repo store.Repo, shas []gitpack.SHA) ([]bool, error)
	Get(ctx context.Context, repo store.Repo, sha gitpack.SHA) (gitpack.Type, []byte, error)
	PutObjects(ctx context.Context, repo store.Repo, objs []gitpack.Object) ([]gitpack.SHA, error)
	ImportPack(ctx context.Context, repo store.Repo, id string, idx, pack []byte) error
	GetOp(ctx context.Context, ws string, kind store.OpKind, id string) ([]byte, error)
	PutOp(ctx context.Context, ws string, kind store.OpKind, id string, data []byte) error
}

func refView(r pg.Ref) api.Ref {
	return api.Ref{Name: r.Name, Commit: r.Commit, Generation: r.Generation}
}

func opHeadsView(h pg.OpHeads) api.OpHeads {
	return api.OpHeads{Epoch: h.Epoch, Generation: h.Generation, Heads: h.Heads}
}
