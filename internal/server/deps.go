package server

import (
	"context"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

// objects is the object service that the handlers call.
type objects interface {
	Reserve(ctx context.Context, bytes int64) (release func(), err error)
	ReserveLand(ctx context.Context, bytes int64) (release func(), err error)
	Commit(ctx context.Context, repo store.Repo, sha gitpack.SHA, grow func(size int64) error) (gitpack.SHA, time.Time, error)
	TreeBulk(ctx context.Context, repo store.Repo, sha gitpack.SHA, maxEntries int, grow func(treeBytes int64) error) ([]store.Tree, error)
	Blobs(ctx context.Context, repo store.Repo, shas []gitpack.SHA, grow func(size int64) error) ([][]byte, error)
	Has(ctx context.Context, repo store.Repo, shas []gitpack.SHA) ([]bool, error)
	Get(ctx context.Context, repo store.Repo, sha gitpack.SHA) (gitpack.Type, []byte, error)
	PutObjects(ctx context.Context, repo store.Repo, objs []gitpack.Object) ([]gitpack.SHA, error)
	Reader(ctx context.Context, repo store.Repo) (*store.Reader, error)
	ReceivePack(ctx context.Context, repo store.Repo, pack []byte, held func(int64) error) error
	ImportPack(ctx context.Context, repo store.Repo, id string, idx, pack []byte) error
	Warm(ctx context.Context, repos []store.Repo, parallel int) error
	Maintain(ctx context.Context, repo store.Repo) error
	CacheStats() store.CacheStats
	MaintStats() store.MaintStats
	GetOp(ctx context.Context, ws string, kind store.OpKind, id string) ([]byte, error)
	PutOp(ctx context.Context, ws string, kind store.OpKind, id string, data []byte) error
}

func refView(r meta.Ref) api.Ref {
	return api.Ref{Name: r.Name, Commit: r.Commit, Generation: r.Generation}
}

func opHeadsView(h meta.OpHeads) api.OpHeads {
	return api.OpHeads{Epoch: h.Epoch, Generation: h.Generation, Heads: h.Heads}
}
