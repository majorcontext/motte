package testserver

import (
	"context"
	"time"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/meta"
)

type faultMeta struct {
	meta.Store
	f Faults
}

func (m faultMeta) FinishLands(ctx context.Context, repo, ref, owner string, now time.Time, done []meta.LandDone) error {
	if m.f.FinishLands != nil {
		if err := m.f.FinishLands(ctx); err != nil {
			return err
		}
	}
	return m.Store.FinishLands(ctx, repo, ref, owner, now, done)
}

func (m faultMeta) ProtectedRefs(ctx context.Context, repo string) ([]string, error) {
	if m.f.ProtectedRefs != nil {
		if err := m.f.ProtectedRefs(ctx); err != nil {
			return nil, err
		}
	}
	return m.Store.ProtectedRefs(ctx, repo)
}

type faultBucket struct {
	bucket.Store
	f Faults
}

func (b faultBucket) Unwrap() bucket.Store { return b.Store }

func (b faultBucket) Get(ctx context.Context, key string) ([]byte, error) {
	if b.f.BucketGet != nil {
		if err := b.f.BucketGet(ctx, key); err != nil {
			return nil, err
		}
	}
	return b.Store.Get(ctx, key)
}

func (b faultBucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
	if b.f.BucketCreate != nil {
		if err := b.f.BucketCreate(ctx, key); err != nil {
			return false, err
		}
	}
	return b.Store.Create(ctx, key, data)
}

func (b faultBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if b.f.BucketGet != nil {
		if err := b.f.BucketGet(ctx, key); err != nil {
			return nil, err
		}
	}
	return b.Store.GetRange(ctx, key, off, n)
}
