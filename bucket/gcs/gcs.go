// Package gcs backs the bucket seam with Google Cloud Storage.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/majorcontext/motte/bucket"
)

type gcsBucket struct {
	b *storage.BucketHandle
}

func New(client *storage.Client, name string) bucket.Store { return gcsBucket{client.Bucket(name)} }

func (g gcsBucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
	w := g.b.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, err := w.Write(data)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("bucket: write: %w", err)
	}
	return true, nil
}

func (g gcsBucket) Get(ctx context.Context, key string) ([]byte, error) {
	return g.read(ctx, key, 0, -1)
}

func (g gcsBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return g.read(ctx, key, off, n)
}

func (g gcsBucket) read(ctx context.Context, key string, off, n int64) ([]byte, error) {
	r, err := g.b.Object(key).NewRangeReader(ctx, off, n)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, bucket.ErrNotFound
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusRequestedRangeNotSatisfiable {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("bucket: read: %w", err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("bucket: read: %w", err)
	}
	return data, nil
}
