// Package gcs backs the objstore seam with Google Cloud Storage.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/majorcontext/motte/internal/objstore"
)

type bucket struct {
	b *storage.BucketHandle
}

func New(client *storage.Client, name string) objstore.Store { return bucket{client.Bucket(name)} }

func (g bucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
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
		return false, fmt.Errorf("objstore: write: %w", err)
	}
	return true, nil
}

func (g bucket) Get(ctx context.Context, key string) ([]byte, error) {
	return g.read(ctx, key, 0, -1)
}

func (g bucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return g.read(ctx, key, off, n)
}

func (g bucket) read(ctx context.Context, key string, off, n int64) ([]byte, error) {
	r, err := g.b.Object(key).NewRangeReader(ctx, off, n)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, objstore.ErrNotFound
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusRequestedRangeNotSatisfiable {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("objstore: read: %w", err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("objstore: read: %w", err)
	}
	return data, nil
}
