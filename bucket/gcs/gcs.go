// Package gcs backs the bucket seam with Google Cloud Storage.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

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

func (g gcsBucket) Version(ctx context.Context, key string) (string, error) {
	a, err := g.b.Object(key).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", bucket.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("bucket: version: %w", err)
	}
	return strconv.FormatInt(a.Generation, 10), nil
}

// Delete sends the generation as ifGenerationMatch, and reads the object again on a 412 to tell a changed object from
// a missing one.
func (g gcsBucket) Delete(ctx context.Context, key, version string) error {
	gen, err := strconv.ParseInt(version, 10, 64)
	if err != nil || gen <= 0 {
		return fmt.Errorf("bucket: delete: invalid version %q", version)
	}
	err = g.b.Object(key).If(storage.Conditions{GenerationMatch: gen}).Delete(ctx)
	var gerr *googleapi.Error
	switch {
	case err == nil, errors.Is(err, storage.ErrObjectNotExist):
		return nil
	case errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed:
		if _, verr := g.Version(ctx, key); errors.Is(verr, bucket.ErrNotFound) {
			return nil
		}
		return bucket.ErrChanged
	}
	return fmt.Errorf("bucket: delete: %w", err)
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
		return nil, readError(err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, readError(err)
	}
	return data, nil
}

func readError(err error) error {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		err = bucket.WithStatus(err, gerr.Code)
	}
	return fmt.Errorf("bucket: read: %w", err)
}
