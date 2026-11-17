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
	"google.golang.org/api/iterator"

	"github.com/majorcontext/motte/internal/objstore"
)

type bucket struct {
	b *storage.BucketHandle
}

func New(client *storage.Client, name string) objstore.Bucket { return bucket{client.Bucket(name)} }

// Buckets opens the buckets of client.
type Buckets struct{ Client *storage.Client }

func (b Buckets) Bucket(name string) objstore.Bucket { return New(b.Client, name) }

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

func notFound(err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return objstore.ErrNotFound
	}
	return err
}

func (g bucket) Attrs(ctx context.Context, key string) (objstore.Attrs, error) {
	a, err := g.b.Object(key).Attrs(ctx)
	if err != nil {
		return objstore.Attrs{}, notFound(err)
	}
	return objstore.Attrs{Size: a.Size, ContentType: a.ContentType, Metadata: a.Metadata, MD5: a.MD5, Generation: a.Generation}, nil
}

func (g bucket) Open(ctx context.Context, key string, generation int64) (io.ReadCloser, objstore.Attrs, error) {
	o := g.b.Object(key)
	if generation > 0 {
		o = o.Generation(generation)
	}
	r, err := o.NewReader(ctx)
	if err != nil {
		return nil, objstore.Attrs{}, notFound(err)
	}
	return r, objstore.Attrs{Size: r.Attrs.Size, ContentType: r.Attrs.ContentType, Generation: r.Attrs.Generation}, nil
}

func (g bucket) List(ctx context.Context, prefix string) ([]string, error) {
	it := g.b.Objects(ctx, &storage.Query{Prefix: prefix})
	var names []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return names, nil
		}
		if err != nil {
			return nil, err
		}
		names = append(names, attrs.Name)
	}
}

func (g bucket) Delete(ctx context.Context, key string) error {
	if err := g.b.Object(key).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return err
	}
	return nil
}

func (g bucket) Copy(ctx context.Context, c objstore.Copy) (int64, error) {
	dst := g.b.Object(c.Destination)
	if c.CreateOnly {
		dst = dst.If(storage.Conditions{DoesNotExist: true})
	}
	src := g.b.Object(c.Source)
	if c.Generation > 0 {
		src = src.Generation(c.Generation)
	}
	copier := dst.CopierFrom(src)
	copier.ObjectAttrs = storage.ObjectAttrs{Metadata: c.Metadata}
	attrs, err := copier.Run(ctx)
	if err != nil {
		return 0, notFound(err)
	}
	if attrs.Generation == 0 {
		return 0, errors.New("objstore: copy returned generation zero")
	}
	return attrs.Generation, nil
}
