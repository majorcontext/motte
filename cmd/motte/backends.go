package main

import (
	"context"
	"time"

	"cloud.google.com/go/storage"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/bucket/fs"
	"github.com/majorcontext/motte/internal/bucket/gcs"
	"github.com/majorcontext/motte/internal/bucket/s3"
	"github.com/majorcontext/motte/internal/meta/postgres"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/sqlite"
)

// gcsClients is how many Cloud Storage clients share the reads. One client funnels every read through the connections
// of one HTTP/2 transport, whose pool and stream locks then serialize the readers.
const gcsClients = 8

// hedgeConfig bounds the duplicate reads of a hedged bucket: at most 5 percent of the reads, after 20 ms to 2 s.
var hedgeConfig = bucket.HedgeConfig{Ratio: 0.05, Min: 20 * time.Millisecond, Max: 2 * time.Second}

// bucketBackend is a value of MOTTE_BUCKET_KIND. vars are the variables that only this kind reads; setting one with
// another kind is an error. open returns the store and, for a kind that hedges while c.hedge is on, a second store on
// other connections for the duplicate reads; the wrapper that hedges is the same for every kind.
type bucketBackend struct {
	kind string
	vars []string
	open func(ctx context.Context, c config) (store, alt bucket.Store, err error)
}

var bucketBackends = []bucketBackend{
	{"gcs", []string{"MOTTE_GCS_HEDGE"}, openGCS},
	{"s3", []string{"MOTTE_S3_ENDPOINT", "MOTTE_S3_REGION", "MOTTE_S3_PATH_STYLE"}, openS3},
	{"fs", nil, openFS},
}

// metaBackend is a value of MOTTE_META. required is the variable that it needs.
type metaBackend struct {
	name     string
	required string
	open     func(ctx context.Context, c config) (meta.Store, func(), error)
}

var metaBackends = []metaBackend{
	{"postgres", "MOTTE_DATABASE_URL", openPostgres},
	{"sqlite", "MOTTE_SQLITE_PATH", openSQLite},
}

func bucketBackendOf(kind string) (bucketBackend, bool) {
	for _, b := range bucketBackends {
		if b.kind == kind {
			return b, true
		}
	}
	return bucketBackend{}, false
}

func metaBackendOf(name string) (metaBackend, bool) {
	for _, b := range metaBackends {
		if b.name == name {
			return b, true
		}
	}
	return metaBackend{}, false
}

func openBucket(ctx context.Context, c config) (bucket.Store, error) {
	b, _ := bucketBackendOf(c.bucketKind)
	store, alt, err := b.open(ctx, c)
	if err != nil || alt == nil {
		return store, err
	}
	return bucket.Hedge(store, alt, hedgeConfig), nil
}

func openMeta(ctx context.Context, c config) (meta.Store, func(), error) {
	b, _ := metaBackendOf(c.meta)
	return b.open(ctx, c)
}

func openFS(_ context.Context, c config) (bucket.Store, bucket.Store, error) {
	root, err := fs.New(c.bucket)
	if err != nil {
		return nil, nil, err
	}
	return root.Bucket("motte"), nil, nil
}

func openS3(ctx context.Context, c config) (bucket.Store, bucket.Store, error) {
	client, err := s3.NewClient(ctx, c.s3)
	if err != nil {
		return nil, nil, err
	}
	return s3.New(client, c.bucket), nil, nil
}

func openGCS(ctx context.Context, c config) (bucket.Store, bucket.Store, error) {
	clients := make([]*storage.Client, gcsClients+1)
	if !c.hedge {
		clients = clients[:gcsClients]
	}
	for i := range clients {
		var err error
		if clients[i], err = storage.NewClient(ctx); err != nil {
			return nil, nil, err
		}
	}
	primary, err := gcs.NewPool(clients[:gcsClients], c.bucket)
	if err != nil || !c.hedge {
		return primary, nil, err
	}
	return primary, gcs.New(clients[gcsClients], c.bucket), nil
}

func openSQLite(ctx context.Context, c config) (meta.Store, func(), error) {
	db, err := sqlite.Open(ctx, c.sqlitePath)
	if err != nil {
		return nil, nil, err
	}
	return db, func() { _ = db.Close() }, nil
}

func openPostgres(ctx context.Context, c config) (meta.Store, func(), error) {
	db, err := postgres.Open(ctx, c.databaseURL)
	if err != nil {
		return nil, nil, err
	}
	return db, db.Close, nil
}
