package main

import (
	"context"
	"crypto/ed25519"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta/sqlite"
)

type slowBucket struct {
	bucket.Store
	d time.Duration
}

func (s slowBucket) wait(ctx context.Context) error {
	t := time.NewTimer(s.d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s slowBucket) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key)
}

func (s slowBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func (s slowBucket) Version(ctx context.Context, key string) (string, error) {
	if err := s.wait(ctx); err != nil {
		return "", err
	}
	return s.Store.Version(ctx, key)
}

func runServe(ctx context.Context, args []string) error {
	fs2 := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := fs2.String("listen", "127.0.0.1:8080", "listen address")
	keyFile := fs2.String("key", "motte.key", "Ed25519 PKCS#8 PEM key")
	kid := fs2.String("kid", "local", "key ID")
	db := fs2.String("sqlite", "motte.db", "SQLite database file")
	dir := fs2.String("bucket", "objects", "directory of the filesystem bucket")
	latency := fs2.Duration("latency", 0, "wait added to each read of the bucket")
	compaction := fs2.Bool("compaction", true, "run maintenance of packs and layers")
	if err := fs2.Parse(args); err != nil {
		return err
	}
	priv, err := loadSigner(*keyFile, *kid)
	if err != nil {
		return err
	}
	meta, err := sqlite.Open(ctx, *db)
	if err != nil {
		return err
	}
	defer meta.Close()
	root, err := fs.New(*dir)
	if err != nil {
		return err
	}
	var store bucket.Store = root.Bucket("motte")
	if *latency > 0 {
		store = slowBucket{store, *latency}
	}
	pub := priv.Key.Public().(ed25519.PublicKey)
	scopes := []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeWrite, grant.ScopeImport, grant.ScopeOperator, grant.ScopeMerge}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := server.New(server.Config{Meta: meta, Bucket: store, Verifier: grant.NewVerifier(grant.Key{Kid: *kid, Public: pub, Scopes: scopes}), Logger: log, Warm: true})
	go func() { _ = srv.Run(ctx) }()
	go srv.Warm(ctx)
	if *compaction {
		go srv.Compact(ctx)
	}
	hs := &http.Server{Addr: *listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = hs.Close()
	}()
	fmt.Fprintln(os.Stderr, "motte-loadtest: serving on", *listen)
	if err := hs.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
