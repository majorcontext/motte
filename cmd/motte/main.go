// Command motte is the store service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/bucket/gcs"
	"github.com/majorcontext/motte/bucket/s3"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/postgres"
	"github.com/majorcontext/motte/meta/sqlite"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("motte stopped", "error", err)
		os.Exit(1)
	}
}

func openBucket(ctx context.Context, c config) (bucket.Store, error) {
	if c.bucketKind == "fs" {
		root, err := fs.New(c.bucket)
		if err != nil {
			return nil, err
		}
		return root.Bucket("motte"), nil
	}
	if c.bucketKind == "s3" {
		client, err := s3.NewClient(ctx, c.s3)
		if err != nil {
			return nil, err
		}
		return s3.New(client, c.bucket), nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return gcs.New(client, c.bucket), nil
}

func openMeta(ctx context.Context, c config) (meta.Store, func(), error) {
	if c.meta == "sqlite" {
		db, err := sqlite.Open(ctx, c.sqlitePath)
		if err != nil {
			return nil, nil, err
		}
		return db, func() { _ = db.Close() }, nil
	}
	db, err := postgres.Open(ctx, c.databaseURL)
	if err != nil {
		return nil, nil, err
	}
	return db, db.Close, nil
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.otlp {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return err
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
		otel.SetTracerProvider(tp)
		defer func() { _ = tp.Shutdown(context.WithoutCancel(ctx)) }()
	}
	otel.SetTextMapPropagator(propagation.TraceContext{})

	db, closeMeta, err := openMeta(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeMeta()
	bucket, err := openBucket(ctx, cfg)
	if err != nil {
		return err
	}
	srv := server.New(server.Config{Meta: db, Bucket: bucket, Verifier: grant.NewVerifier(cfg.keys...), Logger: log, Events: server.EventsConfig{Retention: cfg.retention}})
	workCtx, stopWork := context.WithCancel(ctx)
	workers := make(chan struct{})
	go func() { _ = srv.Run(workCtx); close(workers) }()
	defer func() { stopWork(); <-workers }()
	hs := &http.Server{Addr: cfg.listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("motte listening", "addr", cfg.listen)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := hs.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
