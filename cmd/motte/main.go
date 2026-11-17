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
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/objstore"
	"github.com/majorcontext/motte/internal/objstore/fs"
	"github.com/majorcontext/motte/internal/objstore/gcs"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("motte stopped", "error", err)
		os.Exit(1)
	}
}

func openBucket(ctx context.Context, c config) (objstore.Store, error) {
	if c.bucketKind == "fs" {
		root, err := fs.New(c.bucket)
		if err != nil {
			return nil, err
		}
		return root.Bucket("motte"), nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return gcs.New(client, c.bucket), nil
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

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		return err
	}
	bucket, err := openBucket(ctx, cfg)
	if err != nil {
		return err
	}
	db := &pg.DB{Pool: pool}
	srv := &server.Server{DB: db, Objects: store.New(bucket, db), Verifier: grant.NewVerifier(cfg.keys...), Logger: log}
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
