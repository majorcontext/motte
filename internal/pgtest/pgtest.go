// Package pgtest gives a test a pool on a private migrated schema.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/meta/postgres"
)

// DSNEnv is the environment variable that Pool reads when it gets no DSN.
const DSNEnv = "MOTTE_TEST_DATABASE_URL"

// Pool creates a fresh schema in the database at dsn, applies the migrations and returns a pool on it. An empty dsn
// is read from DSNEnv. When that is empty too, Pool skips the test. Cleanup drops the schema.
func Pool(t testing.TB, dsn string) *pgxpool.Pool {
	t.Helper()
	if dsn == "" {
		dsn = os.Getenv(DSNEnv)
	}
	if dsn == "" {
		t.Skip(DSNEnv + " is not set: the test needs a PostgreSQL database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var raw [6]byte
	_, _ = rand.Read(raw[:])
	schema := "t_" + hex.EncodeToString(raw[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}
