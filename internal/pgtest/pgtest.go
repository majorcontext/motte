// Package pgtest gives a test a pool on a private migrated schema.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/internal/meta/postgres"
	"github.com/majorcontext/tools/testcount"
)

// DSNEnv is the environment variable that Pool reads when it gets no DSN.
const DSNEnv = "MOTTE_TEST_DATABASE_URL"

// Pool creates a fresh schema in the database at dsn, applies the migrations and returns a pool on it. An empty dsn
// is read from DSNEnv. When that is empty too, Pool starts one throwaway cluster for the package from the binaries
// of BinEnv or PATH. Without binaries it skips the test, or fails it when RequireEnv is 1. Cleanup drops the schema.
func Pool(t testing.TB, dsn string) *pgxpool.Pool { return pool(t, dsn, false) }

// RequiredPool is Pool, but it fails the test, not skips it, when no database is available.
func RequiredPool(t testing.TB, dsn string) *pgxpool.Pool { return pool(t, dsn, true) }

func pool(t testing.TB, dsn string, required bool) *pgxpool.Pool {
	t.Helper()
	if dsn == "" {
		dsn = os.Getenv(DSNEnv)
	}
	if dsn == "" {
		var err error
		if dsn, err = startCluster(t); err != nil {
			t.Fatal(err)
		}
	}
	if dsn == "" {
		missingDatabase(t, required)
	}
	testcount.Add(t, testcount.Schemas)
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
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if err := postgres.Migrate(ctx, p); err != nil {
		t.Fatal(err)
	}
	return p
}
