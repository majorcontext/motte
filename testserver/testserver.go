// Package testserver starts a real motte server in the process of a caller's test.
package testserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/objstore/fs"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/store"
)

// DSNEnv is the environment variable that Start reads when it gets no DSN.
const DSNEnv = "MOTTE_TEST_DATABASE_URL"

// Server is a running store. Its control-plane key signs every scope. Its node key signs only node grants.
type Server struct {
	URL string
	// Operator is the control-plane signer.
	Operator grant.Signer
	// Node is the node signer.
	Node grant.Signer
	// Pool is the connection pool of the private schema of the server.
	Pool *pgxpool.Pool
	// Expire is the expiry of the grants that the helpers mint.
	Expire time.Time
}

// Start runs a server on a fresh schema of the database at dsn and a temporary
// filesystem bucket. An empty dsn is read from DSNEnv. When that is empty too,
// Start skips the test. Cleanup drops the schema.
func Start(t testing.TB, dsn string) *Server {
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
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	root, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cpPub, cpKey, _ := ed25519.GenerateKey(rand.Reader)
	nodePub, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	db := &pg.DB{Pool: pool}
	srv := &server.Server{
		DB: db, Objects: store.New(root.Bucket("motte"), db),
		Verifier: grant.NewVerifier(
			grant.Key{Kid: "cp", Public: cpPub, Scopes: []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeImport, grant.ScopeOperator}},
			grant.Key{Kid: "node", Public: nodePub, Scopes: []grant.Scope{grant.ScopeNode}}),
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &Server{
		URL: ts.URL, Operator: grant.Signer{Kid: "cp", Key: cpKey}, Node: grant.Signer{Kid: "node", Key: nodeKey},
		Pool: pool, Expire: time.Now().Add(time.Hour),
	}
}

// Mint signs c with signer and sets the expiry of the server when c has none.
func (s *Server) Mint(t testing.TB, signer grant.Signer, c grant.Claims) string {
	t.Helper()
	if c.Exp == 0 {
		c.Exp = s.Expire.Unix()
	}
	tok, err := signer.Mint(c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// OperatorGrant mints an operator grant.
func (s *Server) OperatorGrant(t testing.TB) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeOperator})
}

// NodeGrant mints a node grant with the node key.
func (s *Server) NodeGrant(t testing.TB, workspace string, epoch uint64, repo string) string {
	t.Helper()
	return s.Mint(t, s.Node, grant.Claims{Scope: grant.ScopeNode, WS: workspace, Epoch: epoch, Repo: repo})
}

// ReadGrant mints a read grant.
func (s *Server) ReadGrant(t testing.TB, repo string) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeRead, Repo: repo})
}

// ImportGrant mints an import grant.
func (s *Server) ImportGrant(t testing.TB, repo string) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeImport, Repo: repo})
}

// Client returns a client that sends token on every call.
func (s *Server) Client(token string, opts ...client.Option) *client.Client {
	return client.New(s.URL, client.StaticGrant(token), opts...)
}
