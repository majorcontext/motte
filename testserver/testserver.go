// Package testserver starts a real motte server in the process of a caller's test.
package testserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/postgres"
	"github.com/majorcontext/motte/meta/sqlite"
)

// DSNEnv is the environment variable that Start reads when it gets no DSN.
const DSNEnv = pgtest.DSNEnv

// Server is a running store. Its control-plane key signs every scope. Its node key signs only node grants.
type Server struct {
	URL string
	// Pool reaches the schema of the server, for a test that sets up a state no route can make. StartLocal leaves it nil.
	Pool *pgxpool.Pool
	// Operator is the control-plane signer.
	Operator grant.Signer
	// Node is the node signer.
	Node grant.Signer
	// Expire is the expiry of the grants that the helpers mint.
	Expire time.Time
}

// Start runs a server on a fresh schema of the database at dsn and a temporary
// filesystem bucket. An empty dsn is read from DSNEnv. When that is empty too,
// Start skips the test. Cleanup drops the schema.
func Start(t testing.TB, dsn string) *Server {
	t.Helper()
	pool := pgtest.Pool(t, dsn)
	s := run(t, &postgres.Store{Pool: pool})
	s.Pool = pool
	return s
}

// StartLocal runs a server on a temporary SQLite database and a temporary
// filesystem bucket. It needs no other service and never skips. Pool is nil.
func StartLocal(t testing.TB) *Server {
	t.Helper()
	db, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return run(t, db)
}

func run(t testing.TB, store meta.Store) *Server {
	t.Helper()
	root, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cpPub, cpKey, _ := ed25519.GenerateKey(rand.Reader)
	nodePub, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	srv := server.New(server.Config{
		Meta: store, Bucket: root.Bucket("motte"),
		Verifier: grant.NewVerifier(
			grant.Key{Kid: "cp", Public: cpPub, Scopes: []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeWrite, grant.ScopeImport, grant.ScopeOperator}},
			grant.Key{Kid: "node", Public: nodePub, Scopes: []grant.Scope{grant.ScopeNode}}),
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &Server{
		URL: ts.URL, Operator: grant.Signer{Kid: "cp", Key: cpKey}, Node: grant.Signer{Kid: "node", Key: nodeKey},
		Expire: time.Now().Add(time.Hour),
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

// WriteGrant mints a write grant.
func (s *Server) WriteGrant(t testing.TB, repo string) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeWrite, Repo: repo})
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
