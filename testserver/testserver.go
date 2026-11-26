// Package testserver starts a real motte server in the process of a caller's test.
package testserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/testcount"
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
	// Meta, Bucket and Verifier are the state and the keys of the server, for a test that runs a second replica on them
	// or sets up a state no route can make.
	Meta     meta.Store
	Bucket   bucket.Store
	Verifier *grant.Verifier
	// Operator is the control-plane signer.
	Operator grant.Signer
	// Node is the node signer.
	Node grant.Signer
	// Expire is the expiry of the grants that the helpers mint.
	Expire time.Time
}

// Limits lowers the size limits of one test, so that it crosses the same boundaries with small bodies. A zero field
// keeps the limit. The limits are process-wide and return when the test ends, so a test that sets them must not run in
// parallel with another test of the package that sends large bodies.
type Limits struct {
	// Object is the largest object that a put accepts, in bytes.
	Object int
	// Window bounds the JSON text that the decoder holds for one object, in bytes.
	Window int64
	// TreeFirst is the first reservation of a tree read, and TreeFixed the fixed part of every later one, in bytes.
	TreeFirst, TreeFixed int64
}

type options struct {
	postgres bool
	dsn      string
	required bool
	template string
	limits   *Limits
	change   func(*server.Config)
}

// Option changes how New starts a server.
type Option func(*options)

// WithPostgres runs the server on a fresh schema of the PostgreSQL database at dsn. An empty dsn is read from DSNEnv,
// and then from the throwaway cluster of the package. Without a database the test skips, unless Required is set.
func WithPostgres(dsn string) Option { return func(o *options) { o.postgres, o.dsn = true, dsn } }

// Required makes a missing PostgreSQL database fail the test instead of skipping it.
func Required() Option { return func(o *options) { o.required = true } }

// Template starts a SQLite server from a copy of the migrated database file at path. It has no effect on PostgreSQL.
func Template(path string) Option { return func(o *options) { o.template = path } }

// WithLimits lowers the size limits of the server for the test.
func WithLimits(l Limits) Option { return func(o *options) { o.limits = &l } }

// New runs a server on a temporary SQLite database and a temporary filesystem bucket, or on PostgreSQL with
// WithPostgres.
func New(t testing.TB, opts ...Option) *Server {
	t.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.limits != nil {
		server.LowerLimits(t, o.limits.Object, o.limits.Window, o.limits.TreeFirst, o.limits.TreeFixed)
	}
	if o.postgres {
		pool := pgtest.Pool
		if o.required {
			pool = pgtest.RequiredPool
		}
		p := pool(t, o.dsn)
		s := run(t, &postgres.Store{Pool: p}, o.change)
		s.Pool = p
		return s
	}
	return startSQLite(t, o)
}

// Start runs a server on a fresh schema of the database at dsn and a temporary
// filesystem bucket. An empty dsn is read from DSNEnv. When that is empty too,
// Start skips the test. Cleanup drops the schema.
func Start(t testing.TB, dsn string) *Server {
	t.Helper()
	pool := pgtest.Pool(t, dsn)
	s := run(t, &postgres.Store{Pool: pool}, nil)
	s.Pool = pool
	return s
}

// StartLocal runs a server on a temporary SQLite database and a temporary
// filesystem bucket. It needs no other service and never skips. Pool is nil.
func StartLocal(t testing.TB) *Server { return StartLocalWith(t, nil) }

// StartLocalWith is StartLocal with a change to the server configuration. A nil change leaves it as is.
func StartLocalWith(t testing.TB, change func(*server.Config)) *Server {
	t.Helper()
	return startSQLite(t, options{change: change})
}

func startSQLite(t testing.TB, o options) *Server {
	t.Helper()
	file := filepath.Join(t.TempDir(), "meta.db")
	if o.template != "" {
		raw, err := os.ReadFile(o.template)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sqlite.Open(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return run(t, db, o.change)
}

func run(t testing.TB, store meta.Store, change func(*server.Config)) *Server {
	t.Helper()
	testcount.Add(t, testcount.Servers)
	root, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cpPub, cpKey, _ := ed25519.GenerateKey(rand.Reader)
	nodePub, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	bkt := root.Bucket("motte")
	verifier := grant.NewVerifier(
		grant.Key{Kid: "cp", Public: cpPub, Scopes: []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeWrite, grant.ScopeImport, grant.ScopeOperator, grant.ScopeMerge}},
		grant.Key{Kid: "node", Public: nodePub, Scopes: []grant.Scope{grant.ScopeNode}})
	cfg := server.Config{Meta: store, Bucket: bkt, Verifier: verifier}
	if change != nil {
		change(&cfg)
	}
	srv := server.New(cfg)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(srv.Close)
	t.Cleanup(ts.Close)
	return &Server{
		URL: ts.URL, Meta: store, Bucket: bkt, Verifier: verifier, Operator: grant.Signer{Kid: "cp", Key: cpKey}, Node: grant.Signer{Kid: "node", Key: nodeKey},
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

// MergeGrant mints a merge grant.
func (s *Server) MergeGrant(t testing.TB, repo string) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeMerge, Repo: repo})
}

// Client returns a client that sends token on every call.
func (s *Server) Client(token string, opts ...client.Option) *client.Client {
	return client.New(s.URL, client.StaticGrant(token), opts...)
}
