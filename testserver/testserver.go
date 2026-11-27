// Package testserver starts a real motte server in the process of a caller's test.
package testserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/bucket/fs"
	"github.com/majorcontext/motte/internal/bucket/mem"
	"github.com/majorcontext/motte/internal/meta/postgres"
	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/internal/server"
	mstore "github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/sqlite"
	"github.com/majorcontext/tools/testcount"
)

// DSNEnv is the environment variable that Start reads when it gets no DSN.
const DSNEnv = pgtest.DSNEnv

// Server is a running store. Its control-plane key signs every scope. Its node key signs only node grants.
type Server struct {
	URL string
	// Pool reaches the schema of the server, for a test that sets up a state no route can make. StartLocal leaves it nil.
	Pool *pgxpool.Pool
	// Replicas are the URLs of the extra servers that WithReplicas started on the state and keys of this one.
	Replicas []string
	// Meta, Bucket and Verifier are the state and the keys of the server, for a test that sets up a state no route can
	// make or runs a server with a configuration that no option sets.
	//
	// Deprecated: use WithReplicas and WithFaults. A caller outside this module still reads Meta.
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
// keeps the limit. The limits belong to the one server.
type Limits struct {
	// Object is the largest object that a put accepts, in bytes.
	Object int
	// Window bounds the JSON text that the decoder holds for one object, in bytes.
	Window int64
	// TreeFirst is the first reservation of a tree read, and TreeFixed the fixed part of every later one, in bytes.
	TreeFirst, TreeFixed int64
	// FetchLarge is the reservation above which a fetch takes one of the FetchSlots slots of large fetches.
	FetchLarge int64
	FetchSlots int
}

type options struct {
	postgres bool
	dsn      string
	required bool
	template string
	memory   bool
	limits   *Limits
	replicas int
	faults   *Faults
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

// InMemory keeps the metadata and the objects of a SQLite server in memory, so no commit waits for a disk. It cannot
// be combined with WithPostgres, and it makes Template redundant: the database starts from the migrated image.
func InMemory() Option { return func(o *options) { o.memory = true } }

// WithLimits lowers the size limits of the server for the test.
func WithLimits(l Limits) Option { return func(o *options) { o.limits = &l } }

// Faults makes the server fail or wait on the calls that tests need to break. A nil field changes nothing. A function
// runs before its call, and a non-nil error fails the call; a function that blocks holds the call.
type Faults struct {
	// FinishLands runs before the leader records the results of a batch of lands.
	FinishLands func(ctx context.Context) error
	// ProtectedRefs runs before the server reads the protected ref patterns of a repository.
	ProtectedRefs func(ctx context.Context) error
	// BucketGet runs before the server reads the object at key, whole or in a range.
	BucketGet func(ctx context.Context, key string) error
	// BucketCreate runs before the server writes the object at key.
	BucketCreate func(ctx context.Context, key string) error
}

// WithIdentity makes the server accept the identity verifier v and the access rules.
func WithIdentity(v identity.Verifier, rules identity.Rules) Option {
	return func(o *options) {
		o.change = func(c *server.Config) { c.Identity, c.Rules = v, rules }
	}
}

// WithFaults applies faults to the server and its replicas. Server.Meta and Server.Bucket stay without them.
func WithFaults(f Faults) Option { return func(o *options) { o.faults = &f } }

// WithReplicas starts n more servers on the state and keys of the first, each with its own instance name. Their URLs
// are Server.Replicas.
func WithReplicas(n int) Option { return func(o *options) { o.replicas = n } }

// Start runs a server on a temporary SQLite database and a temporary filesystem bucket, or on PostgreSQL with
// WithPostgres.
func Start(t testing.TB, opts ...Option) *Server {
	t.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if l := o.limits; l != nil {
		prev := o.change
		o.change = func(c *server.Config) {
			if prev != nil {
				prev(c)
			}
			c.Limits = server.Limits{Object: l.Object, DecodeWindow: l.Window, TreeReserve: l.TreeFirst, TreeSlack: l.TreeFixed, FetchLarge: l.FetchLarge, FetchSlots: l.FetchSlots}
		}
	}
	if o.postgres && o.memory {
		t.Fatal("testserver: InMemory and WithPostgres exclude each other")
	}
	if o.postgres {
		pool := pgtest.Pool
		if o.required {
			pool = pgtest.RequiredPool
		}
		p := pool(t, o.dsn)
		s := run(t, &postgres.Store{Pool: p}, o)
		s.Pool = p
		return s
	}
	return startSQLite(t, o)
}

// StartLocal runs a server on a temporary SQLite database and a temporary filesystem bucket. It needs no other
// service and never skips. Pool is nil.
//
// Deprecated: use Start, which runs on SQLite by default. A caller outside this module still calls StartLocal.
func StartLocal(t testing.TB) *Server { return startSQLite(t, options{}) }

func startSQLite(t testing.TB, o options) *Server {
	t.Helper()
	if o.memory {
		db, err := sqlite.Open(context.Background(), sqlite.Memory)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return run(t, db, o)
	}
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
	return run(t, db, o)
}

func run(t testing.TB, store meta.Store, o options) *Server {
	t.Helper()
	var bkt bucket.Store = mem.New()
	if !o.memory {
		root, err := fs.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		bkt = root.Bucket("motte")
	}
	cpPub, cpKey, _ := ed25519.GenerateKey(rand.Reader)
	nodePub, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	verifier := grant.NewVerifier(
		grant.Key{Kid: "cp", Public: cpPub, Scopes: []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeWrite, grant.ScopeImport, grant.ScopeOperator, grant.ScopeMerge}},
		grant.Key{Kid: "node", Public: nodePub, Scopes: []grant.Scope{grant.ScopeNode}})
	build := func(instance string) *httptest.Server {
		testcount.Add(t, testcount.Servers)
		cfg := server.Config{Meta: store, Bucket: bkt, Verifier: verifier, Events: server.EventsConfig{Instance: instance}, RangeCacheBytes: mstore.DefaultRangeCacheBytes}
		if f := o.faults; f != nil {
			cfg.Meta, cfg.Bucket = faultMeta{store, *f}, faultBucket{bkt, *f}
		}
		if o.change != nil {
			o.change(&cfg)
		}
		srv := server.New(cfg)
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(srv.Close)
		t.Cleanup(ts.Close)
		return ts
	}
	s := &Server{
		Meta: store, Bucket: bkt, Verifier: verifier, Operator: grant.Signer{Kid: "cp", Key: cpKey}, Node: grant.Signer{Kid: "node", Key: nodeKey},
		Expire: time.Now().Add(time.Hour),
	}
	s.URL = build("").URL
	for i := range o.replicas {
		s.Replicas = append(s.Replicas, build(fmt.Sprintf("replica-%d", i+1)).URL)
	}
	return s
}

// PruneEvents removes the events that the server wrote before the time, as retention does, for a test that needs a
// cursor that the server no longer holds.
func (s *Server) PruneEvents(t testing.TB, before time.Time) {
	t.Helper()
	if err := s.Meta.PruneEvents(context.Background(), before); err != nil {
		t.Fatal(err)
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

// MergeGrant mints a merge grant that may move any ref.
func (s *Server) MergeGrant(t testing.TB, repo string) string {
	t.Helper()
	return s.Mint(t, s.Operator, grant.Claims{Scope: grant.ScopeMerge, Repo: repo, Refs: []string{"*"}})
}

// Client returns a client that sends token on every call.
func (s *Server) Client(token string, opts ...client.Option) *client.Client {
	return client.New(s.URL, client.StaticGrant(token), opts...)
}
