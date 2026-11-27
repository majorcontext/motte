package testserver_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/meta/sqlite"
	"github.com/majorcontext/motte/testserver"
)

func TestLimitsLowerTheObjectLimit(t *testing.T) {
	t.Parallel()
	s := testserver.Start(t, testserver.WithLimits(testserver.Limits{Object: 256 << 10}))
	ctx := context.Background()
	if _, err := s.Client(s.OperatorGrant(t)).Claim(ctx, "ws1", api.ClaimRequest{Epoch: 1, Owner: "o1", Repo: "gh:acme/widgets"}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Client(s.NodeGrant(t, "ws1", 1, "gh:acme/widgets")).PutObjects(ctx, []client.Object{{Type: "blob", Data: make([]byte, 256<<10+1)}})
	if !errors.Is(err, client.ErrObjectTooLarge) {
		t.Fatalf("put over the lowered limit = %v, want ErrObjectTooLarge", err)
	}
}

func TestTemplateStartsFromAMigratedCopy(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "template.db")
	db, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := testserver.Start(t, testserver.Template(path))
	if _, err := s.Client(s.OperatorGrant(t)).CreateRepo(context.Background(), "widgets", "o1"); err != nil {
		t.Fatal(err)
	}
}

func claim(t *testing.T, s *testserver.Server, repo string) {
	t.Helper()
	if _, err := s.Client(s.OperatorGrant(t)).Claim(context.Background(), "ws1", api.ClaimRequest{Epoch: 1, Owner: "o1", Repo: repo}); err != nil {
		t.Fatal(err)
	}
}

func TestAReplicaServesTheStateOfTheServer(t *testing.T) {
	t.Parallel()
	s := testserver.Start(t, testserver.WithReplicas(1))
	claim(t, s, "gh:acme/widgets")
	if len(s.Replicas) != 1 {
		t.Fatalf("replicas = %v, want one URL", s.Replicas)
	}
	node := client.New(s.Replicas[0], client.StaticGrant(s.NodeGrant(t, "ws1", 1, "gh:acme/widgets")))
	if _, err := node.PutObjects(context.Background(), []client.Object{{Type: "blob", Data: []byte("x")}}); err != nil {
		t.Fatalf("put through the replica of a claim made on the first server: %v", err)
	}
}

func TestFaultsFailTheCallsThatTheyName(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	s := testserver.Start(t, testserver.WithFaults(testserver.Faults{BucketCreate: func(context.Context, string) error {
		called.Store(true)
		return errors.New("bucket is down")
	}}))
	claim(t, s, "gh:acme/widgets")
	_, err := s.Client(s.NodeGrant(t, "ws1", 1, "gh:acme/widgets")).PutObjects(context.Background(), []client.Object{{Type: "blob", Data: []byte("x")}})
	if err == nil || !called.Load() {
		t.Fatalf("put with a failing bucket = %v (hook called %v), want an error", err, called.Load())
	}
}

func TestTheSharedServerServesThePackage(t *testing.T) {
	t.Parallel()
	s := testserver.Package(t)
	claim(t, s, "gh:acme/shared")
	if _, err := s.Client(s.NodeGrant(t, "ws1", 1, "gh:acme/shared")).PutObjects(context.Background(), []client.Object{{Type: "blob", Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
}

func TestThePackageServerIsOneServerForEveryTest(t *testing.T) {
	t.Parallel()
	first := testserver.Package(t)
	t.Run("again", func(t *testing.T) {
		t.Parallel()
		if testserver.Package(t) != first {
			t.Fatal("Package returned another server")
		}
	})
}

func TestPruneEventsRefusesAnOlderCursorOnAnInMemoryServer(t *testing.T) {
	t.Parallel()
	s := testserver.Start(t, testserver.InMemory())
	claim(t, s, "gh:acme/widgets")
	ctx := context.Background()
	op := s.Client(s.OperatorGrant(t))
	if _, err := s.Client(s.MergeGrant(t, "gh:acme/widgets")).SwapRef(ctx, "a", 0, "0123456789012345678901234567890123456789"); err != nil {
		t.Fatal(err)
	}
	if evs, _, err := op.Events(ctx, "", 0); err != nil || len(evs) != 1 {
		t.Fatalf("events = %v, %v, want one", evs, err)
	}
	s.PruneEvents(t, time.Now().Add(time.Hour))
	if _, _, err := op.Events(ctx, "", 0); !errors.Is(err, client.ErrEventsPruned) {
		t.Fatalf("events after prune = %v, want ErrEventsPruned", err)
	}
}
