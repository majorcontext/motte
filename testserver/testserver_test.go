package testserver_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/meta/sqlite"
	"github.com/majorcontext/motte/testserver"
)

func TestLimitsLowerTheObjectLimit(t *testing.T) {
	t.Parallel()
	s := testserver.New(t, testserver.WithLimits(testserver.Limits{Object: 256 << 10}))
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
	s := testserver.New(t, testserver.Template(path))
	if _, err := s.Client(s.OperatorGrant(t)).CreateRepo(context.Background(), "widgets", "o1"); err != nil {
		t.Fatal(err)
	}
}
