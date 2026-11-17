package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/metatest"
	"github.com/majorcontext/motte/meta/postgres"
)

func TestConformance(t *testing.T) {
	metatest.Run(t, func(t *testing.T) meta.Store { return &postgres.Store{Pool: pgtest.Pool(t, "")} })
}

func TestAWriteDoesNotWaitForAnotherPartition(t *testing.T) {
	pool := pgtest.Pool(t, "")
	s := &postgres.Store{Pool: pool}
	const repo = "gh:acme/widgets"
	held := meta.Partition(repo, "held")
	free := "free"
	for i := 0; meta.Partition(repo, free) == held; i++ {
		free = fmt.Sprint("free", i)
	}
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE event_partitions SET seq = seq + 1 WHERE partition = $1`, held); err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := s.MergeRef(done, repo, free, 0, "1111111111111111111111111111111111111111"); err != nil {
		t.Fatalf("a write of another partition waited for a held partition counter: %v", err)
	}
}
