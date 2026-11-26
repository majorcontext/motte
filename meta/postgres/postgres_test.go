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
	t.Parallel()
	metatest.Run(t, func(t *testing.T) meta.Store { return &postgres.Store{Pool: pgtest.Pool(t, "")} })
}

func TestAWriteDoesNotWaitForAnotherPartition(t *testing.T) {
	t.Parallel()
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

func TestAStalledClaimDoesNotReopenADoneLand(t *testing.T) {
	t.Parallel()
	pool := pgtest.Pool(t, "")
	s := &postgres.Store{Pool: pool}
	const repo, ref, owner = "gh:acme/widgets", "main", "o1"
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	if _, err := s.EnqueueLand(ctx, meta.LandRequest{ID: "a", Repo: repo, Ref: ref, Base: "1", Head: "2", Kind: meta.WriterMerge}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireLandLease(ctx, repo, ref, owner, now, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE land_queue SET state = 'done' WHERE repo = $1 AND id = 'a'`, repo); err != nil {
		t.Fatal(err)
	}
	type claim struct {
		items []meta.LandItem
		err   error
	}
	claimed := make(chan claim, 1)
	go func() {
		items, err := s.ClaimLands(ctx, repo, ref, owner, now, 10)
		claimed <- claim{items, err}
	}()
	for waiting := false; !waiting; {
		select {
		case c := <-claimed:
			t.Fatalf("the claim ended before the row lock was released: %+v", c)
		default:
		}
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%UPDATE land_queue q%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if c := <-claimed; c.err != nil || len(c.items) != 0 {
		t.Fatalf("claim = %+v, want no items: the land was done", c)
	}
	if it, err := s.LandItem(ctx, repo, "a"); err != nil || it.State != meta.LandStateDone {
		t.Fatalf("item = %+v, %v, want it done", it, err)
	}
}
