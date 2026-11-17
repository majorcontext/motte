package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/pgtest"
)

const (
	repoText = "gh:acme/widgets"
	commitA  = "1111111111111111111111111111111111111111"
	commitB  = "2222222222222222222222222222222222222222"
)

// The routes check the epoch before a write; this reaches the write statements
// as a request that passed that check and then lost the race would.
func TestWriteStatementsRefuseAStaleEpochOnTheirOwn(t *testing.T) {
	ctx := context.Background()
	db := &pg.DB{Pool: pgtest.Pool(t, "")}
	if _, err := db.Claim(ctx, "ws1", 2, "o1", repoText); err != nil {
		t.Fatal(err)
	}
	ref, err := db.SwapRef(ctx, "ws1", 2, repoText, "keep", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SwapOpHeads(ctx, "ws1", 2, repoText, 0, []string{"aaaaaaaaaaaaaaaa"}); err != nil {
		t.Fatal(err)
	}
	writes := map[string]func() error{
		"ref create": func() error { _, err := db.SwapRef(ctx, "ws1", 1, repoText, "new", 0, commitA); return err },
		"ref swap": func() error {
			_, err := db.SwapRef(ctx, "ws1", 1, repoText, "keep", ref.Generation, commitB)
			return err
		},
		"ref delete": func() error { return db.DeleteRef(ctx, "ws1", 1, repoText, "keep", ref.Generation) },
		"op heads": func() error {
			_, err := db.SwapOpHeads(ctx, "ws1", 1, repoText, 1, []string{"bbbbbbbbbbbbbbbb"})
			return err
		},
		"first heads": func() error {
			_, err := db.SwapOpHeads(ctx, "ws1", 1, repoText, 0, []string{"bbbbbbbbbbbbbbbb"})
			return err
		},
		"report": func() error { return db.Report(ctx, "ws1", 1, repoText, 1, "", "aaaaaaaaaaaaaaaa", commitA) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			if err := write(); !errors.Is(err, pg.ErrClaimLost) {
				t.Errorf("error = %v, want pg.ErrClaimLost, not a generation conflict", err)
			}
		})
	}
}
