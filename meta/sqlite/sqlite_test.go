package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/majorcontext/motte/internal/meta/metatest"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/sqlite"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	metatest.Run(t, func(t *testing.T) meta.Store {
		s, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "meta.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestReopenKeepsTheState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	for epoch := uint64(1); epoch <= 2; epoch++ {
		s, err := sqlite.Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if w, err := s.Claim(ctx, "ws1", epoch, "o1", "gh:acme/widgets"); err != nil || w.Epoch != epoch {
			t.Fatalf("claim %d = %+v, %v", epoch, w, err)
		}
		_ = s.Close()
	}
	m, err := sqlite.Open(ctx, sqlite.Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
