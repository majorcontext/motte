package server

import (
	"context"
	"time"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/repos"
	"github.com/majorcontext/motte/internal/store"
)

const (
	warmParallel = 16
	warmRetry    = 5 * time.Second
)

// Warm opens the pack index of every repo, so that the first object read does not fetch an index. It retries until
// it succeeds or ctx ends, and /ready answers 503 until then.
func (s *Server) Warm(ctx context.Context) {
	for {
		err := s.warm(ctx)
		if err == nil {
			s.cold.Store(false)
			return
		}
		s.logger.Error("pack index warm-up failed", "error", err)
		if clock.Wait(ctx, s.clock, warmRetry) != nil {
			return
		}
	}
}

func (s *Server) warm(ctx context.Context) error {
	all, err := s.index.Repos(ctx)
	if err != nil {
		return err
	}
	rs := make([]store.Repo, 0, len(all))
	for _, r := range all {
		repo, err := store.ParseRepo(repos.Text(r.Path))
		if err != nil {
			return err
		}
		rs = append(rs, repo)
	}
	return s.objects.Warm(ctx, rs, warmParallel)
}
