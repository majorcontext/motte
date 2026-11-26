package server

import (
	"context"
	"time"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/store"
)

const (
	warmParallel = 16
	warmRetry    = 5 * time.Second
)

// Warm first makes the manifest of every repo name every pack that the pack list holds, and then opens the pack index
// of every repo, so that the first object read does not fetch an index. It retries the second step until it succeeds
// or ctx ends, and /ready answers 503 until then. A repo that adoption cannot repair is logged and does not stop adoption of the others.
func (s *Server) Warm(ctx context.Context) {
	s.adoptPacks(ctx)
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
	all, err := s.index.PackRepos(ctx)
	if err != nil {
		return err
	}
	rs := make([]store.Repo, 0, len(all))
	for _, text := range all {
		repo, err := store.ParseRepo(text)
		if err != nil {
			return err
		}
		rs = append(rs, repo)
	}
	return s.objects.Warm(ctx, rs, warmParallel)
}

// adoptPacks makes the manifest of each repository name every pack that the pack list holds, and logs the number of
// layers that it added. A repository that fails is logged and counted; the next start tries it again.
func (s *Server) adoptPacks(ctx context.Context) {
	all, err := s.index.PackRepos(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "pack list adoption could not list repositories", "error", logSafe(err))
		return
	}
	var added, dropped, failed int
	for _, text := range all {
		repo, err := store.ParseRepo(text)
		if err == nil {
			var a, d int
			a, d, err = s.objects.AdoptPacks(ctx, repo)
			added += a
			dropped += d
		}
		if err != nil && ctx.Err() == nil {
			failed++
			s.logger.ErrorContext(ctx, "pack list adoption of a repository failed", "repo", text, "error", logSafe(err))
		}
	}
	s.logger.InfoContext(ctx, "pack list adoption finished", "repos", len(all), "layers_added", added, "dropped", dropped, "failed", failed)
}
