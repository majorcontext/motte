package server

import (
	"context"
	"time"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/store"
)

// CompactionConfig tunes Compact.
type CompactionConfig struct {
	// Interval is the wait between passes over the repositories. Default 30 seconds.
	Interval time.Duration
	// Grace is how long a retired pack or layer file stays before it is deleted. Default one hour.
	Grace time.Duration
}

func (c CompactionConfig) withDefaults() CompactionConfig {
	if c.Interval <= 0 {
		c.Interval = 30 * time.Second
	}
	if c.Grace <= 0 {
		c.Grace = store.DefaultGrace
	}
	return c
}

// Compact runs maintenance of the packs and layers of every repository until ctx ends: it compacts packs, merges
// layers and deletes retired files. Run it on the writer only. A pass that fails is logged and tried again at the next
// pass.
func (s *Server) Compact(ctx context.Context) {
	for {
		s.compactPass(ctx)
		if clock.Wait(ctx, s.clock, s.compaction.Interval) != nil {
			return
		}
	}
}

func (s *Server) compactPass(ctx context.Context) {
	all, err := s.index.ManifestRepos(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "maintenance could not list repositories", "error", logSafe(err))
		}
		return
	}
	for _, text := range all {
		repo, err := store.ParseRepo(text)
		if err != nil {
			continue
		}
		if err := s.objects.Maintain(ctx, repo); err != nil && ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "maintenance of a repository failed", "repo", text, "error", logSafe(err))
		}
	}
}
