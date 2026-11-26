package server

import (
	"context"
	"sync"

	"github.com/majorcontext/motte/meta"
)

// landHub owns the leaders of this replica. Close ends them and waits for them, so no leader reads the metadata store
// after the caller closes it.
type landHub struct {
	mu      sync.Mutex
	leaders map[meta.LandKey]*leader
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

func newLandHub() *landHub {
	ctx, cancel := context.WithCancel(context.Background())
	return &landHub{leaders: map[meta.LandKey]*leader{}, ctx: ctx, cancel: cancel}
}

// start runs run for a new leader of key, unless the hub is closed. The caller holds h.mu.
func (h *landHub) start(key meta.LandKey, l *leader, run func(ctx context.Context)) {
	if h.closed {
		return
	}
	h.leaders[key] = l
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		run(h.ctx)
	}()
}

// Close cancels every leader and waits until each has returned. A leader that is in a batch ends the batch with the
// error of the canceled context and leaves its items for the next leader.
func (h *landHub) Close() {
	h.mu.Lock()
	h.closed = true
	h.cancel()
	h.mu.Unlock()
	h.wg.Wait()
}

// Close stops the leaders of lands and waits for them. Call it after the server stops taking requests and before the
// metadata store closes.
func (s *Server) Close() { s.hub.Close() }
