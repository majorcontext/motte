package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/meta"
)

// EventsConfig tunes the delivery and the retention of ref events. A zero field takes its default.
type EventsConfig struct {
	// Retention is how long an event is kept. Default 14 days.
	Retention time.Duration
	// Poll is the wait between reads of an idle subscription. Default 1 second.
	Poll time.Duration
	// Lease is the time that one replica holds a subscription before it must renew. Default 30 seconds. The clocks
	// of the replicas must differ by much less.
	Lease time.Duration
	// Timeout is the time limit of one delivery request. Default 10 seconds.
	Timeout time.Duration
	// MaxBackoff is the longest wait between two attempts of one event. Default 5 minutes.
	MaxBackoff time.Duration
	// Instance names the replica in the lease. Default random.
	Instance string
	// HTTP sends the deliveries. It follows no redirect. Default a new client.
	HTTP *http.Client
	// Clock times every wait and every lease. Default the real clock.
	Clock clock.Clock
}

const (
	pruneEvery  = time.Hour
	batchSize   = 100
	backoffBase = time.Second
	drainMax    = 64 << 10
)

func (c EventsConfig) withDefaults() EventsConfig {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.Retention, 14*24*time.Hour)
	def(&c.Poll, time.Second)
	def(&c.Lease, 30*time.Second)
	def(&c.Timeout, 10*time.Second)
	def(&c.MaxBackoff, 5*time.Minute)
	if c.Instance == "" {
		c.Instance = rand.Text()
	}
	h := http.Client{}
	if c.HTTP != nil {
		h = *c.HTTP
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.HTTP = &h
	c.Clock = clock.Or(c.Clock)
	return c
}

// Run delivers ref events to the subscriptions and prunes old events until ctx ends. A replica that does not hold the
// lease of a subscription skips it. Run waits for its workers before it returns.
func (s *Server) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	running := map[string]bool{}
	var pruned time.Time
	defer wg.Wait()
	for {
		if now := s.events.Clock.Now(); pruned.IsZero() || now.Sub(pruned) >= pruneEvery {
			if err := s.index.PruneEvents(ctx, now.Add(-s.events.Retention)); err != nil && ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "event prune failed", "error", logSafe(err))
			}
			pruned = now
		}
		subs, err := s.index.Subscriptions(ctx)
		if err != nil && ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "subscription list failed", "error", logSafe(err))
		}
		mu.Lock()
		for _, sub := range subs {
			if running[sub.ID] {
				continue
			}
			running[sub.ID] = true
			wg.Go(func() {
				s.deliverTo(ctx, sub.ID)
				mu.Lock()
				delete(running, sub.ID)
				mu.Unlock()
			})
		}
		mu.Unlock()
		if clock.Wait(ctx, s.events.Clock, s.events.Poll) != nil {
			return nil
		}
	}
}

// deliverTo is the worker of one subscription. It ends when the subscription is gone or ctx ends.
func (s *Server) deliverTo(ctx context.Context, id string) {
	for ctx.Err() == nil {
		sub, err := s.acquire(ctx, id)
		if err == nil {
			err = s.deliverPending(ctx, sub)
		}
		switch {
		case errors.Is(err, meta.ErrNotFound):
			return
		case errors.Is(err, meta.ErrLeaseHeld), ctx.Err() != nil:
		default:
			s.logger.ErrorContext(ctx, "event delivery failed", "subscription", id, "error", logSafe(err))
		}
		_ = clock.Wait(ctx, s.events.Clock, s.events.Poll)
	}
}

func (s *Server) acquire(ctx context.Context, id string) (meta.Subscription, error) {
	now := s.events.Clock.Now()
	return s.index.AcquireLease(ctx, id, s.events.Instance, now, now.Add(s.events.Lease))
}

// hold waits for d and renews the lease while it waits. It returns the error of a renewal that fails.
func (s *Server) hold(ctx context.Context, id string, d time.Duration) error {
	for d > 0 {
		step := min(d, s.events.Lease/3)
		if err := clock.Wait(ctx, s.events.Clock, step); err != nil {
			return err
		}
		d -= step
		if _, err := s.acquire(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// deliverPending sends every event above the cursor of sub, then keeps reading while the lease holds. Each event is
// sent until the receiver answers 2xx, and the cursor moves after it, so a crash between the two repeats one event.
func (s *Server) deliverPending(ctx context.Context, sub meta.Subscription) error {
	cursor, acked := sub.Cursor, sub.Cursor
	for {
		evs, err := s.index.Events(ctx, cursor, batchSize)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			if err := s.hold(ctx, sub.ID, s.events.Poll); err != nil {
				return err
			}
			continue
		}
		for _, e := range evs {
			cursor = e.Seq
			if sub.Repo != "" && sub.Repo != e.Repo {
				continue
			}
			if _, err := s.acquire(ctx, sub.ID); err != nil {
				return err
			}
			if err := s.send(ctx, sub, e); err != nil {
				return err
			}
			if err := s.index.AdvanceCursor(ctx, sub.ID, s.events.Instance, e.Seq); err != nil {
				return err
			}
			acked = e.Seq
		}
		if acked < cursor {
			if err := s.index.AdvanceCursor(ctx, sub.ID, s.events.Instance, cursor); err != nil {
				return err
			}
			acked = cursor
		}
	}
}

func backoff(attempt int, limit time.Duration) time.Duration {
	if attempt > 20 {
		return limit
	}
	return min(backoffBase<<attempt, limit)
}

// send posts e until the receiver answers 2xx. It waits with backoff between attempts and renews the lease while it
// waits.
func (s *Server) send(ctx context.Context, sub meta.Subscription, e meta.Event) error {
	body, err := json.Marshal(eventView(e))
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(sub.Secret))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	for attempt := 0; ; attempt++ {
		status, err := s.post(ctx, sub.URL, e.Seq, signature, body)
		if err == nil && status/100 == 2 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.WarnContext(ctx, "event delivery refused", "subscription", sub.ID, "seq", e.Seq, "attempt", attempt+1, "status", status, "error", logSafe(err))
		if err := s.hold(ctx, sub.ID, backoff(attempt, s.events.MaxBackoff)); err != nil {
			return err
		}
	}
}

// post sends one delivery. It returns the status, or 0 with the error of a request that got no answer. The error of
// the HTTP client can carry the URL, so the caller logs only its type.
func (s *Server) post(ctx context.Context, url string, seq int64, signature string, body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.events.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(api.HeaderSignature, signature)
	req.Header.Set(api.HeaderSequence, strconv.FormatInt(seq, 10))
	resp, err := s.events.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainMax))
	return resp.StatusCode, nil
}
