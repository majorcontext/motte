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
	// Poll is the wait between passes over the subscriptions. Default 1 second.
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

// Run delivers ref events to the subscriptions and prunes old events until ctx ends. A ref write of this replica wakes
// it at once; the poll finds the writes of other replicas. A worker sends one partition of
// one subscription while it holds that lease, so replicas deliver different partitions of a subscription in parallel.
// Run waits for its workers before it returns.
func (s *Server) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	running := map[string]bool{}
	var pruned time.Time
	defer wg.Wait()
	for {
		now := s.events.Clock.Now()
		s.sweepLands(ctx, pruned.IsZero() || now.Sub(pruned) >= pruneEvery)
		if pruned.IsZero() || now.Sub(pruned) >= pruneEvery {
			if err := s.index.PruneEvents(ctx, now.Add(-s.events.Retention)); err != nil && ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "event prune failed", "error", logSafe(err))
			}
			pruned = now
		}
		subs, err := s.index.Subscriptions(ctx)
		var heads []int64
		if err == nil {
			heads, err = s.index.EventHeads(ctx)
		}
		if err != nil && ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "subscription read failed", "error", logSafe(err))
		}
		if err != nil {
			subs = nil
		}
		mu.Lock()
		for _, sub := range subs {
			for p, cursor := range sub.Cursors {
				key := sub.ID + "/" + strconv.Itoa(p)
				if heads[p] <= cursor {
					continue
				}
				if _, busy := running[key]; busy {
					running[key] = true
					continue
				}
				running[key] = false
				wg.Go(func() {
					for {
						s.deliverTo(ctx, sub.ID, p)
						mu.Lock()
						again := running[key] && ctx.Err() == nil
						running[key] = false
						if !again {
							delete(running, key)
						}
						mu.Unlock()
						if !again {
							return
						}
					}
				})
			}
		}
		mu.Unlock()
		if s.idle(ctx) != nil {
			return nil
		}
	}
}

// idle waits for the next poll, or for a wake of this replica. A delivery that is already running when a wake comes
// is marked, and it reads the partition again before it ends.
func (s *Server) idle(ctx context.Context) error {
	t := s.events.Clock.NewTimer(s.events.Poll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	case <-s.wake:
	}
	return nil
}

// deliverTo takes the lease of one partition of a subscription and sends what is pending. It returns when the
// partition is caught up or the lease is lost. The lease stays with this replica until it expires, so the next pass
// of Run finds it again.
func (s *Server) deliverTo(ctx context.Context, id string, partition int) {
	lease, err := s.acquire(ctx, id, partition)
	if err == nil {
		err = s.deliverPending(ctx, lease)
	}
	if err != nil && ctx.Err() == nil && !errors.Is(err, meta.ErrNotFound) && !errors.Is(err, meta.ErrLeaseHeld) {
		s.logger.ErrorContext(ctx, "event delivery failed", "subscription", id, "partition", partition, "error", logSafe(err))
	}
}

func (s *Server) acquire(ctx context.Context, id string, partition int) (meta.Lease, error) {
	now := s.events.Clock.Now()
	return s.index.AcquireLease(ctx, id, partition, s.events.Instance, now, now.Add(s.events.Lease))
}

// hold waits for d and renews the lease while it waits. It returns the error of a renewal that fails.
func (s *Server) hold(ctx context.Context, l meta.Lease, d time.Duration) error {
	for d > 0 {
		step := min(d, s.events.Lease/3)
		if err := clock.Wait(ctx, s.events.Clock, step); err != nil {
			return err
		}
		d -= step
		if _, err := s.acquire(ctx, l.ID, l.Partition); err != nil {
			return err
		}
	}
	return nil
}

// deliverPending sends every event of the partition above the cursor of l. Each event is sent until the receiver
// answers 2xx, and the cursor moves after it, so a crash between the two repeats one event.
func (s *Server) deliverPending(ctx context.Context, l meta.Lease) error {
	cursor, acked := l.Cursor, l.Cursor
	for {
		evs, err := s.index.Events(ctx, l.Partition, cursor, batchSize)
		if err != nil || len(evs) == 0 {
			return err
		}
		for _, e := range evs {
			cursor = e.Seq
			if l.Repo != "" && l.Repo != e.Repo {
				continue
			}
			if _, err := s.acquire(ctx, l.ID, l.Partition); err != nil {
				return err
			}
			if err := s.send(ctx, l, e); err != nil {
				return err
			}
			if err := s.index.AdvanceCursor(ctx, l.ID, l.Partition, s.events.Instance, e.Seq); err != nil {
				return err
			}
			acked = e.Seq
		}
		if acked < cursor {
			if err := s.index.AdvanceCursor(ctx, l.ID, l.Partition, s.events.Instance, cursor); err != nil {
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
func (s *Server) send(ctx context.Context, sub meta.Lease, e meta.Event) error {
	body, err := json.Marshal(eventView(e))
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(sub.Secret))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	for attempt := 0; ; attempt++ {
		status, err := s.post(ctx, sub.URL, e, signature, body)
		if err == nil && status/100 == 2 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.WarnContext(ctx, "event delivery refused", "subscription", sub.ID, "partition", e.Partition, "seq", e.Seq, "attempt", attempt+1, "status", status, "error", logSafe(err))
		if err := s.hold(ctx, sub, backoff(attempt, s.events.MaxBackoff)); err != nil {
			return err
		}
	}
}

// post sends one delivery. It returns the status, or 0 with the error of a request that got no answer. The error of
// the HTTP client can carry the URL, so the caller logs only its type.
func (s *Server) post(ctx context.Context, url string, e meta.Event, signature string, body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.events.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(api.HeaderSignature, signature)
	req.Header.Set(api.HeaderPartition, strconv.Itoa(e.Partition))
	req.Header.Set(api.HeaderSequence, strconv.FormatInt(e.Seq, 10))
	resp, err := s.events.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainMax))
	return resp.StatusCode, nil
}
