package server

import (
	"crypto/rand"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

const (
	eventsDefault = 100
	eventsMax     = 1000
	secretMin     = 16
	secretMax     = 256
	urlMax        = 2048
)

func (s *Server) registerEvents(mux *http.ServeMux) {
	s.handle(mux, "GET /v1/events", s.listEvents, grant.ScopeOperator, grant.ScopeRead)
	s.handle(mux, "POST /v1/subscriptions", s.createSubscription, grant.ScopeOperator)
	s.handle(mux, "PUT /v1/subscriptions/{name}", s.putSubscription, grant.ScopeOperator)
	s.handle(mux, "GET /v1/subscriptions", s.listSubscriptions, grant.ScopeOperator)
	s.handle(mux, "DELETE /v1/subscriptions/{id}", s.deleteSubscription, grant.ScopeOperator)
}

func eventView(e meta.Event) api.Event {
	return api.Event{Partition: e.Partition, Seq: e.Seq, Repo: e.Repo, Ref: e.Ref, Old: e.Old, New: e.New, Generation: e.Generation, Kind: e.Kind, Time: e.At.UTC().Format(time.RFC3339Nano)}
}

func subscriptionView(s meta.Subscription) api.Subscription {
	return api.Subscription{ID: s.ID, URL: s.URL, Repo: s.Repo, Cursor: encodeCursor(s.Cursors), CreatedAt: s.CreatedAt.UTC().Format(time.RFC3339)}
}

// eventsScan is the most events that one read examines, so a read grant of a quiet repository does not walk the
// whole log. The cursor of the answer passes the events that it examined.
const eventsScan = 5000

var subscriptionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request, c call) error {
	q := r.URL.Query()
	heads, err := s.index.EventHeads(r.Context())
	if err != nil {
		return err
	}
	if from := q.Get("from"); from != "" {
		if from != "latest" || q.Get("cursor") != "" {
			return errBody
		}
		s.writeJSON(w, http.StatusOK, api.Events{Events: []api.Event{}, Cursor: encodeCursor(heads)})
		return nil
	}
	cursor, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		return err
	}
	only := ""
	if c.claims.Scope == grant.ScopeRead {
		only = c.claims.Repo
	}
	limit := int64(eventsDefault)
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.ParseInt(v, 10, 64); err != nil || limit < 1 || limit > eventsMax {
			return errBody
		}
	}
	out := api.Events{Events: []api.Event{}}
	for p, seq := range cursor {
		if seq > heads[p] {
			return errBody
		}
	}
	scanned := 0
	for p := 0; p < meta.Partitions && len(out.Events) < int(limit) && scanned < eventsScan; p++ {
		for heads[p] != cursor[p] && len(out.Events) < int(limit) && scanned < eventsScan {
			n := int(limit) - len(out.Events)
			if only != "" {
				n = min(eventsScan-scanned, eventsMax)
			}
			evs, err := s.index.Events(r.Context(), p, cursor[p], n)
			if err != nil {
				return err
			}
			if len(evs) == 0 {
				break
			}
			for i, e := range evs {
				if only != "" && len(out.Events) == int(limit) {
					evs = evs[:i]
					break
				}
				if only == "" || e.Repo == only {
					out.Events = append(out.Events, eventView(e))
				}
				cursor[p] = e.Seq
			}
			scanned += len(evs)
		}
	}
	out.Cursor = encodeCursor(cursor)
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func validReceiver(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && len(raw) <= urlMax && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil
}

func (s *Server) createSubscription(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.CreateSubscriptionRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if _, err := store.ParseRepo(req.Repo); req.Repo != "" && err != nil || !validReceiver(req.URL) || len(req.Secret) < secretMin || len(req.Secret) > secretMax {
		return errBody
	}
	sub, err := s.index.CreateSubscription(r.Context(), meta.Subscription{ID: "sub_" + strings.ToLower(rand.Text()), URL: req.URL, Secret: req.Secret, Repo: req.Repo})
	if err != nil {
		return err
	}
	s.notify()
	s.writeJSON(w, http.StatusCreated, subscriptionView(sub))
	return nil
}

// putSubscription creates the subscription name or replaces its receiver. A replace keeps the cursors.
func (s *Server) putSubscription(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.PutSubscriptionRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	name := r.PathValue("name")
	if _, err := store.ParseRepo(req.Repo); req.Repo != "" && err != nil || !subscriptionName.MatchString(name) || !validReceiver(req.URL) || len(req.Secret) < secretMin || len(req.Secret) > secretMax {
		return errBody
	}
	want := meta.Subscription{ID: name, URL: req.URL, Secret: req.Secret, Repo: req.Repo}
	status := http.StatusOK
	sub, err := s.index.UpdateSubscription(r.Context(), want)
	if errors.Is(err, meta.ErrNotFound) {
		status = http.StatusCreated
		if sub, err = s.index.CreateSubscription(r.Context(), want); errors.Is(err, meta.ErrExists) {
			status = http.StatusOK
			sub, err = s.index.UpdateSubscription(r.Context(), want)
		}
	}
	if err != nil {
		return err
	}
	s.notify()
	s.writeJSON(w, status, subscriptionView(sub))
	return nil
}

func (s *Server) listSubscriptions(w http.ResponseWriter, r *http.Request, _ call) error {
	subs, err := s.index.Subscriptions(r.Context())
	if err != nil {
		return err
	}
	out := api.Subscriptions{Subscriptions: make([]api.Subscription, len(subs))}
	for i, sub := range subs {
		out.Subscriptions[i] = subscriptionView(sub)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) deleteSubscription(w http.ResponseWriter, r *http.Request, _ call) error {
	if err := s.index.DeleteSubscription(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
