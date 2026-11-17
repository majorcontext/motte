package server

import (
	"crypto/rand"
	"net/http"
	"net/url"
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
	s.handle(mux, "GET /v1/events", s.listEvents, grant.ScopeOperator)
	s.handle(mux, "POST /v1/subscriptions", s.createSubscription, grant.ScopeOperator)
	s.handle(mux, "GET /v1/subscriptions", s.listSubscriptions, grant.ScopeOperator)
	s.handle(mux, "DELETE /v1/subscriptions/{id}", s.deleteSubscription, grant.ScopeOperator)
}

func eventView(e meta.Event) api.Event {
	return api.Event{Seq: e.Seq, Repo: e.Repo, Ref: e.Ref, Old: e.Old, New: e.New, Generation: e.Generation, Kind: e.Kind, Time: e.At.UTC().Format(time.RFC3339Nano)}
}

func subscriptionView(s meta.Subscription) api.Subscription {
	return api.Subscription{ID: s.ID, URL: s.URL, Repo: s.Repo, Cursor: s.Cursor, CreatedAt: s.CreatedAt.UTC().Format(time.RFC3339)}
}

// queryInt reads an integer parameter in [lo, hi]. An absent parameter is def.
func queryInt(r *http.Request, name string, def, lo, hi int64) (int64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < lo || n > hi {
		return 0, errBody
	}
	return n, nil
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request, _ call) error {
	after, err := queryInt(r, "after", 0, 0, 1<<62)
	if err != nil {
		return err
	}
	limit, err := queryInt(r, "limit", eventsDefault, 1, eventsMax)
	if err != nil {
		return err
	}
	evs, err := s.index.Events(r.Context(), after, int(limit))
	if err != nil {
		return err
	}
	out := api.Events{Events: make([]api.Event, len(evs))}
	for i, e := range evs {
		out.Events[i] = eventView(e)
	}
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
	s.writeJSON(w, http.StatusCreated, subscriptionView(sub))
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
