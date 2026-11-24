package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta/postgres"
)

type harness struct {
	*Server
	url   string
	pool  *pgxpool.Pool
	token string
	logs  *bytes.Buffer
	ts    *httptest.Server
}

func start(t *testing.T) *harness {
	t.Helper()
	pool := pgtest.Pool(t, "")
	root, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	h := &harness{pool: pool, logs: &bytes.Buffer{}}
	h.Server = New(Config{
		Meta: &postgres.Store{Pool: pool}, Bucket: root.Bucket("motte"), Logger: slog.New(slog.NewJSONHandler(h.logs, nil)),
		Verifier: grant.NewVerifier(grant.Key{Kid: "cp", Public: pub, Scopes: []grant.Scope{grant.ScopeOperator}}),
	})
	h.token, err = grant.Signer{Kid: "cp", Key: key}.Mint(grant.Claims{Scope: grant.ScopeOperator, Exp: time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) serve(t *testing.T) {
	t.Helper()
	h.ts = httptest.NewServer(h.Handler())
	t.Cleanup(h.ts.Close)
	h.url = h.ts.URL
}

func (h *harness) call(t *testing.T, method, path string, body any) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, h.url+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct{ Error struct{ Code string } }
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out.Error.Code
}

func TestClaimWaitsForAWriteInFlight(t *testing.T) {
	t.Parallel()
	h := start(t)
	h.serve(t)
	claim := func(epoch int) (int, string) {
		return h.call(t, "PUT", "/v1/workspaces/ws1/claim", map[string]any{"epoch": epoch, "owner": "o1", "repo": "gh:acme/widgets"})
	}
	if status, _ := claim(1); status != 200 {
		t.Fatalf("first claim = %d", status)
	}
	ctx := context.Background()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var pid int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM workspaces WHERE id = 'ws1' AND epoch = 1 FOR SHARE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { status, _ := claim(2); done <- status }()
	for blocked := false; !blocked; {
		select {
		case status := <-done:
			t.Fatalf("claim finished while a write held the row: %d", status)
		default:
		}
		if err := h.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if status := <-done; status != 200 {
		t.Fatalf("claim = %d", status)
	}
	if status, code := claim(1); status != 409 || code != "vcs_claim_lost" {
		t.Fatalf("stale claim = %d %q", status, code)
	}
}

func TestHandlerPanicIsLoggedAndAnswered(t *testing.T) {
	t.Parallel()
	h := start(t)
	h.handle(h.mux, "GET /v1/early", func(http.ResponseWriter, *http.Request, call) error { panic("early") }, grant.ScopeOperator)
	h.handle(h.mux, "GET /v1/late", func(w http.ResponseWriter, _ *http.Request, _ call) error {
		w.WriteHeader(http.StatusAccepted)
		panic("late")
	}, grant.ScopeOperator)
	h.serve(t)
	if status, code := h.call(t, "GET", "/v1/early", nil); status != 500 || code != "internal_error" {
		t.Errorf("panic before the answer = %d %q", status, code)
	}
	if status, _ := h.call(t, "GET", "/v1/late", nil); status != 202 {
		t.Errorf("panic after the answer = %d, want the sent 202", status)
	}
	if n := strings.Count(h.logs.String(), `"msg":"handler panic"`); n != 2 || !strings.Contains(h.logs.String(), "goroutine") {
		t.Errorf("log holds %d panic lines or no stack: %s", n, h.logs)
	}
}

func TestLimitDefaultsAreTheSpecValues(t *testing.T) {
	t.Parallel()
	if objectLimit != 16<<20 || decodeWindow != 24<<20 || treeReserve != 8<<20 || treeSlack != 8<<20 || smallBodyMax != 1<<20 {
		t.Errorf("limits = %d %d %d %d %d, want 16 MiB, 24 MiB, 8 MiB, 8 MiB, 1 MiB", objectLimit, decodeWindow, treeReserve, treeSlack, smallBodyMax)
	}
}

func TestLogSafeNamesAKnownCauseAndHidesTextBuiltFromData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want string
		deny string
	}{
		{"a known sentinel under a wrapper", fmt.Errorf("read /secret/path: %w", context.Canceled), "context canceled", "/secret/path"},
		{"text built from data", fmt.Errorf("redirect to %s", "https://host/token"), "", "https://host/token"},
	} {
		got := logSafe(tc.err)
		if !strings.Contains(got, tc.want) || strings.Contains(got, tc.deny) {
			t.Errorf("%s: logSafe = %q, want %q and not %q", tc.name, got, tc.want, tc.deny)
		}
	}
}

func TestARequestThatTheClientAbandonsIsRecordedAs499AndLoggedAsInfo(t *testing.T) {
	t.Parallel()
	h := start(t)
	cases := map[string]func(http.ResponseWriter, *http.Request) error{
		"/v1/cancel-early": func(http.ResponseWriter, *http.Request) error {
			return fmt.Errorf("read of a pack: %w", context.Canceled)
		},
		"/v1/cancel-upload":    func(http.ResponseWriter, *http.Request) error { return errBody },
		"/v1/cancel-streaming": func(w http.ResponseWriter, r *http.Request) error { return abortAfterHeader(h, w, r, context.Canceled) },
		"/v1/cancel-pipe":      func(w http.ResponseWriter, r *http.Request) error { return abortAfterHeader(h, w, r, syscall.EPIPE) },
		"/v1/cancel-http2": func(w http.ResponseWriter, r *http.Request) error {
			return abortAfterHeader(h, w, r, errors.New("http2: stream closed"))
		},
	}
	started := map[string]chan struct{}{}
	for path, fn := range cases {
		started[path] = make(chan struct{})
		h.handle(h.mux, "GET "+path, func(w http.ResponseWriter, r *http.Request, _ call) error {
			close(started[path])
			<-r.Context().Done()
			return fn(w, r)
		}, grant.ScopeOperator)
	}
	h.serve(t)
	for path, ch := range started {
		ctx, cancel := context.WithCancel(t.Context())
		req, _ := http.NewRequestWithContext(ctx, "GET", h.url+path, nil)
		req.Header.Set("Authorization", "Bearer "+h.token)
		go func() { <-ch; cancel() }()
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
	h.ts.Close()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for path := range cases {
		if want := `motte_http_requests_total{route="GET ` + path + `",status="499"} 1`; !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	if strings.Contains(h.logs.String(), `"level":"ERROR"`) || strings.Count(h.logs.String(), `"msg":"request canceled by the client"`) != len(cases) {
		t.Errorf("log = %s, want %d INFO lines and no ERROR line", h.logs, len(cases))
	}
}

// abortAfterHeader sends the header, as a response that streams does, and then fails with err.
func abortAfterHeader(h *harness, w http.ResponseWriter, r *http.Request, err error) error {
	w.WriteHeader(http.StatusOK)
	return h.abortAfterHeaders(w, r, fmt.Errorf("write of a pack: %w", err))
}

func TestThePackWriteReservationGrowsWithTheLargestObjectItFindsWhileWriting(t *testing.T) {
	t.Parallel()
	f := &fetchHold{largest: fetchObject, srv: &Server{largeFetches: make(chan struct{}, 1)}, ctx: t.Context(), reservation: &reservation{take: func(int64) (func(), error) { return func() {}, nil }}}
	if _, err := f.writeFor(1_000_000, 10); err != nil {
		t.Fatal(err)
	}
	if got, want := f.write, int64(fetchAhead+copyAheadMax+160*1_000_000+256*10)+store.CopyPlaceBytes(1_000_000); got != want {
		t.Errorf("the pack write of 1000000 objects in 10 packs reserves %d, the spec says %d", got, want)
	}
	before := f.cost()
	if err := f.object(10 * fetchObject); err != nil {
		t.Fatal(err)
	}
	if got, want := f.cost()-before, 3*9*int64(fetchObject); got != want {
		t.Errorf("a larger object raised the reservation of the pack write by %d, want %d (3 times the growth)", got, want)
	}
}
