package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/meta/postgres"
)

type harness struct {
	*Server
	url   string
	pool  *pgxpool.Pool
	token string
	logs  *bytes.Buffer
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
	ts := httptest.NewServer(h.Handler())
	t.Cleanup(ts.Close)
	h.url = ts.URL
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
