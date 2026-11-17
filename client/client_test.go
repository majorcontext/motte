package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
)

func TestWireCodesMapToSentinels(t *testing.T) {
	cases := map[api.Code]error{
		api.CodeNotConfigured:      client.ErrNotConfigured,
		api.CodeNotFound:           client.ErrNotFound,
		api.CodeGenerationConflict: client.ErrGenerationConflict,
		api.CodeClaimLost:          client.ErrClaimLost,
		api.CodeObjectInvalid:      client.ErrObjectInvalid,
		api.CodeObjectTooLarge:     client.ErrObjectTooLarge,
		api.CodeRepoForbidden:      client.ErrRepoForbidden,
		api.CodeWorkspaceConflict:  client.ErrWorkspaceConflict,
		api.CodeWorkspaceDeleted:   client.ErrWorkspaceDeleted,
		api.CodeUnauthorized:       client.ErrUnauthorized,
		api.CodeInvalidBody:        client.ErrInvalidBody,
		api.CodeInternal:           client.ErrInternal,
	}
	for code, want := range cases {
		t.Run(string(code), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code.Status())
				_ = json.NewEncoder(w).Encode(api.ErrorBody{Error: api.Error{Code: code, Message: "m"}})
			}))
			defer ts.Close()
			_, err := client.New(ts.URL, client.StaticGrant("g")).Repos(context.Background())
			var ce *client.Error
			if !errors.Is(err, want) || !errors.As(err, &ce) || ce.Status != code.Status() || ce.Code != code {
				t.Fatalf("error = %v, want %v with status %d", err, want, code.Status())
			}
			for other, s := range cases {
				if other != code && errors.Is(err, s) {
					t.Errorf("error also matches the sentinel of %s", other)
				}
			}
		})
	}
}

func TestRedirectIsAnErrorAndLeaksNoGrant(t *testing.T) {
	var leaked bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked = true }))
	defer target.Close()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer ts.Close()
	err := client.New(ts.URL, client.StaticGrant("g")).DeleteWorkspace(context.Background(), "ws1")
	if !errors.Is(err, client.ErrRedirect) || leaked {
		t.Fatalf("error = %v, redirect target called = %v; want ErrRedirect and no call", err, leaked)
	}
}

func TestBudgetBoundsACall(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer ts.Close()
	defer close(release)
	start := time.Now()
	_, err := client.New(ts.URL, client.StaticGrant("g"), client.WithBudget(50*time.Millisecond)).Repos(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("error = %v after %v, want a deadline error near the budget", err, time.Since(start))
	}
}

func TestCallCarriesGrantAndTraceContext(t *testing.T) {
	var auth, parent string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, parent = r.Header.Get("Authorization"), r.Header.Get("Traceparent")
		_, _ = w.Write([]byte(`{"repos":[]}`))
	}))
	defer ts.Close()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	if _, err := client.New(ts.URL, client.StaticGrant("tok")).Repos(ctx); err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer tok" || parent != "00-01000000000000000000000000000000-0200000000000000-01" {
		t.Errorf("authorization = %q, traceparent = %q", auth, parent)
	}
}
