package oidc_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/identitytest"
	"github.com/majorcontext/motte/identity/oidc"
)

var _ identity.Verifier = (*oidc.Verifier)(nil)

const (
	issuer = "https://team.example.test"
	aud    = "aud-tag"
	header = "Cf-Access-Jwt-Assertion"
)

var now = time.Unix(1_000_000, 0)

func claims(email string, exp time.Time) map[string]any {
	return map[string]any{"iss": issuer, "aud": []string{aud}, "email": email, "exp": exp.Unix(), "nbf": now.Add(-time.Minute).Unix()}
}

func newAuth(t *testing.T, iss *identitytest.Issuer) *oidc.Verifier {
	t.Helper()
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: iss.URL, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func request(jwt string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.Header.Set(header, jwt)
	return r
}

func TestConformance(t *testing.T) {
	t.Parallel()
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		iss, rogue := identitytest.NewIssuer(t, "k1"), identitytest.NewIssuer(t, "k1")
		return identitytest.Fixture{Verifier: newAuth(t, iss), Subject: "dev@dev.test",
			Valid:   func() *http.Request { return request(iss.Sign(t, claims("dev@dev.test", now.Add(time.Hour)))) },
			Expired: func() *http.Request { return request(iss.Sign(t, claims("dev@dev.test", now.Add(-time.Second)))) },
			Forged:  func() *http.Request { return request(rogue.Sign(t, claims("dev@dev.test", now.Add(time.Hour)))) }}
	})
}

func TestRefusesClaimsOfAnotherApplication(t *testing.T) {
	t.Parallel()
	key := identitytest.NewIssuer(t, "k1")
	a := newAuth(t, key)
	for name, mutate := range map[string]func(map[string]any){
		"audience": func(c map[string]any) { c["aud"] = []string{"other"} },
		"issuer":   func(c map[string]any) { c["iss"] = "https://other.example.test" },
		"not yet":  func(c map[string]any) { c["nbf"] = now.Add(time.Minute).Unix() },
		"no email": func(c map[string]any) { delete(c, "email") },
		"merge":    func(c map[string]any) {},
	} {
		c := claims("dev@example.test", now.Add(time.Hour))
		mutate(c)
		_, err := a.Verify(request(key.Sign(t, c)))
		if (name == "merge") != (err == nil) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

type failableKeys struct {
	down bool
	gate chan struct{}
	body []byte
}

func (f *failableKeys) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.gate != nil {
		<-f.gate
	}
	if f.down {
		return nil, errors.New("key set unreachable")
	}
	if f.body != nil {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(f.body))}, nil
	}
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err == nil {
		f.body, _ = io.ReadAll(resp.Body)
		resp.Body = io.NopCloser(bytes.NewReader(f.body))
	}
	return resp, err
}

func TestRefusesKeysAnHourPastTheirLastGoodFetch(t *testing.T) {
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	keys := &failableKeys{}
	at := now
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: iss.URL, Client: &http.Client{Transport: keys}, Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	r := request(iss.Sign(t, claims("dev@example.test", now.Add(3*time.Hour))))
	check := func(wantOK bool, after time.Duration) {
		t.Helper()
		at = now.Add(after)
		if _, err := a.Verify(r); (err == nil) != wantOK {
			t.Errorf("after %s with the key set down: err = %v, want ok = %v", after, err, wantOK)
		}
	}
	check(true, 0)
	keys.down = true
	check(true, 59*time.Minute)
	check(false, 61*time.Minute)
}

func TestDiscoversTheKeySetAndReadsABearerToken(t *testing.T) {
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	doc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"jwks_uri": iss.URL})
	}))
	t.Cleanup(doc.Close)
	a, err := oidc.New(oidc.Config{Issuer: doc.URL, Audience: aud, Claim: "sub", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	tok := iss.Sign(t, map[string]any{"iss": doc.URL, "aud": aud, "sub": "user-1", "exp": now.Add(time.Hour).Unix()})
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	if p, err := a.Verify(r); err != nil || p.Subject != "user-1" {
		t.Errorf("bearer: principal = %+v, err = %v", p, err)
	}
	r.Header.Del("Authorization")
	r.SetBasicAuth("x", tok)
	if _, err := a.Verify(r); err != nil {
		t.Errorf("basic password: err = %v", err)
	}
}

func refreshing(t *testing.T) (*oidc.Verifier, *failableKeys, *time.Time, *http.Request) {
	t.Helper()
	iss := identitytest.NewIssuer(t, "k1")
	keys := &failableKeys{}
	at := now
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: iss.URL, Client: &http.Client{Transport: keys}, Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	return a, keys, &at, request(iss.Sign(t, claims("dev@example.test", now.Add(3*time.Hour))))
}

func TestRefreshesBeforeTheKeysGoStale(t *testing.T) {
	t.Parallel()
	a, keys, at, r := refreshing(t)
	for _, c := range []struct {
		after time.Duration
		down  bool
		want  bool
	}{{0, false, true}, {46 * time.Minute, false, true}, {61 * time.Minute, true, true}, {107 * time.Minute, true, false}} {
		*at, keys.down = now.Add(c.after), c.down
		if _, err := a.Verify(r); (err == nil) != c.want {
			t.Errorf("after %s: err = %v, want ok = %v", c.after, err, c.want)
		}
	}
}

func TestEmailVerifiedFalseIsRefusedAndAbsentIsAccepted(t *testing.T) {
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	a := newAuth(t, iss)
	for verified, want := range map[any]bool{nil: true, true: true, "true": true, false: false, "false": false} {
		c := claims("dev@example.test", now.Add(time.Hour))
		if verified != nil {
			c["email_verified"] = verified
		}
		if _, err := a.Verify(request(iss.Sign(t, c))); (err == nil) != want {
			t.Errorf("email_verified %v: err = %v, want ok = %v", verified, err, want)
		}
	}
}

func TestRequestsDuringARefreshServeFreshKeysAndWaitOnStaleOnes(t *testing.T) {
	t.Parallel()
	a, keys, at, r := refreshing(t)
	if _, err := a.Verify(r); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		for _, c := range []struct {
			after time.Duration
			done  int
		}{{50 * time.Minute, 1}, {111 * time.Minute, 0}} {
			*at, keys.gate = now.Add(c.after), make(chan struct{})
			errs := make(chan error, 2)
			for range 2 {
				go func() { _, err := a.Verify(r); errs <- err }()
				synctest.Wait()
			}
			if len(errs) != c.done {
				t.Errorf("after %s: %d requests ended while the refresh was blocked, want %d", c.after, len(errs), c.done)
			}
			close(keys.gate)
			for range 2 {
				if err := <-errs; err != nil {
					t.Errorf("after %s: %v", c.after, err)
				}
			}
		}
	})
}
