package grant

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	pubA, privA, _ := ed25519.GenerateKey(nil)
	pubB, privB, _ := ed25519.GenerateKey(nil)
	all := []Scope{ScopeNode, ScopeRead, ScopeWrite, ScopeImport, ScopeOperator, ScopeMerge}
	v := NewVerifier(
		Key{Kid: "cp", Public: pubA, Scopes: all},
		Key{Kid: "node", Public: pubB, Scopes: []Scope{ScopeNode}},
	)
	node := Claims{Scope: ScopeNode, WS: "w1", Epoch: 2, Repo: "n:a", Exp: 2000}
	cp, nodeKey := Signer{Kid: "cp", Key: privA}, Signer{Kid: "node", Key: privB}
	mint := func(s Signer, c Claims) string {
		tok, err := s.Mint(c)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	forged := mint(nodeKey, node)
	forged = strings.Replace(forged, ".", "x.", 1)
	cases := []struct {
		name  string
		token string
		ok    bool
	}{
		{"node key node scope", mint(nodeKey, node), true},
		{"control key any scope", mint(cp, Claims{Scope: ScopeOperator, Exp: 2000}), true},
		{"control key merge scope", mint(cp, Claims{Scope: ScopeMerge, Repo: "n:a", Refs: []string{"*"}, Exp: 2000}), true},
		{"node key merge scope", mint(nodeKey, Claims{Scope: ScopeMerge, Repo: "n:a", Refs: []string{"*"}, Exp: 2000}), false},
		{"node key import scope", mint(nodeKey, Claims{Scope: ScopeImport, Repo: "n:a", Exp: 2000}), false},
		{"write scope with a repo", mint(cp, Claims{Scope: ScopeWrite, Repo: "n:a", Exp: 2000}), true},
		{"node key write scope", mint(nodeKey, Claims{Scope: ScopeWrite, Repo: "n:a", Exp: 2000}), false},
		{"expired", mint(cp, Claims{Scope: ScopeRead, Repo: "n:a", Exp: 1000}), false},
		{"unknown kid", mint(Signer{Kid: "old", Key: privA}, node), false},
		{"wrong key for kid", mint(Signer{Kid: "cp", Key: privB}, node), false},
		{"tampered payload", forged, false},
		{"not a grant", "vg1.a.b", false},
	}
	for _, c := range cases {
		got, err := v.Verify(c.token, now)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok = %v", c.name, err, c.ok)
		}
		if c.ok && got.Exp == 0 {
			t.Errorf("%s: empty claims", c.name)
		}
	}
}

func TestMintRejectsIncompleteClaims(t *testing.T) {
	t.Parallel()
	_, priv, _ := ed25519.GenerateKey(nil)
	for _, c := range []Claims{{Scope: ScopeNode, Repo: "n:a"}, {Scope: ScopeRead}, {Scope: ScopeWrite}, {Scope: ScopeMerge}, {Scope: ScopeMerge, Repo: "n:a"}, {Scope: "x"}} {
		if _, err := (Signer{Kid: "k", Key: priv}).Mint(c); err == nil {
			t.Errorf("Mint(%+v) accepted", c)
		}
	}
}

func TestRefsBelongToMergeGrantsAndMustBePatterns(t *testing.T) {
	t.Parallel()
	pub, priv, _ := ed25519.GenerateKey(nil)
	s, v := Signer{Kid: "k", Key: priv}, NewVerifier(Key{"k", pub, []Scope{ScopeMerge}})
	for _, c := range []Claims{{Scope: ScopeRead, Repo: "n:a", Refs: []string{"main"}}, {Scope: ScopeMerge, Repo: "n:a", Refs: []string{"a b"}}, {Scope: ScopeMerge, Repo: "n:a", Refs: make([]string, MaxRefs+1)}} {
		if _, err := s.Mint(c); err == nil {
			t.Errorf("Mint(%+v) accepted", c)
		}
	}
	tok, err := s.Mint(Claims{Scope: ScopeMerge, Repo: "n:a", Refs: []string{"main", "feature/*"}, Exp: 2000})
	got, verr := v.Verify(tok, time.Unix(1000, 0))
	if err != nil || verr != nil || !got.MovesRef("main") || !got.MovesRef("feature/x") || got.MovesRef("release/1") || got.MovesRef("mainx") {
		t.Errorf("claims = %+v, %v, %v: want main and feature/* only", got, err, verr)
	}
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"k","scope":"merge","repo":"n:a","exp":2000}`))
	signed := "mg1." + payload + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte(payload)))
	if _, err := v.Verify(signed, time.Unix(1000, 0)); err == nil {
		t.Error("Verify accepted a merge grant with no refs")
	}
}

func TestRotation(t *testing.T) {
	t.Parallel()
	pubOld, privOld, _ := ed25519.GenerateKey(nil)
	pubNew, privNew, _ := ed25519.GenerateKey(nil)
	c := Claims{Scope: ScopeOperator, Exp: 2000}
	old, _ := Signer{Kid: "k1", Key: privOld}.Mint(c)
	fresh, _ := Signer{Kid: "k2", Key: privNew}.Mint(c)
	scopes := []Scope{ScopeOperator}
	both := NewVerifier(Key{"k1", pubOld, scopes}, Key{"k2", pubNew, scopes})
	only := NewVerifier(Key{"k2", pubNew, scopes})
	now := time.Unix(1000, 0)
	for _, tok := range []string{old, fresh} {
		if _, err := both.Verify(tok, now); err != nil {
			t.Fatalf("both keys: %v", err)
		}
	}
	if _, err := only.Verify(old, now); err == nil {
		t.Error("old grant accepted after its key was removed")
	}
}
