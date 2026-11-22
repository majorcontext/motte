// Command motte-loadtest builds a synthetic large repository in a motte service and drives concurrent clients against it.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
)

const usage = `usage: motte-loadtest gen|run|serve [flags]
  gen    build a repository of -objects objects and write its manifest and samples to -out
  run    drive the clients of -mix, or the agents of -scenario agents, against the repository of -out and print a report
  serve  run a local service on a filesystem bucket and SQLite, with -latency added to each bucket read`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "gen":
		err = runGen(ctx, os.Args[2:])
	case "run":
		err = runLoad(ctx, os.Args[2:])
	case "serve":
		err = runServe(ctx, os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "motte-loadtest:", err)
		os.Exit(1)
	}
}

type signer struct {
	s    grant.Signer
	repo string
}

func loadSigner(keyFile, kid string) (grant.Signer, error) {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return grant.Signer{}, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return grant.Signer{}, errors.New("the key file holds no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	priv, ok := key.(ed25519.PrivateKey)
	if err != nil || !ok {
		return grant.Signer{}, errors.New("the key is not an Ed25519 PKCS#8 key")
	}
	return grant.Signer{Kid: kid, Key: priv}, nil
}

func (g signer) mint(scope grant.Scope) string {
	tok, err := g.s.Mint(grant.Claims{Scope: scope, Repo: g.repo, Exp: time.Now().Add(24 * time.Hour).Unix()})
	if err != nil {
		panic(err)
	}
	return tok
}

func (g signer) client(url string, scope grant.Scope) *client.Client {
	return client.New(url, client.StaticGrant(g.mint(scope)), client.WithBudget(2*time.Minute), client.WithHTTPClient(sharedHTTP))
}

// Manifest describes a generated repository for the run phase.
type Manifest struct {
	Repo       string  `json:"repo"`
	Path       string  `json:"path"`
	Tip        string  `json:"tip"`
	Objects    int     `json:"objects"`
	Commits    int     `json:"commits"`
	Files      int     `json:"files"`
	Fanout     int     `json:"fanout"`
	Depth      int     `json:"depth"`
	Leaf       int     `json:"leaf"`
	Packs      int     `json:"packs"`
	PackBytes  int64   `json:"pack_bytes"`
	GenSeconds float64 `json:"gen_seconds"`
	ImportSec  float64 `json:"import_seconds"`
	BlobMedian int     `json:"blob_median"`
}

func readManifest(dir string) (Manifest, error) {
	var m Manifest
	raw, err := os.ReadFile(dir + "/manifest.json")
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(raw, &m)
}

// Sample is a sampled object of the repository.
type Sample struct {
	Kind byte
	SHA  string
	Seq  int
}

func readSamples(dir string) ([]Sample, error) {
	raw, err := os.ReadFile(dir + "/samples.txt")
	if err != nil {
		return nil, err
	}
	var out []Sample
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var s Sample
		var kind string
		if _, err := fmt.Sscanf(l, "%s %s %d", &kind, &s.SHA, &s.Seq); err != nil {
			return nil, err
		}
		s.Kind = kind[0]
		out = append(out, s)
	}
	return out, nil
}

var sharedHTTP = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: 30 * time.Second}}
