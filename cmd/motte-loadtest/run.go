package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
)

type runFlags struct {
	url, key, kid, dir, mix, watch string
	conc, casRefs, fetchBack       int
	duration                       time.Duration
	seed                           uint64
	jsonOut                        bool
}

type env struct {
	f       runFlags
	m       Manifest
	blobs   []Sample
	trees   []Sample
	commits []Sample
	read    *client.Client
	merge   *client.Client
	sg      signer
	http    *httpGit
	casRefs []string
	landRef []string
}

type outcome struct {
	op       string
	d        time.Duration
	err      string
	conflict bool
}

type op struct {
	name   string
	weight float64
	fn     func(ctx context.Context, e *env, w *worker) (conflict bool, err error)
}

type worker struct {
	id      int
	rng     *rand.Rand
	iter    int
	landTip string
}

func runLoad(ctx context.Context, args []string) error {
	var f runFlags
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.StringVar(&f.url, "url", "http://127.0.0.1:8080", "service base URL")
	fs.StringVar(&f.key, "key", "motte.key", "Ed25519 PKCS#8 PEM key")
	fs.StringVar(&f.kid, "kid", "local", "key ID")
	fs.StringVar(&f.dir, "out", "lt", "directory of the manifest and samples")
	fs.StringVar(&f.mix, "mix", "object=10,has=5,trees=3,wsread=2,fetch=1,cas=2,land=1,push=1", "operation=weight list")
	fs.StringVar(&f.watch, "watch", "", "name=substring list of processes whose CPU and memory to sample (Linux)")
	fs.IntVar(&f.conc, "c", 16, "concurrent clients")
	fs.IntVar(&f.casRefs, "cas-refs", 1, "refs that the cas operation contends on")
	fs.IntVar(&f.fetchBack, "fetch-back", 100, "commits behind the tip that a delta fetch starts from")
	fs.DurationVar(&f.duration, "d", 30*time.Second, "run time")
	fs.Uint64Var(&f.seed, "seed", 2, "random seed")
	fs.BoolVar(&f.jsonOut, "json", false, "print the report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, err := setup(ctx, f)
	if err != nil {
		return err
	}
	ops, err := e.pickOps(f.mix)
	if err != nil {
		return err
	}
	before := scrape(f.url)
	ws := startWatch(f.watch)
	start := time.Now()
	rctx, cancel := context.WithTimeout(ctx, f.duration)
	defer cancel()
	outs := make([][]outcome, f.conc)
	var wg sync.WaitGroup
	for i := range f.conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := &worker{id: i, rng: rand.New(rand.NewPCG(f.seed, uint64(i)+100))}
			for rctx.Err() == nil {
				o := ops.pick(w.rng)
				octx, ocancel := context.WithTimeout(ctx, 90*time.Second)
				t0 := time.Now()
				conflict, err := o.fn(octx, e, w)
				d := time.Since(t0)
				ocancel()
				if rctx.Err() != nil && err != nil {
					break
				}
				oc := outcome{op: o.name, d: d, conflict: conflict}
				if conflict {
					err = nil
				}
				if err != nil {
					oc.err = errClass(err)
				}
				outs[i] = append(outs[i], oc)
				w.iter++
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	rep := buildReport(f, e.m, elapsed, outs, before, scrape(f.url), ws.stop())
	if f.jsonOut {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		rep.print()
	}
	return nil
}

func errClass(err error) string {
	s := err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(s, "deadline exceeded"):
		return "timeout"
	case strings.Contains(s, "connection refused") || strings.Contains(s, "EOF") || strings.Contains(s, "reset by peer"):
		return "connection"
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

func setup(ctx context.Context, f runFlags) (*env, error) {
	m, err := readManifest(f.dir)
	if err != nil {
		return nil, err
	}
	smp, err := readSamples(f.dir)
	if err != nil {
		return nil, err
	}
	sk, err := loadSigner(f.key, f.kid)
	if err != nil {
		return nil, err
	}
	e := &env{f: f, m: m, sg: signer{s: sk, repo: m.Repo}}
	for _, s := range smp {
		switch s.Kind {
		case 'b':
			e.blobs = append(e.blobs, s)
		case 't':
			e.trees = append(e.trees, s)
		case 'c':
			e.commits = append(e.commits, s)
		}
	}
	e.read = e.sg.client(f.url, grant.ScopeRead)
	e.merge = e.sg.client(f.url, grant.ScopeMerge)
	e.http = newHTTPGit(f.url, m.Path, e.sg.mint(grant.ScopeRead), e.sg.mint(grant.ScopeWrite))
	for i := range f.casRefs {
		name := "lt/cas/" + strconv.Itoa(i)
		if err := e.ensureRef(ctx, name); err != nil {
			return nil, err
		}
		e.casRefs = append(e.casRefs, name)
	}
	if err := e.ensureRef(ctx, "lt/hot"); err != nil {
		return nil, err
	}
	for i := range f.conc {
		name := "lt/land/" + strconv.Itoa(i)
		if err := e.ensureRef(ctx, name); err != nil {
			return nil, err
		}
		e.landRef = append(e.landRef, name)
	}
	return e, nil
}

func (e *env) ensureRef(ctx context.Context, name string) error {
	if _, err := e.merge.Ref(ctx, name); err == nil {
		return nil
	}
	_, err := e.merge.SwapRef(ctx, name, 0, e.m.Tip)
	return err
}

type opSet struct {
	ops []op
	sum float64
}

func (s opSet) pick(r *rand.Rand) op {
	x := r.Float64() * s.sum
	for _, o := range s.ops {
		if x < o.weight {
			return o
		}
		x -= o.weight
	}
	return s.ops[len(s.ops)-1]
}

func (e *env) pickOps(mix string) (opSet, error) {
	all := map[string]op{}
	for _, o := range allOps() {
		all[o.name] = o
	}
	var s opSet
	for _, part := range strings.Split(mix, ",") {
		name, w, ok := strings.Cut(part, "=")
		o, found := all[name]
		wt, err := strconv.ParseFloat(w, 64)
		if !ok || !found || err != nil || wt <= 0 {
			return s, fmt.Errorf("bad mix entry %q", part)
		}
		o.weight = wt
		s.ops = append(s.ops, o)
		s.sum += wt
	}
	return s, nil
}

type report struct {
	Flags    runFlags               `json:"flags"`
	Manifest Manifest               `json:"manifest"`
	Seconds  float64                `json:"seconds"`
	Ops      []opReport             `json:"ops"`
	Server   map[string]float64     `json:"server_counters"`
	Watch    map[string]watchReport `json:"watch"`
}

type opReport struct {
	Op        string         `json:"op"`
	Count     int            `json:"count"`
	PerSec    float64        `json:"per_sec"`
	P50ms     float64        `json:"p50_ms"`
	P95ms     float64        `json:"p95_ms"`
	P99ms     float64        `json:"p99_ms"`
	MaxMs     float64        `json:"max_ms"`
	Conflicts int            `json:"conflicts"`
	Errors    int            `json:"errors"`
	ErrKinds  map[string]int `json:"error_kinds,omitempty"`
}

func buildReport(f runFlags, m Manifest, el time.Duration, outs [][]outcome, before, after map[string]float64, w map[string]watchReport) report {
	by := map[string][]outcome{}
	for _, o := range outs {
		for _, x := range o {
			by[x.op] = append(by[x.op], x)
		}
	}
	r := report{Flags: f, Manifest: m, Seconds: el.Seconds(), Watch: w, Server: map[string]float64{}}
	for k, v := range after {
		if strings.HasSuffix(k, "_total") {
			r.Server[k] = v - before[k]
		} else {
			r.Server[k] = v
		}
	}
	names := make([]string, 0, len(by))
	for n := range by {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		xs := by[n]
		or := opReport{Op: n, Count: len(xs), PerSec: float64(len(xs)) / el.Seconds(), ErrKinds: map[string]int{}}
		var ds []float64
		for _, x := range xs {
			if x.conflict {
				or.Conflicts++
			}
			if x.err != "" {
				or.Errors++
				or.ErrKinds[x.err]++
				continue
			}
			ds = append(ds, float64(x.d)/1e6)
		}
		sort.Float64s(ds)
		pct := func(p float64) float64 {
			if len(ds) == 0 {
				return 0
			}
			return ds[min(len(ds)-1, int(p*float64(len(ds))))]
		}
		or.P50ms, or.P95ms, or.P99ms = pct(0.50), pct(0.95), pct(0.99)
		if len(ds) > 0 {
			or.MaxMs = ds[len(ds)-1]
		}
		r.Ops = append(r.Ops, or)
	}
	return r
}

func (r report) print() {
	fmt.Printf("repo %s objects=%d packs=%d c=%d mix=%s %.0fs\n", r.Manifest.Repo, r.Manifest.Objects, r.Manifest.Packs, r.Flags.conc, r.Flags.mix, r.Seconds)
	fmt.Printf("%-14s %8s %8s %9s %9s %9s %9s %6s %6s\n", "op", "count", "per_s", "p50_ms", "p95_ms", "p99_ms", "max_ms", "conf", "err")
	for _, o := range r.Ops {
		fmt.Printf("%-14s %8d %8.1f %9.1f %9.1f %9.1f %9.1f %6d %6d", o.Op, o.Count, o.PerSec, o.P50ms, o.P95ms, o.P99ms, o.MaxMs, o.Conflicts, o.Errors)
		if len(o.ErrKinds) > 0 {
			b, _ := json.Marshal(o.ErrKinds)
			fmt.Printf("  %s", b)
		}
		fmt.Println()
	}
	keys := make([]string, 0, len(r.Server))
	for k := range r.Server {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s %.0f\n", k, r.Server[k])
	}
	for n, w := range r.Watch {
		fmt.Printf("watch %s: cpu avg %.2f cores, peak %.2f cores, rss peak %d MiB, procs peak %d\n", n, w.AvgCores, w.PeakCores, w.PeakRSSMiB, w.Procs)
	}
	_ = os.Stdout.Sync()
}

func landReq(ref, base, head, id string) api.LandRequest {
	return api.LandRequest{ID: shortID(id), Ref: ref, Base: base, Head: head}
}

func shortID(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:12])
}
