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
	scenario, mainRef              string
	skew, coldRate                 float64
	cluster, commits, editFiles    int
	pushEvery, exploreReads        int
	landRetries, editLines         int
	editWhole                      bool
	think                          time.Duration
}

func (f runFlags) agentic() bool { return f.scenario == "agents" || f.scenario == "vfsd" }

type env struct {
	f       runFlags
	m       Manifest
	shape   *shape
	blobs   []Sample
	trees   []Sample
	commits []Sample
	read    *client.Client
	merge   *client.Client
	sg      signer
	http    *httpGit
	casRefs []string
	landRef []string
	agents  agentStats
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
	fs.IntVar(&f.fetchBack, "fetch-back", 100, "commits behind the tip that a delta fetch starts from; agents start between half and twice this far behind")
	fs.DurationVar(&f.duration, "d", 30*time.Second, "run time")
	fs.Uint64Var(&f.seed, "seed", 2, "random seed")
	fs.BoolVar(&f.jsonOut, "json", false, "print the report as JSON")
	fs.StringVar(&f.scenario, "scenario", "mix", "mix (the weighted micro-operations), agents (one coding-agent session per client, with git clones, fetches and pushes) or vfsd (one agent per client that mounts a lazy workspace through the daemon: bulk tree reads, blob read-ahead, object uploads and ref swaps)")
	fs.StringVar(&f.mainRef, "main-ref", "lt/main", "agents: the ref that agents start from and land on")
	fs.Float64Var(&f.skew, "skew", 0.8, "agents: Zipf exponent of file and directory popularity; 0 is uniform")
	fs.IntVar(&f.cluster, "cluster", 3, "agents: leaf directories in the task cluster of an agent")
	fs.IntVar(&f.commits, "commits", 4, "agents: mean commits of a task")
	fs.IntVar(&f.editFiles, "edit-files", 4, "agents: most files that one commit changes")
	fs.IntVar(&f.pushEvery, "push-every", 2, "agents: commits between two pushes of the task branch")
	fs.IntVar(&f.exploreReads, "explore-reads", 8, "agents: directory reads before the first edit")
	fs.IntVar(&f.landRetries, "land-retries", 5, "agents: retries of a land after a conflict")
	fs.IntVar(&f.editLines, "edit-lines", 20, "agents: most lines that one edit of a file replaces or inserts, at most a quarter of the file")
	fs.BoolVar(&f.editWhole, "edit-whole", false, "agents: rewrite each edited file as a whole instead of changing a region of it")
	fs.Float64Var(&f.coldRate, "cold-rate", 0.05, "agents: chance that a task starts from a cold partial clone")
	fs.DurationVar(&f.think, "think", 50*time.Millisecond, "agents: mean pause between steps")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, err := setup(ctx, f)
	if err != nil {
		return err
	}
	var ops opSet
	if f.scenario == "mix" {
		if ops, err = e.pickOps(f.mix); err != nil {
			return err
		}
	} else if !f.agentic() {
		return fmt.Errorf("unknown scenario %q", f.scenario)
	}
	hot := newHotSet(e.shape, f.skew)
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
			if f.agentic() {
				e.agentLoop(rctx, ctx, w, hot, func(op string, d time.Duration, err error, conflict bool) {
					if conflict {
						err = nil
					}
					oc := outcome{op: op, d: d, conflict: conflict}
					if err != nil {
						oc.err = errClass(err)
					}
					outs[i] = append(outs[i], oc)
				})
				return
			}
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
	if f.agentic() {
		rep.Agents = e.agents.report(elapsed, rep.Server)
	}
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
	sh, err := loadShape(f.dir, m)
	if err != nil {
		return nil, err
	}
	e := &env{f: f, m: m, shape: sh, sg: signer{s: sk, repo: m.Repo}}
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
	if err := e.ensureRef(ctx, f.mainRef); err != nil {
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
	Agents   *agentReport           `json:"agents,omitempty"`
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
	what := "mix=" + r.Flags.mix
	if r.Flags.agentic() {
		what = fmt.Sprintf("scenario="+r.Flags.scenario+" skew=%.2f cluster=%d commits=%d push-every=%d think=%s cold=%.2f", r.Flags.skew, r.Flags.cluster, r.Flags.commits, r.Flags.pushEvery, r.Flags.think, r.Flags.coldRate)
	}
	fmt.Printf("repo %s objects=%d commits=%d packs=%d c=%d %s %.0fs\n", r.Manifest.Repo, r.Manifest.Objects, r.Manifest.Commits, r.Manifest.Packs, r.Flags.conc, what, r.Seconds)
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
	if a := r.Agents; a != nil {
		fmt.Printf("agents: tasks=%d landed=%d (%.1f/min) abandoned=%d first-try=%.1f%%\n", a.Tasks, a.Landed, a.LandedPerMin, a.Abandoned, 100*a.FirstTryRate)
		fmt.Printf("agents: lands=%d conflicts=%d (%.1f%%) retries=%d\n", a.LandAttempts, a.LandConflicts, 100*a.ConflictRate, a.LandRetries)
		fmt.Printf("agents: clones=%d avg %.1f KiB, fetches=%d avg %.1f KiB, bucket range reads per task %.1f (%.1f KiB)\n", a.Clones, a.CloneKiBAvg, a.Fetches, a.FetchKiBAvg, a.RangeReads, a.RangeKiB)
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
