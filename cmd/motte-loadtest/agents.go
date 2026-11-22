package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
)

const (
	stepTimeout = 90 * time.Second
	treeEntries = 256
	landWaits   = 4
)

var errAbandoned = errors.New("land retries spent")

type agentStats struct {
	tasks, landed, firstTry, abandoned, attempts, conflicts, retries atomic.Int64
	clones, fetches, cloneBytes, fetchBytes                          atomic.Int64
}

type agentReport struct {
	Tasks         int64   `json:"tasks"`
	Landed        int64   `json:"landed"`
	Abandoned     int64   `json:"abandoned"`
	LandAttempts  int64   `json:"land_attempts"`
	LandConflicts int64   `json:"land_conflicts"`
	LandRetries   int64   `json:"land_retries"`
	FirstTryRate  float64 `json:"first_try_rate"`
	ConflictRate  float64 `json:"conflict_rate"`
	LandedPerMin  float64 `json:"landed_per_min"`
	Clones        int64   `json:"clones"`
	Fetches       int64   `json:"fetches"`
	CloneKiBAvg   float64 `json:"clone_kib_avg"`
	FetchKiBAvg   float64 `json:"fetch_kib_avg"`
	RangeReads    float64 `json:"range_reads_per_task"`
	RangeKiB      float64 `json:"range_kib_per_task"`
}

func (s *agentStats) report(el time.Duration, server map[string]float64) *agentReport {
	r := &agentReport{
		Tasks: s.tasks.Load(), Landed: s.landed.Load(), Abandoned: s.abandoned.Load(),
		LandAttempts: s.attempts.Load(), LandConflicts: s.conflicts.Load(),
		Clones: s.clones.Load(), Fetches: s.fetches.Load(),
	}
	r.LandRetries = s.retries.Load()
	r.LandedPerMin = float64(r.Landed) / el.Minutes()
	if r.Tasks > 0 {
		r.FirstTryRate = float64(s.firstTry.Load()) / float64(r.Tasks)
	}
	if r.LandAttempts > 0 {
		r.ConflictRate = float64(r.LandConflicts) / float64(r.LandAttempts)
	}
	if r.Clones > 0 {
		r.CloneKiBAvg = float64(s.cloneBytes.Load()) / 1024 / float64(r.Clones)
	}
	if r.Fetches > 0 {
		r.FetchKiBAvg = float64(s.fetchBytes.Load()) / 1024 / float64(r.Fetches)
	}
	if r.Tasks > 0 {
		r.RangeReads = server["motte_bucket_range_reads_total"] / float64(r.Tasks)
		r.RangeKiB = server["motte_bucket_range_read_bytes_total"] / 1024 / float64(r.Tasks)
	}
	return r
}

type branch struct {
	ref  string
	old  string
	sent int
}

type agent struct {
	id      int
	e       *env
	rng     *rand.Rand
	hot     hotSet
	run     context.Context
	rec     func(op string, d time.Duration, err error, conflict bool)
	ed      *editor
	seen    string
	seq     int
	started bool
	nonce   int64
	trees   map[string][]api.TreeEntry
	roots   map[string]string
	content map[string][]byte
}

func (e *env) agentLoop(run, ctx context.Context, w *worker, hot hotSet, rec func(string, time.Duration, error, bool)) {
	a := &agent{id: w.id, e: e, rng: w.rng, hot: hot, run: run, rec: rec, nonce: time.Now().UnixNano() % 1e9,
		ed: &editor{c: e.read, local: map[string][]byte{}, keep: true}, trees: map[string][]api.TreeEntry{}, roots: map[string]string{}, content: map[string][]byte{}}
	a.seen = e.behind(w.rng)
	for run.Err() == nil {
		a.task(ctx)
		a.think()
	}
}

func (a *agent) think() {
	d := time.Duration(float64(a.e.f.think) * (0.5 + a.rng.Float64()))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-a.run.Done():
	}
}

func (a *agent) do(ctx context.Context, op string, fn func(context.Context) error) error {
	if err := a.run.Err(); err != nil {
		return err
	}
	c, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()
	t0 := time.Now()
	err := fn(c)
	if a.run.Err() == nil || err == nil {
		a.rec(op, time.Since(t0), err, errors.Is(err, client.ErrLandConflict))
	}
	return err
}

func (a *agent) task(ctx context.Context) {
	t0 := time.Now()
	a.seq++
	err := a.runTask(ctx)
	if err != nil && a.run.Err() != nil {
		return
	}
	a.e.agents.tasks.Add(1)
	if errors.Is(err, errAbandoned) {
		a.e.agents.abandoned.Add(1)
	}
	a.rec("task", time.Since(t0), err, false)
}

func (a *agent) runTask(ctx context.Context) error {
	f := a.e.f
	base, root, err := a.start(ctx)
	if err != nil {
		return err
	}
	cluster := a.hot.cluster(a.rng, f.cluster)
	for range f.exploreReads {
		dir := a.hot.dir(a.rng)
		if a.rng.IntN(10) < 7 {
			dir = cluster[a.rng.IntN(len(cluster))]
		}
		files := make([]int, 1+a.rng.IntN(4))
		for i := range files {
			files[i] = a.hot.fileIn(a.rng, dir)
		}
		if err := a.readFiles(ctx, root, dir, files); err != nil {
			return err
		}
		a.think()
	}
	edits := make([][]fileEdit, max(1, f.commits-1+a.rng.IntN(3)))
	for i := range edits {
		edits[i] = a.planEdits(a.pickFiles(cluster))
	}
	br := &branch{ref: fmt.Sprintf("refs/heads/lt/agent/%d/%d-%d", a.id, a.nonce, a.seq), old: zeroSHA}
	head, err := a.commitAll(ctx, base, edits, br)
	if err != nil {
		return err
	}
	return a.land(ctx, base, edits, br, head)
}

func (a *agent) start(ctx context.Context) (tip, root string, err error) {
	var r api.Ref
	if err = a.do(ctx, "ref", func(c context.Context) (err error) { r, err = a.e.read.Ref(c, a.e.f.mainRef); return }); err != nil {
		return
	}
	tip = r.Commit
	cold := a.seen == "" || a.started && a.rng.Float64() < a.e.f.coldRate
	a.started = true
	switch {
	case cold:
		clear(a.trees)
		clear(a.roots)
		clear(a.ed.local)
		clear(a.content)
		err = a.pull(ctx, "clone", tip, "", true)
	case a.seen != tip:
		err = a.pull(ctx, "fetch", tip, a.seen, false)
	}
	if err != nil {
		return
	}
	a.seen = tip
	root, err = a.rootOf(ctx, tip)
	return
}

func (a *agent) pull(ctx context.Context, op, want, have string, partial bool) error {
	return a.do(ctx, op, func(c context.Context) error {
		n, err := a.e.http.fetchN(c, want, have, partial)
		if err != nil {
			return err
		}
		if op == "clone" {
			a.e.agents.clones.Add(1)
			a.e.agents.cloneBytes.Add(n)
		} else {
			a.e.agents.fetches.Add(1)
			a.e.agents.fetchBytes.Add(n)
		}
		return nil
	})
}

func (a *agent) rootOf(ctx context.Context, commit string) (string, error) {
	if r, ok := a.roots[commit]; ok {
		return r, nil
	}
	var cm api.Commit
	err := a.do(ctx, "commit", func(c context.Context) (err error) { cm, err = a.e.read.Commit(c, commit); return })
	if err == nil {
		a.roots[commit] = cm.Tree
	}
	return cm.Tree, err
}

func (a *agent) entries(ctx context.Context, sha string) ([]api.TreeEntry, error) {
	if ents, ok := a.trees[sha]; ok {
		return ents, nil
	}
	err := a.do(ctx, "tree", func(c context.Context) error {
		ts, err := a.e.read.Trees(c, sha, treeEntries)
		for _, t := range ts.Trees {
			a.trees[t.SHA] = t.Entries
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	ents, ok := a.trees[sha]
	if !ok {
		return nil, fmt.Errorf("tree %s missing from its answer", sha)
	}
	return ents, nil
}

func lookup(ents []api.TreeEntry, name string) (string, bool) {
	for _, en := range ents {
		if raw, err := base64.StdEncoding.DecodeString(en.Name); err == nil && string(raw) == name {
			return en.SHA, true
		}
	}
	return "", false
}

func (a *agent) readFiles(ctx context.Context, root string, dir int, files []int) error {
	cur := root
	for _, n := range a.e.shape.paths[dir] {
		ents, err := a.entries(ctx, cur)
		if err != nil {
			return err
		}
		var ok bool
		if cur, ok = lookup(ents, n); !ok {
			return fmt.Errorf("no directory %s", n)
		}
	}
	ents, err := a.entries(ctx, cur)
	if err != nil {
		return err
	}
	shas := make([]string, 0, len(files))
	for _, f := range files {
		sha, ok := lookup(ents, fileName(f))
		if !ok {
			return fmt.Errorf("no file %d", f)
		}
		shas = append(shas, sha)
	}
	return a.do(ctx, "blob", func(c context.Context) error { _, err := a.e.read.Blobs(c, shas); return err })
}

func (a *agent) pickFiles(cluster []int) []int {
	seen := map[int]bool{}
	var out []int
	for range 1 + a.rng.IntN(a.e.f.editFiles) {
		f := a.hot.file(a.rng)
		if a.rng.IntN(10) < 8 {
			f = a.hot.fileIn(a.rng, cluster[a.rng.IntN(len(cluster))])
		}
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	return out
}

func (a *agent) commitAll(ctx context.Context, base string, edits [][]fileEdit, br *branch) (string, error) {
	a.ed.objs, br.sent = nil, 0
	parent := base
	tree, err := a.rootOf(ctx, base)
	if err != nil {
		return "", err
	}
	root, work := tree, map[int][]byte{}
	for i, files := range edits {
		tag := fmt.Sprintf("agent %d task %d commit %d", a.id, a.seq, i)
		if !a.e.f.editWhole {
			if err := a.load(ctx, root, work, files); err != nil {
				return "", err
			}
		}
		err := a.do(ctx, "edit", func(c context.Context) error {
			for _, fe := range files {
				data := []byte(fmt.Sprintf("%s file %d\n%s\n", tag, fe.file, strings.Repeat("y", 200+a.rng.IntN(1800))))
				if !a.e.f.editWhole {
					data = fe.apply(work[fe.file], tag)
					work[fe.file] = data
				}
				var err error
				if tree, err = a.ed.setFile(c, a.e.shape.names(fe.file), tree, a.ed.add(gitpack.Blob, data)); err != nil {
					return err
				}
			}
			who := fmt.Sprintf("agent <agent@motte.invalid> %d +0000", time.Now().Unix())
			parent = a.ed.add(gitpack.Commit, []byte(fmt.Sprintf("tree %s\nparent %s\nauthor %s\ncommitter %s\n\n%s\n", tree, parent, who, who, tag)))
			return nil
		})
		if err != nil {
			return "", err
		}
		if (i+1)%a.e.f.pushEvery == 0 || i == len(edits)-1 {
			if err := a.push(ctx, br, parent); err != nil {
				return "", err
			}
		}
		if i < len(edits)-1 {
			a.think()
		}
	}
	return parent, nil
}

func (a *agent) push(ctx context.Context, br *branch, head string) error {
	seen := map[string]bool{}
	var objs []gitpack.Object
	for _, o := range a.ed.objs[br.sent:] {
		if sha := gitpack.Hash(o.Type, o.Data).String(); !seen[sha] {
			seen[sha] = true
			objs = append(objs, o)
		}
	}
	pack, _, _ := gitpack.Build(objs)
	if err := a.do(ctx, "push", func(c context.Context) error { return a.e.http.push(c, br.ref, br.old, head, pack) }); err != nil {
		return err
	}
	br.old, br.sent = head, len(a.ed.objs)
	return nil
}

func (a *agent) land(ctx context.Context, base string, edits [][]fileEdit, br *branch, head string) error {
	for attempt := 0; ; attempt++ {
		id := fmt.Sprintf("agent-%d-%d-%d-%d", a.id, a.nonce, a.seq, attempt)
		a.e.agents.attempts.Add(1)
		var err error
		for range landWaits {
			err = a.do(ctx, "land", func(c context.Context) error {
				_, err := a.e.merge.Land(c, a.e.m.Repo, landReq(a.e.f.mainRef, base, head, id))
				return err
			})
			if !errors.Is(err, client.ErrLandTimeout) {
				break
			}
		}
		if err == nil {
			a.e.agents.landed.Add(1)
			if attempt == 0 {
				a.e.agents.firstTry.Add(1)
			}
			return nil
		}
		if !errors.Is(err, client.ErrLandConflict) {
			return err
		}
		a.e.agents.conflicts.Add(1)
		if attempt >= a.e.f.landRetries {
			return errAbandoned
		}
		a.e.agents.retries.Add(1)
		t0 := time.Now()
		if base, head, err = a.rebase(ctx, edits, br, client.ConflictPaths(err)); err != nil {
			return err
		}
		a.rec("retry", time.Since(t0), nil, false)
	}
}

func (a *agent) rebase(ctx context.Context, edits [][]fileEdit, br *branch, paths []string) (base, head string, err error) {
	var r api.Ref
	if err = a.do(ctx, "ref", func(c context.Context) (err error) { r, err = a.e.read.Ref(c, a.e.f.mainRef); return }); err != nil {
		return
	}
	base = r.Commit
	if base != a.seen {
		if err = a.pull(ctx, "refetch", base, a.seen, false); err != nil {
			return
		}
		a.seen = base
	}
	root, err := a.rootOf(ctx, base)
	if err != nil {
		return
	}
	if !a.e.f.editWhole {
		paths = nil
	}
	for _, p := range paths[:min(len(paths), 3)] {
		var f int
		if _, serr := fmt.Sscanf(p[strings.LastIndexByte(p, '/')+1:], "f%07d.go", &f); serr != nil {
			continue
		}
		if err = a.readFiles(ctx, root, a.e.shape.dirOf(f), []int{f}); err != nil {
			return
		}
	}
	head, err = a.commitAll(ctx, base, edits, br)
	return
}

func (e *env) behind(r *rand.Rand) string {
	var in []string
	for _, c := range e.commits {
		if back := e.m.Commits - c.Seq; back >= e.f.fetchBack/2 && back <= 2*e.f.fetchBack {
			in = append(in, c.SHA)
		}
	}
	if len(in) == 0 {
		return ""
	}
	return in[r.IntN(len(in))]
}
