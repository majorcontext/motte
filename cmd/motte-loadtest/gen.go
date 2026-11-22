package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
)

const maxPackObjects = 90000

type node struct {
	name string
	sha  gitpack.SHA
	kids []*node
	dir  bool
}

func (n *node) encode() []byte {
	var b []byte
	for _, k := range n.kids {
		mode := "100644"
		if k.dir {
			mode = "40000"
		}
		b = append(b, mode...)
		b = append(b, ' ')
		b = append(b, k.name...)
		b = append(b, 0)
		b = append(b, k.sha[:]...)
	}
	return b
}

type layout struct{ fanout, depth, leaf int }

func (l layout) files() int {
	n := l.leaf
	for range l.depth {
		n *= l.fanout
	}
	return n
}

type emitter struct {
	rng        *rand.Rand
	corpus     []byte
	median     int
	sampleN    int
	batch      []gitpack.Object
	batchBytes int
	maxBatch   int
	atCommit   bool
	flush      func([]gitpack.Object)
	total      atomic.Int64
	samples    *bufio.Writer
	commitSeq  int
}

func newCorpus(rng *rand.Rand) []byte {
	words := make([]string, 600)
	for i := range words {
		w := make([]byte, 2+rng.IntN(9))
		for j := range w {
			w[j] = byte('a' + rng.IntN(26))
		}
		words[i] = string(w)
	}
	var sb strings.Builder
	for sb.Len() < 4<<20 {
		sb.WriteString(words[int(math.Abs(rng.NormFloat64()*150))%len(words)])
		if rng.IntN(9) == 0 {
			sb.WriteString(";\n")
		} else {
			sb.WriteByte(' ')
		}
	}
	return []byte(sb.String())
}

func (e *emitter) blob(id int) *node {
	size := int(math.Exp(math.Log(float64(e.median)) + 1.1*e.rng.NormFloat64()))
	size = min(max(size, 32), 256<<10)
	off := e.rng.IntN(len(e.corpus) - size)
	data := append([]byte(fmt.Sprintf("// blob %d\n", id)), e.corpus[off:off+size]...)
	n := &node{sha: e.put(gitpack.Blob, data, 'b')}
	return n
}

func (e *emitter) put(t gitpack.Type, data []byte, kind byte) gitpack.SHA {
	sha := gitpack.Hash(t, data)
	e.batch = append(e.batch, gitpack.Object{Type: t, Data: data})
	e.batchBytes += len(data)
	e.total.Add(1)
	if e.sampleN > 0 && (kind == 'c' && e.commitSeq%25 == 0 || kind != 'c' && e.rng.IntN(e.sampleN) == 0) {
		fmt.Fprintf(e.samples, "%c %s %d\n", kind, sha, e.commitSeq)
	}
	if !e.atCommit && (len(e.batch) >= e.maxBatch || e.batchBytes >= 40<<20) {
		e.flushBatch()
	}
	return sha
}

func (e *emitter) flushBatch() {
	if len(e.batch) > 0 {
		e.flush(e.batch)
		e.batch, e.batchBytes = nil, 0
	}
}

func dirName(d int) string  { return fmt.Sprintf("d%02x", d) }
func fileName(i int) string { return fmt.Sprintf("f%07d.go", i) }

func (e *emitter) build(l layout, level, idx int, blobID *int) *node {
	n := &node{dir: true}
	if level == l.depth {
		for j := range l.leaf {
			i := idx*l.leaf + j
			b := e.blob(*blobID)
			*blobID++
			b.name = fileName(i)
			n.kids = append(n.kids, b)
		}
	} else {
		for d := range l.fanout {
			k := e.build(l, level+1, idx*l.fanout+d, blobID)
			k.name = dirName(d)
			n.kids = append(n.kids, k)
		}
	}
	n.sha = e.put(gitpack.Tree, n.encode(), 't')
	return n
}

func (l layout) path(root *node, file int) []*node {
	dirIdx, j := file/l.leaf, file%l.leaf
	digits := make([]int, l.depth)
	for k := l.depth - 1; k >= 0; k-- {
		digits[k] = dirIdx % l.fanout
		dirIdx /= l.fanout
	}
	p := []*node{root}
	cur := root
	for _, d := range digits {
		cur = cur.kids[d]
		p = append(p, cur)
	}
	return append(p, cur.kids[j])
}

type genFlags struct {
	url, key, kid, out    string
	objects, k, median    int
	fanout, depth, leaf   int
	seed                  uint64
	sampleN, builders, up int
	skew                  float64
	packObjects           int
	via                   string
}

func runGen(ctx context.Context, args []string) error {
	var f genFlags
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	fs.StringVar(&f.url, "url", "http://127.0.0.1:8080", "service base URL")
	fs.StringVar(&f.key, "key", "motte.key", "Ed25519 PKCS#8 PEM key")
	fs.StringVar(&f.kid, "kid", "local", "key ID")
	fs.StringVar(&f.out, "out", "lt", "output directory of the manifest and samples")
	fs.IntVar(&f.objects, "objects", 1000000, "objects to generate")
	fs.IntVar(&f.k, "files-per-commit", 4, "files that each commit changes")
	fs.IntVar(&f.median, "blob-median", 1024, "median blob bytes (log-normal)")
	fs.IntVar(&f.fanout, "fanout", 16, "directories per directory")
	fs.IntVar(&f.depth, "depth", 3, "directory levels above the leaf directories")
	fs.IntVar(&f.leaf, "leaf", 64, "files per leaf directory")
	fs.Uint64Var(&f.seed, "seed", 1, "random seed")
	fs.IntVar(&f.sampleN, "sample", 200, "keep 1 in N blobs and trees as samples")
	fs.Float64Var(&f.skew, "skew", 0, "Zipf exponent of the popularity of the files that history changes; 0 is uniform")
	fs.IntVar(&f.builders, "builders", 4, "pack builders")
	fs.IntVar(&f.up, "uploaders", 2, "pack uploaders")
	fs.IntVar(&f.packObjects, "pack-objects", maxPackObjects, "objects of one pack or one put")
	fs.StringVar(&f.via, "via", "import", "import to send packs by PUT /v1/vcs/packs, or push to send each pack, cut at a commit, by git receive-pack to its own ref")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return generate(ctx, f)
}

func generate(ctx context.Context, f genFlags) error {
	sg, err := loadSigner(f.key, f.kid)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(f.out, 0o755); err != nil {
		return err
	}
	op := signer{s: sg}.client(f.url, grant.ScopeOperator)
	path, err := op.CreateRepo(ctx, "loadtest", "loadtest")
	if err != nil {
		return err
	}
	repo := "native:" + path
	g := signer{s: sg, repo: repo}
	imp, merge := g.client(f.url, grant.ScopeImport), g.client(f.url, grant.ScopeMerge)
	hg := newHTTPGit(f.url, path, g.mint(grant.ScopeRead), g.mint(grant.ScopeWrite))

	sf, err := os.Create(f.out + "/samples.txt")
	if err != nil {
		return err
	}
	defer sf.Close()
	rng := rand.New(rand.NewPCG(f.seed, 7))
	e := &emitter{rng: rng, corpus: newCorpus(rng), median: f.median, sampleN: f.sampleN, maxBatch: f.packObjects, atCommit: f.via == "push", samples: bufio.NewWriterSize(sf, 1<<20)}

	built := make(chan builtPack, f.builders)
	raw := make(chan []gitpack.Object, f.builders)
	e.flush = func(objs []gitpack.Object) {
		select {
		case raw <- objs:
		case <-ctx.Done():
		}
	}
	var packs atomic.Int64
	var packBytes atomic.Int64
	start := time.Now()
	eg, gctx := errgroup.WithContext(ctx)
	var bw sync.WaitGroup
	for range f.builders {
		bw.Add(1)
		eg.Go(func() error {
			defer bw.Done()
			for objs := range raw {
				bp := builtPack{objs: objs}
				if f.via != "push" {
					pack, idx, _ := gitpack.Build(objs)
					sum := sha256.Sum256(pack)
					bp = builtPack{id: hex.EncodeToString(sum[:]), pack: pack, idx: idx}
				}
				select {
				case built <- bp:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}
	var importNanos atomic.Int64
	var uw sync.WaitGroup
	for range f.up {
		uw.Add(1)
		eg.Go(func() error {
			defer uw.Done()
			for p := range built {
				t0 := time.Now()
				var err error
				for try := 0; try < 4; try++ {
					if p.objs != nil {
						err = pushObjects(gctx, hg, p.objs, int(packs.Load()))
					} else {
						err = imp.ImportPack(gctx, p.id, p.idx, p.pack)
					}
					if err == nil {
						break
					}
				}
				if err != nil {
					return fmt.Errorf("import pack: %w", err)
				}
				importNanos.Add(int64(time.Since(t0)))
				packs.Add(1)
				packBytes.Add(int64(len(p.pack)))
				for _, o := range p.objs {
					packBytes.Add(int64(len(o.Data)))
				}
			}
			return nil
		})
	}
	go func() { bw.Wait(); close(built) }()

	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				fmt.Fprintf(os.Stderr, "gen: %d objects, %d packs, %d MiB packs, %.0fs\n", e.total.Load(), packs.Load(), packBytes.Load()>>20, time.Since(start).Seconds())
			}
		}
	}()

	l := layout{f.fanout, f.depth, f.leaf}
	blobID := 0
	hot := newHotSet(l, f.skew)
	root := e.build(l, 0, 0, &blobID)
	commits := 0
	tip := e.commit(root, "", &commits)
	for int(e.total.Load()) < f.objects && ctx.Err() == nil {
		touched := map[*node]int{}
		for range f.k {
			i := rng.IntN(l.files())
			if f.skew > 0 {
				i = hot.file(rng)
			}
			p := l.path(root, i)
			leafBlob := e.blob(blobID)
			blobID++
			leafBlob.name = p[len(p)-1].name
			parent := p[len(p)-2]
			for j, kid := range parent.kids {
				if kid == p[len(p)-1] {
					parent.kids[j] = leafBlob
				}
			}
			for lvl, n := range p[:len(p)-1] {
				touched[n] = lvl
			}
		}
		order := make([]*node, 0, len(touched))
		for n := range touched {
			order = append(order, n)
		}
		sort.Slice(order, func(a, b int) bool { return touched[order[a]] > touched[order[b]] })
		for _, n := range order {
			n.sha = e.put(gitpack.Tree, n.encode(), 't')
		}
		tip = e.commit(root, tip, &commits)
	}
	e.flushBatch()
	close(raw)
	if err := eg.Wait(); err != nil {
		return err
	}
	close(done)
	if err := e.samples.Flush(); err != nil {
		return err
	}
	genSec := time.Since(start).Seconds()
	cur, err := merge.Ref(ctx, "main")
	if err != nil {
		return err
	}
	if _, err := merge.SwapRef(ctx, "main", cur.Generation, tip); err != nil {
		return err
	}
	m := Manifest{
		Repo: repo, Path: path, Tip: tip, Objects: int(e.total.Load()), Commits: commits, Files: l.files(), Fanout: l.fanout, Depth: l.depth,
		Leaf: l.leaf, Packs: int(packs.Load()), PackBytes: packBytes.Load(), GenSeconds: genSec, ImportSec: float64(importNanos.Load()) / 1e9, BlobMedian: f.median,
	}
	raw2, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(raw2))
	return os.WriteFile(f.out+"/manifest.json", raw2, 0o644)
}

type builtPack struct {
	id        string
	pack, idx []byte
	objs      []gitpack.Object
}

func pushObjects(ctx context.Context, h *httpGit, objs []gitpack.Object, n int) error {
	pack, _, _ := gitpack.Build(objs)
	last := objs[len(objs)-1]
	tip := gitpack.Hash(last.Type, last.Data).String()
	return h.push(ctx, fmt.Sprintf("refs/heads/gen/%d-%s", n, tip[:8]), zeroSHA, tip, pack)
}

func (e *emitter) commit(root *node, parent string, seq *int) string {
	*seq++
	e.commitSeq = *seq
	who := fmt.Sprintf("load <load@motte.invalid> %d +0000", 1700000000+*seq*60)
	var sb strings.Builder
	fmt.Fprintf(&sb, "tree %s\n", root.sha)
	if parent != "" {
		fmt.Fprintf(&sb, "parent %s\n", parent)
	}
	fmt.Fprintf(&sb, "author %s\ncommitter %s\n\ncommit %d\n", who, who, *seq)
	sha := e.put(gitpack.Commit, []byte(sb.String()), 'c').String()
	if e.atCommit && (len(e.batch) >= e.maxBatch || e.batchBytes >= 40<<20) {
		e.flushBatch()
	}
	return sha
}
