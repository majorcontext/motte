package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/pktline"
)

var zeroSHA = strings.Repeat("0", 40)

func allOps() []op {
	return []op{
		{name: "object", fn: opObject},
		{name: "has-hit", fn: opHasHit},
		{name: "has-miss", fn: opHasMiss},
		{name: "trees", fn: opTrees},
		{name: "blobs", fn: opBlobs},
		{name: "commit", fn: opCommit},
		{name: "wsread", fn: opWorkspaceRead},
		{name: "fetch", fn: opFetchDelta},
		{name: "clone-full", fn: func(ctx context.Context, e *env, w *worker) (bool, error) {
			return false, e.http.fetch(ctx, e.m.Tip, "", false)
		}},
		{name: "clone-partial", fn: func(ctx context.Context, e *env, w *worker) (bool, error) {
			return false, e.http.fetch(ctx, e.m.Tip, "", true)
		}},
		{name: "push", fn: opPush},
		{name: "land", fn: opLand},
		{name: "land-hot", fn: opLandHot},
		{name: "cas", fn: opCAS},
	}
}

func (w *worker) sample(s []Sample) Sample { return s[w.rng.IntN(len(s))] }

func opObject(ctx context.Context, e *env, w *worker) (bool, error) {
	s := w.sample(e.blobs)
	_, err := e.read.Object(ctx, s.SHA)
	return false, err
}

func opHasHit(ctx context.Context, e *env, w *worker) (bool, error) {
	shas := make([]string, 50)
	for i := range shas {
		shas[i] = w.sample(e.blobs).SHA
	}
	_, err := e.read.Has(ctx, shas)
	return false, err
}

func opHasMiss(ctx context.Context, e *env, w *worker) (bool, error) {
	shas := make([]string, 10)
	for i := range shas {
		var b [20]byte
		for j := range b {
			b[j] = byte(w.rng.Uint32())
		}
		shas[i] = fmt.Sprintf("%x", b)
	}
	_, err := e.read.Has(ctx, shas)
	return false, err
}

func opTrees(ctx context.Context, e *env, w *worker) (bool, error) {
	_, err := e.read.Trees(ctx, w.sample(e.trees).SHA, 2048)
	return false, err
}

func opBlobs(ctx context.Context, e *env, w *worker) (bool, error) {
	shas := make([]string, 8)
	for i := range shas {
		shas[i] = w.sample(e.blobs).SHA
	}
	_, err := e.read.Blobs(ctx, shas)
	return false, err
}

func opCommit(ctx context.Context, e *env, w *worker) (bool, error) {
	_, err := e.read.Commit(ctx, w.sample(e.commits).SHA)
	return false, err
}

func opWorkspaceRead(ctx context.Context, e *env, w *worker) (bool, error) {
	r, err := e.read.Ref(ctx, "main")
	if err != nil {
		return false, err
	}
	c, err := e.read.Commit(ctx, r.Commit)
	if err != nil {
		return false, err
	}
	ts, err := e.read.Trees(ctx, c.Tree, 4096)
	if err != nil {
		return false, err
	}
	if len(ts.Trees) == 0 {
		return false, errors.New("empty tree answer")
	}
	t := ts.Trees[w.rng.IntN(len(ts.Trees))]
	var shas []string
	for _, en := range t.Entries {
		if en.Mode == "100644" && len(shas) < 4 {
			shas = append(shas, en.SHA)
		}
	}
	if len(shas) == 0 {
		return false, nil
	}
	_, err = e.read.Blobs(ctx, shas)
	return false, err
}

func opFetchDelta(ctx context.Context, e *env, w *worker) (bool, error) {
	have := e.commits[0].SHA
	want := e.m.Commits - e.f.fetchBack
	for _, c := range e.commits {
		if c.Seq <= want {
			have = c.SHA
		}
	}
	return false, e.http.fetch(ctx, e.m.Tip, have, false)
}

type httpGit struct {
	hc          *http.Client
	base        string
	readTok     string
	writeTok    string
	fetchBytes  atomic.Int64
	pushCounter atomic.Int64
}

func newHTTPGit(url, path, read, write string) *httpGit {
	tr := &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 0, IdleConnTimeout: 30 * time.Second}
	return &httpGit{hc: &http.Client{Transport: tr}, base: url + "/git/native/" + path + ".git", readTok: read, writeTok: write}
}

func (h *httpGit) fetch(ctx context.Context, want, have string, partial bool) error {
	var b bytes.Buffer
	pw := pktline.NewWriter(&b)
	pw.Linef("command=fetch\n")
	pw.Linef("object-format=sha1\n")
	pw.Delim()
	pw.Linef("want %s\n", want)
	if have != "" {
		pw.Linef("have %s\n", have)
	}
	if partial {
		pw.Linef("filter blob:none\n")
	}
	pw.Linef("done\n")
	pw.Flush()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.base+"/git-upload-pack", &b)
	req.Header.Set("Git-Protocol", "version=2")
	req.Header.Set("Authorization", "Bearer "+h.readTok)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	resp, err := h.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	h.fetchBytes.Add(n)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("fetch status %d", resp.StatusCode)
	}
	return nil
}

func (h *httpGit) push(ctx context.Context, ref, old, newSHA string, pack []byte) error {
	var b bytes.Buffer
	pw := pktline.NewWriter(&b)
	pw.Linef("%s %s %s\x00report-status ofs-delta object-format=sha1\n", old, newSHA, ref)
	pw.Flush()
	b.Write(pack)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.base+"/git-receive-pack", &b)
	req.Header.Set("Authorization", "Bearer "+h.writeTok)
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	resp, err := h.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("push status %d %.100s", resp.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("ok "+ref)) {
		return fmt.Errorf("push refused: %.80q", body)
	}
	return nil
}

type editor struct {
	c     *client.Client
	local map[string][]byte
	objs  []gitpack.Object
}

func (ed *editor) tree(ctx context.Context, sha string) ([]byte, error) {
	if d, ok := ed.local[sha]; ok {
		return d, nil
	}
	o, err := ed.c.Object(ctx, sha)
	if err != nil {
		return nil, fmt.Errorf("tree %s: %w", sha, err)
	}
	return o.Data, nil
}

func (ed *editor) add(t gitpack.Type, data []byte) string {
	ed.objs = append(ed.objs, gitpack.Object{Type: t, Data: data})
	sha := gitpack.Hash(t, data).String()
	if t == gitpack.Tree {
		ed.local[sha] = data
	}
	return sha
}

func entry(tree []byte, name string) (sha string, shaOff int, err error) {
	needle := []byte(" " + name + "\x00")
	i := bytes.Index(tree, needle)
	if i < 0 {
		return "", 0, fmt.Errorf("no entry %s", name)
	}
	off := i + len(needle)
	return fmt.Sprintf("%x", tree[off:off+20]), off, nil
}

func fileNames(l layout, file int) []string {
	dirIdx := file / l.leaf
	names := make([]string, l.depth+1)
	for k := l.depth - 1; k >= 0; k-- {
		names[k] = dirName(dirIdx % l.fanout)
		dirIdx /= l.fanout
	}
	names[l.depth] = fileName(file)
	return names
}

func (ed *editor) setFile(ctx context.Context, l layout, root string, file int, blob string) (string, error) {
	names := fileNames(l, file)
	trees := make([][]byte, 0, len(names))
	cur := root
	for _, n := range names[:len(names)-1] {
		t, err := ed.tree(ctx, cur)
		if err != nil {
			return "", err
		}
		trees = append(trees, t)
		next, _, err := entry(t, n)
		if err != nil {
			return "", err
		}
		cur = next
	}
	leaf, err := ed.tree(ctx, cur)
	if err != nil {
		return "", err
	}
	trees = append(trees, leaf)
	newSHA := blob
	for i := len(trees) - 1; i >= 0; i-- {
		_, off, err := entry(trees[i], names[i])
		if err != nil {
			return "", err
		}
		nt := append([]byte(nil), trees[i]...)
		raw, _ := hex.DecodeString(newSHA)
		copy(nt[off:off+20], raw)
		newSHA = ed.add(gitpack.Tree, nt)
	}
	return newSHA, nil
}

func (e *env) layout() layout { return layout{e.m.Fanout, e.m.Depth, e.m.Leaf} }

func (e *env) newCommit(ctx context.Context, c *client.Client, base string, files []int, tag string) (*editor, string, error) {
	ed := &editor{c: c, local: map[string][]byte{}}
	bc, err := c.Object(ctx, base)
	if err != nil {
		return nil, "", fmt.Errorf("base %s: %w", base, err)
	}
	line, _, _ := strings.Cut(string(bc.Data), "\n")
	root := strings.TrimPrefix(line, "tree ")
	for _, f := range files {
		blob := ed.add(gitpack.Blob, []byte(fmt.Sprintf("loadtest %s file %d\n%s\n", tag, f, strings.Repeat("x", 400))))
		if root, err = ed.setFile(ctx, e.layout(), root, f, blob); err != nil {
			return nil, "", err
		}
	}
	who := fmt.Sprintf("load <load@motte.invalid> %d +0000", time.Now().Unix())
	msg := fmt.Sprintf("tree %s\nparent %s\nauthor %s\ncommitter %s\n\n%s\n", root, base, who, who, tag)
	return ed, ed.add(gitpack.Commit, []byte(msg)), nil
}

func (e *env) fileFor(w *worker, k int) int {
	return (w.id*100003 + w.iter*7919 + k*31) % e.m.Files
}

func opPush(ctx context.Context, e *env, w *worker) (bool, error) {
	tag := fmt.Sprintf("push-%d-%d-%d", w.id, w.iter, time.Now().UnixNano())
	ed, commit, err := e.newCommit(ctx, e.read, e.m.Tip, []int{e.fileFor(w, 0), e.fileFor(w, 1)}, tag)
	if err != nil {
		return false, err
	}
	pack, _, _ := gitpack.Build(ed.objs)
	return false, e.http.push(ctx, "refs/heads/lt/push/"+tag, zeroSHA, commit, pack)
}

func (e *env) land(ctx context.Context, w *worker, ref, base string, hot bool) error {
	tag := fmt.Sprintf("land-%d-%d-%d", w.id, w.iter, time.Now().UnixNano())
	ed, commit, err := e.newCommit(ctx, e.read, base, []int{e.fileFor(w, 0)}, tag)
	if err != nil {
		return err
	}
	pack, _, _ := gitpack.Build(ed.objs)
	if err := e.http.push(ctx, "refs/heads/lt/stage/"+tag, zeroSHA, commit, pack); err != nil {
		return err
	}
	_, err = e.merge.Land(ctx, e.m.Repo, landReq(ref, base, commit, tag))
	return err
}

func opLand(ctx context.Context, e *env, w *worker) (bool, error) {
	ref := e.landRef[w.id]
	r, err := e.merge.Ref(ctx, ref)
	if err != nil {
		return false, err
	}
	err = e.land(ctx, w, ref, r.Commit, false)
	return errors.Is(err, client.ErrLandConflict) || errors.Is(err, client.ErrLandStale), err
}

func opLandHot(ctx context.Context, e *env, w *worker) (bool, error) {
	err := e.land(ctx, w, "lt/hot", e.m.Tip, true)
	return errors.Is(err, client.ErrLandConflict) || errors.Is(err, client.ErrLandStale), err
}

func opCAS(ctx context.Context, e *env, w *worker) (bool, error) {
	ref := e.casRefs[w.rng.IntN(len(e.casRefs))]
	r, err := e.merge.Ref(ctx, ref)
	if err != nil {
		return false, err
	}
	target := e.m.Tip
	if r.Commit == target {
		target = e.commits[0].SHA
	}
	_, err = e.merge.SwapRef(ctx, ref, r.Generation, target)
	return errors.Is(err, client.ErrGenerationConflict), err
}
