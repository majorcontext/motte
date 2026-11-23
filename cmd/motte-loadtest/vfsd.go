package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
)

const (
	// vfsdBulkEntries, vfsdReadAheadFiles and vfsdReadAheadBytes are the settings of the cache of the workspace daemon:
	// the entries of one bulk tree read, and the files and bytes that one blob read-ahead takes.
	vfsdBulkEntries    = 30000
	vfsdReadAheadFiles = 4096
	vfsdReadAheadBytes = 8 << 20
)

// rawTree is the git form of the entries of a tree, in the order that the store answered them.
func rawTree(ents []api.TreeEntry) []byte {
	var out []byte
	for _, e := range ents {
		name, err := base64.StdEncoding.DecodeString(e.Name)
		mode, merr := strconv.ParseUint(e.Mode, 8, 32)
		sha, serr := hex.DecodeString(e.SHA)
		if err != nil || merr != nil || serr != nil || len(sha) != 20 {
			return nil
		}
		out = strconv.AppendUint(out, mode, 8)
		out = append(out, ' ')
		out = append(out, name...)
		out = append(out, 0)
		out = append(out, sha...)
	}
	return out
}

// mount reads the commit and then the trees of the workspace in one bulk read, as the daemon does when its cache
// lacks the root tree of a commit. The editor keeps the trees, so a snapshot reads none.
func (a *agent) mount(ctx context.Context, commit string) (string, error) {
	root, err := a.rootOf(ctx, commit)
	if err != nil {
		return "", err
	}
	err = a.do(ctx, "mount", func(c context.Context) error {
		ts, err := a.e.read.Trees(c, root, vfsdBulkEntries)
		for _, t := range ts.Trees {
			a.trees[t.SHA] = t.Entries
			a.ed.local[t.SHA] = rawTree(t.Entries)
		}
		return err
	})
	return root, err
}

// readAhead reads the blobs that the files of a directory need, and the regular files that sit beside them, up to the
// read-ahead limits, in one request. A blob that the node cache holds costs no request.
func (a *agent) readAhead(ctx context.Context, dir []api.TreeEntry, wanted []string) error {
	var batch []string
	var bytes int64
	add := func(sha string, size int64) bool {
		if _, ok := a.content[sha]; ok {
			return false
		}
		batch = append(batch, sha)
		bytes += size
		return true
	}
	for _, sha := range wanted {
		add(sha, 0)
	}
	if len(batch) == 0 {
		return nil
	}
	have := map[string]bool{}
	for _, s := range batch {
		have[s] = true
	}
	for _, e := range dir {
		if len(batch) >= vfsdReadAheadFiles || bytes+e.Size > vfsdReadAheadBytes {
			break
		}
		if e.Mode == "100644" && !have[e.SHA] {
			have[e.SHA] = add(e.SHA, e.Size)
		}
	}
	return a.do(ctx, "blob", func(c context.Context) error {
		blobs, err := a.e.read.Blobs(c, batch)
		for i, b := range blobs {
			a.content[batch[i]] = b
		}
		return err
	})
}

// claim makes the workspace of the agent, with a node grant for it. A failed claim is an error of the operation claim in
// the report, and it ends the agent.
func (a *agent) claim(ctx context.Context) bool {
	ws := fmt.Sprintf("ws-lt-%d-%d", a.id, a.nonce)
	op := a.e.sg.client(a.e.f.url, grant.ScopeOperator)
	err := a.do(ctx, "claim", func(c context.Context) error {
		_, err := op.Claim(c, ws, api.ClaimRequest{Epoch: 1, Owner: "loadtest", Repo: a.e.m.Repo})
		return err
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "motte-loadtest: claim:", err)
		return false
	}
	a.node = a.e.sg.nodeClient(a.e.f.url, ws, 1)
	return true
}

// upload stores the objects of a snapshot in one request and moves the branch of the task, as a push of the jj backend
// does: the daemon writes the new objects, then swaps the ref with the generation that it read.
func (a *agent) upload(ctx context.Context, br *branch, head string, objs []gitpack.Object) error {
	batch := make([]client.Object, len(objs))
	for i, o := range objs {
		batch[i] = client.Object{Type: o.Type.String(), Data: o.Data}
	}
	if _, err := a.node.PutObjects(ctx, batch); err != nil {
		return err
	}
	r, err := a.node.SwapRef(ctx, strings.TrimPrefix(br.ref, "refs/heads/"), br.gen, head)
	if err != nil {
		return err
	}
	br.gen = r.Generation
	return nil
}
