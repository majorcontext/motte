package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/testhook"
	"github.com/majorcontext/motte/testserver"
)

// objSet builds git objects in memory for a test.
type objSet struct{ objs []gitpack.Object }

func (o *objSet) add(t gitpack.Type, data []byte) string {
	o.objs = append(o.objs, gitpack.Object{Type: t, Data: data})
	return gitpack.Hash(t, data).String()
}

// tree stores the blobs and the nested trees of files, a map of path to content, and returns the root tree.
func (o *objSet) tree(files map[string]string) string {
	type entry struct{ name, mode, sha string }
	dirs := map[string]map[string]string{}
	var entries []entry
	for p, c := range files {
		if dir, rest, nested := strings.Cut(p, "/"); nested {
			if dirs[dir] == nil {
				dirs[dir] = map[string]string{}
			}
			dirs[dir][rest] = c
			continue
		}
		entries = append(entries, entry{p, "100644", o.add(gitpack.Blob, []byte(c))})
	}
	for d, sub := range dirs {
		entries = append(entries, entry{d, "40000", o.tree(sub)})
	}
	key := func(e entry) string {
		if e.mode == "40000" {
			return e.name + "/"
		}
		return e.name
	}
	sort.Slice(entries, func(i, j int) bool { return key(entries[i]) < key(entries[j]) })
	var data []byte
	for _, e := range entries {
		raw, _ := hex.DecodeString(e.sha)
		data = append(data, e.mode+" "+e.name+"\x00"...)
		data = append(data, raw...)
	}
	return o.add(gitpack.Tree, data)
}

// commit stores a commit of files on parents. extra is raw header lines after the committer, each ending in a newline.
func (o *objSet) commit(files map[string]string, extra, msg string, parents ...string) string {
	var b strings.Builder
	b.WriteString("tree " + o.tree(files) + "\n")
	for _, p := range parents {
		b.WriteString("parent " + p + "\n")
	}
	b.WriteString("author Ada <ada@example.com> 1600000000 +0100\ncommitter Grace <grace@example.com> 1600000001 +0200\n" + extra + "\n" + msg + "\n")
	return o.add(gitpack.Commit, []byte(b.String()))
}

// upload imports the objects as one pack.
func (o *objSet) upload(t testing.TB, e *env, repo string) {
	t.Helper()
	pack, idx, _ := gitpack.Build(o.objs)
	sum := sha256.Sum256(pack)
	if err := e.Client(e.importGrant(repo)).ImportPack(context.Background(), hex.EncodeToString(sum[:]), idx, pack); err != nil {
		t.Fatal(err)
	}
	o.objs = nil
}

// landEnv is a repository whose ref main is at an empty root commit.
type landEnv struct {
	*env
	repo                 string
	root                 string
	merge, write, reader *client.Client
}

func newLandEnv(t *testing.T, e *env) *landEnv {
	t.Helper()
	l := &landEnv{env: e, repo: repoText}
	l.merge, l.write, l.reader = e.Client(e.MergeGrant(t, repoText)), e.Client(e.WriteGrant(t, repoText)), e.Client(e.ReadGrant(t, repoText))
	var o objSet
	l.root = o.commit(nil, "", "root")
	o.upload(t, e, repoText)
	if _, err := l.merge.SwapRef(context.Background(), "main", 0, l.root); err != nil {
		t.Fatal(err)
	}
	return l
}

func (l *landEnv) land(c *client.Client, ref, base, head string) (api.LandResponse, error) {
	return c.Land(context.Background(), l.repo, api.LandRequest{Ref: ref, Base: base, Head: head})
}

func (l *landEnv) commitText(sha string) string {
	l.t.Helper()
	obj, err := l.reader.Object(context.Background(), sha)
	if err != nil {
		l.t.Fatal(err)
	}
	return string(obj.Data)
}

// files returns the sorted names of the root tree of a commit, with "/" marking a directory.
func (l *landEnv) files(commit string) []string {
	l.t.Helper()
	c, err := l.reader.Commit(context.Background(), commit)
	if err != nil {
		l.t.Fatal(err)
	}
	trees, err := l.reader.Trees(context.Background(), c.Tree, 0)
	if err != nil {
		l.t.Fatal(err)
	}
	var out []string
	for _, tr := range trees.Trees[:1] {
		for _, en := range tr.Entries {
			name, _ := base64.StdEncoding.DecodeString(en.Name)
			if en.Mode == "40000" {
				name = append(name, '/')
			}
			out = append(out, string(name))
		}
	}
	slices.Sort(out)
	return out
}

func (l *landEnv) tip(ref string) api.Ref {
	l.t.Helper()
	r, err := l.reader.Ref(context.Background(), ref)
	if err != nil {
		l.t.Fatal(err)
	}
	return r
}

// parentsOf returns the parent lines of a commit text.
func parentsOf(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if p, ok := strings.CutPrefix(line, "parent "); ok {
			out = append(out, p)
		}
	}
	return out
}

func eachBackend(t *testing.T, fn func(t *testing.T, l *landEnv)) {
	t.Parallel()
	t.Run("sqlite", func(t *testing.T) {
		t.Parallel()
		ts := testserver.Start(t)
		fn(t, newLandEnv(t, &env{Server: ts, Stack: testhook.Of(ts), t: t, url: ts.URL, cp: ts.Operator, node: ts.Node}))
	})
	t.Run("postgres", func(t *testing.T) { t.Parallel(); fn(t, newLandEnv(t, newEnv(t))) })
}
