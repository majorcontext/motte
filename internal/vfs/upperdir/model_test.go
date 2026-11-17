package upperdir

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
)

type snode struct {
	dir, link, exec bool
	content         string
	src             string // the base path that the node shows through the lower layer
}

// sim applies operations to a model of the merged view and to an in-memory upper directory that follows the
// overlayfs rules for redirect_dir and metacopy.
type sim struct {
	r     *rand.Rand
	lower *memBase
	mv    map[string]snode
	up    *MemFS
}

func fmode(exec bool) fs.FileMode {
	if exec {
		return 0o755
	}
	return 0o644
}

var dirNode = MemNode{Mode: fs.ModeDir | 0o755}

func newSim(seed uint64) *sim {
	s := &sim{r: rand.New(rand.NewPCG(seed, 7)), mv: map[string]snode{}, up: NewMemFS()}
	s.lower = newBase("a/a", "a/b*", "a/c/a", "a/c/b@", "b/a", "b/b/a*", "b/b/b", "c", "d/a", "d/b/a", "e@")
	for p, e := range s.lower.all {
		s.mv[p] = snode{dir: e.Dir, link: e.Symlink, exec: e.Exec, src: p}
		if !e.Dir {
			n := s.mv[p]
			n.content = "base:" + p
			s.mv[p] = n
		}
	}
	return s
}

func (s *sim) lowerChild(par, name string) string {
	o := ""
	if par != "" {
		if o = s.mv[par].src; o == "" {
			return ""
		}
	}
	if _, ok := s.lower.all[join(o, name)]; ok {
		return join(o, name)
	}
	return ""
}

func (s *sim) ensureUp(dir string) {
	for i := 1; i <= len(dir); i++ {
		if i == len(dir) || dir[i] == '/' {
			if _, ok := s.up.Get(dir[:i]); !ok {
				s.up.Put(dir[:i], dirNode)
			}
		}
	}
}

func name(p string) string { return p[strings.LastIndexByte(p, '/')+1:] }

func (s *sim) whiteoutIfLower(p string) {
	if s.lowerChild(parent(p), name(p)) != "" {
		s.ensureUp(parent(p))
		s.up.Put(p, MemNode{Mode: fs.ModeDevice | fs.ModeCharDevice, Whiteout: true})
	}
}

func (s *sim) paths(want func(string, snode) bool) []string {
	var ps []string
	for p, n := range s.mv {
		if want(p, n) {
			ps = append(ps, p)
		}
	}
	sort.Strings(ps)
	return ps
}

func (s *sim) pick(ps []string) (string, bool) {
	if len(ps) == 0 {
		return "", false
	}
	return ps[s.r.IntN(len(ps))], true
}

func (s *sim) newPath() (string, bool) {
	par, _ := s.pick(append(s.paths(func(_ string, n snode) bool { return n.dir }), ""))
	p := join(par, []string{"a", "b", "c", "d", "e"}[s.r.IntN(5)])
	_, taken := s.mv[p]
	return p, !taken
}

func under(k, p string) bool { return k == p || strings.HasPrefix(k, p+"/") }

func (s *sim) rm(p string) {
	delete(s.mv, p)
	for _, q := range s.up.Paths() {
		if under(q, p) {
			s.up.Delete(q)
		}
	}
	s.whiteoutIfLower(p)
}

func (s *sim) rekey(p, q string) {
	for _, k := range s.paths(func(k string, _ snode) bool { return under(k, p) }) {
		s.mv[q+k[len(p):]] = s.mv[k]
		delete(s.mv, k)
	}
	for _, k := range s.up.Paths() {
		if under(k, p) {
			n, _ := s.up.Get(k)
			s.up.Delete(k)
			s.up.Put(q+k[len(p):], n)
		}
	}
}

// step applies one random operation and returns its description, or "" when it does not apply.
func (s *sim) step() string {
	leaf := func(_ string, n snode) bool { return !n.dir && !n.link }
	switch s.r.IntN(7) {
	case 0, 1:
		p, ok := s.pick(s.paths(leaf))
		if !ok {
			if p, ok = s.newPath(); !ok {
				return ""
			}
		}
		s.ensureUp(parent(p))
		s.up.Put(p, MemNode{Mode: fmode(s.mv[p].exec)})
		s.mv[p] = snode{content: "w", exec: s.mv[p].exec}
		return "write " + p
	case 2:
		p, ok := s.pick(s.paths(leaf))
		if !ok {
			return ""
		}
		n := s.mv[p]
		n.exec = !n.exec
		s.mv[p] = n
		u, inUp := s.up.Get(p)
		if !inUp {
			s.ensureUp(parent(p))
			u = MemNode{Xattr: map[string]string{XattrMetacopy: ""}}
		}
		u.Mode = fmode(n.exec)
		s.up.Put(p, u)
		return "chmod " + p
	case 3:
		p, ok := s.pick(s.paths(func(p string, n snode) bool {
			return !n.dir || len(s.paths(func(k string, _ snode) bool { return strings.HasPrefix(k, p+"/") })) == 0
		}))
		if !ok {
			return ""
		}
		s.rm(p)
		return "rm " + p
	case 4:
		p, ok := s.newPath()
		if !ok {
			return ""
		}
		s.ensureUp(parent(p))
		n := dirNode
		if w, was := s.up.Get(p); was && w.Whiteout {
			n.Xattr = map[string]string{XattrOpaque: "y"}
		}
		s.up.Put(p, n)
		s.mv[p] = snode{dir: true}
		return "mkdir " + p
	default:
		return s.move(s.r.IntN(2) == 0)
	}
}

// move renames a file or a directory.
func (s *sim) move(file bool) string {
	if file {
		p, ok := s.pick(s.paths(func(_ string, n snode) bool { return !n.dir }))
		q, ok2 := s.newPath()
		if !ok || !ok2 {
			return ""
		}
		n := s.mv[p]
		s.ensureUp(parent(q))
		moved := MemNode{Mode: fmode(n.exec)}
		if u, inUp := s.up.Get(p); inUp {
			moved = u
		}
		if n.link {
			moved, n.src = MemNode{Mode: fs.ModeSymlink | 0o777}, ""
		}
		if n.src != "" {
			moved.Xattr = map[string]string{XattrMetacopy: "", XattrRedirect: "/" + n.src}
		}
		s.up.Delete(p)
		s.whiteoutIfLower(p)
		s.up.Put(q, moved)
		delete(s.mv, p)
		s.mv[q] = n
		return "mvfile " + p + " " + q
	}
	p, ok := s.pick(s.paths(func(_ string, n snode) bool { return n.dir }))
	q, ok2 := s.newPath()
	if !ok || !ok2 || strings.HasPrefix(q, p+"/") {
		return ""
	}
	n := s.mv[p]
	s.ensureUp(p)
	u, _ := s.up.Get(p)
	x := map[string]string{}
	for k, v := range u.Xattr {
		x[k] = v
	}
	u.Xattr = x
	if n.src != "" {
		u.Xattr[XattrRedirect] = "/" + n.src
	} else if par := parent(q); par == "" || s.mv[par].src != "" {
		u.Xattr[XattrOpaque] = "y"
	}
	s.up.Put(p, u)
	s.ensureUp(parent(q))
	s.rekey(p, q)
	s.whiteoutIfLower(p)
	return "mvdir " + p + " " + q
}

// unmarked reports whether no directory above p carries a redirect or an opaque marker.
func (s *sim) unmarked(p string) bool {
	for a := parent(p); a != ""; a = parent(a) {
		if u, _ := s.up.Get(a); u.Xattr[XattrRedirect] != "" || u.Xattr[XattrOpaque] != "" {
			return false
		}
	}
	return true
}

func val(n snode) string { return fmt.Sprintf("%s|%v|%v", n.content, n.exec, n.link) }

// check requires that the entries of Scan, applied to the base leaves, give the leaves of the merged view.
func (s *sim) check() error {
	got, err := Scan(context.Background(), s.up, "", s.lower)
	if err != nil {
		return err
	}
	applied := map[string]string{}
	for p, e := range s.lower.leaves() {
		applied[p] = val(snode{content: "base:" + p, exec: e.Exec, link: e.Symlink})
	}
	for _, e := range got {
		for k := range applied {
			if under(k, e.Path) {
				delete(applied, k)
			}
		}
		if n := s.mv[e.Path]; e.Kind != KindDeleted && e.Kind != KindDir {
			if n.dir || n == (snode{}) || (e.Kind == KindSymlink) != n.link || e.Kind == KindFile && e.Exec != n.exec {
				return fmt.Errorf("entry %+v does not match the merged view: %+v", e, n)
			}
			if u, _ := s.up.Get(e.Path); n.src == e.Path && n.exec == s.lower.all[e.Path].Exec && s.unmarked(e.Path) {
				if _, meta := u.Xattr[XattrMetacopy]; meta {
					return fmt.Errorf("entry %+v lists a metadata-only change", e)
				}
			}
			applied[e.Path] = val(n)
		}
	}
	want := map[string]string{}
	for p, n := range s.mv {
		if !n.dir {
			want[p] = val(n)
		}
	}
	if fmt.Sprint(applied) != fmt.Sprint(want) {
		return fmt.Errorf("applied %v\nwant    %v\nentries %+v", applied, want, got)
	}
	return nil
}

// TestScanMatchesOverlayModel runs random edit sequences against a model of the merged view and an upper
// directory built by the overlayfs rules. A wrong Scan loses, keeps or invents a file of the merged view.
func TestScanMatchesOverlayModel(t *testing.T) {
	for seed := uint64(0); seed < 400; seed++ {
		s := newSim(seed)
		var ops []string
		for range 30 {
			if op := s.step(); op != "" {
				ops = append(ops, op)
				if err := s.check(); err != nil {
					t.Fatalf("seed %d after %v: %v", seed, ops, err)
				}
			}
		}
	}
}
