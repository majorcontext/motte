package land

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/majorcontext/motte/internal/gitpack"
)

// MaxObjectBytes is the largest object that a land may make.
var MaxObjectBytes = gitpack.MaxObjectBytes

// MaxChangedPaths is the most distinct leaves that one land may change.
var MaxChangedPaths = 100000

const (
	// MaxCommits is the longest range that one land takes.
	MaxCommits = 1000
	// MaxLoggedPaths is the most paths that a log row keeps. A land that changes more is logged as overflow.
	MaxLoggedPaths = 1000
	// MaxRewriteBytes is the most new object data that one land may write.
	MaxRewriteBytes = 32 << 20
	// MaxConflictPaths is the most paths that a conflict names.
	MaxConflictPaths = 1000
)

// Source reads stored objects. before gets the size of an object before the source reads it.
type Source interface {
	Get(ctx context.Context, sha gitpack.SHA, before func(size int64) error) (gitpack.Type, []byte, error)
}

// History emits every path that the stored lands changed from base up to the tip at the start of the batch. It
// reports ok false, and emits nothing, when the stored lands cannot answer. The caller then diffs trees.
type History func(ctx context.Context, base gitpack.SHA, emit func(path string)) (ok bool, err error)

// Landed is one land of a batch. Paths holds the changed leaves, and is nil with Overflow set when there are more
// than MaxLoggedPaths.
type Landed struct {
	Old, New    gitpack.SHA
	FastForward bool
	Noop        bool
	Commits     []Mapped
	Paths       []string
	Overflow    bool
}

// Mapped is a commit of the range and the commit that replaced it.
type Mapped struct{ Old, New gitpack.SHA }

// ConflictError is a change that touches a path that also changed since its base.
type ConflictError struct{ Paths []string }

func (e *ConflictError) Error() string { return "land: conflict on " + strings.Join(e.Paths, ", ") }

// StaleError is a land whose tree is not the tree that the caller expected. Tree is the tree that the land would make.
type StaleError struct{ Tree gitpack.SHA }

func (e *StaleError) Error() string { return "land: the land would make tree " + e.Tree.String() }

// Batch lands changes one after the other on a running tip, and holds the objects that it makes until the caller
// stores them. A later land of the batch sees the earlier ones. A Batch is for one goroutine.
type Batch struct {
	src   Source
	grow  func(retained int64) error
	hist  History
	start gitpack.SHA
	tip   gitpack.SHA
	done  []Landed

	objs     []gitpack.Object
	byID     map[gitpack.SHA]int
	bytes    int
	mark     int
	markSize int
	loaded   int64
	trees    map[gitpack.SHA][]Entry
	produced map[gitpack.SHA][]Entry
	diffs    map[gitpack.SHA]diffResult
	cached   int
	tooBig   bool
}

// diffResult is the paths that differ between a base and the start tip, and false when they are too many to keep.
type diffResult struct {
	paths []string
	ok    bool
}

// NewBatch starts a batch on tip. grow is told how many bytes the batch holds before each read, and may refuse. hist
// answers the stored lands since a base.
func NewBatch(src Source, grow func(retained int64) error, tip gitpack.SHA, hist History) *Batch {
	return &Batch{src: src, grow: grow, hist: hist, start: tip, tip: tip, byID: map[gitpack.SHA]int{},
		trees: map[gitpack.SHA][]Entry{}, produced: map[gitpack.SHA][]Entry{}, diffs: map[gitpack.SHA]diffResult{}}
}

// Tip is the tip after the lands so far.
func (b *Batch) Tip() gitpack.SHA { return b.tip }

// Objects returns the objects that the lands made, in the order made.
func (b *Batch) Objects() []gitpack.Object { return b.objs }

// Bytes is the size of the objects that the lands made.
func (b *Batch) Bytes() int { return b.bytes }

func (b *Batch) put(t gitpack.Type, data []byte) gitpack.SHA {
	sha := gitpack.Hash(t, data)
	if _, ok := b.byID[sha]; !ok {
		b.byID[sha] = len(b.objs)
		b.objs = append(b.objs, gitpack.Object{Type: t, Data: data})
		b.bytes += len(data)
	}
	if len(data) > MaxObjectBytes {
		b.tooBig = true
	}
	return sha
}

func (b *Batch) get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	if i, ok := b.byID[sha]; ok {
		return b.objs[i].Type, b.objs[i].Data, nil
	}
	return b.src.Get(ctx, sha, func(size int64) error {
		b.loaded += size
		return b.grow(int64(b.bytes) + b.loaded)
	})
}

func (b *Batch) commit(ctx context.Context, sha gitpack.SHA) (Commit, error) {
	t, data, err := b.get(ctx, sha)
	if err != nil {
		return Commit{}, fmt.Errorf("commit %s: %w", sha, err)
	}
	if t != gitpack.Commit {
		return Commit{}, Invalid(sha.String() + " is not a commit")
	}
	return ParseCommit(data)
}

type link struct {
	sha gitpack.SHA
	c   Commit
}

// walk returns the commits after base up to head, oldest first. Each has one parent.
func (b *Batch) walk(ctx context.Context, base, head gitpack.SHA) ([]link, error) {
	var rev []link
	for cur := head; cur != base; {
		if len(rev) == MaxCommits {
			return nil, Invalid(fmt.Sprintf("the range has more than %d commits", MaxCommits))
		}
		c, err := b.commit(ctx, cur)
		if err != nil {
			return nil, err
		}
		switch len(c.Parents) {
		case 1:
		case 0:
			return nil, Invalid("base is not an ancestor of head")
		default:
			return nil, Invalid("commit " + cur.String() + " is a merge")
		}
		rev = append(rev, link{cur, c})
		cur = c.Parents[0]
	}
	slices.Reverse(rev)
	return rev, nil
}

// Land lands base..head on the running tip. A head that descends from the tip moves the ref to head as it is. Any
// other range is rebuilt on the tip with the commit time when. It returns a *ConflictError when a path of the range
// also changed since base, and Invalid or ErrTooLarge when the range cannot land. A failed land leaves no trace.
func (b *Batch) Land(ctx context.Context, base, head gitpack.SHA, when time.Time) (Landed, error) {
	return b.LandExpecting(ctx, base, head, when, nil)
}

// LandExpecting is Land that also returns a *StaleError, and moves nothing, when expect is set and the tree of the
// commit that head becomes differs from it. The check follows the conflict check.
func (b *Batch) LandExpecting(ctx context.Context, base, head gitpack.SHA, when time.Time, expect *gitpack.SHA) (Landed, error) {
	b.trees, b.produced = b.produced, map[gitpack.SHA][]Entry{}
	b.loaded, b.tooBig = 0, false
	b.mark, b.markSize = len(b.objs), b.bytes
	l, err := b.land(ctx, base, head, when, expect)
	if err == nil && b.tooBig {
		err = ErrTooLarge
	}
	if err != nil {
		for _, o := range b.objs[b.mark:] {
			delete(b.byID, gitpack.Hash(o.Type, o.Data))
		}
		b.objs, b.bytes = b.objs[:b.mark], b.markSize
		return Landed{}, err
	}
	if !l.Noop {
		b.done = append(b.done, l)
		b.tip = l.New
	}
	return l, nil
}

func (b *Batch) land(ctx context.Context, base, head gitpack.SHA, when time.Time, expect *gitpack.SHA) (Landed, error) {
	if base == head {
		return Landed{}, Invalid("base and head are the same commit")
	}
	baseCommit, err := b.commit(ctx, base)
	if err != nil {
		return Landed{}, err
	}
	chain, err := b.walk(ctx, base, head)
	if err != nil {
		return Landed{}, err
	}
	if head == b.tip {
		tip, err := b.commit(ctx, head)
		if err != nil {
			return Landed{}, err
		}
		return Landed{Old: head, New: head, Noop: true}, stale(expect, tip.Tree)
	}
	if i := b.tipIndex(base, chain); i >= 0 {
		return b.fastForward(ctx, head, baseCommit, chain, expect)
	}
	return b.rebase(ctx, base, baseCommit, chain, when, expect)
}

func stale(expect *gitpack.SHA, tree gitpack.SHA) error {
	if expect != nil && *expect != tree {
		return &StaleError{Tree: tree}
	}
	return nil
}

func (b *Batch) tipIndex(base gitpack.SHA, chain []link) int {
	if b.tip == base {
		return 0
	}
	return slices.IndexFunc(chain, func(l link) bool { return l.sha == b.tip })
}

func (b *Batch) fastForward(ctx context.Context, head gitpack.SHA, baseCommit Commit, chain []link, expect *gitpack.SHA) (Landed, error) {
	if _, _, err := b.scan(ctx, baseCommit, chain, false); err != nil {
		return Landed{}, err
	}
	if err := stale(expect, chain[len(chain)-1].c.Tree); err != nil {
		return Landed{}, err
	}
	tip, err := b.commit(ctx, b.tip)
	if err != nil {
		return Landed{}, err
	}
	paths, err := b.collect(ctx, tip.Tree, chain[len(chain)-1].c.Tree, true)
	if err != nil {
		return Landed{}, err
	}
	l := Landed{Old: b.tip, New: head, FastForward: true}
	l.setPaths(paths)
	return l, nil
}

func (l *Landed) setPaths(paths []string) {
	if len(paths) > MaxLoggedPaths {
		l.Overflow = true
		return
	}
	l.Paths = paths
}

// collect returns the sorted leaf paths that differ between two trees.
func (b *Batch) collect(ctx context.Context, a, c gitpack.SHA, strict bool) ([]string, error) {
	set := newPathSet(strict)
	err := b.diff(ctx, a, c, "", func(ch Change) error { return set.add(ch.Path) })
	return set.sorted(), err
}

// pathSet counts the distinct changed paths. A strict set refuses a path that is not UTF-8, because a log row keeps it.
type pathSet struct {
	seen   map[string]struct{}
	strict bool
}

func newPathSet(strict bool) *pathSet { return &pathSet{seen: map[string]struct{}{}, strict: strict} }

func (p *pathSet) add(path string) error {
	if p.strict && !utf8.ValidString(path) {
		return Invalid("a changed path is not valid UTF-8")
	}
	if _, ok := p.seen[path]; !ok {
		if len(p.seen) == MaxChangedPaths {
			return ErrTooLarge
		}
		p.seen[path] = struct{}{}
	}
	return nil
}

func (p *pathSet) sorted() []string {
	out := make([]string, 0, len(p.seen))
	for path := range p.seen {
		out = append(out, path)
	}
	slices.Sort(out)
	return out
}

// scan diffs each commit of the range against its parent, and checks the changed paths of every commit, so a path
// that one commit adds and a later one removes still counts. It returns the changes when keep is set.
func (b *Batch) scan(ctx context.Context, baseCommit Commit, chain []link, keep bool) ([][]Change, *pathSet, error) {
	changes := make([][]Change, len(chain))
	set := newPathSet(true)
	parent := baseCommit.Tree
	for i, l := range chain {
		err := b.diff(ctx, parent, l.c.Tree, "", func(ch Change) error {
			if keep {
				changes[i] = append(changes[i], ch)
			}
			return set.add(ch.Path)
		})
		if err != nil {
			return nil, nil, err
		}
		parent = l.c.Tree
	}
	return changes, set, nil
}

func (b *Batch) rebase(ctx context.Context, base gitpack.SHA, baseCommit Commit, chain []link, when time.Time, expect *gitpack.SHA) (Landed, error) {
	changes, set, err := b.scan(ctx, baseCommit, chain, true)
	if err != nil {
		return Landed{}, err
	}
	paths := set.sorted()
	if hits, err := b.conflicts(ctx, base, paths); err != nil || len(hits) > 0 {
		if err == nil {
			err = &ConflictError{Paths: hits}
		}
		return Landed{}, err
	}
	tipCommit, err := b.commit(ctx, b.tip)
	if err != nil {
		return Landed{}, err
	}
	root, prev := tipCommit.Tree, b.tip
	out := Landed{Old: b.tip}
	for i, l := range chain {
		if root, err = b.apply(ctx, root, changes[i]); err != nil {
			return Landed{}, err
		}
		data, err := l.c.Rebuild(prev, root, when)
		if err != nil {
			return Landed{}, err
		}
		prev = b.put(gitpack.Commit, data)
		out.Commits = append(out.Commits, Mapped{Old: l.sha, New: prev})
		if b.bytes-b.markSize > MaxRewriteBytes {
			return Landed{}, ErrTooLarge
		}
	}
	if b.tooBig {
		return Landed{}, ErrTooLarge
	}
	if err := stale(expect, root); err != nil {
		return Landed{}, err
	}
	out.New = prev
	out.setPaths(paths)
	return out, nil
}

// conflicts returns the paths that changed since base and clash with the paths of the change.
func (b *Batch) conflicts(ctx context.Context, base gitpack.SHA, paths []string) ([]string, error) {
	c := newClash(paths)
	if err := b.since(ctx, base, c.add); err != nil {
		return nil, err
	}
	return c.result(), nil
}

// since emits every path that changed between base and the running tip. The stored lands answer for the part before
// the batch when they cover base, and else a diff of the trees does, kept for the next land of the batch with the same
// base. The lands of this batch add their own paths. A path may come twice.
func (b *Batch) since(ctx context.Context, base gitpack.SHA, emit func(string)) error {
	if base == b.tip {
		return nil
	}
	from := -1
	for i := len(b.done) - 1; i >= 0 && from < 0; i-- {
		switch base {
		case b.done[i].New:
			from = i + 1
		case b.done[i].Old:
			from = i
		}
	}
	if from < 0 {
		from = 0
		ok, err := b.before(ctx, base, emit)
		if err != nil {
			return err
		}
		if !ok {
			return b.diffTips(ctx, base, emit)
		}
	}
	for _, l := range b.done[from:] {
		if l.Overflow {
			return b.diffTips(ctx, base, emit)
		}
		for _, p := range l.Paths {
			emit(p)
		}
	}
	return nil
}

// before emits the paths that changed between base and the tip at the start of the batch. It reports false when it
// cannot, and then emits nothing.
func (b *Batch) before(ctx context.Context, base gitpack.SHA, emit func(string)) (bool, error) {
	if base == b.start {
		return true, nil
	}
	if b.hist != nil {
		if ok, err := b.hist(ctx, base, emit); ok || err != nil {
			return ok, err
		}
	}
	d, seen := b.diffs[base]
	if !seen {
		from, err := b.commit(ctx, base)
		if err != nil {
			return false, err
		}
		to, err := b.commit(ctx, b.start)
		if err != nil {
			return false, err
		}
		paths, err := b.collect(ctx, from.Tree, to.Tree, false)
		if err != nil && !errors.Is(err, ErrTooLarge) {
			return false, err
		}
		d = diffResult{paths: paths, ok: err == nil}
		if b.cached+len(d.paths) > MaxChangedPaths {
			clear(b.diffs)
			b.cached = 0
		}
		b.diffs[base] = d
		b.cached += len(d.paths)
	}
	if !d.ok {
		return false, nil
	}
	for _, p := range d.paths {
		emit(p)
	}
	return true, nil
}

func (b *Batch) diffTips(ctx context.Context, base gitpack.SHA, emit func(string)) error {
	from, err := b.commit(ctx, base)
	if err != nil {
		return err
	}
	to, err := b.commit(ctx, b.tip)
	if err != nil {
		return err
	}
	return b.diff(ctx, from.Tree, to.Tree, "", func(ch Change) error { emit(ch.Path); return nil })
}
