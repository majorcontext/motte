package gitproto

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"strconv"

	"github.com/majorcontext/motte/internal/gitpack"
)

type commitNode struct {
	sha     gitpack.SHA
	tree    gitpack.SHA
	parents []gitpack.SHA
	when    int64
	// queued is set once the commit is in the walk. visited is set once its parents are in.
	queued, visited, unint bool
}

type commitQueue []*commitNode

func (q commitQueue) Len() int           { return len(q) }
func (q commitQueue) Less(i, j int) bool { return q[i].when > q[j].when }
func (q commitQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *commitQueue) Push(x any)        { *q = append(*q, x.(*commitNode)) }
func (q *commitQueue) Pop() any {
	old := *q
	n := old[len(old)-1]
	*q = old[:len(old)-1]
	return n
}

// collector lists the objects of a fetch: what the wants reach and the haves do not. The commit walk goes by commit
// time, newest first, and stops when no commit that the wants reach is left in the queue. A clock that runs
// backwards can make it stop late, which only adds objects. The trees of the commits at the edge of the haves
// are the objects that the client holds.
type collector struct {
	ctx      context.Context
	objs     Objects
	blobNone bool

	commits map[gitpack.SHA]*commitNode
	queue   commitQueue
	// live counts the queued commits that are not unint.
	live int
	// parentRefs counts the parent names that the parsed commits keep.
	parentRefs int
	skip       map[gitpack.SHA]struct{}
	sent       map[gitpack.SHA]struct{}
	out        []gitpack.SHA

	track    func(objects int) error
	reported int
	err      error
}

// trackStep is how many more objects the sets hold before track is called again.
const trackStep = 1 << 16

func (c *collector) tracked() bool {
	n := len(c.skip) + len(c.sent) + len(c.commits) + c.parentRefs
	if n > MaxTracked {
		return true
	}
	if c.track != nil && n > c.reported && c.err == nil {
		c.reported = n + trackStep
		c.err = c.track(min(c.reported, MaxTracked))
	}
	return c.err != nil
}

// limit is the error of a collector for which tracked is true.
func (c *collector) limit() error {
	if c.err != nil {
		return c.err
	}
	return ErrTooLarge
}

func parseCommit(sha gitpack.SHA, data []byte) (*commitNode, bool) {
	n := &commitNode{sha: sha}
	for rest := data; len(rest) > 0; {
		line, after, _ := bytes.Cut(rest, []byte("\n"))
		rest = after
		switch {
		case len(line) == 0:
			rest = nil
		case bytes.HasPrefix(line, []byte("tree ")):
			t, err := gitpack.ParseSHA(string(line[5:]))
			if err != nil {
				return nil, false
			}
			n.tree = t
		case bytes.HasPrefix(line, []byte("parent ")):
			p, err := gitpack.ParseSHA(string(line[7:]))
			if err != nil {
				return nil, false
			}
			n.parents = append(n.parents, p)
		case bytes.HasPrefix(line, []byte("committer ")):
			if i := bytes.LastIndexByte(line, '>'); i >= 0 {
				if f := bytes.Fields(line[i+1:]); len(f) > 0 {
					n.when, _ = strconv.ParseInt(string(f[0]), 10, 64)
				}
			}
		}
	}
	return n, n.tree != gitpack.SHA{}
}

func (c *collector) commit(sha gitpack.SHA) (*commitNode, error) {
	if n, ok := c.commits[sha]; ok {
		return n, nil
	}
	t, data, err := c.objs.Get(c.ctx, sha)
	if err != nil {
		return nil, err
	}
	if t != gitpack.Commit {
		return nil, errNotCommit
	}
	return c.adopt(sha, data)
}

var (
	errNotCommit = errors.New("gitproto: object is not a commit")
	errMalformed = errors.New("gitproto: malformed object")
)

func (c *collector) adopt(sha gitpack.SHA, data []byte) (*commitNode, error) {
	n, ok := parseCommit(sha, data)
	if !ok {
		return nil, errNotCommit
	}
	c.commits[sha] = n
	if c.parentRefs += len(n.parents); c.tracked() {
		return nil, c.limit()
	}
	return n, nil
}

// mark queues n. When n is already queued, an unint mark flags it and the parents that the walk took from it.
func (c *collector) mark(n *commitNode, unint bool) error {
	if !n.queued {
		n.queued, n.unint = true, unint
		heap.Push(&c.queue, n)
		if !unint {
			c.live++
		}
		if c.tracked() {
			return c.limit()
		}
		return nil
	}
	if !unint || n.unint {
		return nil
	}
	for stack := []*commitNode{n}; len(stack) > 0; {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.unint {
			continue
		}
		n.unint = true
		if !n.visited {
			c.live--
			continue
		}
		for _, p := range n.parents {
			pn, err := c.commit(p)
			if err != nil {
				return err
			}
			stack = append(stack, pn)
		}
	}
	return nil
}

// collect returns the objects that the wants reach and the haves do not hold.
func (c *collector) collect(wants, haves []gitpack.SHA) ([]gitpack.SHA, error) {
	c.commits = map[gitpack.SHA]*commitNode{}
	c.skip = map[gitpack.SHA]struct{}{}
	c.sent = map[gitpack.SHA]struct{}{}
	for _, h := range haves {
		n, err := c.commit(h)
		if errors.Is(err, errNotCommit) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := c.mark(n, true); err != nil {
			return nil, err
		}
	}
	for _, w := range wants {
		if err := c.want(w, 0); err != nil {
			return nil, err
		}
	}
	var picked []*commitNode
	for c.live > 0 {
		n := heap.Pop(&c.queue).(*commitNode)
		if !n.unint {
			c.live--
			picked = append(picked, n)
		}
		n.visited = true
		for _, p := range n.parents {
			pn, err := c.commit(p)
			if err != nil {
				return nil, err
			}
			if err := c.mark(pn, n.unint); err != nil {
				return nil, err
			}
		}
	}
	var trees []gitpack.SHA
	var edges []gitpack.SHA
	for _, n := range picked {
		if n.unint {
			continue
		}
		c.add(n.sha)
		trees = append(trees, n.tree)
		for _, p := range n.parents {
			if pn := c.commits[p]; pn.unint {
				edges = append(edges, pn.tree)
			}
		}
	}
	if err := c.walkTrees(edges, c.skip, nil); err != nil {
		return nil, err
	}
	if err := c.walkTrees(trees, c.sent, c.add); err != nil {
		return nil, err
	}
	return c.out, nil
}

// want adds the object of a want. A commit joins the walk, a tag adds itself and follows its target.
func (c *collector) want(sha gitpack.SHA, depth int) error {
	if depth > 16 {
		return RequestError("tag chain too long")
	}
	t, data, err := c.objs.Get(c.ctx, sha)
	if err != nil {
		return err
	}
	switch t {
	case gitpack.Commit:
		n, ok := c.commits[sha]
		if !ok {
			if n, err = c.adopt(sha, data); err != nil {
				return err
			}
		}
		return c.mark(n, false)
	case gitpack.Tag:
		c.add(sha)
		target, ok := tagTarget(data)
		if !ok {
			return RequestError("malformed tag")
		}
		return c.want(target, depth+1)
	case gitpack.Tree:
		return c.walkTrees([]gitpack.SHA{sha}, c.sent, c.add)
	}
	c.add(sha)
	return nil
}

func (c *collector) add(sha gitpack.SHA) {
	if _, ok := c.skip[sha]; ok {
		return
	}
	if _, ok := c.sent[sha]; !ok {
		c.sent[sha] = struct{}{}
		c.out = append(c.out, sha)
	}
}

// walkTrees records the trees of roots and what they hold in seen, once for each object, one level of trees at a time.
// A tree is recorded when it is queued, so a level holds each tree once and counts toward the bound. add, when set,
// takes each new object in place of seen. Objects reach add level by level.
func (c *collector) walkTrees(roots []gitpack.SHA, seen map[gitpack.SHA]struct{}, add func(gitpack.SHA)) error {
	record := func(sha gitpack.SHA) bool {
		if _, ok := seen[sha]; ok {
			return false
		}
		if _, ok := c.skip[sha]; ok && add != nil {
			return false
		}
		if add != nil {
			add(sha)
		} else {
			seen[sha] = struct{}{}
		}
		return true
	}
	var level []gitpack.SHA
	for _, r := range roots {
		if record(r) {
			level = append(level, r)
		}
	}
	for len(level) > 0 {
		var next []gitpack.SHA
		err := c.eachAhead(level, func(sha gitpack.SHA) error {
			t, data, err := c.objs.Get(c.ctx, sha)
			if err != nil {
				return err
			}
			if t != gitpack.Tree {
				return errMalformed
			}
			err = forEachEntry(data, func(mode uint32, child gitpack.SHA) {
				if c.tracked() {
					return
				}
				switch {
				case mode == 0o160000:
				case mode == 0o40000:
					if record(child) {
						next = append(next, child)
					}
				case !c.blobNone || add == nil:
					record(child)
				}
			})
			if err != nil {
				return err
			}
			if c.tracked() {
				return c.limit()
			}
			return nil
		})
		if err != nil {
			return err
		}
		level = next
	}
	return nil
}

// eachAhead calls fn for each sha in order. While it runs fn on one batch, the objects reader reads the next batch in
// advance.
func (c *collector) eachAhead(shas []gitpack.SHA, fn func(gitpack.SHA) error) error {
	pf, ok := c.objs.(Prefetcher)
	if !ok {
		for _, sha := range shas {
			if err := fn(sha); err != nil {
				return err
			}
		}
		return nil
	}
	ahead := func(batch []gitpack.SHA) chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			if len(batch) > 1 {
				pf.Prefetch(c.ctx, batch)
			}
		}()
		return done
	}
	batch := func(i int) []gitpack.SHA { return shas[min(i, len(shas)):min(i+prefetchBatch, len(shas))] }
	cur := ahead(batch(0))
	for i := 0; i < len(shas); i += prefetchBatch {
		nxt := ahead(batch(i + prefetchBatch))
		<-cur
		for _, sha := range batch(i) {
			if err := fn(sha); err != nil {
				<-nxt
				return err
			}
		}
		cur = nxt
	}
	return nil
}

func forEachEntry(data []byte, fn func(mode uint32, sha gitpack.SHA)) error {
	for len(data) > 0 {
		sp := bytes.IndexByte(data, ' ')
		nul := bytes.IndexByte(data, 0)
		if sp < 0 || nul < sp || len(data) < nul+21 {
			return errMalformed
		}
		mode, err := strconv.ParseUint(string(data[:sp]), 8, 32)
		if err != nil {
			return errMalformed
		}
		var sha gitpack.SHA
		copy(sha[:], data[nul+1:nul+21])
		fn(uint32(mode), sha)
		data = data[nul+21:]
	}
	return nil
}
