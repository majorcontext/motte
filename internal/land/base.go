package land

import (
	"container/heap"
	"context"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
)

// maxBaseWalk is the most commits that the search for a base reads.
const maxBaseWalk = 20000

const (
	paintTip uint8 = 1 << iota
	paintHead
	paintStale
)

type timed struct {
	sha  gitpack.SHA
	when int64
}

type newest []timed

func (h newest) Len() int           { return len(h) }
func (h newest) Less(i, j int) bool { return h[i].when > h[j].when }
func (h newest) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *newest) Push(x any)        { *h = append(*h, x.(timed)) }
func (h *newest) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func committerTime(c Commit) int64 {
	i := strings.LastIndex(c.Committer, "> ")
	if i < 0 {
		return 0
	}
	sec, _, _ := strings.Cut(c.Committer[i+2:], " ")
	n, _ := strconv.ParseInt(sec, 10, 64)
	return n
}

// FindBase returns the first commit of the first-parent chain of head that onto reaches. It does not check that the
// range from there to head is linear: a land does. It returns Invalid when no commit of the chain, up to MaxCommits
// back, is reachable from onto, or when the search reads more than maxBaseWalk commits.
func FindBase(ctx context.Context, src Source, grow func(retained int64) error, onto, head gitpack.SHA) (gitpack.SHA, error) {
	return NewBatch(src, grow, onto, nil).FindBase(ctx, onto, head)
}

// FindBase is the function FindBase over the objects of the batch too, so onto may be a commit that the batch made.
//
// It paints commits from both ends in commit-time order, as git does for a merge base: a commit that onto reaches has
// one mark, a commit of the chain of head has another, and a commit with both is a find, whose ancestors are stale.
// Commit times only order the work: the search ends when every queued commit is stale, so a wrong clock cannot make
// it return a base that is not the first one.
func (b *Batch) FindBase(ctx context.Context, onto, head gitpack.SHA) (gitpack.SHA, error) {
	index := map[gitpack.SHA]int{}
	var chain []gitpack.SHA
	parents := map[gitpack.SHA][]gitpack.SHA{}
	when := map[gitpack.SHA]int64{}
	load := func(sha gitpack.SHA) error {
		if _, ok := when[sha]; ok {
			return nil
		}
		c, err := b.commit(ctx, sha)
		if err != nil {
			return err
		}
		when[sha], parents[sha] = committerTime(c), c.Parents
		return nil
	}
	for cur := head; len(chain) <= MaxCommits; {
		if err := load(cur); err != nil {
			return gitpack.SHA{}, err
		}
		index[cur] = len(chain)
		chain = append(chain, cur)
		if len(parents[cur]) == 0 {
			break
		}
		cur = parents[cur][0]
	}
	if err := load(onto); err != nil {
		return gitpack.SHA{}, err
	}
	marks := map[gitpack.SHA]uint8{}
	queued := map[gitpack.SHA]bool{}
	queue := &newest{}
	live := 0
	add := func(sha gitpack.SHA, m uint8) {
		old := marks[sha]
		if old|m == old {
			return
		}
		marks[sha] = old | m
		switch {
		case queued[sha]:
			if old&paintStale == 0 && m&paintStale != 0 {
				live--
			}
		default:
			queued[sha] = true
			heap.Push(queue, timed{sha, when[sha]})
			if marks[sha]&paintStale == 0 {
				live++
			}
		}
	}
	add(onto, paintTip)
	add(head, paintHead)
	best := -1
	for steps := 0; live > 0; steps++ {
		if steps > maxBaseWalk {
			return gitpack.SHA{}, Invalid("no base was found within the walk limit")
		}
		cur := heap.Pop(queue).(timed).sha
		queued[cur] = false
		m := marks[cur]
		if m&paintStale == 0 {
			live--
		}
		if m&(paintTip|paintHead) == paintTip|paintHead {
			if i, ok := index[cur]; ok && (best < 0 || i < best) {
				best = i
			}
			m |= paintStale
			marks[cur] = m
		}
		ps := parents[cur]
		if m&paintTip == 0 {
			ps = ps[:min(len(ps), 1)]
		}
		for k, p := range ps {
			if err := load(p); err != nil {
				return gitpack.SHA{}, err
			}
			pm := m &^ paintHead
			if m&paintHead != 0 && k == 0 {
				pm |= paintHead
			}
			add(p, pm)
		}
	}
	if best < 0 {
		return gitpack.SHA{}, Invalid("head has no commit that the tip reaches")
	}
	return chain[best], nil
}

// TreeOf returns the root tree of a commit.
func (b *Batch) TreeOf(ctx context.Context, sha gitpack.SHA) (gitpack.SHA, error) {
	c, err := b.commit(ctx, sha)
	return c.Tree, err
}
