package importer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/store"
)

// historyPackBytes is the size at which a range pack is split; the store refuses a pack of 64 MiB.
// historyChunkBytes bounds one pack of a single commit. Tests lower both.
var (
	historyPackBytes  int64 = 48 << 20
	historyChunkBytes int64 = 32 << 20
)

const (
	// historyRangeBytes and historyRangeObjects bound a range by the inflated size and the count of its
	// objects. Both come from the objects alone, so the ranges of a repository stay the same when commits
	// are added after them.
	historyRangeBytes = 256 << 20
	// historyChunkObjects bounds one pack of a single commit; the v2 index takes 28
	// bytes for each object and the store refuses one over 8 MiB.
	historyChunkObjects = 100_000
	historyRangeObjects = 150_000
	hasBatch            = 4096
	// omitBlobs leaves out the blobs that the store refuses for their size. The store cannot list a tree
	// that names one, so a commit with such a tree imports but does not mount.
	omitBlobs = "--filter=blob:limit=16777217"
)

// HistoryStore is the part of the store that ImportHistory uses.
type HistoryStore interface {
	ImportPack(ctx context.Context, idx, pack []byte) error
	Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error)
}

// HistoryStats counts the packs of one ImportHistory run.
type HistoryStats struct {
	Uploaded, Skipped int
	Bytes             int64
}

type historyRange struct{ first, last int }

// ImportHistory fetches the whole history of ref and uploads it as packs of at most about 48 MiB. The history
// is cut along the first-parent chain of the commit: a range holds the commits that its last commit reaches
// and the commit before the range does not, with their objects, but not a blob over 16 MiB. Each pack is self-contained, so the store
// verifies it alone. A range whose commits the store all holds is skipped, so a later run uploads only the
// ranges that gained commits. It returns the commit that ref named.
func ImportHistory(ctx context.Context, st HistoryStore, remote, ref string, logf func(format string, args ...any)) (gitpack.SHA, HistoryStats, error) {
	var stats HistoryStats
	dir, err := os.MkdirTemp("", "motte-import-")
	if err != nil {
		return gitpack.SHA{}, stats, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	g := gitRepo{dir: dir}
	for _, args := range [][]string{{"init", "--bare", "--quiet"}, {"fetch", "--no-tags", "--quiet", remote, "+" + ref + ":" + importRef}} {
		if _, err := g.run(ctx, nil, args...); err != nil {
			return gitpack.SHA{}, stats, err
		}
	}
	head, err := g.run(ctx, nil, "rev-parse", importRef+"^{commit}")
	if err != nil {
		return gitpack.SHA{}, stats, err
	}
	commit, err := gitpack.ParseSHA(head)
	if err != nil {
		return commit, stats, err
	}
	chain, err := g.lines(ctx, "rev-list", "--first-parent", "--reverse", head)
	if err != nil {
		return commit, stats, err
	}
	ranges, err := g.planRanges(ctx, chain)
	if err != nil {
		return commit, stats, err
	}
	for _, r := range ranges {
		if err := g.importRange(ctx, st, chain, r, &stats, logf); err != nil {
			return commit, stats, err
		}
	}
	return commit, stats, nil
}

type gitRepo struct{ dir string }

func (g gitRepo) cmd(ctx context.Context, stdin io.Reader, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir, cmd.Stdin = g.dir, stdin
	return cmd
}

func (g gitRepo) run(ctx context.Context, stdin io.Reader, args ...string) (string, error) {
	var out bytes.Buffer
	cmd := g.cmd(ctx, stdin, args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("vfs: git %s: %w: %s", args[0], err, strings.TrimSpace(out.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func (g gitRepo) lines(ctx context.Context, args ...string) ([]string, error) {
	out, err := g.run(ctx, nil, args...)
	if err != nil || out == "" {
		return nil, err
	}
	return strings.Split(out, "\n"), nil
}

// revs is the stdin of a command that takes the commits of chain[last] that chain[first-1] does not reach.
func revs(chain []string, first, last int) io.Reader {
	s := chain[last] + "\n"
	if first > 0 {
		s += "^" + chain[first-1] + "\n"
	}
	return strings.NewReader(s)
}

// measure returns the inflated bytes and the count of the objects of the range.
func (g gitRepo) measure(ctx context.Context, chain []string, first, last int) (size int64, objects int, err error) {
	list := g.cmd(ctx, revs(chain, first, last), "rev-list", "--objects", "--no-object-names", omitBlobs, "--stdin")
	sizes := g.cmd(ctx, nil, "cat-file", "--batch-check=%(objectsize)")
	pipe, err := list.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	sizes.Stdin = pipe
	out, err := sizes.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	var listErr, sizesErr bytes.Buffer
	list.Stderr, sizes.Stderr = &listErr, &sizesErr
	if err := list.Start(); err != nil {
		return 0, 0, err
	}
	if err := sizes.Start(); err != nil {
		_ = list.Wait()
		return 0, 0, err
	}
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		n, perr := strconv.ParseInt(sc.Text(), 10, 64)
		if perr != nil {
			err = errors.Join(err, perr)
			break
		}
		size += n
		objects++
	}
	_, _ = io.Copy(io.Discard, out)
	if werr := errors.Join(list.Wait(), sizes.Wait()); werr != nil || err != nil {
		return 0, 0, fmt.Errorf("vfs: measure range: %w %s", errors.Join(werr, err), strings.TrimSpace(listErr.String()+sizesErr.String()))
	}
	return size, objects, nil
}

// planRanges cuts chain from its oldest commit. Each range takes the most commits that stay within the
// bounds, and at least one.
func (g gitRepo) planRanges(ctx context.Context, chain []string) ([]historyRange, error) {
	fits := func(first, last int) (bool, error) {
		size, objects, err := g.measure(ctx, chain, first, last)
		return size <= historyRangeBytes && objects <= historyRangeObjects, err
	}
	var out []historyRange
	for first := 0; first < len(chain); {
		best := first
		for step := 1; best < len(chain)-1; step *= 2 {
			next := min(first+step, len(chain)-1)
			ok, err := fits(first, next)
			if err != nil {
				return nil, err
			}
			if ok {
				best = next
				continue
			}
			for lo, hi := best, next; hi-lo > 1; {
				mid := lo + (hi-lo)/2
				if ok, err = fits(first, mid); err != nil {
					return nil, err
				}
				if ok {
					lo, best = mid, mid
				} else {
					hi = mid
				}
			}
			break
		}
		out = append(out, historyRange{first, best})
		first = best + 1
	}
	return out, nil
}

func (g gitRepo) importRange(ctx context.Context, st HistoryStore, chain []string, r historyRange, stats *HistoryStats, logf func(string, ...any)) error {
	held, err := g.held(ctx, st, chain, r)
	if err != nil {
		return err
	}
	if held {
		stats.Skipped++
		logf("range %s..%s: held by the store", chain[r.first][:12], chain[r.last][:12])
		return nil
	}
	return g.upload(ctx, st, chain, r, stats, logf)
}

// held reports whether the store holds every commit of the range.
func (g gitRepo) held(ctx context.Context, st HistoryStore, chain []string, r historyRange) (bool, error) {
	out, err := g.run(ctx, revs(chain, r.first, r.last), "rev-list", "--stdin")
	if err != nil {
		return false, err
	}
	names := strings.Fields(out)
	for len(names) > 0 {
		n := min(hasBatch, len(names))
		batch := make([]gitpack.SHA, n)
		for i, h := range names[:n] {
			if batch[i], err = gitpack.ParseSHA(h); err != nil {
				return false, err
			}
		}
		present, err := st.Has(ctx, batch)
		if err != nil {
			return false, err
		}
		for _, ok := range present {
			if !ok {
				return false, nil
			}
		}
		names = names[n:]
	}
	return true, nil
}

// upload packs the range and sends the pack. A pack over historyPackBytes, or one that the store refuses as too
// large, splits its range in two; a single commit splits its objects into bounded packs.
func (g gitRepo) upload(ctx context.Context, st HistoryStore, chain []string, r historyRange, stats *HistoryStats, logf func(string, ...any)) error {
	label := chain[r.first][:12] + ".." + chain[r.last][:12]
	objects, err := g.run(ctx, revs(chain, r.first, r.last), "rev-list", "--objects", "--no-object-names", omitBlobs, "--stdin")
	if err != nil {
		return err
	}
	pack, idx, size, err := g.packObjects(ctx, objects)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(pack); _ = os.Remove(idx) }()
	if size <= historyPackBytes {
		if err = sendPack(ctx, st, pack, idx, stats, logf, label); !errors.Is(err, store.ErrPackTooLarge) {
			return err
		}
	}
	if r.first == r.last {
		return g.uploadChunks(ctx, st, objects, stats, logf, label)
	}
	mid := r.first + (r.last-r.first)/2
	for _, half := range []historyRange{{r.first, mid}, {mid + 1, r.last}} {
		if err := g.upload(ctx, st, chain, half, stats, logf); err != nil {
			return err
		}
	}
	return nil
}

// packObjects packs the named objects and returns the files and the size of the pack, which it reads from the
// file system and never loads.
func (g gitRepo) packObjects(ctx context.Context, objects string) (pack, idx string, size int64, err error) {
	base := filepath.Join(g.dir, "range")
	name, err := g.run(ctx, strings.NewReader(objects+"\n"), "pack-objects", "--delta-base-offset", "--index-version=2", "-q", base)
	if err != nil {
		return "", "", 0, err
	}
	pack, idx = base+"-"+name+".pack", base+"-"+name+".idx"
	fi, err := os.Stat(pack)
	if err != nil {
		return "", "", 0, err
	}
	return pack, idx, fi.Size(), nil
}

var readFile = os.ReadFile

func sendPack(ctx context.Context, st HistoryStore, packPath, idxPath string, stats *HistoryStats, logf func(string, ...any), label string) error {
	pack, err := readFile(packPath)
	if err != nil {
		return err
	}
	idx, err := readFile(idxPath)
	if err != nil {
		return err
	}
	if err := st.ImportPack(ctx, idx, pack); err != nil {
		return err
	}
	stats.Uploaded++
	stats.Bytes += int64(len(pack))
	logf("range %s: uploaded a pack of %d bytes", label, len(pack))
	return nil
}

// uploadChunks sends the objects of one commit as packs of at most historyChunkBytes inflated. Each pack is
// self-contained and the store verifies only that each object hashes to its name, so the order of the packs does
// not matter; blobs go first, then trees, then the commit, so that a reader never finds a tree before its blobs.
func (g gitRepo) uploadChunks(ctx context.Context, st HistoryStore, objects string, stats *HistoryStats, logf func(string, ...any), label string) error {
	out, err := g.run(ctx, strings.NewReader(objects+"\n"), "cat-file", "--batch-check=%(objecttype) %(objectsize) %(objectname)")
	if err != nil {
		return err
	}
	var all []histObj
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 3 {
			return fmt.Errorf("vfs: unexpected cat-file line %q", line)
		}
		size, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return err
		}
		all = append(all, histObj{map[string]int{"blob": 0, "tree": 1, "commit": 2}[f[0]], size, f[2]})
	}
	for _, names := range chunkObjects(all, historyChunkBytes, historyChunkObjects) {
		pack, idx, _, err := g.packObjects(ctx, strings.Join(names, "\n"))
		if err != nil {
			return err
		}
		err = sendPack(ctx, st, pack, idx, stats, logf, label)
		_ = os.Remove(pack)
		_ = os.Remove(idx)
		if err != nil {
			return err
		}
	}
	return nil
}

type histObj struct {
	rank int
	size int64
	name string
}

// chunkObjects orders the objects (blobs, trees, commits) and cuts them into chunks of at most maxBytes inflated
// and maxObjects names, and at least one object.
func chunkObjects(all []histObj, maxBytes int64, maxObjects int) [][]string {
	slices.SortStableFunc(all, func(a, b histObj) int { return a.rank - b.rank })
	var out [][]string
	for len(all) > 0 {
		var names []string
		var total int64
		for len(all) > 0 && (len(names) == 0 || (total+all[0].size <= maxBytes && len(names) < maxObjects)) {
			names, total, all = append(names, all[0].name), total+all[0].size, all[1:]
		}
		out = append(out, names)
	}
	return out
}
