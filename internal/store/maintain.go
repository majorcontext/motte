package store

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

const (
	// DefaultGrace is how long a retired file stays before it is deleted.
	DefaultGrace = time.Hour
	// CompactMaxBytes bounds the pack that one compaction writes.
	CompactMaxBytes = MaxPackBytes
	// MaxLayerObjects bounds the records of one layer file.
	MaxLayerObjects = 8 << 20
	// inlineMaxObjects bounds the layer merge that a write does before it returns.
	inlineMaxObjects = 1 << 17
	maxFanIn         = 64
	// splitEntryBytes bounds the maps and lists that a split keeps for each object.
	splitEntryBytes = 400
	// maintainBudget bounds the time that one pass spends on the compactions and merges of a repository.
	maintainBudget = 20 * time.Second
	// deleteLease is how long a collector owns the files it marked.
	deleteLease = 10 * time.Minute
)

type maintStats struct {
	compactRuns, compactBytes, compactObjects atomic.Int64
	mergeRuns, mergeObjects                   atomic.Int64
	deleted, failures                         atomic.Int64
	mu                                        sync.Mutex
	layers                                    map[string]int
}

// MaintStats counts the work of Maintain, and the layers of the repos that it has seen.
type MaintStats struct {
	CompactRuns, CompactBytes, CompactObjects int64
	MergeRuns, MergeObjects                   int64
	Deleted, Failures                         int64
	Repos, LayersMax, LayersSum               int64
}

// MaintStats returns the counters of Maintain.
func (s *Service) MaintStats() MaintStats {
	st := &s.stats
	out := MaintStats{
		CompactRuns: st.compactRuns.Load(), CompactBytes: st.compactBytes.Load(), CompactObjects: st.compactObjects.Load(),
		MergeRuns: st.mergeRuns.Load(), MergeObjects: st.mergeObjects.Load(), Deleted: st.deleted.Load(), Failures: st.failures.Load(),
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, n := range st.layers {
		out.Repos++
		out.LayersSum += int64(n)
		out.LayersMax = max(out.LayersMax, int64(n))
	}
	return out
}

func (s *Service) seeLayers(repo Repo, n int) {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	if s.stats.layers == nil {
		s.stats.layers = map[string]int{}
	}
	s.stats.layers[repo.String()] = n
}

func (s *Service) repoLock(repo Repo) *sync.Mutex {
	l, _ := s.maint.LoadOrStore(repo.String(), &sync.Mutex{})
	return l.(*sync.Mutex)
}

// mergeTail merges the small layers at the tail of repo, and returns at once when another merge of the repo runs.
// A failure leaves the layers as they are, and Maintain merges them later.
func (s *Service) mergeTail(ctx context.Context, repo Repo) {
	l := s.repoLock(repo)
	if !l.TryLock() {
		return
	}
	defer l.Unlock()
	_, _ = s.mergeOnce(ctx, repo, inlineMaxObjects, true)
}

// planByClass applies plan to the layers of each class in turn, and returns the layers of the first plan. A layer of
// one class never merges or compacts with a layer of another, so a class keeps its own geometric sizes.
func planByClass(layers []midx.Entry, plan func([]midx.Entry) (midx.Plan, bool)) ([]midx.Entry, bool) {
	for _, class := range []string{"", midx.ClassMeta, midx.ClassBlob} {
		var sub []midx.Entry
		for _, e := range layers {
			if e.Class == class {
				sub = append(sub, e)
			}
		}
		if p, ok := plan(sub); ok {
			return sub[p.Lo:p.Hi], true
		}
	}
	return nil, false
}

// Maintain does one bounded pass over repo: it adds the packs that the manifest lacks, compacts packs, merges layers
// and deletes the retired files whose grace has passed. It repeats the compactions and the merges until no run is left
// or maintainBudget has passed, so a burst of writes does not outrun it. Every step ends in one compare and swap of the
// manifest, so a crash between steps loses nothing, and a repeat finishes the work. Each step holds the repository lock
// alone, so a tail merge of a write can run between steps. It is safe to run on many replicas, which only repeat work.
func (s *Service) Maintain(ctx context.Context, repo Repo) error {
	end := s.clk.Now().Add(maintainBudget)
	for _, f := range []func() (bool, error){
		func() (bool, error) { return s.compactOnce(ctx, repo) },
		func() (bool, error) { return s.mergeOnce(ctx, repo, MaxLayerObjects, false) },
	} {
		for s.clk.Now().Before(end) {
			var did bool
			err := s.locked(repo, func() (err error) { did, err = f(); return err })
			if err != nil {
				return s.failed(err)
			}
			if !did {
				break
			}
		}
	}
	if err := s.locked(repo, func() error { return s.collect(ctx, repo) }); err != nil {
		return s.failed(err)
	}
	if m, _, err := s.loadManifest(ctx, repo); err == nil {
		s.seeLayers(repo, len(m.Layers))
	}
	return nil
}

func (s *Service) locked(repo Repo, f func() error) error {
	l := s.repoLock(repo)
	l.Lock()
	defer l.Unlock()
	return f()
}

func (s *Service) failed(err error) error {
	if !errors.Is(err, context.Canceled) {
		s.stats.failures.Add(1)
	}
	return err
}

// run is the layers of a plan, with the packs that they hold, listed once each in order.
type run struct {
	layers []midx.Entry
	hs     []*layerH
	packs  []midx.Pack
	ords   [][]uint32
}

func (s *Service) openRun(ctx context.Context, prefix string, layers []midx.Entry) (*run, error) {
	r := &run{layers: layers}
	at := map[string]uint32{}
	for _, e := range layers {
		h, err := s.layer(ctx, prefix, e)
		if err != nil {
			return nil, err
		}
		r.hs = append(r.hs, h)
		ords := make([]uint32, len(h.packs))
		for i, p := range h.packs {
			u, ok := at[p.FileKey()]
			if !ok {
				u = uint32(len(r.packs))
				at[p.FileKey()] = u
				r.packs = append(r.packs, p)
			}
			ords[i] = u
		}
		r.ords = append(r.ords, ords)
	}
	return r, nil
}

func (r *run) objects() (n int) {
	for _, e := range r.layers {
		n += e.Objects
	}
	return n
}

func (r *run) bytes() (n int64) {
	for _, p := range r.packs {
		n += p.Bytes
	}
	return n
}

type idxRecords struct{ it *gitpack.IdxIter }

func (x idxRecords) Next() (midx.Record, bool, error) {
	sha, off, ok, err := x.it.Next()
	if err != nil || !ok {
		return midx.Record{}, false, err
	}
	if off > midx.MaxOffset {
		return midx.Record{}, false, midx.ErrCorrupt
	}
	return midx.Record{SHA: sha, Off: uint32(off)}, true, nil
}

// mergeOnce merges the newest run that the rule picks. A write calls it with try set, and it then reserves from
// inlineBudget and does nothing when that lacks the bytes.
// afterLastGraph returns the layers newer than the newest layer that has a graph. A tail merge stops there, so a write
// reads and writes no graph.
func afterLastGraph(layers []midx.Entry) []midx.Entry {
	for i := len(layers) - 1; i >= 0; i-- {
		if layers[i].Graph != "" {
			return layers[i+1:]
		}
	}
	return layers
}

func (s *Service) mergeOnce(ctx context.Context, repo Repo, maxObjects int, try bool) (bool, error) {
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return false, err
	}
	layers := m.Layers
	if try {
		layers = afterLastGraph(layers)
	}
	run, ok := planByClass(layers, func(l []midx.Entry) (midx.Plan, bool) {
		return midx.PlanMerge(l, maxObjects, maxFanIn, CompactMaxBytes)
	})
	if !ok {
		return false, nil
	}
	ctx, end := StartRootSpan(ctx, "vcs.maintain.merge", attribute.Int("layers", len(run)), attribute.String("class", run[0].Class), attribute.Bool("tail", try))
	var did opCount
	err = s.mergeRun(ctx, repo, run, try, &did)
	end(err, attribute.Int64("objects", did.objects))
	if errors.Is(err, errStale) {
		return false, nil
	}
	return err == nil, err
}

func (s *Service) compactOnce(ctx context.Context, repo Repo) (bool, error) {
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return false, err
	}
	run, ok := planByClass(m.Layers, func(l []midx.Entry) (midx.Plan, bool) {
		return midx.PlanCompact(l, CompactMaxBytes, MaxLayerObjects, maxFanIn)
	})
	if !ok {
		return false, nil
	}
	ctx, end := StartRootSpan(ctx, "vcs.maintain.compact", attribute.Int("layers", len(run)), attribute.String("class", run[0].Class))
	var did opCount
	err = s.compactRun(ctx, repo, run, &did)
	end(err, attribute.Int64("bytes_written", did.bytes), attribute.Int64("objects", did.objects))
	if errors.Is(err, errStale) {
		return false, nil
	}
	return err == nil, err
}

// replace swaps the layers old, found by key, for the layers add. The new layers take the place of the newest of old.
// It records the files in retire first, so a crash leaves a file that is retired and still held, which collect
// forgets, and never a file that is dropped and not retired.
func (s *Service) replace(ctx context.Context, repo Repo, old []midx.Entry, add []midx.Entry, retire []string) error {
	if err := s.index.RetireFiles(ctx, repo.String(), retire, s.clk.Now()); err != nil {
		return err
	}
	gone := map[string]bool{}
	for _, e := range old {
		gone[e.Key] = true
	}
	return s.updateManifest(ctx, repo, func(m *midx.Manifest) (bool, error) {
		var out []midx.Entry
		found, at := 0, -1
		for _, e := range m.Layers {
			if gone[e.Key] {
				found++
				at = len(out)
				continue
			}
			out = append(out, e)
		}
		if found != len(old) {
			return false, errStale
		}
		m.Layers = append(append(append([]midx.Entry{}, out[:at]...), add...), out[at:]...)
		return true, nil
	})
}

var errStale = errors.New("store: the layers changed under a merge")

// opCount is what one maintenance run did, for its span.
type opCount struct{ bytes, objects int64 }

func (s *Service) mergeRun(ctx context.Context, repo Repo, layers []midx.Entry, try bool, did *opCount) error {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	r, err := s.openRun(ctx, prefix, layers)
	if err != nil {
		return err
	}
	need, inputSizes := mergeNeed(layers, r.objects())
	var release func()
	if try {
		if release = s.tryReserveInline(need); release == nil {
			return nil
		}
	} else if release, err = s.Reserve(ctx, need); err != nil {
		return err
	}
	defer release()
	ins, err := s.recordIters(ctx, r)
	if err != nil {
		return err
	}
	if inputSizes {
		zs := make([]*midx.Sizes, len(layers))
		var lg errgroup.Group
		for i, e := range layers {
			lg.Go(func() error { zs[i] = s.sizesWhole(ctx, prefix, e); return nil })
		}
		_ = lg.Wait()
		for i, z := range zs {
			if z != nil {
				ins[i] = midx.WithSizes(ins[i], z)
			}
		}
	}
	l, seen, err := midx.Merge(ctx, ins, r.packs, r.objects(), func(in int, rec midx.Record) (midx.Record, error) {
		if int(rec.Ord) >= len(r.ords[in]) {
			return rec, midx.ErrCorrupt
		}
		rec.Ord = r.ords[in][rec.Ord]
		return rec, nil
	})
	if err != nil {
		return err
	}
	if seen != r.objects() {
		return fmt.Errorf("store: merge read %d records of %d", seen, r.objects())
	}
	var entry midx.Entry
	var zkey string
	var pg errgroup.Group
	pg.Go(func() (err error) { entry, err = s.putLayer(ctx, repo, prefix, l, r); return })
	pg.Go(func() (err error) { zkey, err = s.sizeObject(ctx, repo, prefix, l.Sizes, l.Objects); return })
	if err := pg.Wait(); err != nil {
		return err
	}
	if entry, err = s.mergedGraph(ctx, repo, prefix, entry, layers); err != nil {
		return err
	}
	entry.Sizes = zkey
	retire := make([]string, 0, len(layers))
	for _, e := range layers {
		if e.Sizes != "" {
			retire = append(retire, e.Sizes)
		}
		if !e.IsIndex() {
			retire = append(retire, e.Key)
		}
		if e.Graph != "" {
			retire = append(retire, e.Graph)
		}
	}
	if err := s.replace(ctx, repo, layers, []midx.Entry{entry}, retire); err != nil {
		return err
	}
	did.objects = int64(r.objects())
	s.stats.mergeRuns.Add(1)
	s.stats.mergeObjects.Add(did.objects)
	return nil
}

func (s *Service) putLayer(ctx context.Context, repo Repo, prefix string, l midx.Layer, r *run) (midx.Entry, error) {
	key := uniqueKey(l.Key, writeID())
	if _, err := s.st.Create(ctx, prefix+"/"+key, l.Data); err != nil {
		return midx.Entry{}, err
	}
	return midx.Entry{Key: key, Objects: l.Objects, Bytes: r.bytes(), NPacks: len(r.packs), Packs: r.packs, Size: int64(len(l.Data)), Dir: l.Dir, Class: r.layers[0].Class}, nil
}

// recordIters opens the record streams of the layers of a run, from the index of a pack or from a layer file.
func (s *Service) recordIters(ctx context.Context, r *run) ([]midx.Iter, error) {
	out := make([]midx.Iter, len(r.hs))
	for i, h := range r.hs {
		if h.dir != nil {
			out[i] = midx.NewIter(ctx, h.dir, s.readLayer(h))
			continue
		}
		out[i] = idxRecords{gitpack.NewIdxIter(ctx, h.entry.Objects, s.readLayer(h))}
	}
	return out, nil
}

// packOut is a pack that a compaction wrote, with its index files and its layer.
type packOut struct {
	id      string
	pack    []byte
	idx     []byte
	layer   midx.Layer
	class   string
	objects int
	// graph holds the commits of a metadata pack, for its commit graph.
	graph []midx.Commit
}

func (s *Service) compactRun(ctx context.Context, repo Repo, layers []midx.Entry, did *opCount) error {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	r, err := s.openRun(ctx, prefix, layers)
	if err != nil {
		return err
	}
	total := r.bytes()
	if total > CompactMaxBytes {
		return fmt.Errorf("store: compaction of %d bytes is over the limit", total)
	}
	var biggest int64
	for _, p := range r.packs {
		biggest = max(biggest, p.Bytes)
	}
	need := total + biggest + (3*int64(midx.RecordSize)+2*sizeBytes)*int64(r.objects()) + 4<<20 + 3*graphBytes(layers)
	class := layers[0].Class
	splitNeed := need + 2*total + splitEntryBytes*int64(r.objects()) + 2*gitpack.MaxObjectBytes
	split := class != midx.ClassBlob && splitNeed <= MemoryBudget/2
	if split {
		need = splitNeed
	}
	release, err := s.Reserve(ctx, need)
	if err != nil {
		return err
	}
	defer release()
	var outs []packOut
	if split {
		outs, err = s.splitPacks(ctx, repo, prefix, r)
	}
	if !split || errors.Is(err, errInflated) {
		var o packOut
		if o, err = s.copyPacks(ctx, prefix, r, class); err == nil {
			outs = []packOut{o}
		}
	}
	if err != nil {
		return err
	}
	var entries []midx.Entry
	var bytes int64
	objects := 0
	for _, o := range outs {
		wid := writeID()
		pk := midx.Pack{ID: o.id, Bytes: int64(len(o.pack)), Key: uniqueKey(o.id+".pack", wid), Idx: uniqueKey(o.id+".idx", wid)}
		layerKey := uniqueKey(o.layer.Key, wid)
		for key, data := range map[string][]byte{pk.Key: o.pack, pk.Idx: o.idx, layerKey: o.layer.Data} {
			if _, err := s.st.Create(ctx, prefix+"/"+key, data); err != nil {
				return err
			}
		}
		entry := midx.Entry{Key: layerKey, Objects: o.layer.Objects, Bytes: int64(len(o.pack)), NPacks: 1, Packs: []midx.Pack{pk}, Size: int64(len(o.layer.Data)), Dir: o.layer.Dir, Class: o.class}
		if vals, err := layerSizes(o.layer, o.pack); err == nil {
			if entry.Sizes, err = s.sizeObject(ctx, repo, prefix, midx.BuildSizes(vals), len(vals)); err != nil {
				return err
			}
		}
		switch {
		case o.graph != nil:
			entry, err = s.withGraph(ctx, repo, prefix, "", entry, o.graph)
		case o.class != midx.ClassBlob:
			entry, err = s.mergedGraph(ctx, repo, prefix, entry, layers)
		}
		if err != nil {
			return err
		}
		entries = append(entries, entry)
		bytes += int64(len(o.pack))
		objects += o.objects
	}
	retire, err := s.retiredByCompaction(ctx, repo, prefix, r)
	if err != nil {
		return err
	}
	if err := s.replace(ctx, repo, layers, entries, retire); err != nil {
		return err
	}
	did.bytes, did.objects = bytes, int64(objects)
	s.stats.compactRuns.Add(1)
	s.stats.compactBytes.Add(bytes)
	s.stats.compactObjects.Add(int64(objects))
	return nil
}

// finishPack seals the entries of a pack body, copied from sources, into a pack of count entries and its layer.
func (s *Service) finishPack(ctx context.Context, body []byte, count int, class string, ins []midx.Iter, objects int, remap func(int, midx.Record) (midx.Record, error)) (packOut, int, error) {
	if int64(len(body)) > midx.MaxOffset {
		return packOut{}, 0, errors.New("store: compacted pack is too large")
	}
	copy(body, "PACK")
	binary.BigEndian.PutUint32(body[4:], 2)
	binary.BigEndian.PutUint32(body[8:], uint32(count))
	sum := sha1.Sum(body)
	body = append(body, sum[:]...)
	idSum := sha256.Sum256(body)
	id := hex.EncodeToString(idSum[:])
	l, seen, err := midx.Merge(ctx, ins, []midx.Pack{midx.Pack{ID: id, Bytes: int64(len(body))}.Keyed()}, objects, remap)
	if err != nil {
		return packOut{}, 0, err
	}
	idx, err := legacyIndex(ctx, l, sum)
	if err != nil {
		return packOut{}, 0, err
	}
	return packOut{id: id, pack: body, idx: idx, layer: l, class: class, objects: count}, seen, nil
}

// copyPacks copies the entries of the packs of r, in order and unchanged, into one pack of the class.
func (s *Service) copyPacks(ctx context.Context, prefix string, r *run, class string) (packOut, error) {
	out := make([]byte, 12, r.bytes()+16)
	shift := make([]int64, len(r.packs))
	count := 0
	for u, p := range r.packs {
		data, err := s.st.Get(ctx, prefix+"/"+p.FileKey())
		if err != nil {
			return packOut{}, mapNotFound(err)
		}
		n, err := packEntries(data, p.Bytes)
		if err != nil {
			return packOut{}, fmt.Errorf("store: pack %s: %w", p.ID, err)
		}
		shift[u] = int64(len(out) - 12)
		out = append(out, data[12:len(data)-20]...)
		count += n
	}
	ins, err := s.recordIters(ctx, r)
	if err != nil {
		return packOut{}, err
	}
	o, seen, err := s.finishPack(ctx, out, count, class, ins, r.objects(), func(in int, rec midx.Record) (midx.Record, error) {
		if int(rec.Ord) >= len(r.ords[in]) || rec.Off < 12 {
			return rec, midx.ErrCorrupt
		}
		off := int64(rec.Off) + shift[r.ords[in][rec.Ord]]
		if off >= int64(len(out)) {
			return rec, midx.ErrCorrupt
		}
		return midx.Record{SHA: rec.SHA, Off: uint32(off)}, nil
	})
	if err == nil && seen != r.objects() {
		err = fmt.Errorf("store: compaction read %d records of %d", seen, r.objects())
	}
	return o, err
}

// legacyIndex writes the v2 index of a compacted pack from its layer, so a release that reads packs by their own index
// can read the pack. The index names each object once, at its first copy, and its CRC fields are zero, which that
// release does not check.
func legacyIndex(ctx context.Context, l midx.Layer, packSum [20]byte) ([]byte, error) {
	d, err := midx.Parse(l.Data[len(l.Data)-l.Dir:], int64(len(l.Data)))
	if err != nil {
		return nil, err
	}
	it := midx.NewIter(ctx, d, func(_ context.Context, off, n int64) ([]byte, error) { return l.Data[off : off+n], nil })
	entries := make([]gitpack.IdxEntry, 0, l.Objects)
	for {
		r, ok, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return gitpack.BuildIdx(entries, packSum), nil
		}
		entries = append(entries, gitpack.IdxEntry{Name: r.SHA, Off: r.Off})
	}
}

// retiredByCompaction lists the files that a compaction frees: the layers, and the packs that no other layer holds.
func (s *Service) retiredByCompaction(ctx context.Context, repo Repo, prefix string, r *run) ([]string, error) {
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return nil, err
	}
	inRun := map[string]bool{}
	var out []string
	for _, e := range r.layers {
		inRun[e.Key] = true
		if !e.IsIndex() {
			out = append(out, e.Key)
		}
		if e.Graph != "" {
			out = append(out, e.Graph)
		}
		if e.Sizes != "" {
			out = append(out, e.Sizes)
		}
	}
	var rest []midx.Entry
	for _, e := range m.Layers {
		if !inRun[e.Key] {
			rest = append(rest, e)
		}
	}
	held, err := s.heldPacks(ctx, prefix, rest)
	if err != nil {
		return nil, err
	}
	for _, p := range r.packs {
		if _, live := held[p.FileKey()]; !live {
			out = append(out, p.FileKey(), p.IdxKey())
		}
	}
	return out, nil
}

// heldPacks lists the packs of layers by the key of the pack file.
func (s *Service) heldPacks(ctx context.Context, prefix string, layers []midx.Entry) (map[string]midx.Pack, error) {
	held := map[string]midx.Pack{}
	for _, e := range layers {
		h, err := s.layer(ctx, prefix, e)
		if err != nil {
			return nil, err
		}
		for _, p := range h.packs {
			held[p.FileKey()] = p
		}
	}
	return held, nil
}

// packEntries checks a stored pack and returns its number of entries.
func packEntries(data []byte, size int64) (int, error) {
	if int64(len(data)) != size || len(data) < 32 || string(data[:4]) != "PACK" {
		return 0, gitpack.ErrCorrupt
	}
	if v := binary.BigEndian.Uint32(data[4:8]); v != 2 && v != 3 {
		return 0, gitpack.ErrCorrupt
	}
	sum := sha1.Sum(data[:len(data)-20])
	if string(sum[:]) != string(data[len(data)-20:]) {
		return 0, gitpack.ErrCorrupt
	}
	return int(binary.BigEndian.Uint32(data[8:12])), nil
}

// collect deletes the retired files whose grace has passed, and forgets them. It reads the version of each due file,
// then marks it as deleting in the metadata store, and then checks the manifest. It deletes only the version that it
// read, so a file that a writer stored again after a stall is kept, and its record stays for a later pass. A writer that stores the file again claims it first, so a file
// that the manifest will hold is never deleted. A file that a layer holds already, such as a file of a swap that was
// lost, is forgotten and kept.
func (s *Service) collect(ctx context.Context, repo Repo) (err error) {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	retired, err := s.index.RetiredFiles(ctx, repo.String())
	if err != nil {
		return err
	}
	now := s.clk.Now()
	cut := now.Add(-s.grace)
	var due []string
	for _, r := range retired {
		if !r.At.After(cut) {
			due = append(due, r.Key)
		}
	}
	if len(due) == 0 {
		return nil
	}
	versions := make(map[string]string, len(due))
	for _, key := range due {
		v, err := s.st.Version(ctx, prefix+"/"+key)
		if errors.Is(err, bucket.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		versions[key] = v
	}
	taken, err := s.index.BeginDelete(ctx, repo.String(), due, now, now.Add(-deleteLease), cut)
	if err != nil {
		return err
	}
	if len(taken) == 0 {
		return nil
	}
	ctx, end := StartRootSpan(ctx, "vcs.maintain.collect", attribute.Int("files", len(taken)))
	var deleted int64
	defer func() { end(err, attribute.Int64("deleted", deleted)) }()
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return err
	}
	held, err := s.heldPacks(ctx, prefix, m.Layers)
	if err != nil {
		return err
	}
	keys := map[string]bool{}
	for _, p := range held {
		keys[p.FileKey()], keys[p.IdxKey()] = true, true
	}
	for _, e := range m.Layers {
		keys[e.Key] = true
		if e.Graph != "" {
			keys[e.Graph] = true
		}
		if e.Sizes != "" {
			keys[e.Sizes] = true
		}
	}
	done := make([]string, 0, len(taken))
	for _, key := range taken {
		isPack := strings.HasSuffix(key, ".pack")
		if keys[key] {
			done = append(done, key)
			continue
		}
		if v, ok := versions[key]; ok {
			if err := s.st.Delete(ctx, prefix+"/"+key, v); errors.Is(err, bucket.ErrChanged) {
				continue
			} else if err != nil {
				return err
			}
		}
		done = append(done, key)
		if isPack {
			s.rcache.dropPack(prefix + "/" + key)
		}
		s.stats.deleted.Add(1)
		deleted++
	}
	return s.index.ForgetRetired(ctx, repo.String(), done)
}
