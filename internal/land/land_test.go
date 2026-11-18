package land

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/internal/gitpack"
)

type mem map[gitpack.SHA]gitpack.Object

func (m mem) Get(_ context.Context, sha gitpack.SHA, _ func(int64) error) (gitpack.Type, []byte, error) {
	o, ok := m[sha]
	if !ok {
		return 0, nil, errors.New("missing")
	}
	return o.Type, o.Data, nil
}

func (m mem) put(t gitpack.Type, data []byte) gitpack.SHA {
	sha := gitpack.Hash(t, data)
	m[sha] = gitpack.Object{Type: t, Data: data}
	return sha
}

// tree stores a tree of files, written as path to content.
func (m mem) tree(files map[string]string) gitpack.SHA {
	b := &Batch{src: m, grow: func(int64) error { return nil }, byID: map[gitpack.SHA]int{}, trees: map[gitpack.SHA][]Entry{}, produced: map[gitpack.SHA][]Entry{}, diffs: map[gitpack.SHA]diffResult{}}
	var changes []Change
	for p, c := range files {
		e := Entry{Mode: 0o100644, SHA: m.put(gitpack.Blob, []byte(c))}
		changes = append(changes, Change{Path: p, New: &e})
	}
	root, err := b.apply(context.Background(), gitpack.SHA{}, changes)
	if err != nil {
		panic(err)
	}
	for _, o := range b.objs {
		m.put(o.Type, o.Data)
	}
	return root
}

func (m mem) commit(parent gitpack.SHA, tree gitpack.SHA, extra string) gitpack.SHA {
	p := ""
	if parent != (gitpack.SHA{}) {
		p = "parent " + parent.String() + "\n"
	}
	return m.put(gitpack.Commit, fmt.Appendf(nil, "tree %s\n%sauthor A <a@x> 1 +0100\ncommitter C <c@x> 2 +0200\n%s\nmsg\n", tree, p, extra))
}

func (m mem) batch(tip gitpack.SHA) *Batch {
	return NewBatch(m, func(int64) error { return nil }, tip, nil)
}

func (m mem) files(t *testing.T, commit gitpack.SHA) []string {
	t.Helper()
	b := m.batch(commit)
	c, err := b.commit(context.Background(), commit)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	err = b.leaves(context.Background(), c.Tree, "", func(p string, e Entry) error {
		_, blob, _ := m.Get(nil, e.SHA, nil)
		out = append(out, p+"="+string(blob))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRebaseAppliesTheChangesOnTheTip(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1", "d/x": "1", "d/y": "1"}), "")
	tip := m.commit(base, m.tree(map[string]string{"a": "2", "d/x": "1", "d/y": "1"}), "")
	c1 := m.commit(base, m.tree(map[string]string{"a": "1", "d/x": "9", "d/y": "1", "n": "n"}), "change-id abc\ngpgsig -----BEGIN\n sig\n -----END\n")
	c2 := m.commit(c1, m.tree(map[string]string{"a": "1", "d/x": "9", "n": "n"}), "")
	when := time.Unix(1700000000, 0)
	b := m.batch(tip)
	got, err := b.Land(context.Background(), base, c2, when)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range b.Objects() {
		m.put(o.Type, o.Data)
	}
	if fmt.Sprint(m.files(t, got.New)) != "[a=2 d/x=9 n=n]" || fmt.Sprint(got.Paths) != "[d/x d/y n]" {
		t.Fatalf("files = %v, paths = %v", m.files(t, got.New), got.Paths)
	}
	_, data, _ := m.Get(nil, got.Commits[0].New, nil)
	text := string(data)
	for _, want := range []string{"parent " + tip.String(), "author A <a@x> 1 +0100\n", "committer C <c@x> 1700000000 +0000\n", "change-id abc\n"} {
		if !strings.Contains(text, want) {
			t.Errorf("commit lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "gpgsig") || strings.Contains(text, "BEGIN") {
		t.Errorf("commit keeps its signature:\n%s", text)
	}
	again := m.batch(tip)
	if got2, _ := again.Land(context.Background(), base, c2, when); got2.New != got.New {
		t.Errorf("same inputs gave %s then %s", got.New, got2.New)
	}
}

func TestConflictsAreFileLevelAndNameTheirPaths(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1", "d/x": "1", "f/g": "1", "z": "1"}), "")
	tip := m.commit(base, m.tree(map[string]string{"a": "2", "d/x": "1", "f/g": "2", "z": "1"}), "")
	for name, tree := range map[string]map[string]string{
		"same file":           {"a": "3", "d/x": "1", "f/g": "1", "z": "1"},
		"file over directory": {"a": "1", "d/x": "1", "f": "file", "z": "1"},
		"directory over file": {"a/b": "1", "d/x": "1", "f/g": "1", "z": "1"},
	} {
		c := m.commit(base, m.tree(tree), "")
		_, err := m.batch(tip).Land(context.Background(), base, c, time.Unix(1, 0))
		var ce *ConflictError
		if !errors.As(err, &ce) {
			t.Errorf("%s: err = %v, want a conflict", name, err)
			continue
		}
		t.Logf("%s: %v", name, ce.Paths)
	}
	clean := m.commit(base, m.tree(map[string]string{"a": "1", "d/x": "7", "f/g": "1", "z": "1"}), "")
	if _, err := m.batch(tip).Land(context.Background(), base, clean, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestAnEarlierLandOfTheBatchConflictsWithALaterOne(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1", "b": "1"}), "")
	one := m.commit(base, m.tree(map[string]string{"a": "2", "b": "1"}), "")
	two := m.commit(base, m.tree(map[string]string{"a": "3", "b": "1"}), "")
	three := m.commit(base, m.tree(map[string]string{"a": "1", "b": "3"}), "")
	b := m.batch(base)
	if _, err := b.Land(context.Background(), base, one, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	var ce *ConflictError
	if _, err := b.Land(context.Background(), base, two, time.Unix(1, 0)); !errors.As(err, &ce) || !slices.Equal(ce.Paths, []string{"a"}) {
		t.Fatalf("err = %v, want a conflict on a", err)
	}
	if l, err := b.Land(context.Background(), base, three, time.Unix(1, 0)); err != nil || l.Old == base {
		t.Fatalf("a land after a refused one = %+v, %v", l, err)
	}
}

func TestFastForwardKeepsTheCommits(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1"}), "")
	head := m.commit(base, m.tree(map[string]string{"a": "1", "b": "1"}), "")
	l, err := m.batch(base).Land(context.Background(), base, head, time.Unix(1, 0))
	if err != nil || !l.FastForward || l.New != head || len(l.Commits) != 0 || fmt.Sprint(l.Paths) != "[b]" {
		t.Fatalf("land = %+v, %v", l, err)
	}
}

func TestARangeThatCannotLandIsInvalid(t *testing.T) {
	m := mem{}
	tree := m.tree(map[string]string{"a": "1"})
	base := m.commit(gitpack.SHA{}, tree, "")
	other := m.commit(gitpack.SHA{}, tree, "x y\n")
	side := m.commit(base, tree, "x z\n")
	merge := m.put(gitpack.Commit, fmt.Appendf(nil, "tree %s\nparent %s\nparent %s\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 1 +0000\n\nmerge\n", tree, base, side))
	for name, head := range map[string]gitpack.SHA{"unrelated": other, "merge": merge} {
		var inv Invalid
		if _, err := m.batch(base).Land(context.Background(), base, head, time.Unix(1, 0)); !errors.As(err, &inv) {
			t.Errorf("%s: err = %v, want Invalid", name, err)
		}
	}
}

func TestFastForwardRefusesAPathThatIsNotUTF8(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1"}), "")
	head := m.commit(base, m.tree(map[string]string{"a": "1", "\xff": "1"}), "")
	var inv Invalid
	if _, err := m.batch(base).Land(context.Background(), base, head, time.Unix(1, 0)); !errors.As(err, &inv) {
		t.Fatalf("err = %v, want Invalid: a log row cannot keep the path", err)
	}
}

func TestTheLimitCountsDistinctPaths(t *testing.T) {
	defer func(n int) { MaxChangedPaths = n }(MaxChangedPaths)
	MaxChangedPaths = 3
	m := mem{}
	files := func(v string) map[string]string { return map[string]string{"a": v, "b": v, "c": v} }
	base := m.commit(gitpack.SHA{}, m.tree(files("0")), "")
	tip := m.commit(base, m.tree(map[string]string{"a": "0", "b": "0", "c": "0", "z": "z"}), "")
	one := m.commit(base, m.tree(files("1")), "")
	two := m.commit(one, m.tree(files("2")), "")
	if _, err := m.batch(tip).Land(context.Background(), base, two, time.Unix(1, 0)); err != nil {
		t.Fatalf("two commits that edit the same 3 paths: %v", err)
	}
	four := m.commit(two, m.tree(map[string]string{"a": "2", "b": "2", "c": "2", "d": "1"}), "")
	if _, err := m.batch(base).Land(context.Background(), base, four, time.Unix(1, 0)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("4 distinct paths: err = %v, want ErrTooLarge", err)
	}
}

func TestFastForwardChecksEveryCommitOfTheRange(t *testing.T) {
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1"}), "")
	mid := m.commit(base, m.tree(map[string]string{"a": "1", "\xff": "1"}), "")
	head := m.commit(mid, m.tree(map[string]string{"a": "1"}), "")
	var inv Invalid
	if _, err := m.batch(base).Land(context.Background(), base, head, time.Unix(1, 0)); !errors.As(err, &inv) {
		t.Fatalf("err = %v, want Invalid for the path that the middle commit adds", err)
	}
}

func TestTheCacheOfBaseDiffsIsBounded(t *testing.T) {
	defer func(n int) { MaxChangedPaths = n }(MaxChangedPaths)
	MaxChangedPaths = 4
	m := mem{}
	start := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "0"}), "")
	b := m.batch(start)
	for i := range 5 {
		other := m.commit(gitpack.SHA{}, m.tree(map[string]string{fmt.Sprint("x", i): "1", fmt.Sprint("y", i): "1"}), "")
		if ok, err := b.before(context.Background(), other, func(string) {}); err != nil || !ok {
			t.Fatalf("before = %v, %v", ok, err)
		}
		if b.cached > MaxChangedPaths {
			t.Fatalf("cache holds %d paths, want at most %d", b.cached, MaxChangedPaths)
		}
	}
}

func TestALandThatMakesAnObjectAboveTheLimitFailsAloneAndLeavesNoTrace(t *testing.T) {
	defer func(n int) { MaxObjectBytes = n }(MaxObjectBytes)
	m := mem{}
	base := m.commit(gitpack.SHA{}, m.tree(map[string]string{"a": "1"}), "")
	one := m.commit(base, m.tree(map[string]string{"a": "1", "b": "1"}), "")
	two := m.commit(base, m.tree(map[string]string{"a": "1", "c": "1"}), "")
	tip := m.commit(base, m.tree(map[string]string{"a": "1", "d": "1"}), "")
	b := m.batch(tip)
	MaxObjectBytes = 1 << 20
	if _, err := b.Land(context.Background(), base, one, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	held := b.Bytes()
	MaxObjectBytes = 100
	if _, err := b.Land(context.Background(), base, two, time.Unix(1, 0)); !errors.Is(err, ErrTooLarge) || b.Bytes() != held {
		t.Fatalf("err = %v, bytes = %d, want ErrTooLarge and %d", err, b.Bytes(), held)
	}
	wrong := m.tree(map[string]string{"x": "1"})
	if _, err := b.LandExpecting(context.Background(), base, two, time.Unix(1, 0), &wrong); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("with a stale expect_tree: err = %v, want ErrTooLarge before the tree compare", err)
	}
	MaxObjectBytes = 1 << 20
	if _, err := b.Land(context.Background(), base, two, time.Unix(1, 0)); err != nil {
		t.Fatalf("a land after the refused one: %v", err)
	}
}
