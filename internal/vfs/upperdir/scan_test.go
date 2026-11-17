package upperdir

import (
	"reflect"
	"testing"
)

func f(p string) Entry          { return Entry{Path: p, Kind: KindFile} }
func fx(p string) Entry         { return Entry{Path: p, Kind: KindFile, Exec: true} }
func fo(p, origin string) Entry { return Entry{Path: p, Kind: KindFile, Origin: origin} }
func fxo(p, origin string) Entry {
	return Entry{Path: p, Kind: KindFile, Exec: true, Origin: origin}
}
func del(p string) Entry { return Entry{Path: p, Kind: KindDeleted} }
func fnc(p string) Entry { return Entry{Path: p, Kind: KindFile, NoContent: true} }
func dir(p string) Entry { return Entry{Path: p, Kind: KindDir} }
func opq(p string) Entry { return Entry{Path: p, Kind: KindDir, Opaque: true} }

// TestScanShapes pins the dirty set of each upper directory shape against a fixed base. A wrong entry means
// a file that the snapshot would drop, keep or add by mistake.
func TestScanShapes(t *testing.T) {
	base := newBase("a/1", "a/2*", "b/1", "c/d/e", "c/d/f", "c/d/s/t", "c/g", "ln@", "top")
	for _, tc := range []struct {
		name  string
		upper []string
		want  []Entry
	}{
		{"nothing", nil, nil},
		{"modified added exec symlink, fifo skipped", []string{"d a", "f a/1", "x a/2", "f a/new", "l ln", "p fifo"},
			[]Entry{f("a/1"), fx("a/2"), f("a/new"), {Path: "ln", Kind: KindSymlink}}},
		{"fifo over a base file and a base directory", []string{"p top", "p c"}, []Entry{del("c"), del("top")}},
		{"whiteout over file and directory", []string{"w top", "w c"}, []Entry{del("c"), del("top")}},
		{"whiteout over an absent path", []string{"w nothing", "d a", "w a/zzz"}, nil},
		{"new empty directory", []string{"d fresh", "d a", "d a/sub"}, []Entry{dir("a/sub"), dir("fresh")}},
		{"new directory holding a file", []string{"d fresh", "f fresh/x"}, []Entry{f("fresh/x")}},
		{"unchanged merged directory", []string{"d c", "d c/d"}, nil},
		{"opaque directory over a base directory", []string{"d c", "o c/d", "f c/d/x"}, []Entry{opq("c/d"), f("c/d/x")}},
		{"empty opaque directory", []string{"d c", "o c/d"}, []Entry{opq("c/d")}},
		{"file replaces directory", []string{"f c"}, []Entry{f("c")}},
		{"directory replaces file", []string{"o top", "f top/x"}, []Entry{opq("top"), f("top/x")}},
		{"plain directory over a base file", []string{"d top"}, []Entry{opq("top")}},
		{"metacopy with the base mode", []string{"d a", "m a/1", "mx a/2"}, nil},
		{"metacopy with a new exec bit", []string{"d a", "mx a/1", "m a/2"}, []Entry{fxo("a/1", "a/1"), fo("a/2", "a/2")}},
		{"metacopy over an absent base file", []string{"m ghost"}, []Entry{fnc("ghost")}},
		{"metacopy with an invalid redirect", []string{"mr ghost /../x"}, []Entry{fnc("ghost")}},
		{"metacopy redirected to a base directory", []string{"mr ghost /a"}, []Entry{fnc("ghost")}},
		{"metacopy in a directory with no origin", []string{"d fresh", "m fresh/x"}, []Entry{fnc("fresh/x")}},
		{"file renamed by absolute redirect", []string{"d a", "w a/1", "d b", "mr b/n /a/1"}, []Entry{del("a/1"), fo("b/n", "a/1")}},
		{"file renamed by relative redirect", []string{"d a", "w a/1", "mr a/n 1"}, []Entry{del("a/1"), fo("a/n", "a/1")}},
		{"directory renamed by relative redirect", []string{"d c", "w c/d", "r c/h d"},
			[]Entry{del("c/d"), fo("c/h/e", "c/d/e"), fo("c/h/f", "c/d/f"), fo("c/h/s/t", "c/d/s/t")}},
		{"directory renamed by absolute redirect", []string{"w c", "r x /c"},
			[]Entry{del("c"), fo("x/d/e", "c/d/e"), fo("x/d/f", "c/d/f"), fo("x/d/s/t", "c/d/s/t"), fo("x/g", "c/g")}},
		{"redirected directory with a whiteout inside", []string{"w c/d", "d c", "r x /c/d", "w x/e", "w x/ghost"},
			[]Entry{del("c/d"), fo("x/f", "c/d/f"), fo("x/s/t", "c/d/s/t")}},
		{"redirected directory with an upper child that replaces a base one", []string{"r x /c/d", "x x/e", "f x/new"},
			[]Entry{fx("x/e"), fo("x/f", "c/d/f"), f("x/new"), fo("x/s/t", "c/d/s/t")}},
		{"redirected directory with a merged child directory", []string{"r x /c/d", "d x/s", "f x/s/u"},
			[]Entry{fo("x/e", "c/d/e"), fo("x/f", "c/d/f"), fo("x/s/t", "c/d/s/t"), f("x/s/u")}},
		{"redirected directory with an opaque child", []string{"r x /c/d", "o x/s", "f x/s/u"},
			[]Entry{fo("x/e", "c/d/e"), fo("x/f", "c/d/f"), f("x/s/u")}},
		{"redirected directory with a metacopy child", []string{"r x /c/d", "mx x/e"},
			[]Entry{fxo("x/e", "c/d/e"), fo("x/f", "c/d/f"), fo("x/s/t", "c/d/s/t")}},
		{"directory renamed onto a base directory", []string{"d c", "w c/d", "r b /c/d"},
			[]Entry{opq("b"), del("c/d"), fo("b/e", "c/d/e"), fo("b/f", "c/d/f"), fo("b/s/t", "c/d/s/t")}},
		{"redirect to an absent path", []string{"r x /nowhere", "f x/u"}, []Entry{f("x/u")}},
		{"redirect with no entries", []string{"r x /nowhere"}, []Entry{dir("x")}},
		{"redirect outside the tree", []string{"r x /../etc"}, []Entry{dir("x")}},
		{"reserved root names", []string{"l .jj", "d .git", "f .git/HEAD", "w .git2", "d sub", "d sub/.git", "f sub/.git/x"},
			[]Entry{f("sub/.git/x")}},
		{"moved directory keeps the executable bit of base leaves", []string{"w a", "r y /a"}, []Entry{del("a"), fo("y/1", "a/1"), fxo("y/2", "a/2")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Scan(t.Context(), upper(tc.upper...), "", base)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, sorted(tc.want)) && (len(got) != 0 || len(tc.want) != 0) {
				t.Fatalf("got  %+v\nwant %+v", got, sorted(tc.want))
			}
		})
	}
}
