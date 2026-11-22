package linemerge

import (
	"strings"
	"testing"
)

func text(edits map[int]string) string {
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		if e, ok := edits[i]; ok {
			b.WriteString(e)
		} else {
			b.WriteString("line\n")
		}
	}
	return b.String()
}

func TestMergeByLines(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x\n", MaxLines)
	cases := []struct {
		name               string
		base, ours, theirs string
		want               string
		ok                 bool
	}{
		{"apart", text(nil), text(map[int]string{2: "a\n"}), text(map[int]string{8: "b\n"}), text(map[int]string{2: "a\n", 8: "b\n"}), true},
		{"insert and delete", text(nil), text(map[int]string{2: "a\na\n"}), text(map[int]string{8: ""}), "line\na\na\nline\nline\nline\nline\nline\nline\nline\n", true},
		{"same change", text(nil), text(map[int]string{5: "a\n"}), text(map[int]string{5: "a\n"}), text(map[int]string{5: "a\n"}), true},
		{"same line", text(nil), text(map[int]string{5: "a\n"}), text(map[int]string{5: "b\n"}), "", false},
		{"touching lines", text(nil), text(map[int]string{5: "a\n"}), text(map[int]string{6: "b\n"}), "", false},
		{"large insertion", text(nil), text(map[int]string{2: strings.Repeat("a\n", maxEdits+1) + "line\n"}), text(map[int]string{8: "b\n"}), "", false},
		{"same change of a long file", long, long + "a\n", long + "a\n", "", false},
		{"binary", text(nil), text(map[int]string{2: "a\x00\n"}), text(map[int]string{8: "b\n"}), "", false},
	}
	for _, c := range cases {
		got, err := Merge([]byte(c.base), []byte(c.ours), []byte(c.theirs))
		again, _ := Merge([]byte(c.base), []byte(c.ours), []byte(c.theirs))
		if (err == nil) != c.ok || string(got) != c.want || string(again) != string(got) {
			t.Errorf("%s: Merge = %q, %v, want %q, %v", c.name, got, err, c.want, c.ok)
		}
	}
}
