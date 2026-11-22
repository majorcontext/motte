package server_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/majorcontext/motte/client"
)

func tenLines(edits map[int]string) string {
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		if e, ok := edits[i]; ok {
			b.WriteString(e + "\n")
		} else {
			fmt.Fprintf(&b, "line %d\n", i)
		}
	}
	return b.String()
}

func landMergesLinesThatTwoLandsChangeApart(t *testing.T, l *landEnv) {
	var o objSet
	with := func(f string) map[string]string { return map[string]string{"f": f, "g": "1"} }
	start := o.commit(with(tenLines(nil)), "", "start", l.root)
	tip := o.commit(with(tenLines(map[int]string{2: "tip"})), "", "tip", start)
	apart := o.commit(with(tenLines(map[int]string{8: "mine"})), "", "apart", start)
	same := o.commit(with(tenLines(map[int]string{2: "mine"})), "", "same", start)
	o.upload(t, l.env, l.repo)
	for _, c := range []string{start, tip} {
		if _, err := l.land(l.merge, "main", l.root, c); err != nil {
			t.Fatal(err)
		}
	}
	both := o.tree(with(tenLines(map[int]string{2: "tip", 8: "mine"})))
	if r, err := l.landExpecting(start, apart, both); err != nil || len(r.Commits) != 1 || l.treeOf(r.Tip) != both {
		t.Fatalf("land of other lines = %+v, %v, want the tree with both edits", r, err)
	}
	at := l.tip("main")
	_, err := l.land(l.merge, "main", start, same)
	refuses(t, err, client.ErrLandConflict, 409)
	if got := client.ConflictPaths(err); !slices.Equal(got, []string{"f"}) || l.tip("main") != at {
		t.Errorf("same lines: paths = %v, main = %+v, want [f] and %+v", got, l.tip("main"), at)
	}
}
