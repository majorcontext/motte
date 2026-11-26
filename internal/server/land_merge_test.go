package server_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
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

func TestLandTrialMatchesLandAndMovesNothing(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		ctx := context.Background()
		var o objSet
		c1 := o.commit(map[string]string{"a": "1"}, "", "c1", l.root)
		c2 := o.commit(map[string]string{"b": "1"}, "", "c2", l.root)
		c3 := o.commit(map[string]string{"a": "2"}, "", "c3", l.root)
		c5 := o.commit(map[string]string{"e": "1"}, "", "c5", l.root)
		o.upload(t, l.env, l.repo)
		op := l.Client(l.operator())
		before, _, _ := op.Events(ctx, "", 1000)
		at := l.tip("main")
		trial, err := l.merge.LandTrial(ctx, l.repo, api.TrialRequest{Tip: l.root, Changes: []api.TrialChange{{Head: c1}, {Head: c2}, {Head: c3}, {Head: c5}}})
		if err != nil || len(trial.Items) != 4 {
			t.Fatalf("trial = %+v, %v", trial, err)
		}
		if after, _, _ := op.Events(ctx, "", 1000); len(after) != len(before) || l.tip("main") != at {
			t.Errorf("trial moved a ref or wrote an event: %d events, main %+v, want %d and %+v", len(after), l.tip("main"), len(before), at)
		}
		first, second, third := trial.Items[0], trial.Items[1], trial.Items[2]
		if first.Base != l.root || first.Tip != c1 || second.Base != l.root || third.Conflict == nil || !slices.Equal(third.Conflict.Paths, []string{"a"}) || trial.Items[3].Tip == "" {
			t.Fatalf("items = %+v, want bases found, c1 first and a conflict on a", trial.Items)
		}
		if _, err := l.reader.Object(ctx, second.Tip); err != nil {
			t.Errorf("trial objects are not stored: %v", err)
		}
		r1, err1 := l.land(l.merge, "main", l.root, c1)
		r2, err2 := l.land(l.merge, "main", l.root, c2)
		_, err3 := l.land(l.merge, "main", l.root, c3)
		refuses(t, err3, client.ErrLandConflict, 409)
		if err1 != nil || err2 != nil || l.treeOf(r1.Tip) != first.Tree || l.treeOf(r2.Tip) != second.Tree {
			t.Errorf("land trees differ from the trial: %v %v %s %s, want %s %s", err1, err2, l.treeOf(r1.Tip), l.treeOf(r2.Tip), first.Tree, second.Tree)
		}
		c4 := o.commit(map[string]string{"a": "1", "b": "1", "d": "1"}, "", "c4", r2.Tip)
		o.upload(t, l.env, l.repo)
		if r, err := l.merge.Land(ctx, l.repo, api.LandRequest{Ref: "main", Onto: r2.Tip, Head: c4}); err != nil || !r.FastForward {
			t.Errorf("land with onto = %+v, %v, want a fast forward", r, err)
		}
		_, err = l.merge.Land(ctx, l.repo, api.LandRequest{Ref: "main", Onto: r2.Tip, Base: r2.Tip, Head: c4})
		refuses(t, err, client.ErrInvalidBody, 400)
	})
}

func TestLandIgnoresAPathThatTheTargetAddedAndRemoved(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		adds := o.commit(map[string]string{"x": "1"}, "", "add x", l.root)
		removes := o.commit(nil, "", "remove x", adds)
		mine := o.commit(map[string]string{"x": "1", "y": "2"}, "", "mine", l.root)
		o.upload(t, l.env, l.repo)
		for _, step := range [][2]string{{l.root, adds}, {adds, removes}} {
			if _, err := l.land(l.merge, "main", step[0], step[1]); err != nil {
				t.Fatal(err)
			}
		}
		tr, err := l.merge.LandTrial(context.Background(), l.repo, api.TrialRequest{Tip: l.tip("main").Commit, Changes: []api.TrialChange{{Head: mine, Base: l.root}}})
		if err != nil || len(tr.Items) != 1 || tr.Items[0].Conflict != nil || tr.Items[0].Tip == "" {
			t.Fatalf("trial of x = %+v, %v, want no conflict", tr, err)
		}
		if r, err := l.land(l.merge, "main", l.root, mine); err != nil || !slices.Equal(l.files(r.Tip), []string{"x", "y"}) {
			t.Fatalf("land of x = %+v, %v, want x and y", r, err)
		}
		start := l.tip("main").Commit
		asFile := o.commit(map[string]string{"x": "1", "y": "2", "d": "1"}, "", "d as a file", start)
		asDir := o.commit(map[string]string{"x": "1", "y": "2", "d/y": "1"}, "", "d as a directory", asFile)
		beside := o.commit(map[string]string{"x": "1", "y": "2", "d/z": "1"}, "", "beside", start)
		o.upload(t, l.env, l.repo)
		for _, step := range [][2]string{{start, asFile}, {asFile, asDir}} {
			if _, err := l.land(l.merge, "main", step[0], step[1]); err != nil {
				t.Fatal(err)
			}
		}
		if r, err := l.land(l.merge, "main", start, beside); err != nil || !slices.Contains(l.files(r.Tip), "d/") {
			t.Fatalf("land beside a directory = %+v, %v, want d/z added", r, err)
		}
	})
}
