package midx

import (
	"crypto/sha256"
	"math/rand/v2"
	"reflect"
	"testing"
)

func randSHA(r *rand.Rand) (s SHA) {
	for i := range s {
		s[i] = byte(r.Uint32())
	}
	return s
}

func TestAGraphReturnsEachCommitItWasBuiltFromAndRefusesAChangedByte(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	var in []Commit
	for i := range 300 {
		c := Commit{SHA: randSHA(r), Tree: randSHA(r), Time: int64(i), Gen: uint32(i)}
		for range i % 5 {
			c.Parents = append(c.Parents, randSHA(r))
		}
		in = append(in, c)
	}
	b, err := BuildGraph(in)
	if err != nil {
		t.Fatal(err)
	}
	g, err := ParseGraph(b)
	if err != nil || g.Len() != len(in) {
		t.Fatalf("parse: %d commits, %v", g.Len(), err)
	}
	for _, c := range in {
		got, ok := g.Lookup(c.SHA)
		if !ok || !reflect.DeepEqual(got, c) {
			t.Fatalf("lookup of %x = %+v, %v; want %+v", c.SHA, got, ok, c)
		}
	}
	if _, ok := g.Lookup(randSHA(r)); ok {
		t.Error("lookup found a commit that the graph lacks")
	}
	b[len(b)/2] ^= 1
	if _, err := ParseGraph(b); err == nil {
		t.Error("a graph with a changed byte was accepted")
	}
}

func TestMergingGraphsKeepsEveryCommitAndTheLargerGeneration(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(3, 4))
	shared := Commit{SHA: randSHA(r), Tree: randSHA(r), Time: 5}
	a, b := shared, shared
	a.Gen, b.Gen = 0, 7
	only := Commit{SHA: randSHA(r), Tree: randSHA(r), Time: 6, Gen: 1}
	fa, _ := BuildGraph([]Commit{a, only})
	fb, _ := BuildGraph([]Commit{b})
	ga, _ := ParseGraph(fa)
	gb, _ := ParseGraph(fb)
	merged, err := MergeGraphs([]*Graph{ga, gb})
	if err != nil {
		t.Fatal(err)
	}
	g, err := ParseGraph(merged)
	if err != nil || g.Len() != 2 {
		t.Fatalf("merged graph: %v", err)
	}
	if c, _ := g.Lookup(shared.SHA); c.Gen != 7 {
		t.Errorf("a commit in two graphs has generation %d, want 7", c.Gen)
	}
}

func TestAGraphBeyondTheLimitsIsRefused(t *testing.T) {
	t.Parallel()
	many := make([]Commit, MaxGraphCommits+1)
	for i := range many {
		many[i].SHA = SHA{byte(i), byte(i >> 8), byte(i >> 16), 1}
	}
	wide := make([]Commit, MaxGraphOverflow/(20*(MaxGraphParents-1))+1)
	for i := range wide {
		wide[i] = Commit{SHA: SHA{byte(i), byte(i >> 8), 1}, Parents: make([]SHA, MaxGraphParents)}
	}
	for name, cs := range map[string][]Commit{
		"commits":  many,
		"overflow": wide,
		"parents":  {{SHA: SHA{1}, Parents: make([]SHA, MaxGraphParents+1)}},
	} {
		if _, err := BuildGraph(cs); err != ErrGraphTooLarge {
			t.Errorf("BuildGraph over the limit of %s = %v, want ErrGraphTooLarge", name, err)
		}
	}
}

func TestAGraphWithOverlappingOverflowRangesIsRefused(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(7, 8))
	mk := func(np int) Commit {
		c := Commit{SHA: randSHA(r)}
		for range np {
			c.Parents = append(c.Parents, randSHA(r))
		}
		return c
	}
	b, err := BuildGraph([]Commit{mk(4), mk(4)})
	if err != nil {
		t.Fatal(err)
	}
	g, err := ParseGraph(b)
	if err != nil || g.Len() != 2 {
		t.Fatalf("a graph with two octopus commits was refused: %v", err)
	}
	copy(b[graphHeader+graphRecord+76:], b[graphHeader+76:graphHeader+80])
	sum := sha256.Sum256(b[:len(b)-sha256.Size])
	copy(b[len(b)-sha256.Size:], sum[:])
	if _, err := ParseGraph(b); err == nil {
		t.Error("a graph whose two commits share one overflow range was accepted")
	}
}
