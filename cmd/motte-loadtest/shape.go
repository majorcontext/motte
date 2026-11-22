package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type shape struct {
	paths [][]string
	start []int
	group []int
	sibs  [][]int
}

func newShape(paths [][]string, counts []int) *shape {
	s := &shape{paths: paths, start: make([]int, len(paths)+1), group: make([]int, len(paths))}
	ids := map[string]int{}
	for i, p := range paths {
		s.start[i+1] = s.start[i] + counts[i]
		key := strings.Join(p[:max(len(p)-1, 0)], "/")
		g, ok := ids[key]
		if !ok {
			g = len(s.sibs)
			ids[key] = g
			s.sibs = append(s.sibs, nil)
		}
		s.group[i] = g
		s.sibs[g] = append(s.sibs[g], i)
	}
	return s
}

func (s *shape) files() int { return s.start[len(s.paths)] }

func (s *shape) dirOf(file int) int {
	return sort.Search(len(s.paths), func(i int) bool { return s.start[i+1] > file })
}

func (s *shape) names(file int) []string {
	p := s.paths[s.dirOf(file)]
	return append(slices.Clone(p), fileName(file))
}

func gridShape(fanout, depth, leaf int) *shape {
	var paths [][]string
	var counts []int
	var walk func(p []string, level int)
	walk = func(p []string, level int) {
		if level == depth {
			paths, counts = append(paths, p), append(counts, leaf)
			return
		}
		for d := range fanout {
			walk(append(slices.Clone(p), dirName(d)), level+1)
		}
	}
	walk(nil, 0)
	return newShape(paths, counts)
}

type treeSpec struct {
	files, maxDepth int
	filesMean       float64
	filesSigma      float64
}

func treeShape(rng *rand.Rand, t treeSpec) *shape {
	mu := math.Log(t.filesMean) - t.filesSigma*t.filesSigma/2
	parent, depth, bag := []int{-1}, []int{0}, []int{0}
	var counts []int
	for total := 0; total < t.files; {
		p := bag[rng.IntN(len(bag))]
		if depth[p] >= t.maxDepth {
			continue
		}
		id := len(parent)
		parent, depth, bag = append(parent, p), append(depth, depth[p]+1), append(bag, p, p, id)
		n := min(max(int(math.Round(math.Exp(mu+t.filesSigma*rng.NormFloat64()))), 1), 600, t.files-total)
		counts = append(counts, n)
		total += n
	}
	paths := make([][]string, 0, len(parent)-1)
	for id := 1; id < len(parent); id++ {
		var p []string
		for at := id; at > 0; at = parent[at] {
			p = append(p, "d"+strconv.Itoa(at))
		}
		slices.Reverse(p)
		paths = append(paths, p)
	}
	return newShape(paths, counts)
}

func (s *shape) save(file string) error {
	var sb strings.Builder
	for i, p := range s.paths {
		fmt.Fprintf(&sb, "%d %s\n", s.start[i+1]-s.start[i], strings.Join(p, "/"))
	}
	return os.WriteFile(file, []byte(sb.String()), 0o644)
}

func loadShape(dir string, m Manifest) (*shape, error) {
	raw, err := os.ReadFile(dir + "/shape.txt")
	if os.IsNotExist(err) {
		return gridShape(m.Fanout, m.Depth, m.Leaf), nil
	}
	if err != nil {
		return nil, err
	}
	var paths [][]string
	var counts []int
	for _, l := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		n, p, ok := strings.Cut(l, " ")
		c, err := strconv.Atoi(n)
		if !ok || err != nil {
			return nil, fmt.Errorf("bad shape line %q", l)
		}
		var path []string
		if p != "" {
			path = strings.Split(p, "/")
		}
		paths, counts = append(paths, path), append(counts, c)
	}
	return newShape(paths, counts), nil
}
