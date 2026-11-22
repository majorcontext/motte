package main

import (
	"math"
	"math/rand/v2"
	"sort"
)

type zipf struct{ cdf []float64 }

func newZipf(n int, skew float64) zipf {
	cdf := make([]float64, n)
	sum := 0.0
	for i := range cdf {
		sum += 1 / math.Pow(float64(i+1), skew)
		cdf[i] = sum
	}
	for i := range cdf {
		cdf[i] /= sum
	}
	return zipf{cdf}
}

func (z zipf) draw(r *rand.Rand) int {
	return min(sort.SearchFloat64s(z.cdf, r.Float64()), len(z.cdf)-1)
}

type hotSet struct {
	s     *shape
	dirs  zipf
	files map[int]zipf
	mul   int
}

func newHotSet(s *shape, skew float64) hotSet {
	n := len(s.paths)
	mul := 2654435761 % max(n, 1)
	for mul < 2 || gcd(mul, n) != 1 {
		mul++
		if mul >= n {
			mul = 1
			break
		}
	}
	files := map[int]zipf{}
	for i := range n {
		if size := s.start[i+1] - s.start[i]; files[size].cdf == nil {
			files[size] = newZipf(size, skew)
		}
	}
	return hotSet{s: s, dirs: newZipf(n, skew), files: files, mul: mul}
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (h hotSet) dir(r *rand.Rand) int {
	return (h.dirs.draw(r)*h.mul + 7) % len(h.s.paths)
}

func (h hotSet) fileIn(r *rand.Rand, dir int) int {
	size := h.s.start[dir+1] - h.s.start[dir]
	return h.s.start[dir] + h.files[size].draw(r)
}

func (h hotSet) file(r *rand.Rand) int { return h.fileIn(r, h.dir(r)) }

func (h hotSet) cluster(r *rand.Rand, size int) []int {
	anchor := h.dir(r)
	out := []int{anchor}
	for len(out) < size {
		if r.IntN(2) == 0 {
			sib := h.s.sibs[h.s.group[anchor]]
			out = append(out, sib[r.IntN(len(sib))])
		} else {
			out = append(out, h.dir(r))
		}
	}
	return out
}
