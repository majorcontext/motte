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
	l     layout
	dirs  zipf
	files zipf
	mul   int
}

func newHotSet(l layout, skew float64) hotSet {
	n := l.files() / l.leaf
	mul := 2654435761 % max(n, 1)
	for mul < 2 || gcd(mul, n) != 1 {
		mul++
		if mul >= n {
			mul = 1
			break
		}
	}
	return hotSet{l: l, dirs: newZipf(n, skew), files: newZipf(l.leaf, skew), mul: mul}
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (h hotSet) dir(r *rand.Rand) int {
	n := h.l.files() / h.l.leaf
	return (h.dirs.draw(r)*h.mul + 7) % n
}

func (h hotSet) fileIn(r *rand.Rand, dir int) int { return dir*h.l.leaf + h.files.draw(r) }

func (h hotSet) file(r *rand.Rand) int { return h.fileIn(r, h.dir(r)) }

func (h hotSet) cluster(r *rand.Rand, size int) []int {
	anchor := h.dir(r)
	out := []int{anchor}
	for len(out) < size {
		if r.IntN(2) == 0 {
			out = append(out, (anchor-anchor%h.l.fanout+r.IntN(h.l.fanout))%(h.l.files()/h.l.leaf))
		} else {
			out = append(out, h.dir(r))
		}
	}
	return out
}
