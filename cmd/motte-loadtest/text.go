package main

import (
	"bytes"
	"math"
	"math/rand/v2"
)

type textGen struct{ words [][]byte }

func newTextGen(rng *rand.Rand) *textGen {
	t := &textGen{words: make([][]byte, 2000)}
	for i := range t.words {
		w := make([]byte, 2+rng.IntN(9))
		for j := range w {
			w[j] = byte('a' + rng.IntN(26))
		}
		t.words[i] = w
	}
	return t
}

func (t *textGen) line(rng *rand.Rand, out []byte) []byte {
	if rng.IntN(100) < 12 {
		return append(out, '\n')
	}
	for range min(rng.IntN(3)+rng.IntN(2), 4) {
		out = append(out, '\t')
	}
	for i := range 1 + rng.IntN(8) {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, t.words[int(math.Abs(rng.NormFloat64()*250))%len(t.words)]...)
	}
	return append(out, '\n')
}

func (t *textGen) file(rng *rand.Rand, lines int) []byte {
	out := make([]byte, 0, lines*32)
	for range lines {
		out = t.line(rng, out)
	}
	return out
}

func lognormal(rng *rand.Rand, median, sigma float64) float64 {
	return median * math.Exp(sigma*rng.NormFloat64())
}

func (t *textGen) mutate(rng *rand.Rand, data []byte, changed int) []byte {
	lines := bytes.SplitAfter(data, []byte("\n"))
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	ins := rng.IntN(changed + 1)
	del := min(changed-ins, len(lines))
	at := rng.IntN(len(lines) - del + 1)
	out := make([]byte, 0, len(data)+ins*32)
	for _, l := range lines[:at] {
		out = append(out, l...)
	}
	for range ins {
		out = t.line(rng, out)
	}
	for _, l := range lines[at+del:] {
		out = append(out, l...)
	}
	return out
}
