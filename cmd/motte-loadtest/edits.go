package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
)

type fileEdit struct {
	file     int
	pos      float64
	del, ins int
	seed     uint64
}

func (a *agent) planEdits(files []int) []fileEdit {
	out := make([]fileEdit, len(files))
	for i, f := range files {
		n := 1 + a.rng.IntN(max(1, a.e.f.editLines))
		fe := fileEdit{file: f, pos: a.rng.Float64(), del: n, seed: a.rng.Uint64()}
		if a.rng.IntN(4) == 0 {
			fe.del, fe.ins = 0, n
		}
		out[i] = fe
	}
	return out
}

func (fe fileEdit) apply(data []byte, tag string) []byte {
	lines := bytes.SplitAfter(data, []byte("\n"))
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	limit := max(1, len(lines)/4)
	del, ins := min(fe.del, limit), min(fe.ins, limit)
	if fe.del > 0 {
		ins = del
	}
	del = min(del, len(lines))
	at := min(int(fe.pos*float64(len(lines)-del+1)), len(lines)-del)
	rng := rand.New(rand.NewPCG(fe.seed, 1))
	var out bytes.Buffer
	for _, l := range lines[:at] {
		out.Write(l)
	}
	for i := range ins {
		fmt.Fprintf(&out, "// %s line %d %s\n", tag, i, strings.Repeat("z", 20+rng.IntN(60)))
	}
	for _, l := range lines[at+del:] {
		out.Write(l)
	}
	return out.Bytes()
}

func (a *agent) blobSHA(ctx context.Context, root string, file int) (string, error) {
	cur := root
	for _, n := range fileNames(a.e.layout(), file) {
		ents, err := a.entries(ctx, cur)
		if err != nil {
			return "", err
		}
		var ok bool
		if cur, ok = lookup(ents, n); !ok {
			return "", fmt.Errorf("no entry %s", n)
		}
	}
	return cur, nil
}

func (a *agent) load(ctx context.Context, root string, work map[int][]byte, files []fileEdit) error {
	var shas []string
	var need []int
	for _, fe := range files {
		if _, ok := work[fe.file]; ok {
			continue
		}
		sha, err := a.blobSHA(ctx, root, fe.file)
		if err != nil {
			return err
		}
		if data, ok := a.content[sha]; ok {
			work[fe.file] = data
			continue
		}
		shas, need = append(shas, sha), append(need, fe.file)
	}
	if len(shas) == 0 {
		return nil
	}
	var blobs [][]byte
	err := a.do(ctx, "blob", func(c context.Context) (err error) { blobs, err = a.e.read.Blobs(c, shas); return })
	for i, b := range blobs {
		a.content[shas[i]], work[need[i]] = b, b
	}
	return err
}
