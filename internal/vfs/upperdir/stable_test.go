package upperdir

import (
	"fmt"
	"testing"
	"time"
)

// mutating changes the tree during the walk of Scan: at the first Xattr call, or at every one.
type mutating struct {
	*MemFS
	every bool
	calls int
}

type mutDir struct {
	Dir
	m *mutating
}

func (m *mutating) Open(p string) (Dir, error) {
	d, err := m.MemFS.Open(p)
	return mutDir{d, m}, err
}

func (d mutDir) Xattr(name, attr string) (string, bool, error) {
	if d.m.every || d.m.calls == 0 {
		d.m.Put(fmt.Sprint("g", d.m.calls), MemNode{Mode: 0o644, Size: 1, Ctime: 1, Ino: uint64(d.m.calls + 10)})
	}
	d.m.calls++
	return d.Dir.Xattr(name, attr)
}

// TestScanStable requires that an edit during the first walk makes a second walk whose entries include it,
// that an edit during every walk ends with an unsettled token, and that an unsettled first token costs one scan.
func TestScanStable(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	m := &mutating{MemFS: upper("f x")}
	es, tok, err := ScanStable(t.Context(), m, "", newBase(), now)
	if err != nil || len(es) != 2 || !tok.Settled {
		t.Fatalf("one edit: %+v %+v %v", es, tok, err)
	}
	if want, _ := Fingerprint(m, "", now()); !tok.Same(want) {
		t.Fatalf("token %+v is not the token of the final state %+v", tok, want)
	}
	m = &mutating{MemFS: upper("f x"), every: true}
	if es, tok, err = ScanStable(t.Context(), m, "", newBase(), now); err != nil || len(es) == 0 || tok.Settled || tok.Same(tok) {
		t.Fatalf("edits in every walk: %+v %+v %v", es, tok, err)
	}
	m = &mutating{MemFS: upper("f x")}
	m.Put("x", MemNode{Mode: 0o644, Ctime: time.Unix(1000, 0).UnixNano()})
	if _, tok, err = ScanStable(t.Context(), m, "", newBase(), now); err != nil || tok.Settled || m.calls != 1 {
		t.Fatalf("unsettled first token: %+v, %d scans, %v", tok, m.calls, err)
	}
}
