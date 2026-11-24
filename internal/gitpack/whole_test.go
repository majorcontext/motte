package gitpack_test

import (
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

type bytesAt []byte

func (b bytesAt) ReadAt(p []byte, off int64) (int, error) { return copy(p, b[off:]), nil }
func (b bytesAt) Size() int64                             { return int64(len(b)) }

func TestWholeTypeNamesAnObjectStoredWholeAndNotOneStoredAsADelta(t *testing.T) {
	t.Parallel()
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Commit, Data: []byte("tree x\n\nm\n")}})
	p, err := gitpack.NewPack(idx, bytesAt(pack))
	if err != nil {
		t.Fatal(err)
	}
	if ty, n, ok := p.WholeType(names[0]); !ok || ty != gitpack.Commit || n != 10 {
		t.Errorf("WholeType = %v, %d, %v; want a whole commit of 10 bytes", ty, n, ok)
	}
	if _, _, ok := p.WholeType(gitpack.SHA{9}); ok {
		t.Error("WholeType found an object that the pack lacks")
	}
}
