package store

import (
	"testing"
	"unsafe"
)

func TestTheCopyPlanFitsTheBytesThatTheFetchReservesForEachObject(t *testing.T) {
	t.Parallel()
	if got := unsafe.Sizeof(copyPlace{}); got != 32 {
		t.Errorf("copyPlace takes %d bytes, the reservation of the pack write counts 32", got)
	}
	if got := unsafe.Sizeof(copyRun{}); got != 48 {
		t.Errorf("copyRun takes %d bytes, the reservation of the pack write counts 48", got)
	}
}
