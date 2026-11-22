package server

import "testing"

// LowerLimits lowers the size limits of one test, so that it crosses the same boundaries with small bodies. It
// restores the limits when the test ends. A zero argument keeps the limit.
func LowerLimits(t testing.TB, object int, window, treeFirst, treeFixed int64) {
	t.Helper()
	prevObject, prevWindow, prevFirst, prevFixed := objectLimit, decodeWindow, treeReserve, treeSlack
	t.Cleanup(func() {
		objectLimit, decodeWindow, treeReserve, treeSlack = prevObject, prevWindow, prevFirst, prevFixed
	})
	if object > 0 {
		objectLimit = object
	}
	if window > 0 {
		decodeWindow = window
	}
	if treeFirst > 0 {
		treeReserve = treeFirst
	}
	if treeFixed > 0 {
		treeSlack = treeFixed
	}
}

// HoldBudget takes n bytes of the memory budget of s until the test ends.
func HoldBudget(t testing.TB, s *Server, n int64) {
	t.Helper()
	release, err := s.objects.Reserve(t.Context(), n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
}
