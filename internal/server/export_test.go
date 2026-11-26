package server

import "testing"

// HoldBudget takes n bytes of the memory budget of s until the test ends.
func HoldBudget(t testing.TB, s *Server, n int64) {
	t.Helper()
	release, err := s.objects.Reserve(t.Context(), n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
}
