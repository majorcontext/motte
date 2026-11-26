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

// LowerFetchSlots sets the reservation above which a fetch takes a slot, and the number of slots, until the test ends.
func LowerFetchSlots(t testing.TB, large int64, slots int) {
	t.Helper()
	prevLarge, prevSlots := fetchLarge, fetchSlots
	t.Cleanup(func() { fetchLarge, fetchSlots = prevLarge, prevSlots })
	fetchLarge, fetchSlots = large, slots
}
