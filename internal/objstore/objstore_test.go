package objstore

import (
	"context"
	"math"
	"testing"
)

func TestMemoryRangeToEnd(t *testing.T) {
	m := NewMemory()
	_, _ = m.Create(context.Background(), "k", []byte("abc"))
	if got, _ := m.GetRange(context.Background(), "k", 1, -1); string(got) != "bc" {
		t.Errorf("got %q", got)
	}
	if got, _ := m.GetRange(context.Background(), "k", 1, math.MaxInt64); string(got) != "bc" {
		t.Errorf("huge length: got %q", got)
	}
}
