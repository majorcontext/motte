// Package buckettest is the conformance suite of bucket.Store. A backend
// passes it before a server uses the backend.
package buckettest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/majorcontext/motte/internal/bucket"
)

// Caps names the guarantees that a backend gives beyond the base contract.
type Caps struct {
	// ConditionalDelete: Delete of a stale version leaves an object that was
	// deleted and stored again, and returns bucket.ErrChanged. bucket/fs and
	// bucket/gcs have it. bucket/s3 does not: an ETag is the same for the
	// same bytes, so its collector relies on the mark lease alone.
	ConditionalDelete bool
}

// Run runs the suite. newBackend returns an empty bucket for one test. A
// backend that cannot be emptied may return the same bucket: every test
// names its objects with a random prefix.
func Run(t *testing.T, newBackend func(t *testing.T) bucket.Store, caps Caps) {
	for name, fn := range map[string]func(*testing.T, bucket.Store, func(string) string){
		"create stores an object and never replaces it": createNeverReplaces,
		"reads of a range":           rangeReads,
		"a missing key is not found": missingKey,
		"one racer of many creates":  oneCreatorWins,
		"a large object round trips": largeObject,
		"delete removes an object":   deleteRemoves,
		"a stale version of a stored again object": func(t *testing.T, b bucket.Store, key func(string) string) {
			staleVersion(t, b, key, caps)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var raw [8]byte
			_, _ = rand.Read(raw[:])
			prefix := "buckettest/" + hex.EncodeToString(raw[:]) + "/"
			fn(t, newBackend(t), func(k string) string { return prefix + k })
		})
	}
}

func createNeverReplaces(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	k := key("repos/native/Zm9v/packs/a.pack")
	if ok, err := b.Create(ctx, k, []byte("first")); err != nil || !ok {
		t.Fatalf("create = %v, %v; want true", ok, err)
	}
	if ok, err := b.Create(ctx, k, []byte("second")); err != nil || ok {
		t.Fatalf("second create = %v, %v; want false and no error", ok, err)
	}
	if got, err := b.Get(ctx, k); err != nil || string(got) != "first" {
		t.Errorf("get = %q, %v; want the first data", got, err)
	}
	if ok, err := b.Create(ctx, key("empty"), nil); err != nil || !ok {
		t.Fatalf("create empty = %v, %v", ok, err)
	}
	if got, err := b.Get(ctx, key("empty")); err != nil || len(got) != 0 {
		t.Errorf("get empty = %q, %v", got, err)
	}
}

func rangeReads(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	k := key("a/b")
	if _, err := b.Create(ctx, k, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		off, n int64
		want   string
	}{
		{1, 3, "ell"}, {0, 5, "hello"}, {1, -1, "ello"}, {0, -1, "hello"}, {3, 100, "lo"}, {5, 1, ""}, {9, 3, ""},
	} {
		got, err := b.GetRange(ctx, k, c.off, c.n)
		if err != nil || !bytes.Equal(got, []byte(c.want)) {
			t.Errorf("range(%d, %d) = %q, %v; want %q", c.off, c.n, got, err, c.want)
		}
	}
}

func missingKey(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	if _, err := b.Get(ctx, key("missing")); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("get = %v, want bucket.ErrNotFound", err)
	}
	if _, err := b.GetRange(ctx, key("missing"), 0, 4); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("range = %v, want bucket.ErrNotFound", err)
	}
}

func oneCreatorWins(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	const racers = 16
	k := key("race")
	var wg sync.WaitGroup
	wins := make([]bool, racers)
	errs := make([]error, racers)
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wins[i], errs[i] = b.Create(ctx, k, fmt.Appendf(nil, "racer-%d", i))
		}()
	}
	wg.Wait()
	winner := -1
	for i := range racers {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if wins[i] {
			if winner >= 0 {
				t.Fatalf("racers %d and %d both created the key", winner, i)
			}
			winner = i
		}
	}
	if winner < 0 {
		t.Fatal("no racer created the key")
	}
	if got, err := b.Get(ctx, k); err != nil || string(got) != fmt.Sprintf("racer-%d", winner) {
		t.Errorf("get = %q, %v; want the data of racer %d", got, err, winner)
	}
}

func largeObject(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	data := make([]byte, 3<<20+17)
	_, _ = rand.Read(data)
	if ok, err := b.Create(ctx, key("big"), data); err != nil || !ok {
		t.Fatalf("create = %v, %v", ok, err)
	}
	if got, err := b.Get(ctx, key("big")); err != nil || !bytes.Equal(got, data) {
		t.Errorf("get differs: %d bytes, %v", len(got), err)
	}
	if got, err := b.GetRange(ctx, key("big"), 1<<20, 4096); err != nil || !bytes.Equal(got, data[1<<20:1<<20+4096]) {
		t.Errorf("range differs: %d bytes, %v", len(got), err)
	}
}

func deleteRemoves(t *testing.T, b bucket.Store, key func(string) string) {
	ctx := context.Background()
	k := key("gone")
	if err := b.Delete(ctx, k, "1"); err != nil {
		t.Fatalf("delete of a missing key = %v, want nil", err)
	}
	if _, err := b.Version(ctx, k); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("version of a missing key = %v, want bucket.ErrNotFound", err)
	}
	if _, err := b.Create(ctx, k, []byte("data")); err != nil {
		t.Fatal(err)
	}
	v, err := b.Version(ctx, k)
	if err != nil || v == "" {
		t.Fatalf("version = %q, %v; want a name", v, err)
	}
	if again, err := b.Version(ctx, k); err != nil || again != v {
		t.Fatalf("version again = %q, %v; want %q", again, err, v)
	}
	if err := b.Delete(ctx, k, v); err != nil {
		t.Fatalf("delete = %v", err)
	}
	if _, err := b.Get(ctx, k); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("get after delete = %v, want bucket.ErrNotFound", err)
	}
	if ok, err := b.Create(ctx, k, []byte("again")); err != nil || !ok {
		t.Errorf("create after delete = %v, %v; want true", ok, err)
	}
}

func staleVersion(t *testing.T, b bucket.Store, key func(string) string, caps Caps) {
	if got := bucket.DeletesConditionally(b); got != caps.ConditionalDelete {
		t.Errorf("DeletesConditionally = %v, want %v", got, caps.ConditionalDelete)
	}
	if !caps.ConditionalDelete {
		return
	}
	ctx := context.Background()
	k := key("stale")
	if _, err := b.Create(ctx, k, []byte("same bytes")); err != nil {
		t.Fatal(err)
	}
	old, err := b.Version(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, k, old); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.Create(ctx, k, []byte("same bytes")); err != nil || !ok {
		t.Fatalf("create after delete = %v, %v; want true", ok, err)
	}
	if err := b.Delete(ctx, k, old); !errors.Is(err, bucket.ErrChanged) {
		t.Fatalf("delete of a stale version = %v, want bucket.ErrChanged", err)
	}
	if got, err := b.Get(ctx, k); err != nil || string(got) != "same bytes" {
		t.Fatalf("get after a refused delete = %q, %v; want the object", got, err)
	}
	cur, err := b.Version(ctx, k)
	if err != nil || cur == old {
		t.Fatalf("version of the stored again object = %q, %v; want a name other than %q", cur, err, old)
	}
	if err := b.Delete(ctx, k, cur); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, k); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("get after delete = %v, want bucket.ErrNotFound", err)
	}
}
