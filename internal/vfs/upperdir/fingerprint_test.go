package upperdir

import (
	"io/fs"
	"testing"
	"time"
)

// TestFingerprintSettleRule pins the token rules: a change moves the token, equal settled walks agree, and a
// walk that starts within the settle window of the newest change never agrees with any token.
func TestFingerprintSettleRule(t *testing.T) {
	t.Parallel()
	at := time.Unix(1000, 0)
	put := func(u *MemFS, p string, size int64, age time.Duration) {
		u.Put(p, MemNode{Mode: 0o644, Size: size, Ctime: at.Add(-age).UnixNano()})
	}
	u := NewMemFS()
	put(u, "a", 10, time.Second)
	put(u, "b", 5, time.Second)
	u.Put("d", MemNode{Mode: fs.ModeDir | 0o755, Ctime: at.Add(-time.Second).UnixNano()})
	put(u, ".jj", 99, 0)
	first, err := Fingerprint(u, "", at)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := Fingerprint(u, "", at.Add(time.Minute))
	if !first.Settled || !first.Same(again) || first.String("boot") != again.String("boot") {
		t.Fatalf("equal settled walks differ: %+v %+v", first, again)
	}
	if first.Entries != 3 || first.Bytes != 15 {
		t.Fatalf("reserved root name counted or entries missed: %+v", first)
	}
	for name, edit := range map[string]func(){
		"grown":   func() { put(u, "a", 11, time.Second/2) },
		"added":   func() { put(u, "d/c", 0, time.Second/4) },
		"removed": func() { u.Delete("b") },
		"swapped": func() { n, _ := u.Get("a"); n.Ino++; u.Put("a", n) },
	} {
		edit()
		next, _ := Fingerprint(u, "", at.Add(time.Minute))
		if next.Same(first) || next.String("boot") == first.String("boot") {
			t.Fatalf("%s: token did not move", name)
		}
		first = next
	}
	put(u, "late", 1, SettleWindow/2)
	one, _ := Fingerprint(u, "", at)
	two, _ := Fingerprint(u, "", at)
	if one.Settled || one.Same(one) || one.Same(two) || one.String("b") == two.String("b") {
		t.Fatalf("unsettled tokens must never agree: %+v %+v", one, two)
	}
}
