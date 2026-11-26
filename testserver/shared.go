package testserver

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/majorcontext/tools/testcount"
)

var (
	once   sync.Once
	shared *Server
	holder *mainTB
	root   string
)

// Shared runs the tests of m with one server for the whole package, removes its state after the tests, and returns
// the exit code. Call it from TestMain as os.Exit(testserver.Shared(m)). It calls testcount.Run, so it replaces that
// call. Package returns the server, which starts on SQLite at its first call. Tests that share it must use a
// repository of their own, and read no events or subscriptions of other tests.
func Shared(m *testing.M) int {
	dir, err := os.MkdirTemp("", "testserver")
	if err != nil {
		fmt.Fprintln(os.Stderr, "testserver:", err)
		return 1
	}
	root = dir
	code := testcount.Run(m)
	if holder != nil {
		holder.cleanup()
	}
	_ = os.RemoveAll(dir)
	return code
}

// Package returns the server that Shared serves. It fails the test when the package did not call Shared from TestMain.
func Package(t testing.TB) *Server {
	t.Helper()
	if root == "" {
		t.Fatal("testserver: the package did not call Shared from TestMain")
	}
	once.Do(func() {
		holder = &mainTB{TB: t, dir: root}
		shared = Start(holder)
	})
	return shared
}

// mainTB lets Start run before any test exists: it has no test to hold the cleanups, so Shared runs them.
type mainTB struct {
	testing.TB
	dir string
	mu  sync.Mutex
	fns []func()
}

func (*mainTB) Helper() {}

func (b *mainTB) TempDir() string {
	d, err := os.MkdirTemp(b.dir, "s")
	if err != nil {
		panic(err)
	}
	return d
}

func (b *mainTB) Cleanup(f func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fns = append(b.fns, f)
}

func (b *mainTB) cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := len(b.fns) - 1; i >= 0; i-- {
		b.fns[i]()
	}
}

func (*mainTB) Fatal(args ...any) { panic(fmt.Sprint(args...)) }

func (*mainTB) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }
