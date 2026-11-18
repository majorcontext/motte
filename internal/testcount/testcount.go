// Package testcount counts the expensive operations that test helpers start and prints the counts of a package at
// the end of its test run, for internal/testtime to check against testdata/test-time-budget.txt.
package testcount

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// The names of the counts.
const (
	Servers   = "servers"
	Schemas   = "schemas"
	Processes = "processes"
)

// Prefix starts the one output line that carries the counts of a package.
const Prefix = "motte-testcount "

const module = "github.com/majorcontext/motte"

var (
	mu     sync.Mutex
	counts = map[string]int{Servers: 0, Schemas: 0, Processes: 0}
	hooked atomic.Bool
)

// Add counts one operation. In a package of this module that does not call Run from its TestMain, it fails the test,
// because the count would be lost.
func Add(t testing.TB, name string) {
	t.Helper()
	if !hooked.Load() && inModule() {
		t.Fatalf("the package does not call testcount.Run from TestMain, so it cannot count %s", name)
	}
	mu.Lock()
	counts[name]++
	mu.Unlock()
}

// Command counts a started process and returns exec.Command(name, args...).
func Command(t testing.TB, name string, args ...string) *exec.Cmd {
	t.Helper()
	Add(t, Processes)
	return exec.Command(name, args...)
}

// Run runs the tests of m, prints the count line and returns the exit code. Use it as os.Exit(testcount.Run(m)).
func Run(m *testing.M) int {
	hooked.Store(true)
	code := m.Run()
	runs := 1
	if f := flag.Lookup("test.count"); f != nil {
		_, _ = fmt.Sscan(f.Value.String(), &runs)
	}
	mu.Lock()
	line := Line(counts, runs)
	mu.Unlock()
	fmt.Print("\n" + line)
	return code
}

// Line renders the count line for the counts of runs runs of the tests. Each count is divided by runs, rounded up.
func Line(counts map[string]int, runs int) string {
	runs = max(runs, 1)
	one := make(map[string]int, len(counts))
	for k, v := range counts {
		one[k] = (v + runs - 1) / runs
	}
	data, _ := json.Marshal(one)
	return Prefix + string(data) + "\n"
}

// Parse returns the counts of a test output that holds a count line. The bool is false when it holds none.
func Parse(output string) (map[string]int, bool) {
	i := strings.Index(output, Prefix)
	if i < 0 {
		return nil, false
	}
	rest, _, _ := strings.Cut(output[i+len(Prefix):], "\n")
	var out map[string]int
	if err := json.Unmarshal([]byte(rest), &out); err != nil {
		return nil, false
	}
	return out, true
}

func inModule() bool {
	info, ok := debug.ReadBuildInfo()
	return ok && info.Main.Path == module && os.Getenv("MOTTE_TESTCOUNT_OFF") == ""
}
