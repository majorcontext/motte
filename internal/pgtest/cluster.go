package pgtest

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/majorcontext/motte/internal/testcount"
)

// BinEnv names a directory that holds initdb and postgres. Without it, Pool looks for both on PATH.
const BinEnv = "MOTTE_TEST_PG_BIN"

// RequireEnv set to 1 makes Pool fail, not skip, when no database is available.
const RequireEnv = "MOTTE_TEST_REQUIRE_PG"

var (
	clusterOnce sync.Once
	clusterDSN  string
	clusterErr  error
)

// binary returns the path of a PostgreSQL server binary, or "" when there is none.
func binary(name string) string {
	if dir := os.Getenv(BinEnv); dir != "" {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err != nil {
			return ""
		}
		return p
	}
	p, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return p
}

// startCluster starts the throwaway cluster of the package on first use. It returns "" and a nil error when the
// binaries are missing.
func startCluster(t testing.TB) (string, error) {
	t.Helper()
	initdb, postgres := binary("initdb"), binary("postgres")
	if initdb == "" || postgres == "" {
		return "", nil
	}
	clusterOnce.Do(func() {
		testcount.Add(t, testcount.Processes)
		clusterDSN, clusterErr = launch(initdb, postgres)
	})
	return clusterDSN, clusterErr
}

func launch(initdb, postgres string) (string, error) {
	dir, err := os.MkdirTemp("", "motte-pg")
	if err != nil {
		return "", err
	}
	var cmd *exec.Cmd
	stop := func() {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGQUIT)
			_ = cmd.Wait()
		}
		_ = os.RemoveAll(dir)
	}
	if !testcount.AtExit(stop) {
		_ = os.RemoveAll(dir)
		return "", errors.New("the package does not call testcount.Run from TestMain, so it cannot stop the PostgreSQL cluster")
	}
	data := filepath.Join(dir, "data")
	env := append(os.Environ(), "LC_ALL=C")
	init := exec.Command(initdb, "-D", data, "-E", "UTF8", "-U", "postgres", "-A", "trust", "--no-sync")
	init.Env = env
	if out, err := init.CombinedOutput(); err != nil {
		return "", fmt.Errorf("initdb: %w\n%s", err, out)
	}
	cmd = exec.Command(postgres, "-D", data, "-k", dir,
		"-c", "listen_addresses=", "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
		"-c", "max_connections=300")
	cmd.Env = env
	pipe, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(pipe)
		var seen []string
		for sc.Scan() {
			seen = append(seen, sc.Text())
			if strings.Contains(sc.Text(), "ready to accept connections") {
				ready <- nil
				for sc.Scan() {
				}
				return
			}
		}
		ready <- fmt.Errorf("postgres exited before it was ready:\n%s", strings.Join(seen, "\n"))
	}()
	if err := <-ready; err != nil {
		return "", err
	}
	u := url.URL{Scheme: "postgres", User: url.User("postgres"), Path: "/postgres", RawQuery: url.Values{"host": {dir}}.Encode()}
	return u.String(), nil
}

func missingDatabase(t testing.TB) {
	t.Helper()
	msg := DSNEnv + " is not set and no PostgreSQL binaries were found: the test needs a PostgreSQL database"
	if os.Getenv(RequireEnv) == "1" {
		t.Fatal(msg)
	}
	t.Skip(msg)
}
