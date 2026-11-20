package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/majorcontext/motte/internal/testcount"
	"github.com/majorcontext/motte/testserver"
)

func TestMain(m *testing.M) { os.Exit(testcount.Run(m)) }

func TestRepoCreatePrintsThePathOfANewRepository(t *testing.T) {
	t.Parallel()
	ts := testserver.StartLocal(t)
	env := map[string]string{"MOTTE_URL": ts.URL, "MOTTE_TOKEN": ts.OperatorGrant(t)}
	run := func(args ...string) (int, string) {
		var out, errOut bytes.Buffer
		code := runRepo(t.Context(), args, func(k string) string { return env[k] }, &out, &errOut)
		return code, out.String() + errOut.String()
	}
	if code, out := run("create", "--owner", "o1", "widgets"); code != 0 || !strings.HasPrefix(out, "repo_") {
		t.Errorf("create: exit %d, %q, want 0 and a repo path", code, out)
	}
	env["MOTTE_TOKEN"] = ts.ReadGrant(t, "native:x")
	if code, _ := run("create", "widgets"); code != 1 {
		t.Errorf("create with a read grant: exit %d, want 1", code)
	}
	if code, _ := run("create"); code != 2 {
		t.Errorf("create with no name: exit %d, want 2", code)
	}
}
