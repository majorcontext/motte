package importer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/store"
)

const importRef = "refs/motte/import"

// Import fetches ref of the remote with the given history depth (zero is the whole history), packs the
// objects in one pack and uploads it with an import grant. It returns the commit that ref named.
func Import(ctx context.Context, st *store.HTTPStore, remote, ref string, depth int) (gitpack.SHA, error) {
	dir, err := os.MkdirTemp("", "motte-import-")
	if err != nil {
		return gitpack.SHA{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir, cmd.Stdout, cmd.Stderr = dir, &out, &out
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("vfs: git %s: %w: %s", args[0], err, strings.TrimSpace(out.String()))
		}
		return strings.TrimSpace(out.String()), nil
	}
	fetch := []string{"fetch", "--no-tags", "--quiet"}
	if depth > 0 {
		fetch = append(fetch, "--depth="+strconv.Itoa(depth))
	}
	for _, args := range [][]string{{"init", "--bare", "--quiet"}, append(fetch, remote, "+"+ref+":"+importRef), {"repack", "-a", "-d", "-q"}} {
		if _, err := run(args...); err != nil {
			return gitpack.SHA{}, err
		}
	}
	head, err := run("rev-parse", importRef+"^{commit}")
	if err != nil {
		return gitpack.SHA{}, err
	}
	commit, err := gitpack.ParseSHA(head)
	if err != nil {
		return commit, err
	}
	packs, err := filepath.Glob(filepath.Join(dir, "objects", "pack", "pack-*.pack"))
	if err != nil || len(packs) != 1 {
		return commit, fmt.Errorf("vfs: repack left %d packs: %v", len(packs), err)
	}
	pack, err := os.ReadFile(packs[0])
	if err != nil {
		return commit, err
	}
	idx, err := os.ReadFile(strings.TrimSuffix(packs[0], ".pack") + ".idx")
	if err != nil {
		return commit, err
	}
	return commit, st.ImportPack(ctx, idx, pack)
}
