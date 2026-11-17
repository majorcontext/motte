package vfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
)

// ImportPack uploads one pack and its v2 index. The pack ID is the SHA-256 of the pack.
func (s *HTTPStore) ImportPack(ctx context.Context, idx, pack []byte) error {
	sum := sha256.Sum256(pack)
	req := api.ImportPackRequest{Index: base64.StdEncoding.EncodeToString(idx), Pack: base64.StdEncoding.EncodeToString(pack)}
	ctx, cl := s.begin(ctx, "vcs.client.pack.import", "/v1/vcs/packs/{pack_id}")
	cl.span.SetAttributes(attribute.Int64("pack_bytes", int64(len(pack))))
	err := s.do(ctx, cl, http.MethodPut, "/v1/vcs/packs/"+hex.EncodeToString(sum[:]), req, nil)
	cl.end(ctx, 0, err)
	return packTooLarge(err)
}

// ErrPackTooLarge is the store refusing a pack for its size, or for the work that verifying it takes.
var ErrPackTooLarge = errors.New("vfs: the store refuses the pack as too large")

func packTooLarge(err error) error {
	var se *statusError
	if !errors.As(err, &se) {
		return err
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(se.body, &body) != nil || body.Error.Code != tooLargeCode {
		return err
	}
	return fmt.Errorf("%w: %w", ErrPackTooLarge, err)
}

const (
	importRef    = "refs/motte/import"
	tooLargeCode = "vcs_object_too_large"
)

// Import fetches ref of the remote with the given history depth (zero is the whole history), packs the
// objects in one pack and uploads it with an import grant. It returns the commit that ref named.
func Import(ctx context.Context, st *HTTPStore, remote, ref string, depth int) (gitpack.SHA, error) {
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
