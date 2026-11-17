// Package fs backs the objstore seam with a directory.
package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/majorcontext/motte/internal/objstore"
)

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

// Root keeps objects under one directory. Use one process per root.
type Root struct{ root string }

func New(root string) (*Root, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, ".tmp"), 0o700); err != nil {
		return nil, err
	}
	return &Root{root: root}, nil
}

// Bucket opens one bucket of the directory.
func (r *Root) Bucket(name string) objstore.Store { return bucket{r, name} }

type bucket struct {
	r    *Root
	name string
}

func (b bucket) path(key string) (string, error) {
	if !bucketName.MatchString(b.name) {
		return "", fmt.Errorf("fs: invalid bucket %q", b.name)
	}
	if key == "" || len(key) > 1024 || strings.ContainsAny(key, "\x00\\") {
		return "", errors.New("fs: invalid object name")
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", errors.New("fs: invalid object name")
		}
	}
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(b.r.root, b.name, name[:2], name), nil
}

func (b bucket) Create(_ context.Context, key string, data []byte) (bool, error) {
	final, err := b.path(key)
	if err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Join(b.r.root, ".tmp"), "put-*")
	if err != nil {
		return false, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	if _, err := tmp.Write(data); err != nil {
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return false, err
	}
	if err := os.Link(tmp.Name(), final); err != nil {
		if errors.Is(err, iofs.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, b.syncDirs(final)
}

// syncDirs persists the directory entries from path up to the root, so a
// crash after a write cannot lose the object or a new directory.
func (b bucket) syncDirs(path string) error {
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = d.Sync()
		_ = d.Close()
		if err != nil {
			return err
		}
		if dir == b.r.root || filepath.Dir(dir) == dir {
			return nil
		}
	}
}

func (b bucket) Get(ctx context.Context, key string) ([]byte, error) {
	return b.GetRange(ctx, key, 0, -1)
}

func (b bucket) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	p, err := b.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, iofs.ErrNotExist) {
		return nil, objstore.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if off >= info.Size() {
		return nil, nil
	}
	if n < 0 || n > info.Size()-off {
		n = info.Size() - off
	}
	out := make([]byte, n)
	if _, err := f.ReadAt(out, off); err != nil {
		return nil, err
	}
	return out, nil
}
