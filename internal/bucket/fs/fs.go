// Package fs backs the bucket seam with a directory.
package fs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/majorcontext/motte/internal/bucket"
)

// headerSize is the fixed block that precedes the bytes of an object file: a 4-byte big-endian length and the JSON
// attributes, zero padded. The format is on disk and does not change.
const headerSize = 4096

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

type attrs struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	MD5         string `json:"md5"`
	Generation  int64  `json:"generation"`
}

// Root keeps objects under one directory. Use one process per root.
type Root struct {
	root string
	mu   sync.Mutex
	gen  atomic.Int64
}

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
func (r *Root) nextGeneration() int64 {
	for {
		last := r.gen.Load()
		if n := max(time.Now().UnixNano(), last+1); r.gen.CompareAndSwap(last, n) {
			return n
		}
	}
}

func (r *Root) Bucket(name string) bucket.Store { return dirBucket{r, name} }

type dirBucket struct {
	r    *Root
	name string
}

func (b dirBucket) path(key string) (string, error) {
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

func (b dirBucket) Create(_ context.Context, key string, data []byte) (bool, error) {
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
	sum := md5.Sum(data)
	raw, err := json.Marshal(attrs{Name: key, Size: int64(len(data)), ContentType: "application/octet-stream", MD5: hex.EncodeToString(sum[:]), Generation: b.r.nextGeneration()})
	if err != nil || len(raw) > headerSize-4 {
		return false, errors.New("fs: object attributes are too large")
	}
	var block [headerSize]byte
	binary.BigEndian.PutUint32(block[:4], uint32(len(raw)))
	copy(block[4:], raw)
	if _, err := tmp.Write(block[:]); err != nil {
		return false, err
	}
	if _, err := tmp.Write(data); err != nil {
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return false, err
	}
	b.r.mu.Lock()
	err = os.Link(tmp.Name(), final)
	b.r.mu.Unlock()
	if err != nil {
		if errors.Is(err, iofs.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, b.syncDirs(final)
}

// syncDirs persists the directory entries from path up to the root, so a
// crash after a write cannot lose the object or a new directory.
func (b dirBucket) syncDirs(path string) error {
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

// readAttrs opens the object file and reads its header block.
func readAttrs(p string) (*os.File, attrs, error) {
	f, err := os.Open(p)
	if errors.Is(err, iofs.ErrNotExist) {
		return nil, attrs{}, bucket.ErrNotFound
	}
	if err != nil {
		return nil, attrs{}, err
	}
	var block [headerSize]byte
	if _, err := io.ReadFull(f, block[:]); err != nil {
		_ = f.Close()
		return nil, attrs{}, fmt.Errorf("fs: read header: %w", err)
	}
	hl := binary.BigEndian.Uint32(block[:4])
	var a attrs
	if hl == 0 || hl > headerSize-4 || json.Unmarshal(block[4:4+hl], &a) != nil {
		_ = f.Close()
		return nil, attrs{}, errors.New("fs: corrupt header")
	}
	return f, a, nil
}

func (b dirBucket) Version(_ context.Context, key string) (string, error) {
	p, err := b.path(key)
	if err != nil {
		return "", err
	}
	f, a, err := readAttrs(p)
	if err != nil {
		return "", err
	}
	_ = f.Close()
	return strconv.FormatInt(a.Generation, 10), nil
}

func (b dirBucket) Caps() bucket.Caps { return bucket.Caps{ConditionalDelete: true} }

// Delete holds the lock of the root across its check and its removal, and Create holds it across its link, so a
// version that matched cannot be replaced before the removal.
func (b dirBucket) Delete(_ context.Context, key, version string) error {
	p, err := b.path(key)
	if err != nil {
		return err
	}
	b.r.mu.Lock()
	defer b.r.mu.Unlock()
	f, a, err := readAttrs(p)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	_ = f.Close()
	if strconv.FormatInt(a.Generation, 10) != version {
		return bucket.ErrChanged
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, iofs.ErrNotExist) {
		return err
	}
	return nil
}

func (b dirBucket) Get(ctx context.Context, key string) ([]byte, error) {
	return b.GetRange(ctx, key, 0, -1)
}

func (b dirBucket) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	p, err := b.path(key)
	if err != nil {
		return nil, err
	}
	f, a, err := readAttrs(p)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if off >= a.Size {
		return nil, nil
	}
	if n < 0 || n > a.Size-off {
		n = a.Size - off
	}
	out := make([]byte, n)
	if _, err := f.ReadAt(out, headerSize+off); err != nil {
		return nil, err
	}
	return out, nil
}
