// Package fs backs the objstore seam with a directory.
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
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/majorcontext/motte/internal/objstore"
)

// headerSize is the fixed block that precedes the bytes of an object file.
const headerSize = 4096

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

type attrs struct {
	Name        string            `json:"name"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	MD5         string            `json:"md5"`
	Generation  int64             `json:"generation"`
}

// Store keeps objects under root. Use one process per root.
type Store struct {
	root    string
	lastGen atomic.Int64
}

func New(root string) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, ".tmp"), 0o700); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

// Bucket opens one bucket of the directory.
func (s *Store) Bucket(name string) objstore.Bucket { return bucket{s, name} }

type bucket struct {
	s    *Store
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
	return filepath.Join(b.s.root, b.name, name[:2], name), nil
}

func readAttrs(f *os.File) (attrs, error) {
	var block [headerSize]byte
	if _, err := io.ReadFull(f, block[:]); err != nil {
		return attrs{}, fmt.Errorf("fs: read header: %w", err)
	}
	n := binary.BigEndian.Uint32(block[:4])
	if n == 0 || n > headerSize-4 {
		return attrs{}, errors.New("fs: corrupt header")
	}
	var a attrs
	if err := json.Unmarshal(block[4:4+n], &a); err != nil {
		return attrs{}, fmt.Errorf("fs: corrupt header: %w", err)
	}
	return a, nil
}

func (b bucket) open(key string) (*os.File, attrs, error) {
	p, err := b.path(key)
	if err != nil {
		return nil, attrs{}, err
	}
	f, err := os.Open(p)
	if errors.Is(err, iofs.ErrNotExist) {
		return nil, attrs{}, objstore.ErrNotFound
	}
	if err != nil {
		return nil, attrs{}, err
	}
	a, err := readAttrs(f)
	if err != nil {
		_ = f.Close()
		return nil, attrs{}, err
	}
	return f, a, nil
}

func (s *Store) nextGeneration() int64 {
	for {
		prev := s.lastGen.Load()
		next := max(time.Now().UnixNano(), prev+1)
		if s.lastGen.CompareAndSwap(prev, next) {
			return next
		}
	}
}

// put writes data atomically. createOnly reports false instead of replacing.
func (b bucket) put(key, contentType string, meta map[string]string, data []byte, createOnly bool) (attrs, bool, error) {
	final, err := b.path(key)
	if err != nil {
		return attrs{}, false, err
	}
	tmp, err := os.CreateTemp(filepath.Join(b.s.root, ".tmp"), "put-*")
	if err != nil {
		return attrs{}, false, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	sum := md5.Sum(data)
	a := attrs{Name: key, Size: int64(len(data)), ContentType: contentType, Metadata: meta, MD5: hex.EncodeToString(sum[:]), Generation: b.s.nextGeneration()}
	raw, err := json.Marshal(a)
	if err != nil || len(raw) > headerSize-4 {
		return attrs{}, false, errors.New("fs: object attributes are too large")
	}
	var block [headerSize]byte
	binary.BigEndian.PutUint32(block[:4], uint32(len(raw)))
	copy(block[4:], raw)
	if _, err := tmp.Write(block[:]); err != nil {
		return attrs{}, false, err
	}
	if _, err := tmp.Write(data); err != nil {
		return attrs{}, false, err
	}
	if err := tmp.Sync(); err != nil {
		return attrs{}, false, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return attrs{}, false, err
	}
	if createOnly {
		if err := os.Link(tmp.Name(), final); err != nil {
			if errors.Is(err, iofs.ErrExist) {
				return attrs{}, false, nil
			}
			return attrs{}, false, err
		}
	} else if err := os.Rename(tmp.Name(), final); err != nil {
		return attrs{}, false, err
	}
	return a, true, b.syncDirs(final)
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
		if dir == b.s.root || filepath.Dir(dir) == dir {
			return nil
		}
	}
}

func (b bucket) Create(_ context.Context, key string, data []byte) (bool, error) {
	_, created, err := b.put(key, "application/octet-stream", nil, data, true)
	return created, err
}

func (b bucket) Get(ctx context.Context, key string) ([]byte, error) {
	return b.GetRange(ctx, key, 0, -1)
}

func (b bucket) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	f, a, err := b.open(key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if off >= a.Size {
		return nil, nil
	}
	if n < 0 || off+n > a.Size {
		n = a.Size - off
	}
	out := make([]byte, n)
	if _, err := f.ReadAt(out, headerSize+off); err != nil {
		return nil, err
	}
	return out, nil
}

func (b bucket) Attrs(_ context.Context, key string) (objstore.Attrs, error) {
	f, a, err := b.open(key)
	if err != nil {
		return objstore.Attrs{}, err
	}
	_ = f.Close()
	md5, err := hex.DecodeString(a.MD5)
	if err != nil {
		return objstore.Attrs{}, err
	}
	return objstore.Attrs{Size: a.Size, ContentType: a.ContentType, Metadata: a.Metadata, MD5: md5, Generation: a.Generation}, nil
}

type bodyReader struct {
	io.Reader
	f *os.File
}

func (r bodyReader) Close() error { return r.f.Close() }

func (b bucket) Open(_ context.Context, key string, generation int64) (io.ReadCloser, objstore.Attrs, error) {
	f, a, err := b.open(key)
	if err != nil {
		return nil, objstore.Attrs{}, err
	}
	if generation > 0 && a.Generation != generation {
		_ = f.Close()
		return nil, objstore.Attrs{}, objstore.ErrNotFound
	}
	body := io.NewSectionReader(f, headerSize, a.Size)
	return bodyReader{body, f}, objstore.Attrs{Size: a.Size, ContentType: a.ContentType, Generation: a.Generation}, nil
}

func (b bucket) List(_ context.Context, prefix string) ([]string, error) {
	if !bucketName.MatchString(b.name) {
		return nil, fmt.Errorf("fs: invalid bucket %q", b.name)
	}
	var out []string
	err := filepath.WalkDir(filepath.Join(b.s.root, b.name), func(p string, d iofs.DirEntry, err error) error {
		if errors.Is(err, iofs.ErrNotExist) || (err == nil && d.IsDir()) {
			return nil
		}
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if errors.Is(err, iofs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		a, err := readAttrs(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		if strings.HasPrefix(a.Name, prefix) {
			out = append(out, a.Name)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func (b bucket) Delete(_ context.Context, key string) error {
	p, err := b.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, iofs.ErrNotExist) {
		return err
	}
	return nil
}

func (b bucket) Copy(ctx context.Context, c objstore.Copy) (int64, error) {
	f, a, err := b.open(c.Source)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if c.Generation > 0 && a.Generation != c.Generation {
		return 0, objstore.ErrNotFound
	}
	data, err := io.ReadAll(io.NewSectionReader(f, headerSize, a.Size))
	if err != nil {
		return 0, err
	}
	out, created, err := b.put(c.Destination, a.ContentType, c.Metadata, data, c.CreateOnly)
	if err != nil {
		return 0, err
	}
	if !created {
		return 0, errors.New("fs: destination exists")
	}
	return out.Generation, nil
}

var _ objstore.Bucket = bucket{}
