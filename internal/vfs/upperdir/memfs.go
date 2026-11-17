package upperdir

import (
	"io/fs"
	"sort"
	"strings"
)

// MemNode is one entry of a MemFS.
type MemNode struct {
	Mode     fs.FileMode
	Whiteout bool
	Size     int64
	Ctime    int64
	Ino      uint64
	Xattr    map[string]string
	// Data is the content of a regular file, or the target of a symlink.
	Data []byte
}

// MemFS is an in-memory FS. The root exists and has no entry of its own in the map.
type MemFS struct {
	nodes map[string]MemNode
}

func NewMemFS() *MemFS { return &MemFS{nodes: map[string]MemNode{}} }

func (m *MemFS) Put(p string, n MemNode) { m.nodes[p] = n }

func (m *MemFS) Get(p string) (MemNode, bool) {
	n, ok := m.nodes[p]
	return n, ok
}

func (m *MemFS) Delete(p string) { delete(m.nodes, p) }

// Paths lists every path in order.
func (m *MemFS) Paths() []string {
	ps := make([]string, 0, len(m.nodes))
	for p := range m.nodes {
		ps = append(ps, p)
	}
	sort.Strings(ps)
	return ps
}

func dirent(name string, n MemNode) Dirent {
	return Dirent{Name: name, Mode: n.Mode, Whiteout: n.Whiteout, Size: n.Size, Ctime: n.Ctime, Ino: n.Ino}
}

type memDir struct {
	m    *MemFS
	path string
}

func (m *MemFS) Open(p string) (Dir, error) {
	if n, ok := m.nodes[p]; p != "" && (!ok || !n.Mode.IsDir()) {
		return nil, fs.ErrNotExist
	}
	return memDir{m, p}, nil
}

func (d memDir) Close() error { return nil }

func (d memDir) Entries() ([]Dirent, error) {
	var out []Dirent
	for p, n := range d.m.nodes {
		if parent(p) == d.path && p != "" {
			out = append(out, dirent(p[strings.LastIndexByte(p, '/')+1:], n))
		}
	}
	return out, nil
}

func (d memDir) Stat() (Dirent, error) {
	if d.path == "" {
		return Dirent{Mode: fs.ModeDir | 0o755}, nil
	}
	return d.m.Lstat(d.path)
}

func (m *MemFS) Lstat(p string) (Dirent, error) {
	n, ok := m.nodes[p]
	if !ok {
		return Dirent{}, fs.ErrNotExist
	}
	return dirent(p[strings.LastIndexByte(p, '/')+1:], n), nil
}

func (d memDir) Lstat(name string) (Dirent, error) { return d.m.Lstat(join(d.path, name)) }

func (d memDir) Open(name string) (Dir, error) { return d.m.Open(join(d.path, name)) }

func (d memDir) Attr(attr string) (string, bool, error) {
	v, ok := d.m.nodes[d.path].Xattr[attr]
	return v, ok, nil
}

func (d memDir) Xattr(name, attr string) (string, bool, error) {
	n, ok := d.m.nodes[join(d.path, name)]
	if !ok {
		return "", false, fs.ErrNotExist
	}
	v, ok := n.Xattr[attr]
	return v, ok, nil
}

func (d memDir) Read(name string, limit int64) (Blob, error) {
	n, ok := d.m.nodes[join(d.path, name)]
	b := Blob{Mode: n.Mode, Size: n.Size}
	switch {
	case !ok:
		return Blob{}, fs.ErrNotExist
	case n.Mode&fs.ModeSymlink != 0:
		b.Data, b.HasData, b.Size, b.Read = n.Data, true, int64(len(n.Data)), int64(len(n.Data))
	case n.Mode.IsRegular() && int64(len(n.Data)) <= limit:
		b.Data, b.HasData, b.Size, b.Read = n.Data, true, int64(len(n.Data)), int64(len(n.Data))
	case !n.Mode.IsRegular():
		return Blob{}, fs.ErrNotExist
	}
	return b, nil
}
