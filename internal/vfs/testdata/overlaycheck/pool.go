//go:build linux

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// pool hands out base paths from the lower mount so that no two checks touch the same file or directory.
type pool struct {
	files, dirs []string
	used        []string
}

const maxVisited = 5000

// discover lists plain base files of 1 byte to 64 KiB and base directories that hold at least two of them,
// from the first part of the lower tree.
func discover(lower string) (*pool, error) {
	p := &pool{}
	perDir := map[string]int{}
	n := 0
	err := filepath.WalkDir(lower, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(lower, path)
		if d.IsDir() {
			if rel == ".git" || rel == ".jj" || d.Name() == "node_modules" {
				return fs.SkipDir
			}
			return nil
		}
		if n++; n > maxVisited {
			return fs.SkipAll
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 != 0 || info.Size() < 1 || info.Size() > 64<<10 || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		p.files = append(p.files, rel)
		if dir := filepath.Dir(rel); dir != "." {
			if perDir[dir]++; perDir[dir] == 2 {
				p.dirs = append(p.dirs, dir)
			}
		}
		return nil
	})
	if err == nil && (len(p.files) < 20 || len(p.dirs) < 4) {
		err = fmt.Errorf("the lower tree has %d plain files and %d directories with two of them; the checks need a bigger repository", len(p.files), len(p.dirs))
	}
	return p, err
}

func (p *pool) take(path string) { p.used = append(p.used, path) }

// free reports whether the path lies inside or above no taken path.
func (p *pool) free(path string) bool {
	for _, u := range p.used {
		if u == path || strings.HasPrefix(u, path+"/") || strings.HasPrefix(path, u+"/") {
			return false
		}
	}
	return true
}

// file takes a base file that lies in no taken directory.
func (p *pool) file() (string, error) {
	for _, f := range p.files {
		if p.free(f) {
			p.take(f)
			return f, nil
		}
	}
	return "", errors.New("no unused base file is left")
}

// dir takes a base directory that holds no taken path and at most 40 leaves, and returns it with its
// plain files.
func (p *pool) dir(lower string) (string, []string, error) {
	for _, d := range p.dirs {
		if !p.free(d) {
			continue
		}
		ents, err := leaves(lower, d)
		if err != nil || len(ents) > 40 {
			continue
		}
		p.take(d)
		return d, ents, nil
	}
	return "", nil, errors.New("no unused small base directory is left")
}

// leaves lists the files and symlinks below dir, relative to dir.
func leaves(root, dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(filepath.Join(root, dir), path)
			out = append(out, rel)
		}
		return nil
	})
	return out, err
}
