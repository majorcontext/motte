package land

import (
	"slices"
	"strings"
)

// clash finds the paths that clash with a set of changed paths. Two paths clash when they are equal or when one is a
// directory above the other, such as a file that becomes a directory.
type clash struct {
	paths, dirs map[string]struct{}
	hit         map[string]struct{}
}

func newClash(paths []string) *clash {
	c := &clash{paths: make(map[string]struct{}, len(paths)), dirs: map[string]struct{}{}, hit: map[string]struct{}{}}
	for _, p := range paths {
		c.paths[p] = struct{}{}
		for i := range len(p) {
			if p[i] == '/' {
				c.dirs[p[:i]] = struct{}{}
			}
		}
	}
	return c
}

func (c *clash) add(p string) {
	if _, ok := c.hit[p]; ok {
		return
	}
	_, equal := c.paths[p]
	_, above := c.dirs[p]
	if !equal && !above {
		for i := strings.IndexByte(p, '/'); i >= 0 && !equal; {
			_, equal = c.paths[p[:i]]
			if j := strings.IndexByte(p[i+1:], '/'); j >= 0 {
				i += j + 1
			} else {
				i = -1
			}
		}
	}
	if equal || above {
		c.hit[p] = struct{}{}
	}
}

func (c *clash) result() []string {
	out := make([]string, 0, len(c.hit))
	for p := range c.hit {
		out = append(out, p)
	}
	slices.Sort(out)
	return out[:min(len(out), MaxConflictPaths)]
}
