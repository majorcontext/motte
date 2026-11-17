package vfs

import "strings"

func join(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func split(p string) (parent, name string) {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "", p
	}
	return p[:i], p[i+1:]
}
