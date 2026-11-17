//go:build linux

package vfs

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// mountEntry is one line of a mountinfo file.
type mountEntry struct {
	Mountpoint string
	FSType     string
	// Opts are the super options. A key without a value has the value "".
	Opts map[string]string
	// PID and NS name the process and the mount namespace that the entry was read from; they are zero for the plugin.
	PID int
	NS  string
}

func (m mountEntry) upperdir() string { return m.Opts["upperdir"] }

// parseMountinfo parses the text of a mountinfo file. A line that does not parse is an error.
func parseMountinfo(text string) ([]mountEntry, error) {
	var out []mountEntry
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		sep := 6
		for sep < len(f) && f[sep] != "-" {
			sep++
		}
		if sep+2 >= len(f) {
			return nil, fmt.Errorf("mountinfo: malformed line %q", sc.Text())
		}
		e := mountEntry{Mountpoint: unescapeMount(f[4]), FSType: f[sep+1], Opts: map[string]string{}}
		if sep+3 < len(f) {
			for _, o := range strings.Split(f[sep+3], ",") {
				k, v, _ := strings.Cut(o, "=")
				e.Opts[k] = unescapeMount(v)
			}
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// unescapeMount decodes the octal escapes (\040 for a space) of a mountinfo field.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// hostMounts reads the mountinfo of the plugin and of one live process for each other mount namespace in the
// procfs at proc. A process that exits before the read is replaced by the next process of its namespace; a namespace
// whose processes all exited is empty. Any other failure to name a namespace or to read or parse its mountinfo fails
// the scan, because the namespace could hold the overlay. The scan sees other pods only when the plugin shares the
// PID namespace of the host.
func hostMounts(proc string) ([]mountEntry, error) {
	selfNS, _ := os.Readlink(filepath.Join(proc, "self", "ns", "mnt"))
	text, err := os.ReadFile(filepath.Join(proc, "self", "mountinfo"))
	if err != nil {
		return nil, err
	}
	out, err := parseMountinfo(string(text))
	if err != nil {
		return nil, err
	}
	var order []string
	pidsOf := map[string][]string{}
	dirs, _ := filepath.Glob(filepath.Join(proc, "[0-9]*"))
	for _, dir := range dirs {
		ns, err := os.Readlink(filepath.Join(dir, "ns", "mnt"))
		if gone(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if ns == selfNS {
			continue
		}
		if _, ok := pidsOf[ns]; !ok {
			order = append(order, ns)
		}
		pidsOf[ns] = append(pidsOf[ns], dir)
	}
	for _, ns := range order {
		es, err := readNamespace(ns, pidsOf[ns])
		if err != nil {
			return nil, err
		}
		out = append(out, es...)
	}
	return out, nil
}

// readNamespace reads the mountinfo of the first of dirs that still exists.
func readNamespace(ns string, dirs []string) ([]mountEntry, error) {
	for _, dir := range dirs {
		text, err := os.ReadFile(filepath.Join(dir, "mountinfo"))
		if gone(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		es, err := parseMountinfo(string(text))
		if err != nil {
			return nil, err
		}
		pid, _ := strconv.Atoi(filepath.Base(dir))
		for i := range es {
			es[i].PID, es[i].NS = pid, ns
		}
		return es, nil
	}
	return nil, nil
}

// gone reports whether err means that the process exited.
func gone(err error) bool { return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) }
