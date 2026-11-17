//go:build linux

package overlaykit

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// Entry is one path of the dirty reply.
type Entry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Exec   bool   `json:"exec,omitempty"`
	Opaque bool   `json:"opaque,omitempty"`
}

// Reply is the part of a control reply that the overlay tools read.
type Reply struct {
	Boot     string  `json:"boot"`
	Err      string  `json:"err"`
	Token    string  `json:"token"`
	BaseTree string  `json:"base_tree"`
	Entries  []Entry `json:"entries"`
}

// Ctl talks to the control socket of a mount, one JSON object per line.
type Ctl struct{ Sock string }

// Call sends one request and decodes the reply. An answer with err is an error.
func (c Ctl) Call(op string) (Reply, error) {
	conn, err := net.DialTimeout("unix", c.Sock, 5*time.Second)
	if err != nil {
		return Reply{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))
	if _, err := fmt.Fprintf(conn, "{\"op\":%q}\n", op); err != nil {
		return Reply{}, err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return Reply{}, err
	}
	var r Reply
	if err := json.Unmarshal(line, &r); err != nil {
		return Reply{}, err
	}
	if r.Err != "" {
		return r, fmt.Errorf("ctl %s: %s", op, r.Err)
	}
	return r, nil
}

// Syncfs runs syncfs on the file system of path, as the shim does before it asks.
func Syncfs(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Syncfs(fd)
}

// StableDirty runs syncfs on mnt (when sync is set), reads the token, asks dirty, runs syncfs again and reads
// the token. It accepts the answer when the three tokens are equal, and tries again after wait otherwise. A token of an
// unsettled walk equals no other token, so an edit that is younger than the settle window costs a retry.
// retries counts the extra rounds.
func (c Ctl) StableDirty(mnt string, sync bool, tries int, wait time.Duration) (r Reply, retries int, err error) {
	for i := 0; i < tries; i++ {
		if err = c.syncfs(mnt, sync); err != nil {
			return r, i, err
		}
		before, err := c.Call("token")
		if err != nil {
			return r, i, err
		}
		r, err = c.Call("dirty")
		if err != nil {
			return r, i, err
		}
		if err = c.syncfs(mnt, sync); err != nil {
			return r, i, err
		}
		after, err := c.Call("token")
		if err != nil {
			return r, i, err
		}
		if before.Token == r.Token && r.Token == after.Token && r.Boot == before.Boot {
			return r, i, nil
		}
		time.Sleep(wait)
	}
	return r, tries, errors.New("token never settled: the dirty set may be incomplete")
}

func (Ctl) syncfs(mnt string, sync bool) error {
	if !sync {
		return nil
	}
	return Syncfs(mnt)
}
