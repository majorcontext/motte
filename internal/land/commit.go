// Package land rebases a linear range of commits onto the tip of a ref by
// applying its tree changes, with a file-level conflict check. It is pure over
// a read-only object source: it stores nothing and holds no lock.
package land

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/majorcontext/motte/internal/gitpack"
)

// Invalid is a change that cannot land, whatever the state of the ref. Its text names the reason.
type Invalid string

func (e Invalid) Error() string { return "land: " + string(e) }

// ErrTooLarge is a land that needs more than the limits allow.
var ErrTooLarge = errors.New("land: change is too large")

// Commit is a parsed commit object.
type Commit struct {
	Tree      gitpack.SHA
	Parents   []gitpack.SHA
	Author    string
	Committer string
	extra     []header
	Message   []byte
}

type header struct{ key, raw string }

var signatureKeys = map[string]bool{"gpgsig": true, "gpgsig-sha256": true}

// ParseCommit reads the headers and the message of a commit object.
func ParseCommit(data []byte) (Commit, error) {
	head, msg, found := bytes.Cut(data, []byte("\n\n"))
	if !found {
		head = bytes.TrimSuffix(data, []byte("\n"))
	}
	c := Commit{Message: msg}
	var haveTree, haveAuthor, haveCommitter bool
	for _, line := range strings.Split(string(head), "\n") {
		if strings.HasPrefix(line, " ") && c.lastHeader() != nil {
			h := c.lastHeader()
			h.raw += "\n" + line
			continue
		}
		key, value, _ := strings.Cut(line, " ")
		switch {
		case key == "tree" && !haveTree:
			sha, err := gitpack.ParseSHA(value)
			if err != nil {
				return Commit{}, Invalid("commit has a malformed tree")
			}
			c.Tree, haveTree = sha, true
		case key == "parent" && !haveAuthor:
			sha, err := gitpack.ParseSHA(value)
			if err != nil {
				return Commit{}, Invalid("commit has a malformed parent")
			}
			c.Parents = append(c.Parents, sha)
		case key == "author" && !haveAuthor:
			c.Author, haveAuthor = value, true
		case key == "committer" && haveAuthor && !haveCommitter:
			c.Committer, haveCommitter = value, true
		case haveCommitter:
			c.extra = append(c.extra, header{key: key, raw: line})
		default:
			return Commit{}, Invalid("commit headers are not in the git order")
		}
	}
	if !haveTree || !haveAuthor || !haveCommitter {
		return Commit{}, Invalid("commit lacks a tree, an author or a committer")
	}
	return c, nil
}

func (c *Commit) lastHeader() *header {
	if len(c.extra) == 0 {
		return nil
	}
	return &c.extra[len(c.extra)-1]
}

// Rebuild returns the bytes of c on top of parent with tree. The author, the message and every header except a
// signature stay. The committer keeps its name and address, with when as the time in UTC.
func (c Commit) Rebuild(parent, tree gitpack.SHA, when time.Time) ([]byte, error) {
	end := strings.LastIndex(c.Committer, ">")
	if end < 0 {
		return nil, Invalid("commit has a malformed committer")
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "tree %s\nparent %s\nauthor %s\ncommitter %s %s +0000\n", tree, parent, c.Author, c.Committer[:end+1], strconv.FormatInt(when.Unix(), 10))
	for _, h := range c.extra {
		if !signatureKeys[h.key] {
			b.WriteString(h.raw + "\n")
		}
	}
	b.WriteByte('\n')
	b.Write(c.Message)
	return b.Bytes(), nil
}
