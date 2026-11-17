package gitproto

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/pktline"
)

// MaxCommands bounds the ref commands of one push.
const MaxCommands = 1000

// Command is one ref update of a push. A zero Old creates the ref and a zero New deletes it.
type Command struct {
	Old, New gitpack.SHA
	Ref      string
}

// Push is a parsed receive-pack request.
type Push struct {
	Commands     []Command
	ReportStatus bool
	// Pack is the pack that follows the commands. It is empty when every command deletes.
	Pack []byte
}

// ParsePush reads the commands and the pack of a receive-pack request body.
func ParsePush(body []byte) (Push, error) {
	br := bytes.NewReader(body)
	pr := pktline.NewReader(br)
	var p Push
	for {
		line, kind, err := pr.Next()
		if err != nil {
			return Push{}, RequestError("malformed receive-pack request")
		}
		if kind == pktline.Flush {
			break
		}
		if kind != pktline.Data {
			return Push{}, RequestError("malformed receive-pack request")
		}
		text, caps, first := strings.Cut(strings.TrimSuffix(string(line), "\n"), "\x00")
		if len(p.Commands) == 0 && first {
			for _, c := range strings.Fields(caps) {
				switch {
				case c == "report-status":
					p.ReportStatus = true
				case strings.HasPrefix(c, "object-format=") && c != "object-format=sha1":
					return Push{}, RequestError("only sha1 repositories are served")
				}
			}
		}
		cmd, err := parseCommand(text)
		if err != nil {
			return Push{}, err
		}
		if p.Commands = append(p.Commands, cmd); len(p.Commands) > MaxCommands {
			return Push{}, RequestError("too many ref commands")
		}
	}
	if len(p.Commands) == 0 {
		return Push{}, RequestError("no ref command")
	}
	p.Pack = body[len(body)-br.Len():]
	return p, nil
}

func parseCommand(text string) (Command, error) {
	oldText, rest, ok := strings.Cut(text, " ")
	newText, ref, ok2 := strings.Cut(rest, " ")
	if !ok || !ok2 || ref == "" {
		return Command{}, RequestError("malformed ref command")
	}
	var c Command
	var err error
	if c.Old, err = gitpack.ParseSHA(oldText); err != nil {
		return Command{}, RequestError("malformed ref command")
	}
	if c.New, err = gitpack.ParseSHA(newText); err != nil {
		return Command{}, RequestError("malformed ref command")
	}
	c.Ref = ref
	return c, nil
}

// Result is the outcome of one ref command. A reason makes the command fail.
type Result struct {
	Ref    string
	Reason string
}

// WriteReport writes the report-status reply. unpackErr is empty when the pack was stored.
func WriteReport(w io.Writer, unpackErr string, results []Result) error {
	pw := pktline.NewWriter(w)
	if unpackErr == "" {
		pw.Linef("unpack ok\n")
	} else {
		pw.Linef("unpack %s\n", unpackErr)
	}
	for _, r := range results {
		if r.Reason == "" {
			pw.Linef("ok %s\n", r.Ref)
		} else {
			pw.Linef("ng %s %s\n", r.Ref, r.Reason)
		}
	}
	pw.Flush()
	return pw.Err()
}

// AdvertiseReceive writes the ref advertisement of receive-pack. version is the protocol version that the client
// asked for, 0 or 1.
func AdvertiseReceive(w io.Writer, refs []Ref, version int) error {
	pw := pktline.NewWriter(w)
	pw.Linef("# service=git-receive-pack\n")
	pw.Flush()
	if version == 1 {
		pw.Linef("version 1\n")
	}
	caps := "report-status delete-refs ofs-delta object-format=sha1 agent=" + Agent
	for i, r := range refs {
		if i == 0 {
			pw.Linef("%s %s\x00%s\n", r.SHA, r.Name, caps)
		} else {
			pw.Linef("%s %s\n", r.SHA, r.Name)
		}
	}
	if len(refs) == 0 {
		pw.Linef("%s capabilities^{}\x00%s\n", fmt.Sprintf("%040x", 0), caps)
	}
	pw.Flush()
	return pw.Err()
}
