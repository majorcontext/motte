package gitproto

import (
	"bufio"
	"context"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/pktline"
)

const (
	maxWants = 10000
	maxHaves = 100000
)

// Upload serves the protocol v2 commands ls-refs and fetch of one repository.
type Upload struct {
	Objects Objects
	// Refs are the refs under their git names.
	Refs []Ref
	// Head is the git name of the ref that HEAD names, or empty.
	Head string
	// Track, when set, is called with the objects that a fetch holds in its sets, before it holds them, each time
	// that count grows. An error of Track stops the fetch.
	Track func(objects int) error
	// Phase, when set, is called with the name of each phase of a fetch as it ends: has_wants, has_haves, commit_walk,
	// edge_trees, tree_walk and pack_write.
	Phase func(name string)
}

// AdvertiseUpload writes the protocol v2 capability advertisement.
func AdvertiseUpload(w io.Writer) error {
	pw := pktline.NewWriter(w)
	pw.Linef("version 2\n")
	pw.Linef("agent=%s\n", Agent)
	pw.Linef("ls-refs\n")
	pw.Linef("fetch=filter\n")
	pw.Linef("object-format=sha1\n")
	pw.Flush()
	return pw.Err()
}

// Serve answers one command from body. An error before the first byte of w is a refusal. After the first byte it
// is a failure that the caller must report by closing the connection.
func (u *Upload) Serve(ctx context.Context, w io.Writer, body io.Reader) error {
	pr := pktline.NewReader(body)
	var command string
	var args []string
	inArgs := false
	for {
		line, kind, err := pr.Next()
		if err != nil {
			return RequestError("malformed upload-pack request")
		}
		if kind == pktline.Flush {
			break
		}
		if kind == pktline.Delim && !inArgs {
			inArgs = true
			continue
		}
		if kind != pktline.Data {
			return RequestError("malformed upload-pack request")
		}
		text := strings.TrimSuffix(string(line), "\n")
		switch {
		case inArgs:
			args = append(args, text)
		case strings.HasPrefix(text, "command="):
			command = strings.TrimPrefix(text, "command=")
		case strings.HasPrefix(text, "object-format=") && text != "object-format=sha1":
			return RequestError("only sha1 repositories are served")
		}
	}
	switch command {
	case "ls-refs":
		return u.lsRefs(ctx, w, args)
	case "fetch":
		return u.fetch(ctx, w, args)
	}
	return RequestError("unsupported command")
}

func (u *Upload) lsRefs(ctx context.Context, w io.Writer, args []string) error {
	var symrefs, peel bool
	var prefixes []string
	for _, a := range args {
		switch {
		case a == "symrefs":
			symrefs = true
		case a == "peel":
			peel = true
		case a == "unborn":
		case strings.HasPrefix(a, "ref-prefix "):
			prefixes = append(prefixes, strings.TrimPrefix(a, "ref-prefix "))
		default:
			return RequestError("unsupported ls-refs argument")
		}
	}
	match := func(name string) bool {
		return len(prefixes) == 0 || slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(name, p) })
	}
	type line struct {
		name, text string
		sha        gitpack.SHA
	}
	var lines []line
	for _, r := range u.Refs {
		if r.Name == u.Head && match("HEAD") {
			text := ""
			if symrefs {
				text = " symref-target:" + u.Head
			}
			lines = append(lines, line{"HEAD", text, r.SHA})
		}
	}
	for _, r := range u.Refs {
		if !match(r.Name) {
			continue
		}
		text := ""
		if peel && strings.HasPrefix(r.Name, "refs/tags/") {
			target, err := u.peel(ctx, r.SHA)
			if err != nil {
				return err
			}
			if target != r.SHA {
				text = " peeled:" + target.String()
			}
		}
		lines = append(lines, line{r.Name, text, r.SHA})
	}
	pw := pktline.NewWriter(w)
	for _, l := range lines {
		pw.Linef("%s %s%s\n", l.sha, l.name, l.text)
	}
	pw.Flush()
	return pw.Err()
}

func (u *Upload) peel(ctx context.Context, sha gitpack.SHA) (gitpack.SHA, error) {
	for range 16 {
		t, data, err := u.Objects.Get(ctx, sha)
		if err != nil || t != gitpack.Tag {
			return sha, err
		}
		next, ok := tagTarget(data)
		if !ok {
			return sha, nil
		}
		sha = next
	}
	return sha, nil
}

func tagTarget(data []byte) (gitpack.SHA, bool) {
	text, ok := strings.CutPrefix(string(data[:min(len(data), 48)]), "object ")
	if !ok || len(text) < 40 {
		return gitpack.SHA{}, false
	}
	sha, err := gitpack.ParseSHA(text[:40])
	return sha, err == nil
}

type fetchRequest struct {
	wants, haves []gitpack.SHA
	done         bool
	blobNone     bool
}

func parseFetch(args []string) (fetchRequest, error) {
	var f fetchRequest
	for _, a := range args {
		verb, value, _ := strings.Cut(a, " ")
		switch verb {
		case "want", "have":
			sha, err := gitpack.ParseSHA(value)
			if err != nil {
				return f, RequestError("malformed object name")
			}
			if verb == "want" {
				f.wants = append(f.wants, sha)
			} else {
				f.haves = append(f.haves, sha)
			}
		case "done":
			f.done = true
		case "thin-pack", "no-progress", "ofs-delta", "include-tag":
		case "filter":
			if value != "blob:none" {
				return f, RequestError("only the filter blob:none is supported")
			}
			f.blobNone = true
		default:
			return f, RequestError("unsupported fetch argument " + verb)
		}
	}
	if len(f.wants) == 0 || len(f.wants) > maxWants || len(f.haves) > maxHaves {
		return f, RequestError("fetch needs 1 to 10000 wants and at most 100000 haves")
	}
	return f, nil
}

func (u *Upload) fetch(ctx context.Context, w io.Writer, args []string) error {
	req, err := parseFetch(args)
	if err != nil {
		return err
	}
	phase := u.Phase
	if phase == nil {
		phase = func(string) {}
	}
	have, err := u.Objects.Has(ctx, req.wants)
	if err != nil {
		return err
	}
	phase("has_wants")
	for i, ok := range have {
		if !ok {
			return RequestError("not our object " + req.wants[i].String())
		}
	}
	var common []gitpack.SHA
	if have, err = u.Objects.Has(ctx, req.haves); err != nil {
		return err
	}
	phase("has_haves")
	for i, ok := range have {
		if ok {
			common = append(common, req.haves[i])
		}
	}
	ready := req.done || len(common) > 0
	pw := pktline.NewWriter(w)
	if !ready {
		pw.Linef("acknowledgments\n")
		pw.Linef("NAK\n")
		pw.Flush()
		return pw.Err()
	}
	c := &collector{objs: u.Objects, ctx: ctx, blobNone: req.blobNone, track: u.Track, phase: phase}
	shas, err := c.collect(req.wants, common)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return RequestError("an object of the fetch is missing")
		}
		return err
	}
	if !req.done {
		pw.Linef("acknowledgments\n")
		for _, sha := range common {
			pw.Linef("ACK %s\n", sha)
		}
		pw.Linef("ready\n")
		pw.Delim()
	}
	pw.Linef("packfile\n")
	bw := bufio.NewWriterSize(pktline.SideBand(pw), pktline.MaxData-1)
	if err := writePack(ctx, bw, u.Objects, shas); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	phase("pack_write")
	pw.Flush()
	return pw.Err()
}
