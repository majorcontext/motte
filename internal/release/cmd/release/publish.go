package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/majorcontext/motte/internal/release/publish"
	dirpub "github.com/majorcontext/motte/internal/release/publish/dir"
	"github.com/majorcontext/motte/internal/release/publish/github"
)

// publishCmd sends every file of -dir to -dest. A dest is "github", which
// attaches the files to the release of -tag through the gh CLI (GH_TOKEN and
// GH_REPO select the account and repository), or "dir=PATH", which copies the
// files to PATH, such as a mounted bucket.
func publishCmd(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	dir := fs.String("dir", "", "directory of artifacts")
	dest := fs.String("dest", "github", "github or dir=PATH")
	tag := fs.String("tag", "", "release tag; required for github")
	dry := fs.Bool("dry-run", false, "print the commands and run none")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("publish needs -dir")
	}
	entries, err := os.ReadDir(*dir)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, filepath.Join(*dir, e.Name()))
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no files in %s", *dir)
	}
	r := runner{dryRun: *dry, out: out}
	var p publish.Publisher
	switch {
	case *dest == "github":
		p = github.Publisher{Run: func(args ...string) error { return r.run("", "gh", args...) }}
	case strings.HasPrefix(*dest, "dir="):
		if *dry {
			fmt.Fprintln(out, "+ copy", len(files), "files to", strings.TrimPrefix(*dest, "dir="))
			return nil
		}
		p = dirpub.Publisher{To: strings.TrimPrefix(*dest, "dir=")}
	default:
		return fmt.Errorf("unknown dest %q", *dest)
	}
	return p.Publish(*tag, files)
}
