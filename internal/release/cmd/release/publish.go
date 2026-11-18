package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	switch {
	case *dest == "github":
		return publishGitHub(r, *tag, files)
	case strings.HasPrefix(*dest, "dir="):
		return publishDir(r, strings.TrimPrefix(*dest, "dir="), files)
	}
	return fmt.Errorf("unknown dest %q", *dest)
}

func publishGitHub(r runner, tag string, files []string) error {
	if tag == "" {
		return errors.New("github needs -tag")
	}
	// Two jobs may create the release at once; the loser reads the winner.
	if r.run("", "gh", "release", "view", tag) == nil {
		return upload(r, tag, files)
	}
	if err := r.run("", "gh", "release", "create", tag, "--title", tag, "--notes", ""); err != nil {
		if err := r.run("", "gh", "release", "view", tag); err != nil {
			return err
		}
	}
	return upload(r, tag, files)
}

func upload(r runner, tag string, files []string) error {
	return r.run("", "gh", append([]string{"release", "upload", tag, "--clobber"}, files...)...)
}

func publishDir(r runner, to string, files []string) error {
	if r.dryRun {
		fmt.Fprintln(r.out, "+ copy", len(files), "files to", to)
		return nil
	}
	if err := os.MkdirAll(to, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(to, filepath.Base(f)), data, 0o755); err != nil {
			return err
		}
	}
	return nil
}
