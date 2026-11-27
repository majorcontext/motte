package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// The binary links the glibc of its build system. Building on bookworm
// (glibc 2.36) lets it run on bookworm and every newer glibc.
const (
	buildImage = "rust:1.99.1-bookworm@sha256:e9da6f6c3ed5095fd5d33bf7cdf309830a178a8147888f1b2f76d5769bd9306c"
	smokeImage = "debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc"
)

func jjCmd(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("jj", flag.ContinueOnError)
	outDir := fs.String("out", "", "directory for jj-linux-ARCH and jj-linux-ARCH.sha256")
	arch := fs.String("arch", runtime.GOARCH, "amd64 or arm64")
	src := fs.String("src", "jj", "the jj crate directory")
	version := fs.String("version", "", "the version that jj version prints, such as the release tag")
	dry := fs.Bool("dry-run", false, "print the commands and run none")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outDir == "" {
		return errors.New("jj needs -out")
	}
	if *arch != "amd64" && *arch != "arm64" {
		return fmt.Errorf("arch %q is not amd64 or arm64", *arch)
	}
	crate, err := filepath.Abs(*src)
	if err != nil {
		return err
	}
	r := runner{dryRun: *dry, out: out}
	plat := "linux/" + *arch
	build := []string{"run", "--rm", "--platform", plat, "-v", crate + ":/w", "-w", "/w"}
	if *version != "" {
		build = append(build, "-e", "MOTTE_VERSION="+*version)
	}
	build = append(build, buildImage, "cargo", "build", "--release", "--locked", "--bin", "jj")
	if err := r.run("", "docker", build...); err != nil {
		return err
	}
	bin := filepath.Join(crate, "target", "release", "jj")
	if err := r.run("", "docker", "run", "--rm", "--platform", plat, "-v", filepath.Dir(bin)+":/b:ro",
		smokeImage, "/b/jj", "version"); err != nil {
		return err
	}
	if *dry {
		return nil
	}
	return writeArtifact(bin, *outDir, "jj-linux-"+*arch)
}

func writeArtifact(bin, dir, name string) error {
	data, err := os.ReadFile(bin)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o755); err != nil {
		return err
	}
	sum := fmt.Sprintf("%x  %s\n", sha256.Sum256(data), name)
	return os.WriteFile(filepath.Join(dir, name+".sha256"), []byte(sum), 0o644)
}
