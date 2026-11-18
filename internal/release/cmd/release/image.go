package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var semver = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// imageTags returns the tags of a version: the full version, and major.minor
// for a version that is not a prerelease.
func imageTags(version string) ([]string, error) {
	m := semver.FindStringSubmatch(version)
	if m == nil {
		return nil, fmt.Errorf("version %q is not semver", version)
	}
	tags := []string{m[1] + "." + m[2] + "." + m[3] + m[4]}
	if m[4] == "" {
		tags = append(tags, m[1]+"."+m[2])
	}
	return tags, nil
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func imageCmd(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("image", flag.ContinueOnError)
	name := fs.String("name", "", "image name; the Dockerfile defaults to images/NAME/Dockerfile")
	repo := fs.String("repo", "", "image repository, such as registry.example/org/name")
	version := fs.String("version", "", "semver tag; required with -push")
	push := fs.Bool("push", false, "push the image; otherwise build only")
	platforms := fs.String("platforms", "linux/amd64,linux/arm64", "build platforms")
	file := fs.String("file", "", "Dockerfile path")
	dry := fs.Bool("dry-run", false, "print the commands and run none")
	var buildArgs listFlag
	fs.Var(&buildArgs, "build-arg", "KEY=VALUE build argument; repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *repo == "" {
		return errors.New("image needs -name and -repo")
	}
	if *file == "" {
		*file = "images/" + *name + "/Dockerfile"
	}
	var tags []string
	if *version != "" {
		t, err := imageTags(*version)
		if err != nil {
			return err
		}
		tags = t
	} else if *push {
		return errors.New("-push needs -version")
	}
	r := runner{dryRun: *dry, out: out}
	if *push {
		if err := login(r, *repo); err != nil {
			return err
		}
	}
	bx := []string{"buildx", "build", "--platform", *platforms, "-f", *file}
	for _, t := range tags {
		bx = append(bx, "-t", *repo+":"+t)
	}
	for _, a := range buildArgs {
		bx = append(bx, "--build-arg", a)
	}
	switch {
	case *push:
		bx = append(bx, "--provenance=mode=max", "--sbom=true", "--push")
	case !strings.Contains(*platforms, ","):
		bx = append(bx, "--load")
	}
	return r.run("", "docker", append(bx, ".")...)
}

// login signs in to the registry host of repo when REGISTRY_PASSWORD is set.
// With it unset, the caller has already signed in.
func login(r runner, repo string) error {
	pass := os.Getenv("REGISTRY_PASSWORD")
	if pass == "" {
		return nil
	}
	host, _, _ := strings.Cut(repo, "/")
	user := os.Getenv("REGISTRY_USERNAME")
	return r.run(pass, "docker", "login", host, "-u", user, "--password-stdin")
}
