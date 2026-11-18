// Command release builds and publishes release artifacts with no CI provider.
// Use: go run ./internal/release/cmd/release image|jj|publish -h
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type runner struct {
	dryRun bool
	out    io.Writer
}

func (r runner) run(stdin string, name string, args ...string) error {
	fmt.Fprintln(r.out, "+", name, strings.Join(args, " "))
	if r.dryRun {
		return nil
	}
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	return cmd.Run()
}

func main() {
	if err := dispatch(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "release:", err)
		os.Exit(1)
	}
}

func dispatch(args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: release image|jj|publish [flags]")
	}
	switch args[0] {
	case "image":
		return imageCmd(args[1:], out)
	case "jj":
		return jjCmd(args[1:], out)
	case "publish":
		return publishCmd(args[1:], out)
	}
	return fmt.Errorf("unknown command %q", args[0])
}
