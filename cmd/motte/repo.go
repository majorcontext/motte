package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/majorcontext/motte/client"
)

// accessHeader carries the token that `cloudflared access token` prints. The Access edge turns it into the
// assertion that the service verifies.
const accessHeader = "cf-access-token"

type addHeader struct{ key, value string }

func (h addHeader) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set(h.key, h.value)
	return http.DefaultTransport.RoundTrip(r)
}

// runRepo is `motte repo create [--owner OWNER] NAME`. It reads MOTTE_URL and the credential from MOTTE_TOKEN, an
// operator grant, or MOTTE_ACCESS_TOKEN. It prints the path of the new repository and returns the exit code.
func runRepo(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	owner := fs.String("owner", "operator", "owner of the repository")
	if len(args) < 1 || args[0] != "create" || fs.Parse(args[1:]) != nil || fs.NArg() != 1 || getenv("MOTTE_URL") == "" ||
		getenv("MOTTE_TOKEN") == "" && getenv("MOTTE_ACCESS_TOKEN") == "" {
		fmt.Fprintln(stderr, "usage: MOTTE_URL=... MOTTE_TOKEN=... (or MOTTE_ACCESS_TOKEN=...) motte repo create [--owner OWNER] NAME")
		return 2
	}
	var opts []client.Option
	if tok := getenv("MOTTE_ACCESS_TOKEN"); tok != "" {
		opts = append(opts, client.WithHTTPClient(&http.Client{Transport: addHeader{accessHeader, tok}}))
	}
	path, err := client.New(getenv("MOTTE_URL"), client.StaticGrant(getenv("MOTTE_TOKEN")), opts...).CreateRepo(ctx, fs.Arg(0), *owner)
	if err != nil {
		fmt.Fprintln(stderr, "motte repo create:", err)
		return 1
	}
	fmt.Fprintln(stdout, path)
	return 0
}
