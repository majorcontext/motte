package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/devicelogin"
)

// runCLI runs the command line subcommands and reports ok false for a name that is not one.
func runCLI(ctx context.Context, name string, args []string, getenv func(string) string, stdout, stderr io.Writer) (int, bool) {
	switch name {
	case "repo":
		return runRepo(ctx, args, getenv, stdout, stderr), true
	case "token":
		return runToken(ctx, args, getenv, stdout, stderr), true
	case "operator-grant":
		return runMint(args, stdout, stderr), true
	case "login":
		return runLogin(ctx, args, getenv, stdout, stderr), true
	}
	return 0, false
}

// credential is MOTTE_TOKEN, or else the token that `motte login` saved. MOTTE_TOKEN is an operator grant, a
// per-user token or an OIDC JWT.
func credential(getenv func(string) string) string {
	if tok := getenv("MOTTE_TOKEN"); tok != "" {
		return tok
	}
	if path, err := devicelogin.DefaultPath("motte"); err == nil {
		if t, err := devicelogin.Load(path, time.Now()); err == nil {
			return t.Value
		}
	}
	return ""
}

func newClient(getenv func(string) string, stderr io.Writer, usage string) *client.Client {
	tok := credential(getenv)
	if getenv("MOTTE_URL") == "" || tok == "" {
		fmt.Fprintln(stderr, "usage: MOTTE_URL=... MOTTE_TOKEN=... "+usage)
		return nil
	}
	return client.New(getenv("MOTTE_URL"), client.StaticGrant(tok))
}

// runRepo is `motte repo create [--owner OWNER] NAME`. It prints the path of the new repository and returns the exit code.
func runRepo(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	owner := fs.String("owner", "operator", "owner of the repository")
	const usage = "motte repo create [--owner OWNER] NAME"
	if len(args) < 1 || args[0] != "create" || fs.Parse(args[1:]) != nil || fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: "+usage)
		return 2
	}
	c := newClient(getenv, stderr, usage)
	if c == nil {
		return 2
	}
	path, err := c.CreateRepo(ctx, fs.Arg(0), *owner)
	if err != nil {
		fmt.Fprintln(stderr, "motte repo create:", err)
		return 1
	}
	fmt.Fprintln(stdout, path)
	return 0
}

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

// runToken is `motte token create|list|revoke`. The service accepts it only with an operator grant.
func runToken(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	const usage = "motte token create --principal P --scope read|write|admin [--repo R]... [--ttl D] | list | revoke ID"
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: "+usage)
		return 2
	}
	fs := flag.NewFlagSet("token "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	principal := fs.String("principal", "", "who the token is for, such as an email")
	scope := fs.String("scope", "", "read, write, or admin with no repository")
	ttl := fs.Duration("ttl", 0, "lifetime, 0 for none")
	var repos list
	fs.Var(&repos, "repo", "repository text such as native:repo_X (repeatable)")
	if fs.Parse(args[1:]) != nil {
		return 2
	}
	if *ttl < 0 || *ttl > 0 && *ttl < time.Second {
		fmt.Fprintln(stderr, "motte token: --ttl must be 0 or at least 1s")
		return 2
	}
	want := map[string]int{"create": 0, "list": 0, "revoke": 1}
	if n, ok := want[args[0]]; !ok || fs.NArg() != n {
		fmt.Fprintln(stderr, "usage: "+usage)
		return 2
	}
	c := newClient(getenv, stderr, usage)
	if c == nil {
		return 2
	}
	var err error
	switch args[0] {
	case "create":
		var out api.CreateUserTokenResponse
		out, err = c.CreateUserToken(ctx, api.CreateUserTokenRequest{Principal: *principal, Scope: *scope, Repos: repos, TTLSeconds: int64(ttl.Seconds())})
		if err == nil {
			fmt.Fprintf(stdout, "%s\n", out.Token)
			fmt.Fprintf(stderr, "id %s: this is the only time that the token is shown\n", out.UserToken.ID)
		}
	case "list":
		var rows []api.UserToken
		if rows, err = c.UserTokens(ctx); err == nil {
			w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
			for _, r := range rows {
				state := "active"
				if r.RevokedAt != "" {
					state = "revoked"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.Principal, r.Scope, strings.Join(r.Repos, ","), r.ExpiresAt, state)
			}
			err = w.Flush()
		}
	case "revoke":
		err = c.RevokeUserToken(ctx, fs.Arg(0))
	}
	if err != nil {
		fmt.Fprintln(stderr, "motte token "+args[0]+":", err)
		return 1
	}
	return 0
}

// runLogin is `motte login [--print]`. Without --print it runs the device flow against MOTTE_OIDC_ISSUER with
// MOTTE_OIDC_CLIENT_ID and saves the token. With --print it prints the saved token.
func runLogin(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(stderr)
	print := fs.Bool("print", false, "print the saved token and exit")
	scope := fs.String("scope", "", "OAuth scope, default \"openid email\"")
	if fs.Parse(args) != nil || fs.NArg() != 0 {
		return 2
	}
	path, err := devicelogin.DefaultPath("motte")
	if err != nil {
		fmt.Fprintln(stderr, "motte login:", err)
		return 1
	}
	if *print {
		t, err := devicelogin.Load(path, time.Now())
		if err != nil {
			fmt.Fprintln(stderr, "motte login:", err)
			return 1
		}
		fmt.Fprintln(stdout, t.Value)
		return 0
	}
	if getenv("MOTTE_OIDC_ISSUER") == "" || getenv("MOTTE_OIDC_CLIENT_ID") == "" {
		fmt.Fprintln(stderr, "usage: MOTTE_OIDC_ISSUER=... MOTTE_OIDC_CLIENT_ID=... motte login [--print] [--scope S]")
		return 2
	}
	tok, err := devicelogin.Login(ctx, devicelogin.Config{Issuer: getenv("MOTTE_OIDC_ISSUER"), ClientID: getenv("MOTTE_OIDC_CLIENT_ID"), Scope: *scope,
		Prompt: func(uri, code, complete string) {
			if complete != "" {
				uri = complete
			}
			fmt.Fprintf(stderr, "Open %s and enter the code %s\n", uri, code)
		}})
	if err == nil {
		err = devicelogin.Save(path, tok)
	}
	if err != nil {
		fmt.Fprintln(stderr, "motte login:", err)
		return 1
	}
	fmt.Fprintln(stderr, "signed in")
	return 0
}
