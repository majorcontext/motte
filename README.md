# motte

motte is a version control store and a set of virtual workspaces for coding agents. It is built on [jj](https://github.com/jj-vcs/jj).

Status: early. The repository holds milestone M1: the specification, the grant format, the wire types and the store primitives. The store service, the client, the workspace daemon and the jj backend follow.

Run it with no other service: `go test ./...` runs the tests that need no database, and the section Run locally of `docs/architecture.md` starts the service on SQLite and a directory. The metadata store and the object store are interfaces (`meta`, `bucket`) with PostgreSQL, SQLite, Cloud Storage and filesystem backends.

Read `docs/architecture.md` for the design. Read `AGENTS.md` before you change the repository.

The GitHub repository is a read-only mirror. Changes land in the canonical motte repository through [ward](https://github.com/majorcontext/ward), which mirrors its branches and tags to GitHub. Pull requests on GitHub cannot merge. Open an issue to propose a change.

## Developing

Develop motte on motte with no workspace box. A deployment that turns on Access identities (see `docs/architecture.md` "Access identities") serves the git routes to a person behind Cloudflare Access. `HOST` is the host name of the deployment and `REPO` is its repository path.

```
cloudflared access login https://HOST
git -c http.extraHeader="cf-access-token: $(cloudflared access token -app=https://HOST)" clone https://HOST/git/REPO.git motte
cd motte
ward check
git switch -c my-change
git -c http.extraHeader="cf-access-token: $(cloudflared access token -app=https://HOST)" push origin my-change
ward land REPO my-change
```

A new repository needs no other system: `MOTTE_URL=https://HOST MOTTE_ACCESS_TOKEN="$(cloudflared access token -app=https://HOST)" motte repo create NAME` prints its path, for an identity that the deployment lists as an administrator.

`ward check` runs the checks of `.ward/checks.json` on your machine. `git push` writes a branch and never `main`: only `ward land` lands a branch on `main`. The token lasts until the Access session ends; run `cloudflared access login` again when a command answers 401.

License: MIT. The `jj/` crate is Apache-2.0, because it adapts code from jj-lib.
