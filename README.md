# motte

motte is a version control store and a set of virtual workspaces for coding agents. It is built on [jj](https://github.com/jj-vcs/jj).

## Status

The store service, the client, the workspace daemon and the jj backend exist. Each item below is in `docs/architecture.md`.

- Native repositories: `motte repo create NAME` creates one.
- Git smart HTTP (protocol v2): clone, fetch and push.
- The jj backend (`jj motte`) and the workspace daemon `motte-vfsd`. The daemon runs on Linux.
- Protected refs. Only a land with a `merge` grant changes `main`.
- The land API: it rebases a change onto the tip, answers 409 for a conflict or a stale `expect_tree`, and batches lands on one ref.
- Ref events and subscriptions.
- Metadata in PostgreSQL or SQLite, and objects in Cloud Storage, S3 or a directory, each behind an interface.
- The service opens the pack indexes before it answers ready.
- Per-user tokens and OIDC identities for a person (see Developing).
- Releases: service and daemon images, and the `jj` binary for Linux amd64 and arm64.

## Run locally

Run motte with no other service: `go test ./...` runs the tests that need no database, and the section Run locally of `docs/architecture.md` starts the service on SQLite and a directory. The service serves TLS when `MOTTE_TLS_CERT` and `MOTTE_TLS_KEY` are set, and runs behind any reverse proxy or tunnel otherwise.

Read `docs/architecture.md` for the design. Read `AGENTS.md` before you change the repository.

The GitHub repository is a read-only mirror. Changes land in the canonical motte repository through [ward](https://github.com/majorcontext/ward), which mirrors its branches and tags to GitHub. Pull requests on GitHub cannot merge. Open an issue to propose a change.

## Developing

A person needs no identity provider, no workspace box and no tunnel. `URL` is the address of the deployment and `REPO` is the repository path.

An administrator who holds an operator grant (`MOTTE_TOKEN`; `motte operator-grant --key KEYFILE --kid KID` prints one) issues a token for you:

```
MOTTE_URL=URL MOTTE_TOKEN=... motte token create --principal you@example.test --scope write --repo native:REPO
```

The command prints the token once. Git sends it as the password:

```
git clone https://you:TOKEN@URL/git/REPO.git motte
cd motte
ward check
git switch -c my-change
git push origin my-change
ward land REPO my-change
```

`motte token list` shows the tokens and `motte token revoke ID` revokes one. `motte repo create NAME` creates a repository for a token with the scope `admin`.

`ward check` runs the checks of `.ward/checks.json` on your machine. `git push` writes a branch and never `main`: only `ward land` lands a branch on `main`.

A deployment that trusts an OIDC issuer (`MOTTE_OIDC_*`) lets you sign in without a token. `MOTTE_OIDC_ISSUER=ISSUER MOTTE_OIDC_CLIENT_ID=ID motte login` runs the OAuth device flow, which any issuer with device support offers, and saves the token in your configuration directory with mode 0600. `git -c http.extraHeader="Authorization: Bearer $(motte login --print)" clone ...` uses it. An identity-aware proxy that signs a JWT in its own header, such as Cloudflare Access, is one configuration of the same verifier: set `MOTTE_OIDC_HEADER`, `MOTTE_OIDC_ISSUER`, `MOTTE_OIDC_AUDIENCE` and `MOTTE_OIDC_KEYS_URL` to its values.

License: MIT. The `jj/` crate is Apache-2.0, because it adapts code from jj-lib.
