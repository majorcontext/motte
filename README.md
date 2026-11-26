# motte

motte is a version control store and a set of virtual workspaces for coding agents. It is built on [jj](https://github.com/jj-vcs/jj).

## Status

The store service, the client, the workspace daemon and the jj backend exist. Each item below is described in `docs/architecture.md`.

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

Clone without `--depth`: the service does not offer shallow fetches.

`motte token list` shows the tokens and `motte token revoke ID` revokes one. `motte repo create NAME` creates a repository for a token with the scope `admin`.

`ward check` runs the checks of `.ward/checks.json` on your machine. `git push` writes a branch and never `main`: only `ward land` lands a branch on `main`.

A deployment that trusts an OIDC issuer (`MOTTE_OIDC_*`) lets you sign in without a token. `MOTTE_OIDC_ISSUER=ISSUER MOTTE_OIDC_CLIENT_ID=ID motte login` runs the OAuth device flow, which any issuer with device support offers, and saves the token in your configuration directory with mode 0600. `git -c http.extraHeader="Authorization: Bearer $(motte login --print)" clone ...` uses it. An identity-aware proxy that signs a JWT in its own header, such as Cloudflare Access, is one configuration of the same verifier: set `MOTTE_OIDC_HEADER`, `MOTTE_OIDC_ISSUER`, `MOTTE_OIDC_AUDIENCE` and `MOTTE_OIDC_KEYS_URL` to its values.

License: MIT. The `jj/` crate is Apache-2.0, because it adapts code from jj-lib.

## Benchmarking on a host

`motte-loadtest bench` needs a Linux host with Docker, `gcloud` and a bucket. It makes none of them. One way to make them:

```
gcloud iam service-accounts create bench --project P
gcloud storage buckets create gs://B --project P --location us-central1 --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets add-iam-policy-binding gs://B --member=serviceAccount:bench@P.iam.gserviceaccount.com --role=roles/storage.objectAdmin
gcloud compute instances create bench-1 --project P --zone us-central1-a --machine-type n2-standard-16 --image-family debian-12 --image-project debian-cloud --boot-disk-size 200GB --boot-disk-type pd-ssd --service-account bench@P.iam.gserviceaccount.com --scopes storage-rw
```

Install Docker on the host, copy two builds of `motte` (static, Linux) and one of `motte-loadtest`, and run `motte-loadtest bench pairs -a motte-old -b motte-new -bucket B -key motte.key -c 64 -n 4 -docker "sudo docker"`. Delete the host, the bucket and the service account when the measurement ends.
