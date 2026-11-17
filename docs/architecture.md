# Architecture

This document is the contract for motte. A change in behavior updates this document in the same pull request.

## Goal and non-goals

Goal: motte stores the repositories of coding agents and the state of their virtual workspaces. A control plane (the caller) creates repositories, claims workspaces and flushes their state. A node daemon and the jj backend read and write objects, refs and operation logs.

Non-goals:

- No git smart-HTTP, web UI, review or pull-request model, access lists, or users.
- No multi-region store. One store serves all callers.
- No compaction or deletion of packs, and no purge of deleted workspaces.

## Components

| Component | Responsibility | Phase |
|---|---|---|
| `grant` | Claims, Ed25519 signer, verifier. Public package. | M1 |
| `api` | Wire types and error codes. Public package. | M1 |
| `internal/objstore` | Object store seam with Cloud Storage and filesystem buckets. | M1 |
| `internal/gitpack` | Reader and writer of git pack files and v2 indexes. | M1 |
| Store service | HTTP service, PostgreSQL index, the epoch fence. | M2 |
| `client` | Typed Go client for callers. | M3 |
| `vfsd` and `volume` | Node daemon that serves a virtual workspace, and its volume attributes. | M4 |
| jj backend | `jj/`: the jj binary whose stores and working copy use the workspace daemon. Released for linux amd64 and arm64. | M5 |

## Planes

The store plane serves the node daemon and the jj backend. The control plane serves the caller. Every route needs a grant in the `Authorization: Bearer` header. A grant names the repository and the workspace. No route takes either from the request.

### Service

`cmd/motte` reads its configuration from the environment:

| Variable | Meaning |
|---|---|
| `MOTTE_LISTEN` | Listen address. Default `:8080`. |
| `MOTTE_DATABASE_URL` | PostgreSQL DSN. |
| `MOTTE_BUCKET_KIND` | `gcs` or `fs`. |
| `MOTTE_BUCKET` | Bucket name for `gcs`, directory for `fs`. |
| `MOTTE_TRUSTED_KEYS` | JSON list of `{"kid","key","scopes"}`. `key` is the standard base64 of an Ed25519 public key. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional. Enables OTLP trace export. |

The service applies its embedded migrations at start, under a PostgreSQL advisory lock, so replicas can start together. `GET /health` answers 200. `GET /ready` answers 200 while the database answers a ping. `GET /metrics` serves Prometheus metrics. These three routes need no grant.

### Store plane

| Route | Scopes |
|---|---|
| `GET /v1/vcs/commits/{sha}` | `node`, `read` |
| `GET /v1/vcs/trees/{sha}` | `node`, `read` |
| `GET /v1/vcs/objects/{sha}` | `node`, `read` |
| `POST /v1/vcs/blobs/batch` | `node`, `read` |
| `GET /v1/vcs/refs`, `GET /v1/vcs/refs/{name...}` | `node`, `read` |
| `POST /v1/vcs/objects/has` | `node`, `read`, `import` |
| `POST /v1/vcs/objects` | `node` |
| `PUT /v1/vcs/ops/{kind}/{id}` | `node` |
| `GET /v1/vcs/ops/{kind}/{id}` | `node` |
| `GET /v1/vcs/op-heads`, `PUT /v1/vcs/op-heads` | `node` |
| `PUT /v1/vcs/workspace` | `node` |
| `PUT /v1/vcs/refs/{name...}`, `DELETE /v1/vcs/refs/{name...}` | `node` |
| `PUT /v1/vcs/packs/{pack_id}` | `import` |

A ref route serves any repository, whether or not it has a `repos` row.

### Control plane

| Route | Scope |
|---|---|
| `POST /v1/repos` with `{name, owner}` answers 201 with `{path}` | `operator` |
| `GET /v1/repos` lists repositories, newest first | `operator` |
| `GET /v1/repos/{path}` | `operator` |
| `PUT /v1/workspaces/{id}/claim` with `{epoch, owner, repo}` returns `{epoch, op, snapshot}` | `operator` |
| `POST /v1/workspaces/{id}/flush` with `{epoch, generation, prev, op, snapshot}` | `operator` |
| `DELETE /v1/workspaces/{id}` | `operator` |

## Data model

```
repos(path pk, name, owner, created_at)
packs(repo, pack_id, bytes, created_at, pk(repo, pack_id))
refs(repo, name, commit_sha, generation, updated_at, pk(repo, name)); sequence ref_generation
workspaces(id pk, repo, owner, epoch bigint, op, snapshot, report_generation, created_at, deleted_at)
op_heads(workspace_id pk -> workspaces, epoch, generation, heads text[], updated_at)
```

A repository text has the form `<prefix>:<path>`. The prefix `native` is reserved for the repositories that `POST /v1/repos` creates. The prefix matches `^[a-z0-9][a-z0-9-]{0,62}$`. The path has 1 to 512 bytes of UTF-8. Object keys are `repos/<prefix>/<base64url path>/...` and `ops/<workspace>/...`. A repository that the caller owns elsewhere has no `repos` row. The store keeps only the packs and refs of such a repository.

A ref generation comes from the sequence `ref_generation`. It is never reused, including when a ref is deleted and created again. A swap with `expect` 0 creates the ref.

### Epoch fence

`workspaces.epoch` is the highest epoch that the caller claimed.

- Claim: `INSERT ... ON CONFLICT (id) DO UPDATE SET epoch = GREATEST(workspaces.epoch, EXCLUDED.epoch) WHERE deleted_at IS NULL RETURNING epoch, op, snapshot`. A claim below the stored epoch answers 409 `vcs_claim_lost`. A claim on a deleted workspace answers 410. `owner` and `repo` are set once. A different value answers 409.
- A claim needs `epoch` of 1 or more. A claim on a `native` repository that has no row, or whose `repos.owner` differs from `owner`, answers 403 `vcs_repo_forbidden`. A claim whose `owner` or `repo` differs from the stored value answers 409 `vcs_workspace_conflict`. A claim on a deleted workspace answers 410 `vcs_workspace_deleted`.
- Write: every ref swap, ref delete and op-heads swap is one statement. It starts with `WITH guard AS (SELECT id FROM workspaces WHERE id = $ws AND epoch = $epoch AND deleted_at IS NULL FOR SHARE)`. The write applies only when the guard row exists. A workspace report is one `UPDATE` of the workspace row with the same epoch, repository and `deleted_at` conditions in its `WHERE`. It takes the row lock itself, because two reports that hold `FOR SHARE` and then update the row can deadlock.
- The claim `UPDATE` takes the row lock that `FOR SHARE` conflicts with. A write in flight finishes first, and every later write of the old epoch fails. `vcs_claim_lost` wins over `vcs_generation_conflict`. An op-heads write also needs `op_heads.epoch <= $epoch`.
- A `node` route compares the grant epoch with `workspaces.epoch` before it runs, for reads and writes. A missing or deleted workspace answers 401. A repository that differs from `workspaces.repo` answers 403 `vcs_repo_forbidden`. A different epoch answers 409 `vcs_claim_lost`. Every write statement also needs `workspaces.repo` to equal the grant repository.

### Raise before commit

The caller raises the epoch before it commits its own increment. The caller calls claim with the next epoch, then commits the increment. A failed claim fails the step before the commit, and the step retries. Claim is idempotent. `GREATEST` keeps the highest of two racing claimers. If the commit then loses to another claimer, that claimer raises again. A raise with no commit refuses only the writes of an epoch that the caller already replaces. A workspace of the new epoch, and so its first grant, does not exist before the raise.

### Workspace state

`op`, `snapshot` and `report_generation` live on `workspaces`. A node report and a flush write one row with one ordering rule: the higher `report_generation` wins, and an equal one writes again, because a flush at the generation of a report carries a newer `snapshot`. A flush with generation 0 writes only while the stored `op` equals `prev`. A write that loses on this ordering rule answers success and changes nothing. A flush runs under the same epoch fence as a node write: a stale `epoch` answers 409 `vcs_claim_lost`, a deleted workspace 410, and an unknown workspace 404. A flush answers 204.

### Delete

`DELETE /v1/workspaces/{id}` sets `deleted_at`. Every later read and write of that workspace fails. The call is idempotent, answers 204, and does nothing for an unknown workspace. The store removes no op-head row and no object.

### Repository creation

`POST /v1/repos` creates the repository with one root commit under the ref `main`. The root commit has the empty tree, the author and committer `motte <motte@motte.invalid>` and the creation time as its time. The store writes the root objects first, then the `repos` row and the ref in one transaction, so a failed create leaves no repository without `main`. The path is `repo_` and random base32 text. The name is trimmed and has 1 to 100 bytes with no control character, else 400 `invalid_body`. A workspace can start on `main` at once.

## jj backend

The crate `jj/` builds the binary `jj`: jj 0.47.1 (`jj-lib` and `jj-cli`, pinned exactly) with four shims and the subcommand `jj motte`. The binary keeps the name `jj` because it is jj with extra stores. A release attaches it for linux amd64 and arm64, with sha256 sums.

### Shims

| Shim | Registered name | Role |
|---|---|---|
| `Backend` | `motte-objects` | Reads and writes git objects through the workspace daemon. |
| `OpStore` | `motte-op-store` | jj's simple op store, with the objects mirrored to the daemon. |
| `OpHeadsStore` | `motte-op-heads` | Op heads in local files, swapped into the store with a generation. |
| `WorkingCopy` | `motte-vfs` | A working copy on a kernel overlay over the lazy lower layer of the daemon. |

The names are written into `.jj/repo/*/type` and `.jj/working_copy/type`. A rename is a migration.

The backend writes the bytes of `GitBackend::write_commit` (the `change-id` and `jj:trees` headers, the signing path, and the retry that lowers the committer time when a commit ID exists with other extras) and git trees in git entry order. A commit ID is the git SHA. `jj/tests/differential.rs` proves that the object IDs equal those of stock `GitBackend`. The working copy reads the dirty set of the daemon instead of walking the tree. `.jj` is a symlink in the mount to a directory outside it. `set_sparse_patterns` is unsupported.

### Daemon channel

The shims speak to the workspace daemon with one JSON object per line over the unix socket that `MOTTE_VFS_SOCK` names. An error answer carries `err`. The shim hashes every object itself and refuses an answer whose SHA differs. A daemon that is not in overlay mode fails every command that needs the dirty set. The operations are:

| op | request | answer |
|---|---|---|
| `get` | `sha` | `type`, `data` (base64) |
| `put` | `type`, `data` (base64) | `sha` |
| `putbatch` | `objects`: a list of `{type, data}` | `shas`, in request order |
| `opheads` | `expect` and `heads` (both optional) | `gen`, `heads` |
| `refs` | `name` (optional) | `refs`: a list of `{name, commit, generation}`; `ref` with `name` |
| `swapref` | `name`, `expect`, `commit`, or `delete: true` with `expect` | `ref`; none after a delete; on a lost swap `code` `vcs_generation_conflict` with `err` and the stored `ref` |
| `putop` | `type` (`operations` or `views`), `sha`, `data` (base64) | none |
| `getop` | `type`, `sha` | `data` (base64) |
| `getbatch` | `shas`, `max_size` (optional) | `datas` (base64); `large`: the indices of blobs over `max_size`, sent without data |
| `has` | `shas` | `present`: one bool for each name; `unknown op` from a daemon that cannot answer |
| `changes` | none | `boot`, `token`, `base_tree`, `changes`: a list of `{path, status, old_sha, old_mode, old_size, new_sha, new_mode, size}` |
| `dirty` | none | `boot`, `token`, `base_tree`, `entries`: a list of `{path, kind, exec, opaque}` |
| `token` | none | `boot`, `token`, `base_tree` |

Every request carries the W3C `traceparent` of the innermost open span, or the `TRACEPARENT` environment variable. With `OTEL_EXPORTER_OTLP_ENDPOINT` set, the binary exports OTLP over plain HTTP, with no retry and a 500 ms limit: one span `jj.command` for each command and one for each operation (`jj.motte.snapshot`, `jj.snapshot`, `jj.checkout`, `jj.read_tree`, `jj.write_commit`, `jj.op_heads.update`, `jj.push`, `jj.fetch`), with the counts `objects` and `bytes`. `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` turns tracing off. A span holds no path, file content or credential. A snapshot sends blobs with `putbatch`, at most 64 blobs or 8 MiB a request, and falls back to `put` on `unknown op`.

### Op heads

`opheads` without `heads` reads the op heads of the workspace; a store `vcs_not_found` before the first write is generation 0 and no heads. With `heads` it swaps them when the stored generation is `expect`. Each update of `motte-op-heads` first reads the remote heads, so a writer whose claim is lost fails with `vcs_claim_lost` before its local write. It then writes the local heads, removes from the remote set the replaced operations and every remote head that the local op store holds and the local heads do not, adds the new head and swaps. On `vcs_generation_conflict` it reads the winning heads and applies its update again. All daemon calls of one update share a budget of 2 seconds. A daemon that answers `unknown op`, any other store failure, or a spent budget leaves the remote heads behind the local ones and the command succeeds; the failure adds the new head to the marker `motte_unsynced` in the repo directory. A lost claim at the swap fails the command after the local write.

### Subcommands

`jj motte` has the subcommands `init`, `restore`, `attach`, `verify`, `flush`, `sync`, `snapshot`, `changes`, `push`, `fetch` and `dirty`. `init`, `restore` and `attach` need the `base_tree` of the daemon to be the tree of the mount commit. Every jj command except `jj motte snapshot`, and the ones that never snapshot (`--ignore-working-copy`), holds a shared flock on `motte_command.lock` in the repo directory.

`jj motte changes` prints the work-tree diff as JSON for a caller that sets `HARNESS_CHANGES_SCOPE` (`branch` or `uncommitted`), `HARNESS_CHANGES_BASE` and `HARNESS_CHANGES_PATCH_BYTES` (default 1 MiB). It loads the op head that ended last, in whole seconds, then the larger ID. It never snapshots and writes no operation. It diffs the target tree against the daemon base tree and overrides each path that `changes` names. The patch is in git format and ends at the last whole file within the byte cap, with `truncated` set. A file larger than 2 MiB is `large` with no hunk. Renames are not detected.

### Push and fetch to a git origin

A workspace has a git origin when `init` or `restore` got `--origin-url`, an http(s) URL with no user information, and `--commit` or `--base`, which set the repo config key `motte.origin-base` to the commit that origin holds. `--origin-branch <name>` makes, at the start commit, the local bookmark `<name>` and the tracked bookmark `<name>@origin`, and sets `revset-aliases.'trunk()'` to `"<name>"@origin`. The only remote is `origin`. The commands keep a bare repo `motte_git` in the repo directory as the transport, run the `git` binary over HTTPS with `GIT_TERMINAL_PROMPT=0`, and share a deadline of 10 minutes.

`jj motte push -b <bookmark>...` refuses a missing or conflicted bookmark, a new commit that is conflicted, has no description or is the root commit, a rewrite of the default branch of origin, and a workspace with no `MOTTE_VFS_SOCK`. It probes the claim with `opheads`, reads the tips of origin (`git ls-remote --symref`), fetches the base of each bookmark by SHA with no blobs (falling back to heads at depth 20, 200, 1,000, then unshallow), writes the loose objects of the new commits from the daemon, probes the claim again, and runs `git push --atomic --force-with-lease=refs/heads/<name>:<commit of NAME@origin>`. One jj transaction then sets `NAME@origin`. A bookmark whose tip already equals its commit is recorded without an update. The span `jj.push` carries `remote`, `refs` and `outcome`: `ok`, `refused`, `stale`, `rejected`, `network` or `claim_lost`.

`jj motte fetch [-b <bookmark>...]` probes the claim, reads the heads of origin (at most 100 without `-b`, the default branch first), clears `refs/motte/origin/` and `refs/motte/base/` of `motte_git`, and fetches the heads that differ into `refs/motte/origin/` with `--no-tags --no-filter --depth=N`, `N` being 1, 20, 200 and 1,000 (`--refetch` when a `refs/motte/base/` ref existed). A commit whose parent is in neither `motte_git` nor the daemon at depth 1,000 is a shallow root: the backend records it in the `shallow` file, published whole under an exclusive lock on `shallow.lock`, and shows it with no parents. The command refuses a commit whose `encoding` header is not UTF-8. It writes the new commits to the daemon with `putbatch`, parents first, each after the blobs and trees that differ from its first parent. One jj transaction makes the tips heads and sets `NAME@origin`. A local bookmark moves to the tip when `NAME@origin` is tracked, the local target equals the old `NAME@origin` and the old commit is an ancestor of the tip, or when neither existed. The span `jj.fetch` has the outcomes of push except `stale`.

### Push and fetch to a native repository

A native workspace has no git origin: the store is the origin. `init` and `restore` take `--origin-native`, which excludes `--origin-url`, and set `motte.origin-native` to true and `motte.origin-base` to the start commit. `--origin-default-branch` requires it and sets `motte.origin-default-branch`. A native workspace without `motte.origin-base` or `motte.origin-default-branch` makes push, fetch and delete fail as `refused`; none falls back to a git origin. A ref of the store is a bookmark of `origin`. The commands run no `git` and make no network call of their own. They need no flush, because `put`, `putbatch` and `snapshot` answer after the upload to `POST /v1/vcs/objects`.

`jj motte push -b <bookmark>...` refuses what the git push refuses, and a rewrite of the default branch or the start branch: the stored tip must be an ancestor of the commit. The new commits are the ancestors of the bookmarks that are not ancestors of `motte.origin-base` or a `NAME@origin`, at most 1,000 for each bookmark. The steps are:

1. Probe the claim with `opheads`.
2. Read the ref of each bookmark with `refs` and `name`.
3. Check that the store holds the new commits, their trees and their blobs, comparing each commit only with its first parent and reading only trees whose ID differs. Blobs are asked with `has`; a daemon that answers `unknown op` gets `getbatch` with a size limit of one byte. A missing object stops the push as `network`.
4. Probe the claim again.
5. In name order, send `swapref` with `expect` set to the stored generation, or 0 to create the ref. A ref that is at the commit needs no swap. A ref that differs from the commit that `NAME@origin` records ends the push as `stale` with "origin moved NAME; run jj motte fetch first". `vcs_generation_conflict` is `stale`. `vcs_claim_lost` is `claim_lost`.
6. One jj transaction sets `NAME@origin`, tracked, for each bookmark that the store holds at the commit.

The swaps are not atomic across refs: the first failure stops the push, and the error names the bookmarks that this call moved. There is no force option: a rewrite is a compare-and-swap against `NAME@origin`. `jj motte push --delete NAME` (repeatable, never with `-b`) deletes refs of a native repository only. It refuses the default branch and the start branch, and a name with no ref and no `NAME@origin`. It ends as `stale` when the stored commit differs from the commit that `NAME@origin` records. It forgets `NAME@origin` without a swap when the ref is already gone, sends `swapref` with `delete` for the rest, and forgets `NAME@origin` of the deleted refs in one transaction. The local bookmark stays.

`jj motte fetch [-b <bookmark>...]` probes the claim, lists the refs with `refs`, and takes the named bookmarks, or the default branch and the first 99 others in name order. It refuses a named bookmark with no ref and no `NAME@origin`. A bookmark with no ref but with a `NAME@origin` was deleted: the command forgets `NAME@origin`, keeps the local bookmark and prints `NAME: deleted on origin`. For each other bookmark it reads the commit with `get` and refuses a missing commit or an object that is not a commit. It walks the commits that the jj index lacks, at most 1,000 for each bookmark, with the shallow-root rules of the git fetch. One jj transaction adds the roots to the `shallow` file, makes the tips heads and sets `NAME@origin` with the local-bookmark rules of the git fetch.

## Grant

A grant is `mg1.<base64url payload>.<base64url signature>`. The payload is JSON. The signature is Ed25519 over the base64url payload text. The encoding has no padding.

| Claim | Meaning |
|---|---|
| `kid` | Key identifier. The verifier selects the public key by `kid`. |
| `scope` | One of `node`, `read`, `import`, `operator`. |
| `ws` | Workspace identifier. |
| `epoch` | Claim epoch of the workspace when the grant was minted. |
| `repo` | Repository text. |
| `owner` | Owner of the workspace. |
| `subject` | The creator, for traces only. It grants nothing. |
| `exp` | Expiry as Unix seconds. A grant is valid while `now < exp`. |

Required claims by scope: `node` needs `ws`, `epoch` and `repo`. `read` and `import` need `repo`. `operator` needs none.

The verifier holds only public keys. Each key has a `kid` and a set of allowed scopes. The verifier rejects a grant whose scope is not allowed for its key. Every rejection answers 401 with the same error, and the response never carries the grant.

### Keys

The caller owns the private keys. Two signers exist:

- The control-plane key may sign all scopes.
- The node key lives on the node daemon and may sign only `node`. A compromised node cannot mint `import` or `operator` grants. It can mint `node` grants, bounded by the repository and epoch of the workspace row.

### Rotation

1. Add the new public key to the store.
2. Switch the signer to the new key.
3. Remove the old key after the longest grant life: 1 hour for `node`, 60 seconds for `operator`, `read` and `import`.

Both keys are valid between step 1 and step 3.

### Read authorization

Authorization happens at claim time. For a repository that the store keeps (it has a `repos` row), claim checks `repos.owner` against `owner`. For any other repository, the caller checks that the creator may read it and signs the claim. The store trusts that and binds the workspace to the repository.

## Versioning

The path prefix `/v1` is additive. New fields and new routes are allowed, and clients ignore unknown fields. A breaking change gets `/v2`, served beside `/v1`. The module is tagged with semantic versions.

Wire error codes keep their `vcs_` text: `vcs_claim_lost`, `vcs_generation_conflict`, `vcs_not_found`, `vcs_object_invalid`, `vcs_object_too_large`, `vcs_repo_forbidden`, `vcs_workspace_conflict`, `vcs_workspace_deleted`, `vcs_not_configured`. The error body is `{"error":{"code","message","details"}}`. A conflict on a ref carries the stored ref in `details.ref`.

## Observability

- Traces: OTLP, with the endpoint from the environment and no vendor in the code. Span names start with `vcs.serve.` and end with the route, such as `vcs.serve.put.refs`. Requests accept W3C trace headers.
- Metrics: Prometheus at `/metrics`, with names that start with `motte_`: `motte_http_requests_total` and `motte_http_request_duration_seconds`, by route.
- Logs: JSON. A log line never holds a grant, a path or an object body.
- Instrumentation is additive. It never changes behavior or errors.
