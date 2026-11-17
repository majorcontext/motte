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
| `client` | Typed Go client for callers. Public package. | M3 |
| `testserver` | Starts a real server in a caller's test. Public package. | M3 |
| `internal/vfs`, `cmd/motte-vfsd`, `volume` | Node daemon that serves a virtual workspace, and its public volume attributes. | M4 |
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

`opheads` without `heads` reads the op heads of the workspace; a store `vcs_not_found` before the first write is generation 0 and no heads. With `heads` it swaps them when the stored generation is `expect`. Each update of `motte-op-heads` first reads the remote heads, so a writer whose claim is lost fails with `vcs_claim_lost` before its local write. It then writes the local heads, removes from the remote set the replaced operations and every remote head that the local op store holds and the local heads do not, adds the new head and swaps. On `vcs_generation_conflict` it reads the winning heads and applies its update again. All daemon calls of one update share a budget of 2 seconds. A daemon that answers `unknown op` leaves the remote heads behind the local ones and the command succeeds. Any other store failure, or a spent budget, does the same and also adds the new head to the marker `motte_unsynced` in the repo directory. A lost claim at the swap fails the command after the local write.

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

## Client

Package `client` has one typed method for each route of both planes. A `Client` sends one grant. The caller gives a `GrantFunc` that the client calls before each request, so a caller holds one `Client` for each scope that it uses.

- Each call has a time budget of 2 seconds, which `WithBudget` changes. A deadline on the call context that is earlier than the budget wins.
- The client injects the W3C trace context of the call context into the request headers.
- The client follows no redirect. A 3xx answer is the error `ErrRedirect`, and the grant goes to no other host.
- A failure answer is an `*Error` with the HTTP status, the wire code, the message and the details. `errors.Is` matches the sentinel of the code: `ErrNotConfigured`, `ErrNotFound`, `ErrGenerationConflict`, `ErrClaimLost`, `ErrObjectInvalid`, `ErrObjectTooLarge`, `ErrRepoForbidden`, `ErrWorkspaceConflict`, `ErrWorkspaceDeleted`, `ErrUnauthorized`, `ErrInvalidBody` and `ErrInternal`. A failure answer with no wire error body is `ErrUnexpected`.
- `CurrentRef` returns the stored ref that a ref generation conflict carries.

Package `testserver` runs the real server in the process of a test, on a fresh schema of the PostgreSQL database that the caller names (or `MOTTE_TEST_DATABASE_URL`) and a temporary filesystem bucket. It skips the test when no DSN is set. It returns the base URL, the control-plane and node signers, and helpers that mint operator, node, read and import grants.

## Observability

- Traces: OTLP, with the endpoint from the environment and no vendor in the code. Span names start with `vcs.serve.` and end with the route, such as `vcs.serve.put.refs`. Requests accept W3C trace headers.
- Metrics: Prometheus at `/metrics`, with names that start with `motte_`: `motte_http_requests_total` and `motte_http_request_duration_seconds`, by route.
- Logs: JSON. A log line of the service never holds a grant, a path or an object body. A log line of the node daemon never holds a grant, and it carries the error text of its own process, which can name a node path.
- Instrumentation is additive. It never changes behavior or errors.

## Virtual workspace daemon

`internal/vfs` is the node daemon of the virtual workspace. `cmd/motte-vfsd` runs it. It reads and writes the store plane with a `node` grant, so a workspace holds no store credential. The FUSE server and the overlay build on Linux only. On other systems `motte-vfsd` prints a message and exits 1. The daemon writes JSON logs, and `OTEL_EXPORTER_OTLP_ENDPOINT` enables trace export. A client span name is `vcs.client.` and the route, such as `vcs.client.ref.swap`.

`HTTPStore` is the store client of the daemon. It sends the grant as a bearer token, injects W3C trace headers, follows no redirect, and answers 403 with `ErrGrantRefused`, which no retry passes. A 409 `vcs_generation_conflict` answer becomes `RefConflictError` with the stored ref from `details.ref`. A test drives `HTTPStore` against the real service.

### Commands

| Command | Meaning |
|---|---|
| `motte-vfsd mount` | Serves the tree of one commit as the read-only lower layer of a kernel overlay. Needs `-api`, `-commit`, `-mnt`, `-upper` and `-ctl-sock`, and `MOTTE_VFS_GRANT` in the environment. It does not refresh the grant. |
| `motte-vfsd csi` | The CSI node plugin. Needs `-api` and `-grant-key-file`. Flags `-driver` (default `vfs.motte.dev`), `-endpoint`, `-node-id`, `-state`, `-uid` and `-gid`. |
| `motte-vfsd materialize` | Writes the checkout of a commit to `-dest`. A marker file records a finished checkout, and a later run changes nothing, whatever commit it names. |
| `motte-vfsd import` | Fetches a repository with `git`, packs it and uploads it with an `import` grant (`PUT /v1/vcs/packs/{pack_id}`). `-depth 0` imports the whole history as packs of about 48 MiB and skips the ranges that the store holds. |

The key file of `csi` is JSON `{"kid","key"}`. `key` is the standard base64 of an Ed25519 private key or of its 32-byte seed. The store must trust the public key for the scope `node`. The plugin signs a `node` grant for each store call. The grant names the workspace, the claim epoch and the repository of the volume attributes, and it expires after one hour.

### Mount mode

The FUSE mount is the read-only lower layer. Every mutating operation answers `EROFS`: `mkdir`, `symlink`, `create`, `unlink`, `rmdir`, `rename`, `setattr`, `write`, `fallocate`, and an `open` with a write flag. `fsync` answers success. Entry, attribute and negative timeouts are one hour, because the lower never changes while the mount lives. A lookup reads trees in bulk (`GET /v1/vcs/trees/{sha}`). The first read of a file reads its blob and the blobs that follow it in tree order in one batch (`POST /v1/vcs/blobs/batch`). A node cache that all workspaces on the node share keeps the blobs.

`mount` refuses to start without `-upper` and `-ctl-sock`. It binds the socket before it mounts. It runs `upperdir.ProbeTrustedBeside` first and fails with a `CAP_SYS_ADMIN` error when the process cannot read `trusted.*` attributes on the file system of the upper directory. The probe uses a new sibling directory and never writes into the upper directory.

The control socket lies in a directory that no other workspace can reach. The daemon refuses a directory that is open to others, creates the socket with mode 0660 and gives it to the uid and gid of the mount.

### Node plugin

The plugin implements the identity and node services of the CSI spec on a unix socket. The chart runs it as a privileged DaemonSet with `hostPID`, and its kubelet pods directory propagates to the host. `csi` refuses to start unless it shares the PID namespace of the node and has another mount namespace. The `CSIDriver` sets `attachRequired: false`, `podInfoOnMount: true` and the lifecycle mode `Ephemeral`. A restart of the plugin ends the mounts that it serves, so the DaemonSet updates with `OnDelete`.

`NodePublishVolume` of a workspace volume runs these steps in order:

1. Refuse a volume id with `,`, `:`, `\` or `/`. Read the `mountinfo` of the plugin and of one process of each other mount namespace.
2. Run `ProbeTrustedBeside` on `<vol>/upper`.
3. Unmount a stale overlay at the target, then a stale lower at `<vol>/lower`. `EBUSY` answers `Unavailable`. While any overlay with `upperdir=<vol>/upper` is mounted anywhere, the publish fails with `Unavailable`.
4. Discard `upper` when it has entries and `<vol>/base` is missing or names another commit. Recreate `work`, create `upper`, and write `<vol>/base` (write `base.new`, `fsync`, rename, `fsync` of `<vol>`).
5. Open the VFS, bind the control socket, mount the lower at `<vol>/lower`, serve the socket, and mount overlayfs at the target with `lowerdir=<vol>/lower,upperdir=<vol>/upper,workdir=<vol>/work,redirect_dir=on,metacopy=on,index=off,xino=off,nfs_export=off`.

A failure in step 5 undoes the earlier steps in reverse order. A publish for a volume that the plugin serves returns OK and changes nothing. The control socket of a pod lies in `ctl/<pod UID>` of the state directory, mode 0700, owned by the uid and gid of the mount. The pod UID comes from the kubelet, so a pod cannot name the directory of another pod, and a publish without it fails. A volume with the attributes `volume=ctl` binds that directory at its target, in either publish order.

`NodeUnpublishVolume` unmounts the overlay, reads the `mountinfo` again, and fails with `Unavailable` while an overlay still references `<vol>/upper`. Only then it unmounts the lower (lazily only when the plain unmount fails), stops the socket, closes the VFS, and deletes the control directory of the pod and `<vol>`. It never runs `stat` on `<vol>/lower`, because a dead FUSE answers `ENOTCONN`. The directories `upper` and `work` live on the disk of the node with the lifetime of the volume. Only `NodeUnpublishVolume` deletes them.

### Volume attributes

Package `volume` is the public contract between a caller and the plugin. A workspace volume has the attributes `workspace`, `epoch`, `repo`, `commit` and `runtime`. `runtime` only labels telemetry. `ParsePublication` rejects a missing workspace or repository and a commit that is not a SHA-1. The control socket volume has `volume=ctl`. The workspace sees the socket at `volume.CtlDir/volume.CtlSocket` (`/run/motte-vfs/ctl.sock`), and `MOTTE_VFS_SOCK` names it. The CSI driver name is a flag of `csi` and the chart value `driverName`.

### Control protocol

One JSON object for each line over the unix socket. An error answer carries `err`. A lost ref swap carries `code` `vcs_generation_conflict`, `err` and the stored `ref`. An unknown op answers `unknown op`.

| op | request | answer |
|---|---|---|
| `get` | `sha` | `type`, `data` (base64) |
| `put` | `type`, `data` (base64) | `sha` |
| `putbatch` | `objects`: list of `{type, data}` | `shas`, in request order |
| `opheads` | `expect`, `heads` (both optional) | `gen`, `heads` |
| `refs` | `name` (optional) | `refs`: list of `{name, commit, generation}`; `ref` with `name` |
| `swapref` | `name`, `expect`, `commit`, or `delete: true` with `expect` | `ref`, none after a delete |
| `putop` | `type` (`operations` or `views`), `sha`, `data` (base64) | none |
| `getop` | `type`, `sha` | `data` (base64) |
| `getbatch` | `shas`, `max_size` (optional) | `datas` in request order; `large`: indices of blobs over `max_size`, sent without data |
| `has` | `shas` | `present`: one bool for each name, from `POST /v1/vcs/objects/has` |
| `changes` | none | `boot`, `token`, `base_tree`, `changes`: list of `{path, status, old_sha, old_mode, old_size, new_sha, new_mode, size}` |
| `dirty` | none | `boot`, `token`, `base_tree`, `entries`: list of `{path, kind, exec, opaque}` |
| `token` | none | `boot`, `token`, `base_tree` |
| `workspace` | `sha` (the head operation), `commit`, `generation` | none |
| `stats` | none | cache counters `blob_hits`, `blob_misses`, `tree_calls` |

### Dirty set

The daemon builds the dirty set from a walk of the upper directory with the `trusted.overlay.*` attributes, always against the base tree of the mount. `upperdir.Scan` applies these rules:

| Upper entry | Dirty set |
|---|---|
| Regular file or symlink | `file` or `symlink`, with `exec` from the upper mode |
| Fifo, socket or device that is no whiteout | Nothing when the base lacks the path, else `deleted` |
| Whiteout | `deleted` when the base has the path |
| Directory with `trusted.overlay.opaque=y` | `dir` with `opaque` when the base has the path; its children are new |
| Directory that only holds entries | Nothing for the directory; its changed children |
| Empty upper directory that the base lacks | `dir` |
| File with `trusted.overlay.metacopy` | Nothing when it lies at its own base path, its redirect names that path and its exec bit equals the base; else `file` |
| Entry with `trusted.overlay.redirect` | The origin is the base path in the value. The merged children are the origin subtree plus the upper children. |
| Directory over a base file, file over a base directory | `dir` with `opaque`, or `file` |

A directory that merges a base directory at another path lists every base leaf below the origin at its new path, and `Origin` names the base path of the content. The root entries `.jj` and `.git` are skipped. The result is sorted by path. A submodule is an empty directory in the mount.

`OSFS` opens each directory once, one component at a time, and checks with `fstatat` and `fstat` that each opened inode is the directory that `lstat` found, so a swapped-in symlink is never followed. `changes` takes the base side from the base tree. It hashes the bytes of an upper file only when the base has its path, at most 2 MiB for each file and 64 MiB in all.

### Change token

`token` and `dirty` return `<boot>:<fingerprint>`. The fingerprint comes from an `lstat` walk: the entry count, the newest change time, the sum of the sizes of the regular files, the sum of the change times and the sum of the inode numbers. A walk is settled only when the newest change time is at least 50 ms older than the start of the walk. An unsettled walk returns a token that equals no other token. `ScanStable` reads the token, scans, reads the token again, and retries up to 3 times while two settled tokens differ. The daemon runs no `syncfs`. A store through a mapping that is already dirty changes no token, so a consumer forces a snapshot at intervals.

### Chart and image

`charts/motte-vfsd` installs the `CSIDriver` and the DaemonSet with the plugin container and the kubelet registrar. Values set the image (digest or tag), the store URL `apiURL`, the Secret of the grant key, `driverName`, `kubeletDir`, `stateDir`, `runtimeClassName`, node selector, node affinity expressions, tolerations, resources and extra environment. The release namespace needs the privileged Pod Security level. The chart creates no Secret and no namespace. `images/motte-vfsd/Dockerfile` builds the image for `linux/amd64` and `linux/arm64`. It holds the daemon and `git`. The workflow `image.yml` builds both on a pull request. On a tag `v*` it publishes `ghcr.io/majorcontext/motte-vfsd` with the tag version and with provenance and SBOM attestations.

### Tests

`internal/vfs` pins the daemon over an in-memory upper directory, the plugin over a fake of the mount seam, the ref and op-head wire against a fake store, and `HTTPStore` against the real service. `internal/vfs/upperdir` tests `Scan` against a model of overlayfs and, as root, against a real overlay. No Go test mounts FUSE. The harness under `internal/vfs/testdata` (`vfsfuzz`, `overlaycheck`, `overlaykit`) checks a real overlay over a running daemon and needs root, `/dev/fuse` and overlayfs. CI vets the tools and runs the tests of `overlaykit`.
