# Architecture

This document is the contract for motte. A change in behavior updates this document in the same pull request.

## Goal and non-goals

Goal: motte stores the repositories of coding agents and the state of their virtual workspaces. A control plane (the caller) creates repositories, claims workspaces and flushes their state. A node daemon and the jj backend read and write objects, refs and operation logs.

Non-goals:

- No git dumb HTTP, web UI, review or pull-request model, access lists, or users.
- No multi-region store. One store serves all callers.
- No compaction or deletion of packs, and no purge of deleted workspaces.

## Components

| Component | Responsibility | Phase |
|---|---|---|
| `grant` | Claims, Ed25519 signer, verifier. Public package. | M1 |
| `api` | Wire types and error codes. Public package. | M1 |
| `bucket` | Object store seam with Cloud Storage (`bucket/gcs`) and filesystem (`bucket/fs`) buckets, and its conformance suite `bucket/buckettest`. Public package. | M1 |
| `internal/gitpack` | Reader and writer of git pack files and v2 indexes, and the reader of a pushed pack. | M1 |
| `internal/pktline`, `internal/gitproto` | The pkt-line framing, and the git smart HTTP protocol over an object reader: advertisement, v2 fetch and ls-refs, receive-pack. | M6 |
| `meta` | Metadata seam: repositories, workspaces and the epoch fence, op heads, refs, protected ref patterns, ref events, event subscriptions, pack listing. Backends `meta/postgres` and `meta/sqlite`, and the suite `meta/metatest`. Public package. | M2 |
| Store service | HTTP service over a `meta` store and a `bucket` store, and the epoch fence. | M2 |
| `client` | Typed Go client for callers. Public package. | M3 |
| `testserver` | Starts a real server in a caller's test, on PostgreSQL (`Start`) or on SQLite and a temporary directory (`StartLocal`). Public package. | M3 |
| `internal/vfs/...`, `cmd/motte-vfsd`, `volume` | Node daemon that serves a virtual workspace, and its public volume attributes. | M4 |
| jj backend | `jj/`: the jj binary whose stores and working copy use the workspace daemon. Released for linux amd64 and arm64. | M5 |

## Planes

The store plane serves the node daemon and the jj backend. The control plane serves the caller. Every route needs a grant in the `Authorization: Bearer` header. The git routes also take it as the password of HTTP Basic. A grant names the repository and the workspace. No route takes either from the request.

### Service

`cmd/motte` reads its configuration from the environment:

| Variable | Meaning |
|---|---|
| `MOTTE_LISTEN` | Listen address. Default `:8080`. |
| `MOTTE_META` | Metadata backend: `postgres` or `sqlite`. Default `postgres`. |
| `MOTTE_DATABASE_URL` | PostgreSQL DSN. Required for `postgres`. |
| `MOTTE_SQLITE_PATH` | SQLite database file. Required for `sqlite`. |
| `MOTTE_BUCKET_KIND` | `gcs` or `fs`. |
| `MOTTE_BUCKET` | Bucket name for `gcs`, directory for `fs`. |
| `MOTTE_TRUSTED_KEYS` | JSON list of `{"kid","key","scopes"}`. `key` is the standard base64 of an Ed25519 public key. `scopes` holds only `node`, `read`, `write`, `import`, `operator` and `merge`. The service refuses to start on any other scope text. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional. Enables OTLP trace export. |

The service refuses to start on a `MOTTE_META` or `MOTTE_BUCKET_KIND` value that it does not know. The service applies the embedded migrations of its metadata backend at start. The PostgreSQL backend applies them under an advisory lock, so replicas can start together. `GET /health` answers 200. `GET /ready` answers 200 while the metadata backend answers a ping. `GET /metrics` serves Prometheus metrics. These three routes need no grant. A request for a path or method that no route serves answers 404 `vcs_not_found` or 405 `method_not_allowed` in the error envelope.

### Run locally

The service runs with the `sqlite` metadata backend and the `fs` bucket and no other service. The key pair is any Ed25519 pair, and this command makes one and starts the service on `:8080`:

```
openssl genpkey -algorithm ed25519 -out motte.key && MOTTE_META=sqlite MOTTE_SQLITE_PATH=motte.db MOTTE_BUCKET_KIND=fs MOTTE_BUCKET=objects MOTTE_TRUSTED_KEYS="[{\"kid\":\"local\",\"key\":\"$(openssl pkey -in motte.key -pubout -outform DER | tail -c 32 | base64)\",\"scopes\":[\"node\",\"read\",\"import\",\"operator\"]}]" go run ./cmd/motte
```

A Go test starts the same composition with `testserver.StartLocal`, which needs no service and never skips. `testserver.Start` runs on PostgreSQL and skips without `MOTTE_TEST_DATABASE_URL`. One process uses one SQLite file and one `fs` directory.

### Limits and memory

- An object has at most 16 MiB (`gitpack.MaxObjectBytes`). A put of a larger object, and a read of a larger object in an imported pack, answer 413 `vcs_object_too_large`. A caller must not store a file above 16 MiB.
- The service reads a request body completely before it takes memory for the request. A body above its route limit answers 413 `vcs_object_too_large`, whatever its content. The limits are 1 MiB for a JSON route, 8 MiB of base64 data plus 1 KiB for `PUT /v1/vcs/ops/{kind}/{id}` (the data limit is 8 MiB before encoding), and 96 MiB plus 4 KiB for `POST /v1/vcs/objects` and `PUT /v1/vcs/packs/{pack_id}`. The upload routes allow 2 minutes to read the body.
- At most 4 upload bodies are read or wait for a reservation at one time. An upload takes one of the 4 reader slots before it reads the first byte and gives it back after it reserves memory, so a fifth upload waits. A read request takes no reader slot, so a slow upload never blocks a read.
- A put has at most 100000 objects and 64 MiB of object data. A blob batch or a `has` call names at most 4096 SHAs, and a blob batch returns at most 16 MiB.
- The service keeps a memory budget of 512 MiB for requests at work. A request reserves the peak of its live buffers before it allocates them, and waits while the budget is spent. A reservation never exceeds the budget. Apart from the reader slots, the budget is the only limit on concurrency: no request excludes the others, and a slow sender holds no reservation because the body is read before the reservation. B is the length of the body. The peak of each route comes from its code path:

| Route | Reservation | At the largest request |
|---|---|---|
| `POST /v1/vcs/objects` | B, the decoded objects (3B/4), the pack body and the pack (each 3B/4 plus 2 percent), and 80 MiB for the decoder: B + 2.5B + 80 MiB | 416 MiB |
| `PUT /v1/vcs/packs/{pack_id}` | the body, the two base64 strings, the decoded pack and index, and pack verification: 3B + 96 MiB | 384 MiB |
| `PUT /v1/vcs/ops/{kind}/{id}` | the body, the Data string and the decoded op: 3B | 32 MiB |
| `GET /v1/vcs/objects/{sha}`, `POST /v1/vcs/blobs/batch` | 16 MiB of data, its base64 text and an encoder buffer that doubled once: 80 MiB | 80 MiB |
| `GET /v1/vcs/trees/{sha}` | 48 MiB at first. Before it reads a tree, the request reads the size from the pack index and, when 8 times the tree bytes read so far plus 16 MiB exceeds what it holds, releases its reservation and takes the larger one. It never holds one reservation while it waits for another. A reservation above the budget waits for the whole budget and does not fail. | 48 MiB, or 8 times the tree bytes plus 16 MiB |
| `GET /v1/vcs/commits/{sha}` | a delta, its base and its result, 3 objects: 48 MiB | 48 MiB |
| `GET /v1/vcs/ops/{kind}/{id}` | 8 MiB of data, its base64 text and the encoder buffer: 40 MiB | 40 MiB |
| `POST /v1/vcs/objects/has` | 1 MiB | 1 MiB |

  `POST /v1/vcs/objects` reads its JSON through a decoder that refuses 24 MiB of unparsed text, so one element cannot grow the buffer: it answers 413 `vcs_object_too_large`. The memory of the process is at most 4 times 96 MiB for bodies in the reader slots, plus the 512 MiB budget, plus the 256 MiB pack cache: 1152 MiB. The chart default request is 1536Mi.
- A lookup of a SHA reads the pack indexes of the repository, newest pack first, and a SHA that no pack holds reads every index. The cost grows linearly with the number of packs of a repository. The service accepts this cost: it never compacts packs.

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

### Git smart HTTP

Ordinary `git` clones, fetches and pushes a repository over HTTPS. The routes need no `/v1` grant header, and the grant goes in the way that git sends credentials.

| Route | Scope |
|---|---|
| `GET /git/<prefix>/<path>.git/info/refs?service=git-upload-pack` | `read`, `write` |
| `POST /git/<prefix>/<path>.git/git-upload-pack` | `read`, `write` |
| `GET /git/<prefix>/<path>.git/info/refs?service=git-receive-pack` | `write` |
| `POST /git/<prefix>/<path>.git/git-receive-pack` | `write` |

- Repository: the URL names the repository as `<prefix>/<path>` where the grant names `<prefix>:<path>`, for example `/git/native/repo_X.git` for `native:repo_X`. The path may hold `/`. The part up to the last `.git/` is the repository. The repository of the URL must equal the `repo` claim of the grant, or the answer is 403 `vcs_repo_forbidden`. A repository that does not exist answers 404 `vcs_not_found`, so a push never creates one: the control plane creates repositories. A repository exists when it has a `repos` row (prefix `native`), or has at least one ref (any other prefix).
- Grant: the client sends HTTP Basic with any user name and a grant as the password, or `Authorization: Bearer <grant>`. A missing or refused grant answers 401 with `WWW-Authenticate: Basic realm="motte"`, so git asks for credentials or runs its credential helper. A grant of another scope for the route (a `read` grant for receive-pack) answers 403.
- Protocols: fetch speaks protocol v2 only (`Git-Protocol: version=2`, git 2.26 and later). The advertisement lists `ls-refs`, `fetch=filter` and `object-format=sha1`. A request for upload-pack without v2 answers 400. Push speaks v0 and v1 receive-pack with the capabilities `report-status`, `delete-refs`, `ofs-delta` and `object-format=sha1`. The dumb protocol, `shallow` and `deepen` requests, `want-ref`, `packfile-uris`, `atomic`, `push-options`, signed pushes and `side-band` on receive-pack are not offered. A filter other than `blob:none` is refused with 400.
- Ref names: a stored ref named `main` is the git ref `refs/heads/main`. A stored name that starts with `refs/` other than `refs/heads/` has the same git name. HEAD names `refs/heads/main` when it exists. A push may write `refs/heads/<name>` and any other `refs/...` name that is valid, and `<name>` must not start with `refs/`. A stored ref named under `refs/heads/` has no git name and is not listed.
- Fetch: a `want` may name any object of the repository. The service lists the commits that the wants reach and the `have` commits that it holds do not reach, walking by commit time. It adds the trees and blobs that those commits reach and that the trees of the edge commits (the held parents) do not hold. With `filter=blob:none` it adds no blob that a tree names, and it sends a blob that a `want` names. It never sends a delta: the pack holds whole objects, so `thin-pack`, `ofs-delta`, `include-tag` and `no-progress` have no effect. A client fetches an annotated tag that it needs with a second `want`. It answers `ready` as soon as one `have` is common, and the pack then leaves out what that commit holds. It streams the pack through side-band 1 with one object in memory at a time, and reads each pack through the cache of ranged reads.
- Push: the service reads the whole request body, inflates a `Content-Encoding: gzip` body first, reads the ref commands, and unpacks the pack. A thin pack is complete: the service resolves each ref delta base from the repository. It stores the objects as one whole pack of the repository, with the SHA-256 of the stored pack as its id, and adds it to the pack listing. Every object is hashed, so the pack is checked as `PUT /v1/vcs/packs/{pack_id}` checks it. The service does not check that the objects of a new commit are connected. It checks only that the new value of a ref exists, is a commit (or a tag, outside `refs/heads/`).
- Ref update: each ref command goes, in order, through the one function `updateRef` of `internal/server/git.go`, which calls `meta.Store.UpdateRef`. The update is a compare and swap on the commit: `old` is the commit that the client saw, a zero `old` creates the ref and a zero `new` deletes it. A ref that is not at `old` fails that command with `ng <ref> stale info` and the other commands go on. There is no atomic push. The service does not tell a force push from a plain one, because git sends no flag for it: the client refuses a non-fast-forward unless it is forced, and the service accepts any update that passes the compare and swap, as git does with `receive.denyNonFastForwards` unset. The reply is `report-status`: `unpack ok` or the refusal of the pack, then `ok <ref>` or `ng <ref> <reason>` for each command.
- Limits: a push body has at most 64 MiB plus 1 MiB of commands, after gzip. A pack has at most 100000 objects, 64 MiB of inflated entry data and 64 MiB of objects, and each object has at most 16 MiB (`gitpack.MaxObjectBytes`). A command list has at most 1000 commands. A fetch request body has at most 8 MiB, with at most 10000 wants and 100000 haves. A fetch holds at most 1000000 objects and commits in its sets, and a larger fetch answers 413 `vcs_object_too_large`. A fetch has no limit on the size of the pack that it streams. A push reserves its body, three times 64 MiB, 4 times 16 MiB and the verification work: B + 192 MiB + 64 MiB + 48 MiB. A fetch reserves 176 MiB. A push takes one of the 4 reader slots while it reads its body, as an upload does.
- The git routes take no part in the epoch fence. A `write` grant moves a ref with no workspace, so a workspace that works on the same ref sees the new commit through the ref generation.

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

The tables below are the PostgreSQL schema. Every backend of `meta.Store` keeps the same fields. See Backends.

```
repos(path pk, name, owner, created_at)
packs(repo, pack_id, bytes, created_at, pk(repo, pack_id))
refs(repo, name, commit_sha, generation, updated_at, pk(repo, name)); sequence ref_generation
workspaces(id pk, repo, owner, epoch bigint, op, snapshot, report_generation, created_at, deleted_at)
op_heads(workspace_id pk -> workspaces, epoch, generation, heads text[], updated_at)
counters(name pk, value)   rows event_seq, events_floor
events(seq pk, repo, ref, old_commit, new_commit, generation, kind, at)
protected_refs(repo pk, patterns text[])
subscriptions(id pk, url, secret, repo, cursor, created_at, lease_owner, lease_until)
```

A repository text has the form `<prefix>:<path>`. The prefix `native` is reserved for the repositories that `POST /v1/repos` creates. The prefix matches `^[a-z0-9][a-z0-9-]{0,62}$`. The path has 1 to 512 bytes of UTF-8. Object keys are `repos/<prefix>/<base64url path>/...` and `ops/<workspace>/...`. A repository that the caller owns elsewhere has no `repos` row. The store keeps only the packs and refs of such a repository.

A ref generation comes from the sequence `ref_generation`. It is never reused, including when a ref is deleted and created again. A swap with `expect` 0 creates the ref.

### Epoch fence

`workspaces.epoch` is the highest epoch that the caller claimed.

- Claim: the PostgreSQL statement is `INSERT ... ON CONFLICT (id) DO UPDATE SET epoch = GREATEST(workspaces.epoch, EXCLUDED.epoch) WHERE deleted_at IS NULL RETURNING epoch, op, snapshot`. A claim below the stored epoch answers 409 `vcs_claim_lost`. A claim on a deleted workspace answers 410. `owner` and `repo` are set once. A different value answers 409.
- A claim needs `epoch` of 1 or more. A claim on a `native` repository that has no row, or whose `repos.owner` differs from `owner`, answers 403 `vcs_repo_forbidden`. A claim whose `owner` or `repo` differs from the stored value answers 409 `vcs_workspace_conflict`. A claim on a deleted workspace answers 410 `vcs_workspace_deleted`.
- Write: in PostgreSQL, every ref swap, ref delete and op-heads swap is one statement. It starts with `WITH guard AS (SELECT id FROM workspaces WHERE id = $ws AND epoch = $epoch AND deleted_at IS NULL FOR SHARE)`. The write applies only when the guard row exists. A workspace report is one `UPDATE` of the workspace row with the same epoch, repository and `deleted_at` conditions in its `WHERE`. It takes the row lock itself, because two reports that hold `FOR SHARE` and then update the row can deadlock.
- In PostgreSQL, the claim `UPDATE` takes the row lock that `FOR SHARE` conflicts with. In every backend, a write in flight finishes first, and every later write of the old epoch fails. `vcs_claim_lost` wins over `vcs_generation_conflict`. An op-heads write also needs `op_heads.epoch <= $epoch`.
- A `node` route compares the grant epoch with `workspaces.epoch` before it runs, for reads and writes. A missing or deleted workspace answers 401. A repository that differs from `workspaces.repo` answers 403 `vcs_repo_forbidden`. A different epoch answers 409 `vcs_claim_lost`. Every write statement also needs `workspaces.repo` to equal the grant repository.

### Raise before commit

The caller raises the epoch before it commits its own increment. The caller calls claim with the next epoch, then commits the increment. A failed claim fails the step before the commit, and the step retries. Claim is idempotent. `GREATEST` keeps the highest of two racing claimers. If the commit then loses to another claimer, that claimer raises again. A raise with no commit refuses only the writes of an epoch that the caller already replaces. A workspace of the new epoch, and so its first grant, does not exist before the raise.

### Workspace state

`op`, `snapshot` and `report_generation` live on `workspaces`. A node report and a flush write one row with one ordering rule: the higher `report_generation` wins, and an equal one writes again, because a flush at the generation of a report carries a newer `snapshot`. A flush with generation 0 writes only while the stored `op` equals `prev`. A write that loses on this ordering rule answers success and changes nothing. A flush runs under the same epoch fence as a node write: a stale `epoch` answers 409 `vcs_claim_lost`, a deleted workspace 410, and an unknown workspace 404. A flush answers 204.

### Delete

`DELETE /v1/workspaces/{id}` sets `deleted_at`. Every later read and write of that workspace fails. The call is idempotent, answers 204, and does nothing for an unknown workspace. The store removes no op-head row and no object.

### Repository creation

`POST /v1/repos` creates the repository with one root commit under the ref `main`. The root commit has the empty tree, the author and committer `motte <motte@motte.invalid>` and the creation time as its time. The store writes the root objects first, then the `repos` row and the ref in one transaction, so a failed create leaves no repository without `main`. The path is `repo_` and random base32 text. The name is trimmed and has 1 to 100 bytes with no control character, else 400 `invalid_body`. A workspace can start on `main` at once.

## Backends

A caller of the service chooses where the service keeps its state. Two seams hold it. `meta.Store` keeps the small mutable records. `bucket.Store` keeps the immutable objects. The server and `internal/store` depend only on these two interfaces and on their neutral types and errors. A backend is a Go type that implements one of them. The operator writes or picks a backend, and `cmd/motte` builds the server from it. The server needs no change.

| Interface | Holds | Backends |
|---|---|---|
| `meta.Store` | Repositories, workspaces and their epoch fence, op heads, refs, protected ref patterns, ref events, event subscriptions, the pack listing. | `meta/postgres`, `meta/sqlite` |
| `bucket.Store` | Pack files, pack indexes and op objects. | `bucket/gcs`, `bucket/fs` |

The suites `meta/metatest` and `bucket/buckettest` export `Run(t, newBackend)`. A backend passes its suite before a server uses it. The suite is the executable form of this section, and a difference between them is a defect.

### Contract of meta.Store

Every method that writes is one atomic unit. The unit is one conditional statement, or one atomic batch of statements, or one read-write transaction. No method needs a transaction that spans calls, a row lock that the caller holds, or a read whose result changes the write. The database decides each condition. This is the weakest set that a backend can offer: a database with atomic batches and no interactive transactions, such as Cloudflare D1, meets it.

- Fence: the fence of workspace `ws` at `epoch` and `repo` holds while the workspace row has that id, that epoch and that repo and is not deleted. A write that needs the fence checks it in the same unit as the write. A claim and a write are ordered. A write that is ordered after the claim sees the raised epoch and fails.
- Failed condition: a write whose condition fails changes nothing. The backend then reads the workspace row, and the ref or op-heads row when the method names it, and picks the error. `meta.Fence` holds the precedence, so every backend gives the same one: `ErrClaimLost` wins over a generation conflict. These reads are not part of the unit. The error answers for a state that is at least as new as the write attempt.
- Generation: the generation of a ref is positive and unique in the store. No ref ever takes a generation that was taken before, including a ref that is deleted and created again. The backend takes it from one counter in the unit of the write. A generation is not ordered across refs, and a failed write can skip one.
- Order: names, repositories, pack ids and workspace ids are opaque text. A list sorts them by byte value of their UTF-8 text.
- Time: `Repo.CreatedAt` and the creation time of a pack come from the clock of the backend, with a resolution of 1 millisecond or finer.
- Racers: of N concurrent calls that need the same free condition, exactly one wins and the others get the error of a lost condition.
- Events: every write that creates, moves or deletes a ref appends one event in the same unit as the ref write, and a write that changes nothing appends none. The event holds the repo, the ref, the old and new commit (empty when absent), the generation (that of the deleted ref for a delete), the writer kind and the time. The kind is `node` for `SwapRef` and `DeleteRef`, `merge` for `MergeRef` and `MergeDeleteRef`, and `operator` for the `main` that `CreateRepo` writes. `Seq` is positive and unique, and it follows commit order: a reader that sees `Seq` N sees every event below N that will ever exist, so a consumer that keeps the last `Seq` it read loses no event. Retention removes events and keeps the highest removed `Seq` as a floor. A read from a `Seq` below the floor is `ErrEventsPruned`.
- Lease: a subscription row holds its lease. A lease write is one conditional statement on that row, and `now` is an argument, so the clock of the caller decides expiry. The lease passes to a new owner only when `lease_until` is at or before `now`. Owners must have clocks that differ by much less than the lease time.
- Visibility: a method that returns nil has committed durably. A read that starts after it sees the write, in every client of the store.

| Method | Condition | Effect | Error on a failed condition |
|---|---|---|---|
| `CreateRepo(r, repoText, commit)` | No `repos` row has path `r.Path`, and no ref `main` exists in `repoText`. | Inserts the repo row, the ref `main` at `commit` with a new generation, and one event of kind `operator`. | `ErrExists`. Neither row exists. |
| `Repo(path)` | None. | Returns the row. | `ErrNotFound`. |
| `Repos()` | None. | Lists newest first: `created_at` descending, then `path` descending. | None. |
| `Claim(id, epoch, owner, repo)` | The workspace is absent, or it is not deleted and has the same owner and repo. | Inserts the row, or sets `epoch` to the higher of the stored one and `epoch`. Returns the stored epoch, op and snapshot. | `ErrGone` for a deleted workspace. Else `ErrWorkspaceConflict`. A lower `epoch` is not an error: the caller compares the returned epoch. |
| `Live(id, epoch, repo)` | None. | Reads the workspace row. | The first of `ErrNotFound`, `ErrGone`, `ErrWrongRepo`, `ErrClaimLost`. |
| `Delete(id)` | None. | Sets `deleted_at` once. The call keeps an earlier `deleted_at`. | None. An unknown workspace is not an error. |
| `Report(ws, epoch, repo, generation, prev, op, snapshot)` | The fence holds. With `generation` above 0: the stored `report_generation` is unset or at most `generation`. With `generation` 0: the stored `op` equals `prev`. | Sets `op` and `snapshot`. Sets `report_generation` when `generation` is above 0. | `ErrClaimLost` when the fence fails. When the fence holds and the ordering rule fails, the call changes nothing and returns nil. |
| `Flush(ws, epoch, generation, prev, op, snapshot)` | As `Report`, with the repo of the stored row. | As `Report`. | `ErrNotFound`, `ErrGone`, `ErrClaimLost` by `Fence.FlushLost`. Nil when the ordering rule fails. |
| `OpHeads(ws)` | None. | Returns epoch, generation and heads. | `ErrNotFound`. |
| `SwapOpHeads(ws, epoch, repo, 0, heads)` | The fence holds and no op-heads row exists. | Inserts generation 1 with `epoch` and `heads`. | `ErrClaimLost`, else `*OpHeadsConflict` with the stored row as `Current`. |
| `SwapOpHeads(ws, epoch, repo, expect, heads)` | The fence holds, the stored generation is `expect` and the stored epoch is at most `epoch`. | Sets `epoch` and `heads`. Adds 1 to the generation. | `ErrClaimLost` when the fence fails or the stored epoch is above `epoch`, else `*OpHeadsConflict` with `Current` nil when no row exists. |
| `Ref(repo, name)` | None. | Returns the ref. | `ErrNotFound`. |
| `Refs(repo)` | None. | Lists the refs of the repo by name. | None. |
| `SwapRef(ws, epoch, repo, name, 0, sha)` | The fence holds and the ref is absent. | Inserts the ref with a new generation and one event of kind `node`. | `ErrClaimLost`, else `*RefConflict` with the stored ref as `Current`. |
| `SwapRef(ws, epoch, repo, name, expect, sha)` | The fence holds and the stored generation is `expect`. | Sets the commit and a new generation, and appends one event. | `ErrClaimLost`, else `*RefConflict` with `Current` nil when the ref is absent. |
| `DeleteRef(ws, epoch, repo, name, expect)` | The fence holds and the stored generation is `expect`. | Deletes the ref and appends one event. | As `SwapRef`. |
| `UpdateRef(repo, name, old, new)` | No fence. The stored commit is `old`, or the ref is absent when `old` is empty. | Creates the ref when `old` is empty, deletes it when `new` is empty, else sets the commit. Every write takes a new generation. | `*RefConflict` with the stored ref as `Current`, nil when the ref is absent. |
| `MergeRef(repo, name, 0, sha)` | The ref is absent. | Inserts the ref with a new generation and one event of kind `merge`. | `*RefConflict` with the stored ref as `Current`. |
| `MergeRef(repo, name, expect, sha)` | The stored generation is `expect`. | Sets the commit and a new generation, and appends one event. | `*RefConflict` with `Current` nil when the ref is absent. |
| `MergeDeleteRef(repo, name, expect)` | The stored generation is `expect`. | Deletes the ref and appends one event. | As `MergeRef`. |
| `ProtectedRefs(repo)` | None. | Returns the patterns, in the order that `SetProtectedRefs` stored them. | None. A repo with none returns an empty list. |
| `SetProtectedRefs(repo, patterns)` | None. | Replaces the whole list. | None. |
| `Events(after, limit)` | `after` is not below the floor. | Lists up to `limit` events with `Seq` above `after`, by `Seq`. | `ErrEventsPruned`. |
| `PruneEvents(before)` | None. | Deletes the events with a time before `before` and raises the floor to the highest deleted `Seq`. | None. |
| `CreateSubscription(s)` | No subscription has the id. | Inserts the row with its cursor at the newest `Seq` that ever existed, and returns it with its creation time. | `ErrExists`. |
| `Subscriptions()` | None. | Lists by creation time, then id. | None. |
| `DeleteSubscription(id)` | The subscription exists. | Deletes it and its lease. | `ErrNotFound`. |
| `AcquireLease(id, owner, now, until)` | The lease owner is `owner`, or its `lease_until` is at or before `now`. | Sets the owner and `lease_until` to `until`, and returns the row. | `ErrNotFound`, else `ErrLeaseHeld`. |
| `AdvanceCursor(id, owner, cursor)` | The lease owner is `owner`. | Sets the cursor to the higher of the stored one and `cursor`. | `ErrNotFound`, else `ErrLeaseHeld`. |
| `AddPack(repo, id, bytes)` | No pack row has the repo and id. | Inserts the row with the creation time. | None. A repeat changes nothing and keeps the first `bytes`. |
| `Packs(repo)` | None. | Lists the packs of the repo, oldest first, then by id. | None. |

A `repo` argument of a fenced write is the repo of the grant. It must equal the repo of the workspace row. Op `heads` is a non-empty list of text. The server validates every other value before it calls the store.

### Contract of bucket.Store

- `Create(key, data)` writes `data` when no object has `key`, and returns true. It returns false and changes nothing when an object has `key`. Of N concurrent creators of one key, exactly one gets true. A stored object is never replaced and never deleted.
- `Get(key)` returns the whole object. `GetRange(key, off, n)` returns up to `n` bytes from `off`, fewer at the end, none when `off` is at or past the end, and the rest of the object when `n` is negative. A missing key is `bucket.ErrNotFound` for both.
- A read that starts after a `Create` that returned sees the whole object. The `fs` backend keeps its on-disk format: a 4096-byte header block, then the data.

### Provided backends

- `meta/postgres` is the production backend. It keeps the schema in the section Data model, applies `meta/postgres/migrations` at start under an advisory lock, and meets each write with one statement, except a ref write and `CreateRepo`. The fence of a ref or op-heads write is a `FOR SHARE` read of the workspace row inside the statement. A report is one `UPDATE` that locks the row itself. The generation comes from the sequence `ref_generation`. A ref write and `CreateRepo` are one transaction. It first takes the next event number with `UPDATE counters SET value = value + 1 WHERE name = 'event_seq' RETURNING value`. The row lock lasts to the commit, so `Seq` order is commit order, and the number comes before any workspace lock, so a claim is not starved by writes that wait for the counter. One statement then writes the ref and the event, and the transaction rolls back when no row changed, so a failed write leaves no gap. All ref writes of one store are therefore serial at the counter row.
- `meta/sqlite` is for tests and for a service that runs with no other process. It keeps the same fields in the tables of `meta/sqlite/migrations`. It uses a pure Go driver, so the service builds with CGO disabled. It opens one connection, so its writes are ordered, and one process uses one database file. Each write is one transaction of statements in which no read feeds a write. The fence is an `EXISTS` condition of the statement. The generation comes from the row `ref_generation` of the table `counters`, which the same transaction increments. A ref write inserts its event first, with the condition of the ref write, and `events.seq` is `AUTOINCREMENT`, so a rolled-back event takes no number. Times are integer microseconds, and the heads are JSON text.
- `bucket/gcs` and `bucket/fs` are the object backends. `gcs` writes with the precondition that the object does not exist. `fs` links a synced temporary file to its name.

### A new backend

A backend implements the interface and returns the sentinel errors of its package. It does not map them to wire codes: the server does. It adds a test that calls `metatest.Run` or `buckettest.Run` with a constructor that returns an empty store. It adds a branch to `cmd/motte` that chooses it from the environment. The backends below are not in the repository. Each paragraph states how the backend meets the contract.

- Cloud Spanner (`meta`): each write is one read-write transaction. The transaction reads the rows that the condition names, tests the condition, writes, and commits. Spanner orders transactions strictly, so a claim and a write are ordered. The generation counter is one row that the transaction updates, or a Spanner sequence, which is unique and not ordered. The creation time is the commit timestamp. Reads that pick an error are strong reads. The heads are an `ARRAY<STRING>`.
- Cloudflare D1 (`meta`): each write is one `batch` call. D1 runs a batch as one transaction and rolls it back when a statement fails. D1 has no interactive transaction and no row lock, and the contract needs neither. The conditions are the `WHERE` clauses and `INSERT ... SELECT ... WHERE EXISTS` fences of `meta/sqlite`, which is SQLite SQL. The counter is a row that the first statement of the batch increments. The result of a statement reports the rows that it changed, and a conditional statement that changed no row is the failed condition. Reads that pick an error go to the primary.
- Cloudflare R2 (`bucket`): R2 has an S3-compatible API. `Create` is `PutObject` with `If-None-Match: *`, which answers 412 when the key exists. `Get` and `GetRange` are `GetObject` with an optional `Range` header, and a 416 answer is an empty result. R2 reads see a completed write.

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

The shims speak to the workspace daemon with the control protocol (see Control protocol) over a unix socket. The socket is the path in `MOTTE_VFS_SOCK`, or else the path in the file `store/ctl_sock` of the repo directory, which `init` writes. One function finds it. A command that finds none fails and names both places. The one exception is the update of the op heads, which then skips the remote heads, warns, and marks nothing as unsynced.

A shim branches on the `code` of an error answer. It reads the `err` text only of an answer that has no `code`, which a daemon released before the code sends (see Control protocol); one function holds that fallback. The shim hashes every object itself and refuses an answer whose SHA differs. An operation or a view that it reads back with `getop` must hash (blake2b-512 of its content, as jj's op store names it) to the ID it asked for. A daemon that is not in overlay mode fails every command that needs the dirty set.

The shim refuses an object over 16 MiB before it sends one byte: a snapshot of a file over that size fails and names the path and the limit. A request that gets no answer is sent once more only when sending it twice has the effect of sending it once. A `swapref` or an `opheads` swap is never sent again on a lost answer: the shim reads the ref or the heads. The change landed when the stored state is the one the request wanted, it is sent again when the stored generation is still `expect`, and it lost the race otherwise. The code `line_too_long` is an answer and is never retried.

Every request carries the W3C `traceparent` of the innermost open span, or the `TRACEPARENT` environment variable. With `OTEL_EXPORTER_OTLP_ENDPOINT` set, the binary exports OTLP over plain HTTP, with no retry and a 500 ms limit: one span `jj.command` for each command and one for each operation (`jj.motte.snapshot`, `jj.snapshot`, `jj.checkout`, `jj.read_tree`, `jj.write_commit`, `jj.op_heads.update`, `jj.push`, `jj.fetch`), with the counts `objects` and `bytes`. `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` turns tracing off. A span holds no path, file content or credential. A snapshot sends blobs with `putbatch`, at most 64 blobs or 8 MiB a request, and falls back to `put` on the code `unknown_op`.

### Op heads

`opheads` without `heads` reads the op heads of the workspace; a store `vcs_not_found` before the first write is generation 0 and no heads. With `heads` it swaps them when the stored generation is `expect`. Each update of `motte-op-heads` first reads the remote heads, so a writer whose claim is lost fails with `vcs_claim_lost` before its local write. It then writes the local heads, removes from the remote set the replaced operations and every remote head that the local op store holds and the local heads do not, adds the new head and swaps. On `vcs_generation_conflict` it reads the winning heads and applies its update again. All daemon calls of one update share a budget of 2 seconds. A daemon that answers the code `unknown_op` leaves the remote heads behind the local ones and the command succeeds. Any other store failure, or a spent budget, does the same and also adds the new head to the marker `motte_unsynced` in the repo directory. A lost claim at the swap fails the command after the local write.

### Subcommands

`jj motte` has the subcommands `init`, `restore`, `attach`, `verify`, `flush`, `sync`, `snapshot`, `changes`, `push`, `fetch` and `dirty`. `init`, `restore` and `attach` need the `base_tree` of the daemon to be the tree of the mount commit. Every jj command except `jj motte snapshot`, and the ones that never snapshot (`--ignore-working-copy`), holds a shared flock on `motte_command.lock` in the repo directory until it exits. `jj motte snapshot` holds the lock exclusive through its snapshot and skips with `busy` when a command holds it. `jj motte flush` waits for the lock at most 45 seconds and then fails, so that it ends before the caller that runs it.

`jj motte changes` prints the work-tree diff as JSON for a caller that sets `HARNESS_CHANGES_SCOPE` (`branch` or `uncommitted`), `HARNESS_CHANGES_BASE` and `HARNESS_CHANGES_PATCH_BYTES` (default 1 MiB). It loads the op head that ended last, in whole seconds, then the larger ID. It never snapshots and writes no operation. It diffs the target tree against the daemon base tree and overrides each path that `changes` names. The patch is in git format and ends at the last whole file within the byte cap, with `truncated` set. A file larger than 2 MiB is `large` with no hunk. Renames are not detected.

### Push and fetch to a git origin

A workspace has a git origin when `init` or `restore` got `--origin-url`, an http(s) URL with no user information, and `--commit` or `--base`, which set the repo config key `motte.origin-base` to the commit that origin holds. `--origin-branch <name>` makes, at the start commit, the local bookmark `<name>` and the tracked bookmark `<name>@origin`, and sets `revset-aliases.'trunk()'` to `"<name>"@origin`. The only remote is `origin`. The commands keep a bare repo `motte_git` in the repo directory as the transport, run the `git` binary over HTTPS with `GIT_TERMINAL_PROMPT=0`, and share a deadline of 10 minutes.

`jj motte push -b <bookmark>...` refuses a missing or conflicted bookmark, a new commit that is conflicted, has no description or is the root commit, a rewrite of the default branch of origin, and a workspace with no daemon socket. It probes the claim with `opheads`, reads the tips of origin (`git ls-remote --symref`), fetches the base of each bookmark by SHA with no blobs (falling back to heads at depth 20, 200, 1,000, then unshallow), writes the loose objects of the new commits from the daemon, probes the claim again, and runs `git push --atomic --no-thin --force-with-lease=refs/heads/<name>:<commit of NAME@origin>`. The push sends full packs, not thin ones, because `motte_git` is a blob-less partial clone: a thin pack would read a delta base blob that it lacks. One jj transaction then sets `NAME@origin`. A bookmark whose tip already equals its commit is recorded without an update. The span `jj.push` carries `remote`, `refs` and `outcome`: `ok`, `refused`, `stale`, `rejected`, `network` or `claim_lost`. A daemon call that fails with `vcs_claim_lost` at any step of a push or a fetch ends the command as `claim_lost`, and any other daemon failure ends it as `network`.

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
| `scope` | One of `node`, `read`, `write`, `import`, `operator`, `merge`. |
| `ws` | Workspace identifier. |
| `epoch` | Claim epoch of the workspace when the grant was minted. |
| `repo` | Repository text. |
| `owner` | Owner of the workspace. |
| `subject` | The creator, for traces only. It grants nothing. |
| `exp` | Expiry as Unix seconds. A grant is valid while `now < exp`. |

Required claims by scope: `node` needs `ws`, `epoch` and `repo`. `read`, `write`, `import` and `merge` need `repo`. `operator` needs none.

The verifier holds only public keys. Each key has a `kid` and a set of allowed scopes. The verifier rejects a grant whose scope is not allowed for its key. Every rejection answers 401 with the same error, and the response never carries the grant.

### Keys

The caller owns the private keys. Two signers exist:

- The control-plane key may sign all scopes. A `write` grant lets the bearer move any ref of its repository without a workspace: only the control plane or a trusted pipeline holds one.
- The node key lives on the node daemon and may sign only `node`. A compromised node cannot mint `import`, `operator` or `merge` grants. The plugin mints a `node` grant only for a pod of an allowed namespace (see Node plugin). It can mint `node` grants, bounded by the repository and epoch of the workspace row.

### Rotation

1. Add the new public key to the store.
2. Switch the signer to the new key.
3. Remove the old key after the longest grant life: 1 hour for `node`, 60 seconds for `operator`, `read`, `write`, `import` and `merge`.

Both keys are valid between step 1 and step 3.

### Read authorization

Authorization happens at claim time. For a repository that the store keeps (it has a `repos` row), claim checks `repos.owner` against `owner`. For any other repository, the caller checks that the creator may read it and signs the claim. The store trusts that and binds the workspace to the repository.

## Versioning

The path prefix `/v1` is additive. New fields and new routes are allowed, and clients ignore unknown fields. A breaking change gets `/v2`, served beside `/v1`. The module is tagged with semantic versions.

Wire error codes keep their `vcs_` text: `vcs_claim_lost`, `vcs_generation_conflict`, `vcs_not_found`, `vcs_object_invalid`, `vcs_object_too_large`, `vcs_repo_forbidden`, `vcs_workspace_conflict`, `vcs_workspace_deleted`. Three codes have no prefix: `unauthorized` (401), `invalid_body` (400) and `internal_error` (500). `method_not_allowed` (405) answers a method that its path does not serve. A panic in a handler is logged with its stack and answers 500 `internal_error` when the handler sent no status yet. The error body is `{"error":{"code","message","details"}}`. A conflict on a ref carries the stored ref in `details.ref`.

## Client

Package `client` has one typed method for each route of both planes. A `Client` sends one grant. The caller gives a `GrantFunc` that the client calls before each request, so a caller holds one `Client` for each scope that it uses.

- Each call has a time budget of 2 seconds, which `WithBudget` changes. A deadline on the call context that is earlier than the budget wins.
- The client injects the W3C trace context of the call context into the request headers.
- The client follows no redirect. A 3xx answer is the error `ErrRedirect`, and the grant goes to no other host.
- A failure answer is an `*Error` with the HTTP status, the wire code, the message and the details. `errors.Is` matches the sentinel of the code: `ErrNotFound`, `ErrGenerationConflict`, `ErrClaimLost`, `ErrObjectInvalid`, `ErrObjectTooLarge`, `ErrRepoForbidden`, `ErrWorkspaceConflict`, `ErrWorkspaceDeleted`, `ErrUnauthorized`, `ErrInvalidBody` and `ErrInternal`. A failure answer with no wire error body is `ErrUnexpected`.
- `CurrentRef` returns the stored ref that a ref generation conflict carries.

Package `testserver` runs the real server in the process of a test, on a fresh schema of the PostgreSQL database that the caller names (or `MOTTE_TEST_DATABASE_URL`) and a temporary filesystem bucket. It skips the test when no DSN is set. It returns the base URL, the pool of its schema (for a test that sets up a state no route can make), the control-plane and node signers, and helpers that mint operator, node, read, write and import grants.

## Observability

- Traces: OTLP, with the endpoint from the environment and no vendor in the code. Span names start with `vcs.serve.` and end with the route, such as `vcs.serve.put.refs`. Requests accept W3C trace headers.
- Metrics: Prometheus at `/metrics`, with names that start with `motte_`: `motte_http_requests_total` and `motte_http_request_duration_seconds`, by route.
- Logs: JSON. A log line of the service never holds a grant, a path or an object body. A log line of the node daemon never holds a grant, and it carries the error text of its own process, which can name a node path.
- Instrumentation is additive. It never changes behavior or errors.

## Deployment

`images/motte/Dockerfile` builds the service image for `linux/amd64` and `linux/arm64`. It holds a static `cmd/motte` binary on a distroless non-root base that the Dockerfile pins by digest. The workflow `image.yml` builds it, and the `motte-vfsd` image, on a pull request. On a tag `v*` it publishes `ghcr.io/majorcontext/motte` with the same tag scheme as `motte-vfsd` and with provenance and SBOM attestations.

`charts/motte` installs a `Deployment` (2 replicas by default), a `ServiceAccount` and a `Service`. The pod runs as non-root with a read-only root file system. The probes use `/health` and `/ready`, and `/metrics` is on the service port. Values set:

- The image (digest or tag), `replicas`, `resources` (default request 1536Mi of memory, which covers the bound in Limits and memory), node selector, tolerations and affinity.
- `serviceAccount.create` (default `true`), `serviceAccount.name` and `serviceAccount.annotations`, for workload identity. With `create` false the chart makes no `ServiceAccount` and the pod uses the account that `serviceAccount.name` names, or the release name when it is empty.
- `listenPort` (default 8080), the container port. It sets `MOTTE_LISTEN`.
- `prometheusAnnotations` (default `true`), which adds the `prometheus.io` scrape annotations for `/metrics`, and `podAnnotations`, more annotations for the pod.
- `service.type` (default `ClusterIP`), `service.loadBalancerIP` and `service.annotations`, so a caller can ask for an internal load balancer.
- `database.dsnSecret`, the name and key of an existing Secret that holds the PostgreSQL DSN.
- `bucket.kind` and `bucket.url`. An `fs` bucket is an `emptyDir` volume, for tests only, and the chart rejects it with more than 1 replica.
- `trustedKeys`, a list of `{kid, key, scopes}`, `trustedKeysConfigMap`, an existing ConfigMap that holds the JSON list, or `trustedKeysSecret`, the name and key of an existing Secret that holds it. The Deployment sets `MOTTE_TRUSTED_KEYS` from the one source that is set. The chart fails to render when none or more than one is set.
- `otlpEndpoint`, optional, and `extraEnv`.

The chart creates no Secret and no namespace. A release needs `database.dsnSecret.name`, `bucket.url` and exactly one source of trusted keys. CI runs `helm lint` and `helm template` over the files in `charts/motte/ci`.

## Virtual workspace daemon

The packages under `internal/vfs` are the node daemon of the virtual workspace. `cmd/motte-vfsd` runs it. It reads and writes the store plane with a `node` grant, so a workspace holds no store credential. The FUSE server and the overlay build on Linux only. On other systems `motte-vfsd` prints a message and exits 1. The daemon writes JSON logs, and `OTEL_EXPORTER_OTLP_ENDPOINT` enables trace export. A client span name is `vcs.client.` and the route, such as `vcs.client.ref.swap`.

| Package | Role |
|---|---|
| `internal/vfs/store` | Store client, tree and blob types, and the node cache. |
| `internal/vfs/lower` | The FUSE lower layer, and the dirty set and changes of the upper directory. |
| `internal/vfs/ctl` | The control protocol and its socket. |
| `internal/vfs/csi` | The CSI node plugin, the mount steps and the `mountinfo` reader. |
| `internal/vfs/importer` | `import`: fetch with `git`, pack, upload. |
| `internal/vfs/materialize` | `materialize`: write the checkout of a commit. |
| `internal/vfs/tel` | Correlation, spans, log records and the health record. |
| `internal/vfs/upperdir` | The scan of the upper directory. |

`HTTPStore` is the store client of the daemon. It is `client.Client` with the spans of the daemon, so it follows no redirect, trims a trailing `/` of the base URL, and matches errors with `errors.Is` on the sentinels of `client`. It answers a `vcs_repo_forbidden` answer with `ErrGrantRefused`, which no retry passes. A call has no budget of its own; the timeout of the HTTP client bounds it. A grant that fails to mint fails the call, and no request leaves with an empty token. A test drives `HTTPStore` against the real service.

### Commands

| Command | Meaning |
|---|---|
| `motte-vfsd mount` | Standalone, for a host that has `fusermount`; `csi` mounts with `mount(2)`. Serves the tree of one commit as the read-only lower layer of a kernel overlay. Needs `-api`, `-commit`, `-mnt`, `-upper` and `-ctl-sock`, and `MOTTE_VFS_GRANT` in the environment. It does not refresh the grant. |
| `motte-vfsd csi` | The CSI node plugin. Needs `-api`, `-grant-key-file` and `-allowed-namespaces` (comma-separated, no wildcard). Flags `-driver` (default `vfs.motte.dev`), `-endpoint`, `-node-id`, `-state`, `-uid` and `-gid`. |
| `motte-vfsd materialize` | Writes the checkout of a commit to `-dest`. A marker file records a finished checkout, and a later run changes nothing, whatever commit it names. |
| `motte-vfsd import` | Fetches a repository with `git`, packs it and uploads it with an `import` grant (`PUT /v1/vcs/packs/{pack_id}`). `-depth 0` imports the whole history as packs of about 48 MiB and skips the ranges that the store holds. |

The key file of `csi` is JSON `{"kid","key"}`. `key` is the standard base64 of an Ed25519 private key or of its 32-byte seed. The store must trust the public key for the scope `node`. The plugin signs a `node` grant for each store call. The grant names the workspace, the claim epoch and the repository of the volume attributes, and it expires after one hour (`internal/vfs/nodegrant`). A publish whose claims are incomplete, such as epoch 0, fails.

### Mount mode

The FUSE mount is the read-only lower layer. Every mutating operation answers `EROFS`: `mkdir`, `symlink`, `create`, `unlink`, `rmdir`, `rename`, `setattr`, `write`, `fallocate`, and an `open` with a write flag. `fsync` answers success. Entry, attribute and negative timeouts are one hour, because the lower never changes while the mount lives. A lookup reads trees in bulk (`GET /v1/vcs/trees/{sha}`). The first read of a file reads its blob and the blobs that follow it in tree order in one batch (`POST /v1/vcs/blobs/batch`). A node cache that all workspaces on the node share keeps the blobs. The cache is keyed by SHA only: the control op `getbatch` serves a cached blob to any workspace that names its SHA, whatever its repository.

`mount` refuses to start without `-upper` and `-ctl-sock`. It binds the socket before it mounts. It runs `upperdir.ProbeTrustedBeside` first and fails with a `CAP_SYS_ADMIN` error when the process cannot read `trusted.*` attributes on the file system of the upper directory. The probe uses a new sibling directory and never writes into the upper directory.

The control socket lies in a directory that no other workspace can reach. The daemon refuses a directory that is open to others, creates the socket with mode 0660 and gives it to the uid and gid of the mount.

### Node plugin

The plugin implements the identity and node services of the CSI spec on a unix socket.

Trust boundary: the volume attributes of a workspace volume come from the pod spec, so a pod names its own workspace, epoch and repository. The plugin signs a `node` grant for whatever those name. It therefore serves only the pods of the namespaces in `-allowed-namespaces`, which the kubelet reports as `csi.storage.k8s.io/pod.namespace`. `NodePublishVolume` refuses a request whose pod namespace is missing (`InvalidArgument`) or not listed (`PermissionDenied`), for a workspace volume and a control socket volume alike, and an empty list refuses every pod. The control plane that creates the pods and the namespaces in the list are trusted to name the right workspace; the plugin does not check a claim against the store. The chart runs it as a privileged DaemonSet with `hostPID`, and its kubelet pods directory propagates to the host. `csi` refuses to start unless it shares the PID namespace of the node and has another mount namespace. The `CSIDriver` sets `attachRequired: false`, `podInfoOnMount: true` and the lifecycle mode `Ephemeral`. A restart of the plugin ends the mounts that it serves, so the DaemonSet updates with `OnDelete`.

`NodePublishVolume` of a workspace volume runs these steps in order:

1. Refuse a volume id with `,`, `:`, `\` or `/`. Read the `mountinfo` of the plugin and of one process of each other mount namespace.
2. Run `ProbeTrustedBeside` on `<vol>/upper`.
3. Unmount a stale overlay at the target, then a stale lower at `<vol>/lower`. `EBUSY` answers `Unavailable`. While any overlay with `upperdir=<vol>/upper` is mounted anywhere, the publish fails with `Unavailable`.
4. Discard `upper` when it has entries and `<vol>/base` is missing or names another commit. Recreate `work`, create `upper`, and write `<vol>/base` (write `base.new`, `fsync`, rename, `fsync` of `<vol>`).
5. Open the VFS, bind the control socket, mount the lower at `<vol>/lower`, serve the socket, and mount overlayfs at the target with `lowerdir=<vol>/lower,upperdir=<vol>/upper,workdir=<vol>/work,redirect_dir=on,metacopy=on,index=off,xino=off,nfs_export=off`.

A failure in step 5 undoes the earlier steps in reverse order. A publish for a volume that the plugin serves returns OK and changes nothing. The control socket of a workspace lies in `ctl/<hash>` of the state directory, where `<hash>` is 32 hex digits of the SHA-256 of `<pod UID>/<workspace>`, so the socket path stays under the 107 bytes of Linux for any pod UID and workspace. A publish fails with `InvalidArgument` and names the path when the state directory makes it longer. The directory has mode 0700, owned by the uid and gid of the mount, so a pod with two workspace volumes has two sockets. The pod UID comes from the kubelet, so a pod cannot name the directory of another pod, and a publish without it fails. A second volume for the same workspace in the same pod fails with `FailedPrecondition`. A volume with the attributes `volume=ctl` and `workspace` binds that directory at its target, in either publish order.

`NodeUnpublishVolume` unmounts the overlay, reads the `mountinfo` again, and fails with `Unavailable` while an overlay still references `<vol>/upper`. Only then it unmounts the lower (lazily only when the plain unmount fails), stops the socket and closes its connections, closes the VFS, and deletes the control directory of the workspace and `<vol>`. The plugin holds no lock across an unmount, a `mountinfo` read or a delete: a call for a volume that another call works on fails with `Aborted`, and no volume waits on another. It never runs `stat` on `<vol>/lower`, because a dead FUSE answers `ENOTCONN`. The directories `upper` and `work` live on the disk of the node with the lifetime of the volume. Only `NodeUnpublishVolume` deletes them.

### Volume attributes

Package `volume` is the public contract between a caller and the plugin. A workspace volume has the attributes `workspace`, `epoch`, `repo`, `commit` and `runtime`. A workspace is 1 to 64 characters of `A-Za-z0-9_-`, because it names a directory. `runtime` only labels telemetry. `ParsePublication` rejects a missing workspace or repository and a commit that is not a SHA-1. The control socket volume has `volume=ctl` and the `workspace` of the volume whose socket it binds. The workspace sees the socket at `volume.CtlDir/volume.CtlSocket` (`/run/motte-vfs/ctl.sock`), and `MOTTE_VFS_SOCK` names it. The CSI driver name is a flag of `csi` and the chart value `driverName`.

### Control protocol

The daemon answers each line of the unix socket with one line. A request is a JSON object with `op`, the fields of the op, and an optional `traceparent`. A request line over 64 MiB is answered with the code `line_too_long`, and the connection stays open. The daemon closes every connection when its mount ends.

A success answer holds exactly the fields that the table names. A field is never omitted, and a list is `[]` when it is empty. An answer with no fields is `{}`. An error answer is `{"err", "code"}`, with `ref` on a lost ref swap. `err` is for people, and a consumer branches on `code`. The `err` text always contains the `code` text, except for `unknown_op`, whose text is `unknown op`. A shim released before `code` matches these substrings (`too_large`, `unknown op`, `not found`, `vcs_claim_lost`, `vcs_generation_conflict`, `vcs_not_found`), so a daemon keeps them for as long as such a shim is deployed. A daemon and a shim are deployed apart, so a shim also talks to a released daemon that sends no `code` and may omit `gen` and `heads` of `opheads` at generation 0. The shim reads an answer with no `code` by those same substrings and reads the missing fields as generation 0 and no heads. This compatibility window ends when no deployed daemon omits the code. The codes:

| code | Cause |
|---|---|
| `invalid_request` | The line is no JSON object, or a field is missing, malformed or over a limit. |
| `unknown_op` | The op does not exist, or the daemon cannot answer it. |
| `line_too_long` | The request line is over 64 MiB. |
| `store_unavailable` | The store call got no answer, or an answer that is no wire error. |
| `internal_error` | The daemon failed on its own side, such as a read of the upper directory. |
| `vcs_object_too_large` | An object is over 16 MiB, or a `getbatch` answer would pass 16 MiB, or the store answers 413 (see Limits and memory). |
| `vcs_not_found`, `vcs_generation_conflict`, `vcs_claim_lost`, `vcs_object_invalid`, `vcs_repo_forbidden`, `vcs_workspace_conflict`, `vcs_workspace_deleted`, `unauthorized`, `invalid_body`, `method_not_allowed` | The wire code of the store answer. |

| op | request | answer |
|---|---|---|
| `get` | `sha` | `type`, `data` (base64) |
| `put` | `type`, `data` (base64) | `sha` |
| `putbatch` | `objects`: list of `{type, data}`, at most 4096 | `shas`, in request order |
| `has` | `shas`, at most 4096 | `present`: one bool for each name, from `POST /v1/vcs/objects/has` |
| `opheads` | `expect`, `heads` (both optional) | `gen`, `heads` |
| `refs` | `name` (optional) | `refs`: list of `{name, commit, generation}` without `name`; `ref` with `name` |
| `swapref` | `name`, `expect`, `commit`, or `delete: true` with `expect` of at least 1 | `ref`; `{}` after a delete; on a lost swap the error answer carries the stored `ref`, when one exists |
| `putop` | `type` (`operations` or `views`), `sha`, `data` (base64) | `{}` |
| `getop` | `type`, `sha` | `data` (base64) |
| `getbatch` | `shas`, at most 4096, `max_size` (optional) | `datas` in request order; `large`: indices of blobs over `max_size`, whose `datas` entry is empty |
| `workspace` | `sha` (the head operation), `commit`, `generation` | `{}` |
| `changes` | none | `boot`, `token`, `base_tree`, `changes`: list of `{path, status, old_sha, old_mode, old_size, new_sha, new_mode, size}` |
| `dirty` | none | `boot`, `token`, `base_tree`, `entries`: list of `{path, kind, exec, opaque}` |
| `token` | none | `boot`, `token`, `base_tree` |
| `stats` | none | cache counters `blob_hits`, `blob_misses`, `tree_calls` (numbers) |

`opheads` without `heads` reads the op heads: generation 0 and no heads before the first write. With `heads` it swaps them when the stored generation is `expect`. The objects in `changes` and `entries` omit a field that is zero or empty. `put` and `putbatch` name each object by the hash of its bytes. `swapref` with `expect` 0 creates the ref. A ref name is a valid ref name, and a commit is 40 lowercase hex digits.

The fixtures in `testdata/ctl` pin the table. A file `<group>.json` is `{"group", "steps"}`. A step is `{"name", "request" or "raw", "answer"}` and may have `"given"`. A consumer opens one connection to a fresh daemon for each file and replays the steps in order. It sends `request` as one line, or `raw` as one line of text, and compares the answer line with `answer`. Objects match key for key, with no key missing or surplus, and lists match element for element. The string `"*"` in an answer matches any value that is present. `given` names a state of the upper directory that only the daemon under test sets up, and other consumers ignore it. `edited` is an upper directory with `a` rewritten and `n` added over the base tree of the fixtures; `clean` is the empty upper directory. The Go tests replay every file against the real socket, the real service and a lower layer on an in-memory upper directory. A shim and its fake replay the same files.

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

`charts/motte-vfsd` installs the `CSIDriver` and the DaemonSet with the plugin container and the kubelet registrar. Values set the image (digest or tag), the store URL `apiURL`, the Secret of the grant key, `allowedNamespaces` (required, no default), `driverName`, `kubeletDir`, `stateDir`, `runtimeClassName`, node selector, node affinity expressions, tolerations, resources and extra environment. The release namespace needs the privileged Pod Security level. The chart creates no Secret and no namespace. `images/motte-vfsd/Dockerfile` builds the image for `linux/amd64` and `linux/arm64`. It holds the daemon and `git`. The workflow `image.yml` builds both on a pull request. On a tag `v*` it publishes `ghcr.io/majorcontext/motte-vfsd` with the tag version and with provenance and SBOM attestations.

### Tests

The packages under `internal/vfs` pin the daemon over an in-memory upper directory, the plugin over a fake of the mount seam, the control protocol fixtures against the real service, and `HTTPStore` against the real service. `internal/vfs/upperdir` tests `Scan` against a model of overlayfs and, as root, against a real overlay. No Go test mounts FUSE. The harness under `internal/vfs/testdata` (`vfsfuzz`, `overlaycheck`, `overlaykit`) checks a real overlay over a running daemon and needs root, `/dev/fuse` and overlayfs. CI vets the tools and runs the tests of `overlaykit`.
