# Architecture

This document is the contract for motte. A change in behavior updates this document in the same pull request.

## Goal and non-goals

Goal: motte stores the repositories of coding agents and the state of their virtual workspaces. A control plane (the caller) creates repositories, claims workspaces and flushes their state. A node daemon and the jj backend read and write objects, refs and operation logs.

Non-goals:

- No git dumb HTTP, web UI, review or pull-request model, access lists, or users.
- No policy about review or merging. The store keeps protected ref patterns and ref events, and the caller decides what they mean.
- No multi-region store. One store serves all callers.
- No compaction or deletion of packs, and no purge of deleted workspaces.

## Components

| Component | Responsibility | Phase |
|---|---|---|
| `grant` | Claims, Ed25519 signer, verifier. Public package. | M1 |
| `api` | Wire types and error codes. Public package. | M1 |
| `bucket` | Object store seam with Cloud Storage (`bucket/gcs`), S3-compatible (`bucket/s3`) and filesystem (`bucket/fs`) buckets, and its conformance suite `bucket/buckettest`. Public package. | M1 |
| `internal/gitpack` | Reader and writer of git pack files and v2 indexes, and the reader of a pushed pack. | M1 |
| `internal/pktline`, `internal/gitproto` | The pkt-line framing, and the git smart HTTP protocol over an object reader: advertisement, v2 fetch and ls-refs, receive-pack. | M6 |
| `internal/land` | Pure rebase of a linear range of commits onto a tip: tree diff, tree apply and the file-level conflict check. | M7 |
| `meta` | Metadata seam: repositories, workspaces and the epoch fence, op heads, refs, protected ref patterns, ref events, event subscriptions, the land queue, lease and log, pack listing. Backends `meta/postgres` and `meta/sqlite`, and the suite `meta/metatest`. Public package. | M2 |
| Store service | HTTP service over a `meta` store and a `bucket` store, and the epoch fence. | M2 |
| `client` | Typed Go client for callers. Public package. | M3 |
| `testserver` | Starts a real server in a caller's test, on PostgreSQL (`Start`) or on SQLite and a temporary directory (`StartLocal`). Public package. | M3 |
| `internal/testcount` | Counts the servers, PostgreSQL schemas and processes that the test helpers start, and prints the counts of a package at the end of its test run for `internal/testtime`. | M3 |
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
| `MOTTE_BUCKET_KIND` | `gcs`, `s3` or `fs`. |
| `MOTTE_BUCKET` | Bucket name for `gcs` and `s3`, directory for `fs`. |
| `MOTTE_S3_ENDPOINT` | Optional, `s3` only. Base URL of an S3-compatible service, such as R2 or MinIO. Default: the AWS endpoint of the region. |
| `MOTTE_S3_REGION` | `s3` only. Region of the bucket. Default: the region of the AWS SDK default chain. Use `auto` for R2. |
| `MOTTE_S3_PATH_STYLE` | Optional, `s3` only. `true` addresses the bucket in the URL path, not the host name. Default `false`. MinIO needs `true`. |
| `MOTTE_TRUSTED_KEYS` | JSON list of `{"kid","key","scopes"}`. `key` is the standard base64 of an Ed25519 public key. `scopes` holds only `node`, `read`, `write`, `import`, `operator` and `merge`. The service refuses to start on any other scope text. |
| `MOTTE_EVENT_RETENTION` | Optional. How long a ref event is kept, as a Go duration. Default `336h`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional. Enables OTLP trace export. |

The `s3` backend takes its credentials from the AWS SDK default chain: the environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), the shared files, or a role. The service refuses to start on a `MOTTE_META` or `MOTTE_BUCKET_KIND` value that it does not know, on a `MOTTE_S3_*` variable with another kind, and on a `MOTTE_S3_PATH_STYLE` value that is not `true` or `false`. The service applies the embedded migrations of its metadata backend at start. The PostgreSQL backend applies them under an advisory lock, so replicas can start together. `GET /health` answers 200. `GET /ready` answers 200 while the metadata backend answers a ping. `GET /metrics` serves Prometheus metrics. These three routes need no grant. A request for a path or method that no route serves answers 404 `vcs_not_found` or 405 `method_not_allowed` in the error envelope.

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
| `POST /v1/repos/{repo}/lands` | Nothing: the handler holds a body of at most 1 MiB. The leader that ends the land reserves for each batch (see Land). | 16 MiB plus 4 times the bytes that the batch holds |

  `POST /v1/vcs/objects` reads its JSON through a decoder that refuses 24 MiB of unparsed text, so one element cannot grow the buffer: it answers 413 `vcs_object_too_large`. The memory of the process is at most 4 times 96 MiB for bodies in the reader slots, plus the 512 MiB budget, plus the 256 MiB pack cache: 1152 MiB. The chart default request is 1536Mi.
- A lookup of a SHA reads the pack indexes of the repository, newest pack first, and a SHA that no pack holds reads every index. The cost grows linearly with the number of packs of a repository. The service accepts this cost: it never compacts packs.

### Store plane

| Route | Scopes |
|---|---|
| `GET /v1/vcs/commits/{sha}` | `node`, `read` |
| `GET /v1/vcs/trees/{sha}` | `node`, `read` |
| `GET /v1/vcs/objects/{sha}` | `node`, `read` |
| `POST /v1/vcs/blobs/batch` | `node`, `read` |
| `GET /v1/vcs/refs`, `GET /v1/vcs/refs/{name...}` | `node`, `read`, `merge` |
| `POST /v1/vcs/objects/has` | `node`, `read`, `import` |
| `POST /v1/vcs/objects` | `node` |
| `PUT /v1/vcs/ops/{kind}/{id}` | `node` |
| `GET /v1/vcs/ops/{kind}/{id}` | `node` |
| `GET /v1/vcs/op-heads`, `PUT /v1/vcs/op-heads` | `node` |
| `PUT /v1/vcs/workspace` | `node` |
| `PUT /v1/vcs/refs/{name...}`, `DELETE /v1/vcs/refs/{name...}` | `node`, `merge` |
| `PUT /v1/vcs/packs/{pack_id}` | `import` |

A ref route serves any repository, whether or not it has a `repos` row. See Protected refs for the ref write rules of the `node` and `merge` scopes.

### Git smart HTTP

Ordinary `git` clones, fetches and pushes a repository over HTTPS. The routes need no `/v1` grant header, and the grant goes in the way that git sends credentials.

| Route | Scope |
|---|---|
| `GET /git/<prefix>/<path>.git/info/refs?service=git-upload-pack` | `read`, `write`, `merge` |
| `POST /git/<prefix>/<path>.git/git-upload-pack` | `read`, `write`, `merge` |
| `GET /git/<prefix>/<path>.git/info/refs?service=git-receive-pack` | `write`, `merge` |
| `POST /git/<prefix>/<path>.git/git-receive-pack` | `write`, `merge` |

- Repository: the URL names the repository as `<prefix>/<path>` where the grant names `<prefix>:<path>`, for example `/git/native/repo_X.git` for `native:repo_X`. The path may hold `/`. The part up to the last `.git/` is the repository. The repository of the URL must equal the `repo` claim of the grant, or the answer is 403 `vcs_repo_forbidden`. A repository that does not exist answers 404 `vcs_not_found`, so a push never creates one: the control plane creates repositories. A repository exists when it has a `repos` row (prefix `native`), or has at least one ref (any other prefix).
- Grant: the client sends HTTP Basic with any user name and a grant as the password, or `Authorization: Bearer <grant>`. A missing or refused grant answers 401 with `WWW-Authenticate: Basic realm="motte"`, so git asks for credentials or runs its credential helper. A grant of another scope for the route (a `read` grant for receive-pack) answers 403.
- Protocols: fetch speaks protocol v2 only (`Git-Protocol: version=2`, git 2.26 and later). The advertisement lists `ls-refs`, `fetch=filter` and `object-format=sha1`. A request for upload-pack without v2 answers 400. Push speaks v0 and v1 receive-pack with the capabilities `report-status`, `delete-refs`, `ofs-delta` and `object-format=sha1`. The dumb protocol, `shallow` and `deepen` requests, `want-ref`, `packfile-uris`, `atomic`, `push-options`, signed pushes and `side-band` on receive-pack are not offered. A filter other than `blob:none` is refused with 400.
- Ref names: a stored ref named `main` is the git ref `refs/heads/main`. A stored name that starts with `refs/` other than `refs/heads/` has the same git name. HEAD names `refs/heads/main` when it exists. A push may write `refs/heads/<name>` and any other `refs/...` name that is valid, and `<name>` must not start with `refs/`. A stored ref named under `refs/heads/` has no git name and is not listed.
- Fetch: a `want` may name any object of the repository. The service lists the commits that the wants reach and the `have` commits that it holds do not reach, walking by commit time. It adds the trees and blobs that those commits reach and that the trees of the edge commits (the held parents) do not hold. With `filter=blob:none` it adds no blob that a tree names, and it sends a blob that a `want` names. It never sends a delta: the pack holds whole objects, so `thin-pack`, `ofs-delta`, `include-tag` and `no-progress` have no effect. A client fetches an annotated tag that it needs with a second `want`. It answers `ready` as soon as one `have` is common, and the pack then leaves out what that commit holds. It streams the pack through side-band 1 with one object in memory at a time, and reads each pack through the cache of ranged reads.
- Push: the service reads the whole request body, inflates a `Content-Encoding: gzip` body first, reads the ref commands, and unpacks the pack. A thin pack is complete: the service resolves each ref delta base from the repository. It stores the objects as one whole pack of the repository, with the SHA-256 of the stored pack as its id, and adds it to the pack listing. Every object is hashed, so the pack is checked as `PUT /v1/vcs/packs/{pack_id}` checks it. The service does not check that the objects of a new commit are connected. It checks only that the new value of a ref exists, is a commit (or a tag, outside `refs/heads/`).
- Ref update: each ref command goes, in order, through the one function `updateRef` of `internal/server/git.go`, which calls `writeRefAs` (see Protected refs) and so `meta.Store.UpdateRef`. The update is a compare and swap on the commit: `old` is the commit that the client saw, a zero `old` creates the ref and a zero `new` deletes it. A ref that is not at `old` fails that command with `ng <ref> stale info`. A protected ref, written with a `write` grant, fails it with `ng <ref> protected`. The other commands go on. There is no atomic push. The service does not tell a force push from a plain one, because git sends no flag for it: the client refuses a non-fast-forward unless it is forced, and the service accepts any update that passes the compare and swap, as git does with `receive.denyNonFastForwards` unset. The reply is `report-status`: `unpack ok` or the refusal of the pack, then `ok <ref>` or `ng <ref> <reason>` for each command.
- Limits: a push body has at most 64 MiB plus 1 MiB of commands, after gzip. A pack has at most 100000 objects, 64 MiB of inflated entry data and 64 MiB of objects, and each object has at most 16 MiB (`gitpack.MaxObjectBytes`). A command list has at most 1000 commands. A fetch request body has at most 8 MiB, with at most 10000 wants and 100000 haves. A fetch holds at most 1000000 objects and commits in its sets, and a larger fetch answers 413 `vcs_object_too_large`. A fetch has no limit on the size of the pack that it streams. A push reserves its body, three times 64 MiB, 4 times 16 MiB and the verification work: B + 192 MiB + 64 MiB + 48 MiB. A fetch reserves 176 MiB. A push takes one of the 4 reader slots while it reads its body, as an upload does.
- The git routes take no part in the epoch fence. A `write` or `merge` grant moves a ref with no workspace, so a workspace that works on the same ref sees the new commit through the ref generation.

### Control plane

| Route | Scope |
|---|---|
| `POST /v1/repos` with `{name, owner}` answers 201 with `{path}` | `operator` |
| `GET /v1/repos` lists repositories, newest first | `operator` |
| `GET /v1/repos/{path}` | `operator` |
| `PUT /v1/workspaces/{id}/claim` with `{epoch, owner, repo}` returns `{epoch, op, snapshot}` | `operator` |
| `POST /v1/workspaces/{id}/flush` with `{epoch, generation, prev, op, snapshot}` | `operator` |
| `DELETE /v1/workspaces/{id}` | `operator` |
| `PUT /v1/repos/{repo}/protected-refs` with `{patterns}` replaces the list and answers 200 with `{patterns}`. `GET` of the same path returns it. `{repo}` is the repository text, with `/` as `%2F`. | `operator` |
| `POST /v1/repos/{repo}/lands` with `{id, ref, base, head, expect_tree}` lands the commits `base..head` on `ref` and answers 200 with `{id, ref, generation, tip, fast_forward, commits}`. See Land. `{repo}` is the repository text, with `/` as `%2F`. | `merge`, `write` |
| `GET /v1/events?cursor=<cursor>&limit=<n>` returns `{events, cursor}` | `operator` |
| `POST /v1/subscriptions` with `{url, secret, repo}` answers 201 with the subscription | `operator` |
| `GET /v1/subscriptions` lists the subscriptions, oldest first | `operator` |
| `DELETE /v1/subscriptions/{id}` answers 204 | `operator` |

### Protected refs

Policy about which ref is special belongs to the caller. The store keeps one list of protected ref patterns for each repository text, and it applies one rule. A pattern is a ref name, or a prefix that ends in `*`: `release/*` matches every ref that starts with `release/`, and `*` matches every ref. A list has at most 100 patterns. A new repository has an empty list, so `main` is not protected.

- A ref write is a create, a swap or a delete. It comes from `PUT` or `DELETE` of `/v1/vcs/refs/{name...}` or from a ref command of a git push. A single function of the server, `writeRefAs`, performs every ref write, and it wakes the delivery worker after a write that succeeds. Any other route that moves a ref must call it, except the land: its leader moves a ref with `meta.Store.LandRef` and applies the same protection rule through `refProtected`, the function that `writeRefAs` also calls (see Land).
- A `node` grant writes a ref on the store plane under its epoch fence, as before. It compares on the generation. When the ref name matches a pattern of the repository of the grant, the write answers 403 `vcs_ref_protected` and changes nothing. The check runs before the write.
- A `write` grant writes a ref by git push with no workspace. It compares on the commit (`UpdateRef`). A push command on a protected ref fails with `ng <ref> protected`, the other commands go on, and the ref does not change. The store plane routes refuse a `write` grant. The pattern is matched against the stored ref name, which is the git name without `refs/heads/`.
- A `merge` grant writes any ref of its repository, protected or not, with no workspace. On the store plane it compares on the generation: `PUT` with `{expect, commit}` where `expect` 0 creates the ref, and `DELETE` with `?expect=`. No epoch is read or checked. A lost swap answers 409 `vcs_generation_conflict` with the stored ref in `details.ref`, as for a `node` write. By git push it compares on the commit, as a `write` grant does. A `merge` grant also reads refs through the two `GET` ref routes.
- `read`, `import` and `operator` grants move no ref.
- `UpdateRef` and `MergeRef` differ only in the condition: `UpdateRef` needs the stored commit to be `old` (git has no generation), and `MergeRef` needs the stored generation to be `expect`. Both take no fence, take a new generation and append one event. The event kind is the writer: `write` or `merge` for `UpdateRef`, `merge` for `MergeRef`.
- Setting the list is not ordered with a ref write that has already passed its check. A write that starts after the `PUT` of the list returns sees the new list.
- The list names no workspace and needs no `repos` row. The pattern text is checked with the ref name rule. The check of `{repo}` is the repository text rule.

### Ref events

Every create, swap and delete of a ref appends one event in the same unit as the ref write, including the `main` that repository creation writes. See Contract of meta.Store. An event has these fields:

| Field | Meaning |
|---|---|
| `partition` | 0 to 63: the hash of the repo and the ref name, modulo 64. |
| `seq` | Positive. With `partition` it identifies the event. In a partition it has no gap and follows commit order, so a consumer that keeps the last `seq` of each partition loses none. |
| `repo`, `ref` | Repository text and ref name. |
| `old`, `new` | Commit before and after. `old` is empty for a create and `new` is empty for a delete. |
| `generation` | Generation of the ref after the write. For a delete, the generation that the ref had. |
| `kind` | `node`, `write`, `merge` or `operator`: the scope of the grant that wrote the ref. The event never holds a grant. |
| `time` | RFC 3339 time of the write, from the clock of the backend. |

`GET /v1/events?cursor=C&limit=M` returns up to `M` events and a new cursor. `limit` defaults to 100 and is at most 1000. The cursor is opaque: it holds the position in every partition. An empty or absent `cursor` starts at the first event of every partition. A consumer passes the `cursor` of an answer as the `cursor` of its next call, and the route returns the events after it. An answer with no events returns the same cursor. A cursor that the route did not return, or that is malformed, answers 400 `invalid_body`. The route is for a consumer that was down. The main path is delivery to a subscription.

Ordering: the 64 partitions of a store are fixed (`meta.Partitions`). An event is identified by `(partition, seq)`. The events of one ref are in one partition and arrive in commit order with no gap. No order holds between two refs, and a consumer must not assume one. The route fills its answer from the lowest partition up, so a page can hold events of some partitions only, and later pages hold the rest.

Retention: the store keeps an event for `MOTTE_EVENT_RETENTION`, 14 days by default. A server removes older events every hour and keeps the highest removed `seq` of each partition. A read from a position below that floor in any partition that the route reads answers 410 `vcs_events_pruned`, and the consumer must read the refs again. A subscription whose cursor in a partition falls below the floor stops delivering that partition and logs an error. The operator deletes the subscription and creates it again.

Delivery is push, at least once:

- An operator registers a subscription with `POST /v1/subscriptions`: a receiver `url` (http or https, with a host and no user information, at most 2048 bytes), a `secret` of 16 to 256 bytes, and an optional `repo` filter. Its cursors start at the newest event of every partition, so it gets no history. The answer carries `cursor`, the acknowledged positions in the form of an event cursor. The store keeps the secret and never returns it.
- Every server replica runs the delivery worker. A subscription has one cursor and one lease for each partition. A worker delivers a partition of a subscription only while it holds that lease, so replicas deliver different partitions of one subscription in parallel, and the order holds in each partition. The lease is columns of the row of the partition, taken by one conditional write. It is 30 seconds, and the holder renews it before each event and while it waits. A replica that crashes loses its leases when they expire, and another replica takes over from the stored cursors. All waits and lease times use the injected `internal/clock`.
- A worker reads the subscriptions and the newest `seq` of every partition, and starts a delivery for each partition that has events above its cursor. A delivery ends when the partition is caught up. A ref write that a replica commits, and the creation of a repository or a subscription, wakes the worker of that replica at once through a non-blocking signal, so a delivery starts with no wait. A delivery that is running when the signal comes reads its partition again before it ends. A write of another replica is found by the periodic pass, which runs every second (`Poll`). The worst-case latency from commit to the first delivery attempt is therefore about 0 on the replica that wrote and one poll, 1 second, on the others, plus the time of one pass. A retry after a refusal waits for the backoff.
- A delivery reads the events of a partition above the cursor and sends them one at a time, in `seq` order. It skips an event that the `repo` filter excludes, and it moves the cursor past it. One request is `POST` of the JSON event to the `url`, with `Content-Type: application/json`. `X-Motte-Partition` and `X-Motte-Sequence` are the partition and the `seq`; the pair identifies the event. `X-Motte-Signature` is `sha256=` and the lowercase hex HMAC-SHA256 of the exact request body under the secret. The request has a limit of 10 seconds, and the worker follows no redirect.
- A 2xx answer acknowledges the event, and the worker then raises the stored cursor. Any other answer, or no answer, makes the worker send the same event again after a wait of 1 second, doubled after each attempt, up to 5 minutes. The worker sends no later event first. A crash between the answer and the cursor write repeats one event, so a receiver must treat a repeated `(partition, seq)` as a repeat. A receiver gets the events of one ref in order, and the events of different partitions in no fixed order.
- A log line of delivery names the subscription, the `seq`, the status and the type of an error. It never holds the URL, the secret or the body.

### Land

A land takes a change that an agent built on an older base, and moves a ref to a tip that holds it. The store checks file-level conflicts against what landed since that base, rebases the change onto the current tip, and moves the ref. Many agents that land on one ref at once therefore need no read, rebase and compare-and-swap loop of their own.

`POST /v1/repos/{repo}/lands` takes `{id, ref, base, head, expect_tree}` and waits for the result, so the caller never polls.

- Grant: `{repo}` is the repository text, and it must equal the `repo` claim of the grant, else 403 `vcs_repo_forbidden`. A `merge` grant lands on any ref. A `write` grant lands only on a ref that no protected pattern matches, else 403 `vcs_ref_protected`. The check runs when the request is accepted and again when the leader runs the land, so a write that starts after `PUT` of the pattern list returns never moves a protected ref. A `node`, `read`, `import` or `operator` grant answers 401. A land takes no workspace and no epoch.
- Request: `ref` is a stored ref name. `base` and `head` are 40 lowercase hex digits. The commits after `base` up to `head` are the change. They are linear: each has one parent, and the first-parent chain of `head` reaches `base`. The objects are stored before the request: a caller puts them with `PUT /v1/vcs/packs/{pack_id}`, `POST /v1/vcs/objects` or a git push to another ref. `id` is optional, 8 to 64 characters of `A-Z a-z 0-9 _ -`. A repeat of an accepted `id` with the same `ref`, `base` and `head` returns the first result and never lands twice, and a repeat with other values answers 400 `invalid_body`. A result is kept for 24 hours.
- Refusals that depend on the request alone: a body, `ref` or `id` that is not valid answers 400 `invalid_body`. `base` equal to `head`, a merge commit in the range, a range that does not reach `base`, a range of more than 1000 commits, and a changed path that is not valid UTF-8 answer 400 `vcs_land_invalid`. A range of more than 100000 changed leaves, a rebase that writes more than 32 MiB of new objects, and a rebase that makes one object above 16 MiB (a tree that grows too big), answer 413 `vcs_object_too_large`. A missing ref, or a missing object of the range or of `base`, answers 404 `vcs_not_found`.
- Conflict: the change touches a path that also changed between `base` and the current tip. The check is by path, not by line. A path is a leaf of the tree: a file, a link or a submodule. A change of the content or the mode, an addition and a removal each count. A rename is a removal and an addition. Two paths clash when they are equal or when one is a directory above the other, so a file that becomes a directory clashes with a change below it. A conflict answers 409 `vcs_land_conflict` with `details.paths`: the sorted paths that changed since `base` and clash with the change, at most 1000. It moves nothing, and it does not stop the other lands of its batch. The paths of the later lands of one batch include the paths of the earlier lands of that batch.
- Expected tree: `expect_tree` is optional, 40 lowercase hex digits, else 400 `invalid_body`. When it is set, the land moves the ref only if the tree of the commit that `head` becomes equals it. Else the land answers 409 `vcs_land_stale` with `details.tree`, the tree that the land would make, and moves nothing. The check runs in the batch on the running tip, after the conflict check, so it sees the earlier lands of the batch. A stale land does not stop the other lands of its batch. A fast-forward compares the tree of `head`, and a head that is the tip compares the tree of the tip. A caller that tested a tree sets it, so the ref holds exactly the tree that it tested. The tree of a rebase depends only on the tip and the change, never on the land time, so a caller can make the tree first by a land of the same `base` and `head` onto another ref that holds the same tip. A repeat of an accepted `id` must carry the same `expect_tree`.
- Result: 200 with `id`, `ref`, `generation`, `tip` and `commits`. `tip` is the commit that `head` became on the ref, and `generation` is the generation of the ref move that holds it. The ref can move on before the answer arrives. `commits` maps each commit of the range to the commit that replaced it, oldest first, as `{old, new}`.
- Fast forward: when the head already descends from the tip, which is when `base` is the tip or the tip is a commit of the range, the ref moves to `head` as it is. `fast_forward` is true and `commits` is empty. A head that is the tip answers 200 with the current generation and moves nothing.
- Rebase: otherwise the store applies the tree changes of each commit of the range, in order, to the tree of the running tip, and writes one new commit for each. It rewrites only the directories that changed. A new commit has the running tip as its parent, the tree that results, the author line as it was, the message as it was, and every other header as it was, except `gpgsig` and `gpgsig-sha256`: the bytes change, so the signature would not hold. A `change-id` header stays. The committer keeps the name and address of the original commit, and its time is the land time in UTC (`+0000`). The land time is the time of the clock of the leader when the batch starts, and every commit of a batch has it. The same inputs and the same land time give the same commits.
- Wait: the request waits for its result for at most 30 seconds. After that it answers 504 `vcs_land_timeout`. The land stays queued and can still land, so the caller repeats the request with the same `id` to get the result. The client method `Land` allows 35 seconds unless `WithBudget` sets another limit.

Queue and leader. Any replica accepts a land. It writes the land to the queue table (`land_queue`) before it does anything else, so a crash after the answer of the write loses nothing. For each `(repo, ref)` one leader at a time drains the queue. The leader is the replica that holds the land lease of the ref, one row of `land_leases` that one conditional write takes. The lease is 10 seconds, and the leader renews it before each batch. All waits and lease times use the injected `internal/clock`.

- A batch: the leader claims up to 256 queued lands, oldest first, and lands them one after the other on a running tip, so a later land sees the earlier ones. It rejects a land that conflicts or is not valid by itself. It writes the new objects of the batch as one pack, then moves the ref once. A batch takes no more lands after its new objects reach 16 MiB, and the rest stay queued for the next batch. One unit (`LandRef`) moves the ref from the tip that the leader read to the new tip, appends the land log rows and one ref event, and ends every item of the batch, with its result. A land that moved the ref has the generation of that move. If another writer, such as a git push, moved the ref since the leader read it, the unit changes nothing, and the leader starts the batch again from the new tip, at most 8 times.
- Wake: a request on a replica that finds no leader there starts one in its process and wakes it, so a land that finds a free ref starts at once. A leader hands each result to its waiting request in the same process and does not poll the store for it. A request on a replica that is not the leader reads its item every 50 milliseconds (`LandsConfig.Poll`) and starts a leader at each poll when the ref has none, so it is found after the lease ends. A leader that finds the queue empty for 4 polls (200 milliseconds) ends the lease. The worst-case extra latency on another replica is therefore about 250 milliseconds plus the time of the batch. The periodic pass of `Run` starts a leader for every ref that has queued lands, so a land whose requester is gone is ended too.
- Crash: a leader that crashes loses the lease when it expires. The next leader claims every item that is not done, including an item that the lost leader claimed and did not end, and lands it. An item lands once: the unit that moves the ref also ends its items, and it needs the lease and the old tip, so a leader that lost its lease or lost a race changes nothing, and an item that the unit ended is never claimed again. A caller that gets no answer repeats the request with the same `id`.
- Failure: a batch that fails with an error that no result can stand for, such as a failed read of the object store, is tried again. After 3 failures in a row the leader ends its items with 500 `internal_error`.

Conflict index. A conflict check needs the paths that changed between `base` and the tip. The land log (`land_log`) has one row for each landed change of a ref: the tip before it, the tip after it, and its changed paths, written in the unit that moves the ref. The paths come from the log when its rows form one unbroken chain from `base` to the tip at the start of the batch. `base` may be the old or the new tip of a row, and the latest such row counts. A chain that is broken by a write of another writer, a `base` that no row names, more than 1000 rows, or a row that kept no paths makes the check diff the tree of `base` against the tree of the tip instead. A diff is made once for each `base` in a batch. A land that changes more than 1000 paths is logged with no paths. A ref keeps its last 10000 log rows, and a unit that adds rows removes the older ones. A path set from the log can hold a path that changed and changed back, so a log check can name a conflict that a diff of the trees would not. A land never finds a conflict in a path that did not change.

Memory. The leader reserves 16 MiB plus 4 bytes for each byte that the batch holds, at its start, and swaps the reservation for a larger one before it reads an object that would pass it. The bytes that a batch holds are the new objects, and the objects that the current land has read. It never holds the old reservation while it waits for the new one, and a reservation above the budget waits for the whole budget. A batch stores one pack, so a repository that lands at a high rate has many small packs, and a lookup reads their indexes (see Limits and memory). The store never compacts them.

Events. A batch that moves a ref appends one event, as any ref write does: `old` is the tip before the batch and `new` is the tip after it, with the generation of the move. Its kind is `merge` when a `merge` grant made any land of the batch, and else `write`. The leader wakes the delivery worker of its replica after the unit. The landed commits are the commits between `old` and `new`.

## Data model

The tables below are the PostgreSQL schema. Every backend of `meta.Store` keeps the same fields. See Backends.

```
repos(path pk, name, owner, created_at)
packs(repo, pack_id, bytes, created_at, pk(repo, pack_id))
refs(repo, name, commit_sha, generation, updated_at, pk(repo, name)); sequence ref_generation
workspaces(id pk, repo, owner, epoch bigint, op, snapshot, report_generation, created_at, deleted_at)
op_heads(workspace_id pk -> workspaces, epoch, generation, heads text[], updated_at)
event_partitions(partition pk, seq, floor)   64 rows
events(partition, seq, repo, ref, old_commit, new_commit, generation, kind, at, pk(partition, seq))
protected_refs(repo pk, patterns text[])
subscriptions(id pk, url, secret, repo, created_at)
subscription_cursors(sub_id, partition, cursor, lease_owner, lease_until, pk(sub_id, partition))
land_queue(repo, id, seq, ref, base, head, expect_tree, kind, state, owner, result, generation, enqueued_at, done_at, pk(repo, id))   seq is unique and rises
land_leases(repo, ref, owner, lease_until, pk(repo, ref))
land_heads(repo, ref, n, pk(repo, ref))   rows ever logged
land_log(repo, ref, n, old_tip, new_tip, paths, pk(repo, ref, n))   paths is null for a change with too many
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
| `meta.Store` | Repositories, workspaces and their epoch fence, op heads, refs, protected ref patterns, ref events, event subscriptions, the land queue, lease and log, the pack listing. | `meta/postgres`, `meta/sqlite` |
| `bucket.Store` | Pack files, pack indexes and op objects. | `bucket/gcs`, `bucket/s3`, `bucket/fs` |

The suites `meta/metatest` and `bucket/buckettest` export `Run(t, newBackend)`. A backend passes its suite before a server uses it. The suite is the executable form of this section, and a difference between them is a defect.

### Contract of meta.Store

Every method that writes is one atomic unit. The unit is one conditional statement, or one atomic batch of statements, or one read-write transaction. No method needs a transaction that spans calls, a row lock that the caller holds, or a read whose result changes the write. The database decides each condition. This is the weakest set that a backend can offer: a database with atomic batches and no interactive transactions, such as Cloudflare D1, meets it.

- Fence: the fence of workspace `ws` at `epoch` and `repo` holds while the workspace row has that id, that epoch and that repo and is not deleted. A write that needs the fence checks it in the same unit as the write. A claim and a write are ordered. A write that is ordered after the claim sees the raised epoch and fails.
- Failed condition: a write whose condition fails changes nothing. The backend then reads the workspace row, and the ref or op-heads row when the method names it, and picks the error. `meta.Fence` holds the precedence, so every backend gives the same one: `ErrClaimLost` wins over a generation conflict. These reads are not part of the unit. The error answers for a state that is at least as new as the write attempt.
- Generation: the generation of a ref is positive and unique in the store. No ref ever takes a generation that was taken before, including a ref that is deleted and created again. The backend takes it from one counter in the unit of the write. A generation is not ordered across refs, and a failed write can skip one.
- Order: names, repositories, pack ids and workspace ids are opaque text. A list sorts them by byte value of their UTF-8 text.
- Time: `Repo.CreatedAt` and the creation time of a pack come from the clock of the backend, with a resolution of 1 millisecond or finer.
- Racers: of N concurrent calls that need the same free condition, exactly one wins and the others get the error of a lost condition.
- Events: every write that creates, moves or deletes a ref appends one event in the same unit as the ref write, and a write that changes nothing appends none. The event holds the repo, the ref, the old and new commit (empty when absent), the generation (that of the deleted ref for a delete), the writer kind and the time. The kind is `node` for `SwapRef` and `DeleteRef`, `merge` for `MergeRef` and `MergeDeleteRef`, the `kind` argument for `UpdateRef`, and `operator` for the `main` that `CreateRepo` writes.
- Partitions: a store has `meta.Partitions` = 64 event partitions. The partition of an event is `meta.Partition(repo, ref)`: the FNV-1a 32-bit hash of the repo, a zero byte and the ref name, modulo 64. The backend gets the partition as an argument, so no SQL computes it. All events of one ref are in one partition. Changing the number is a migration that renumbers the events.
- Event order: each partition has its own counter, taken in the unit of the write. An event is identified by `(partition, seq)`. In a partition `seq` starts at 1, has no gap, and follows commit order: a reader that sees `seq` N sees every event of that partition below N, so a consumer that keeps the last `seq` of each partition loses no event. This gives the order of the events of one ref. There is no order across partitions. Writes to refs in different partitions share no counter and do not wait for each other.
- Retention: retention removes events and keeps, for each partition, the highest removed `seq` as a floor. A read of a partition from a `seq` below its floor is `ErrEventsPruned`. The other partitions are not affected.
- Lease: the lease of a partition of a subscription is columns of its row in `subscription_cursors`. A lease write is one conditional statement on that row, and `now` is an argument, so the clock of the caller decides expiry. The lease passes to a new owner only when `lease_until` is at or before `now`. Owners must have clocks that differ by much less than the lease time. A subscription has one lease and one cursor for each partition.
- Land queue: `EnqueueLand` inserts an item with the next `seq`, in the state pending, and a repeat of a repo and id returns the stored item. The `seq` order is arrival order, and two items that arrive at once have no fixed order. The `result` and `generation` of an item are set only by `LandRef` and `FinishLands`, and an item that is done never changes.
- Land lease: the lease of a ref is one row of `land_leases`. A lease write is one conditional statement, and `now` is an argument, so the clock of the caller decides expiry. The lease passes to a new owner only when `lease_until` is at or before `now`, and the owner may renew it. `ClaimLands`, `LandRef` and `FinishLands` apply only while the owner holds the lease at `now`.
- Land unit: `LandRef` is one atomic unit with the conditions that the lease is held and the ref is at the old commit. It moves the ref with a new generation, appends one event, adds the rows of the batch to `land_log` with the next `n` of `land_heads`, removes the rows older than `meta.LandLogRetention` rows, and ends the items of the batch. A condition that fails changes nothing. The backend then reads the lease and the ref, and picks the error: `ErrLeaseHeld` wins over `*RefConflict`.
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
| `UpdateRef(repo, name, old, new, kind)` | No fence. The stored commit is `old`, or the ref is absent when `old` is empty. | Creates the ref when `old` is empty, deletes it when `new` is empty, else sets the commit. Every write takes a new generation and appends one event of kind `kind` (`write` or `merge`). | `*RefConflict` with the stored ref as `Current`, nil when the ref is absent. |
| `MergeRef(repo, name, 0, sha)` | The ref is absent. | Inserts the ref with a new generation and one event of kind `merge`. | `*RefConflict` with the stored ref as `Current`. |
| `MergeRef(repo, name, expect, sha)` | The stored generation is `expect`. | Sets the commit and a new generation, and appends one event. | `*RefConflict` with `Current` nil when the ref is absent. |
| `MergeDeleteRef(repo, name, expect)` | The stored generation is `expect`. | Deletes the ref and appends one event. | As `MergeRef`. |
| `ProtectedRefs(repo)` | None. | Returns the patterns, in the order that `SetProtectedRefs` stored them. | None. A repo with none returns an empty list. |
| `SetProtectedRefs(repo, patterns)` | None. | Replaces the whole list. | None. |
| `Events(partition, after, limit)` | `after` is not below the floor of the partition. | Lists up to `limit` events of the partition with `seq` above `after`, by `seq`. | `ErrEventsPruned`. |
| `EventHeads()` | None. | Returns the newest `seq` of each partition, 0 for none. | None. |
| `PruneEvents(before)` | None. | Deletes the events with a time before `before` and raises the floor of each partition to its highest deleted `seq`. | None. |
| `CreateSubscription(s)` | No subscription has the id. | Inserts the row and one cursor for each partition, at the newest `seq` that ever existed in it, and returns the row with its cursors and creation time. | `ErrExists`. |
| `Subscriptions()` | None. | Lists by creation time, then id, each with its cursors. | None. |
| `DeleteSubscription(id)` | The subscription exists. | Deletes it, its cursors and its leases. | `ErrNotFound`. |
| `AcquireLease(id, partition, owner, now, until)` | The lease owner of that partition is `owner`, or its `lease_until` is at or before `now`. | Sets the owner and `lease_until` to `until`, and returns the subscription fields and the cursor of the partition. | `ErrNotFound`, else `ErrLeaseHeld`. |
| `AdvanceCursor(id, partition, owner, cursor)` | The lease owner of that partition is `owner`. | Sets the cursor of the partition to the higher of the stored one and `cursor`. | `ErrNotFound`, else `ErrLeaseHeld`. |
| `AddPack(repo, id, bytes)` | No pack row has the repo and id. | Inserts the row with the creation time. | None. A repeat changes nothing and keeps the first `bytes`. |
| `Packs(repo)` | None. | Lists the packs of the repo, oldest first, then by id. | None. |
| `EnqueueLand(r, now)` | None. | Inserts a pending item with the next `seq`, or returns the item that has the repo and id of `r`. | None. |
| `LandItem(repo, id)` | None. | Returns the item, with its result and generation once it is done. | `ErrNotFound`. |
| `LandPending()` | None. | Lists the `(repo, ref)` pairs that have an item that is not done. | None. |
| `AcquireLandLease(repo, ref, owner, now, until)` | The owner of the lease of the ref is `owner`, or its `lease_until` is at or before `now`, or no lease exists. | Sets the owner and `lease_until` to `until`. | `ErrLeaseHeld`. |
| `ReleaseLandLease(repo, ref, owner)` | `owner` holds the lease. | Sets `lease_until` to the epoch. | None. A lease of another owner stays. |
| `ClaimLands(repo, ref, owner, now, limit)` | `owner` holds the lease at `now`. | Marks the oldest `limit` items that are not done as claimed by `owner`, and returns them by `seq`. An item that an earlier owner claimed and did not end comes again. | `ErrLeaseHeld`. |
| `LandRef(m, owner, now)` | `owner` holds the lease at `now`, and the ref is at `m.Old`. | The land unit. Returns the ref with the new generation. | `ErrLeaseHeld`, else `*RefConflict` with the stored ref as `Current`, nil when the ref is absent. |
| `FinishLands(repo, ref, owner, now, results)` | `owner` holds the lease at `now`. | Ends each item of `results`, with its result. An unknown id is skipped. | `ErrLeaseHeld`. |
| `LandLogSince(repo, ref, base, limit)` | None. | Returns the log rows from the latest row that has `base` as its old or new tip, in order. | None. It returns no row when `base` is in no row or more than `limit` rows follow it. |
| `PruneLands(before)` | None. | Deletes the items that ended before `before`. | None. |

A `repo` argument of a fenced write is the repo of the grant. It must equal the repo of the workspace row. Op `heads` is a non-empty list of text. The server validates every other value before it calls the store.

### Contract of bucket.Store

- `Create(key, data)` writes `data` when no object has `key`, and returns true. It returns false and changes nothing when an object has `key`. Of N concurrent creators of one key, exactly one gets true. A stored object is never replaced and never deleted.
- `Get(key)` returns the whole object. `GetRange(key, off, n)` returns up to `n` bytes from `off`, fewer at the end, none when `off` is at or past the end, and the rest of the object when `n` is negative. A missing key is `bucket.ErrNotFound` for both.
- A read that starts after a `Create` that returned sees the whole object. The `fs` backend keeps its on-disk format: a 4096-byte header block, then the data.

### Provided backends

- `meta/postgres` is the production backend. It keeps the schema in the section Data model, applies `meta/postgres/migrations` at start under an advisory lock, and meets each write with one statement, except a ref write, `CreateRepo` and `LandRef`. The fence of a ref or op-heads write is a `FOR SHARE` read of the workspace row inside the statement. A report is one `UPDATE` that locks the row itself. The generation comes from the sequence `ref_generation`. A ref write, `CreateRepo` and `LandRef` are one transaction. It first takes the next event number of the partition with `UPDATE event_partitions SET seq = seq + 1 WHERE partition = $1 RETURNING seq`. The row lock lasts to the commit, so `seq` order is commit order in the partition, and the number comes before any workspace lock, so a claim is not starved by writes that wait for the counter. One statement then writes the ref and the event, and the transaction rolls back when no row changed, so a failed write leaves no gap. Only writes to refs of the same partition wait for each other. `LandRef` takes the event number the same way, then runs one statement of data-modifying parts: the lease check, the ref move, the event, the log rows, the prune and the item results, all of them conditional on the ref that moved. A land lease write and a claim are one statement.
- `meta/sqlite` is for tests and for a service that runs with no other process. It keeps the same fields in the tables of `meta/sqlite/migrations`. It uses a pure Go driver, so the service builds with CGO disabled. It opens one connection, so its writes are ordered, and one process uses one database file. Each write is one transaction of statements in which no read feeds a write. The fence is an `EXISTS` condition of the statement. The generation comes from the row `ref_generation` of the table `counters`, which the same transaction increments. A ref write inserts its event first, with the condition of the ref write, and the number `event_partitions.seq + 1`. A second statement raises `event_partitions.seq` only when that event row exists. The ref write follows with its own condition. A write whose condition fails therefore leaves no event and no gap, in a batch that does not roll back too. `LandRef` follows the same pattern: its event insert carries the lease and ref conditions, and each later statement (the log row of each land and the end of each item, one statement for each) needs that event row to exist for the generation of the unit. Times are integer microseconds, and the heads are JSON text.
- `bucket/gcs`, `bucket/s3` and `bucket/fs` are the object backends. `gcs` writes with the precondition that the object does not exist. `fs` links a synced temporary file to its name.
- `bucket/s3` uses the AWS SDK for Go v2, and only `bucket/s3` imports it. `s3.NewClient` builds the client from the endpoint, region and addressing style. `Create` is `PutObject` with `If-None-Match: *`. An answer of 412 is `created` false. An answer of 409 (`ConditionalRequestConflict`, a concurrent write of the same key is in flight) is retried, and the retry answers 412 or succeeds, so exactly one creator gets true. The retries are bounded: `Create` makes at most 8 attempts, and waits between them on the clock of the package `internal/clock`, for a random time between half of and all of 20 ms doubled for each retry and capped at 1 s. After the eighth answer of 409, `Create` returns an error that wraps it. A wait ends early with the error of the context. `Get` and `GetRange` are `GetObject`, and `GetRange` sends a `Range` header: `bytes=off-` for a negative `n` or an end that overflows, otherwise `bytes=off-(off+n-1)`. An `n` of 0 reads the first byte of the range and returns none, so a missing key is still `bucket.ErrNotFound`. An answer of 416 is an empty result, and `NoSuchKey` is `bucket.ErrNotFound`. A negative `off` is an error. The client sends checksums only where the API needs them, because some S3-compatible services reject the default checksum headers. S3 reads see a completed write, so the read-after-`Create` rule holds. A service must support `If-None-Match` on `PutObject`: AWS S3, R2 and MinIO do. A service that ignores the header breaks the rule that a stored object is never replaced, and `buckettest` shows it.

### A new backend

A backend implements the interface and returns the sentinel errors of its package. It does not map them to wire codes: the server does. It adds a test that calls `metatest.Run` or `buckettest.Run` with a constructor that returns an empty store. It adds a branch to `cmd/motte` that chooses it from the environment. The backends below are not in the repository. Each paragraph states how the backend meets the contract.

- Cloud Spanner (`meta`): each write is one read-write transaction. The transaction reads the rows that the condition names, tests the condition, writes, and commits. Spanner orders transactions strictly, so a claim and a write are ordered. The generation counter is one row that the transaction updates, or a Spanner sequence, which is unique and not ordered. The creation time is the commit timestamp. Reads that pick an error are strong reads. The heads are an `ARRAY<STRING>`.
- Cloudflare D1 (`meta`): each write is one `batch` call. D1 runs a batch as one transaction and rolls it back when a statement fails. D1 has no interactive transaction and no row lock, and the contract needs neither. The conditions are the `WHERE` clauses and `INSERT ... SELECT ... WHERE EXISTS` fences of `meta/sqlite`, which is SQLite SQL. The counter is a row that the first statement of the batch increments. The result of a statement reports the rows that it changed, and a conditional statement that changed no row is the failed condition. Reads that pick an error go to the primary.
- Cloudflare R2 (`bucket`): `bucket/s3` serves R2 with `MOTTE_S3_ENDPOINT` set to the account endpoint of R2 and `MOTTE_S3_REGION=auto`. It needs no other backend.

## Provider seams

Every external provider sits behind an interface that its consumer owns. A provider is a database, an object store, a forge, a cloud API or a remote credential source. A seam has a conformance suite that every implementation runs, and at least two implementations: a local or in-memory implementation counts. The table lists each seam. `internal/seams` checks the table against the code in CI.

| Seam | Interface | Suite | Implementations | Gap |
|---|---|---|---|---|
| meta-store | `meta.Store` | `meta/metatest.Run` | `meta/postgres`, `meta/sqlite` | |
| bucket-store | `bucket.Store` | `bucket/buckettest.Run` | `bucket/gcs`, `bucket/fs`, `bucket/s3` | |
| release-publisher | `internal/release/publish.Publisher` | `internal/release/publish/publishtest.Run` | `internal/release/publish/dir`, `internal/release/publish/github` | |

- A cell that names Go code is `dir.Identifier`: the directory is relative to the module root.
- A seam that meets the rule has an interface, a suite and at least two implementations. A seam that does not has a reason in the Gap column, and its name is a line of `testdata/seam-gaps.txt`. The list only shrinks: CI fails a gap that the merge base does not list.
- A package under the directory of the interface (the suite excepted), or a package that holds `var _ pkg.Interface = ...`, is an implementation. The table must list it.
- Each implementation has a test that calls the suite function.
- Only an implementation package, a package under `cmd/`, or a package in `testdata/provider-sdk-importers.txt` imports a provider SDK. The SDK prefixes are in `internal/seams/providers.go`.
- A provider host name (`ghcr.io`, `github.com`, `api.github.com`, `googleapis.com`, `amazonaws.com`, `r2.cloudflarestorage.com`) appears only in a Go string literal or a YAML file of `.github` or `charts` that `testdata/provider-hosts.txt` lists. The module path and import paths are not literals for this rule.
- A change to a seam updates the table in the same diff.

## jj backend

The crate `jj/` builds the binary `jj`: jj 0.47.1 (`jj-lib` and `jj-cli`, pinned exactly) with four shims and the subcommand `jj motte`. The binary keeps the name `jj` because it is jj with extra stores. A release attaches it for linux amd64 and arm64, with sha256 sums. `go run ./internal/release/cmd/release jj` builds it and `publish` sends it (see Release entry points).

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

`jj motte push -b <bookmark>...` refuses a missing or conflicted bookmark, a new commit that is conflicted, has no description or is the root commit, a rewrite of the default branch of origin, and a workspace with no daemon socket. It probes the claim with `opheads`, reads the tips of origin (`git ls-remote --symref`), fetches the base of each bookmark by SHA with no blobs (falling back to heads at depth 20, 200, 1,000, then unshallow), writes the loose objects of the new commits from the daemon, probes the claim again, and runs `git -c pack.window=0 push --atomic --force-with-lease=refs/heads/<name>:<commit of NAME@origin>`. `pack.window=0` stops the delta search, so the push sends full blobs and never reads a delta base blob that `motte_git`, a blob-less partial clone, lacks. `--no-thin` does not do this: over smart HTTP git ignores it and still makes a thin pack. One jj transaction then sets `NAME@origin`. A bookmark whose tip already equals its commit is recorded without an update. The span `jj.push` carries `remote`, `refs` and `outcome`: `ok`, `refused`, `stale`, `rejected`, `network` or `claim_lost`. A daemon call that fails with `vcs_claim_lost` at any step of a push or a fetch ends the command as `claim_lost`, and any other daemon failure ends it as `network`.

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

Wire error codes keep their `vcs_` text: `vcs_claim_lost`, `vcs_events_pruned` (410), `vcs_generation_conflict`, `vcs_land_conflict` (409), `vcs_land_invalid` (400), `vcs_land_stale` (409), `vcs_land_timeout` (504), `vcs_not_found`, `vcs_object_invalid`, `vcs_object_too_large`, `vcs_ref_protected` (403), `vcs_repo_forbidden`, `vcs_workspace_conflict`, `vcs_workspace_deleted`. Three codes have no prefix: `unauthorized` (401), `invalid_body` (400) and `internal_error` (500). `method_not_allowed` (405) answers a method that its path does not serve. A panic in a handler is logged with its stack and answers 500 `internal_error` when the handler sent no status yet. The error body is `{"error":{"code","message","details"}}`. A conflict on a ref carries the stored ref in `details.ref`.

## Client

Package `client` has one typed method for each route of both planes. A `Client` sends one grant. The caller gives a `GrantFunc` that the client calls before each request, so a caller holds one `Client` for each scope that it uses.

- Each call has a time budget of 2 seconds, which `WithBudget` changes. A deadline on the call context that is earlier than the budget wins.
- The client injects the W3C trace context of the call context into the request headers.
- The client follows no redirect. A 3xx answer is the error `ErrRedirect`, and the grant goes to no other host.
- A failure answer is an `*Error` with the HTTP status, the wire code, the message and the details. `errors.Is` matches the sentinel of the code: `ErrNotFound`, `ErrGenerationConflict`, `ErrClaimLost`, `ErrObjectInvalid`, `ErrObjectTooLarge`, `ErrRefProtected`, `ErrEventsPruned`, `ErrLandConflict`, `ErrLandInvalid`, `ErrLandTimeout`, `ErrRepoForbidden`, `ErrWorkspaceConflict`, `ErrWorkspaceDeleted`, `ErrUnauthorized`, `ErrInvalidBody` and `ErrInternal`. A failure answer with no wire error body is `ErrUnexpected`.
- `CurrentRef` returns the stored ref that a ref generation conflict carries, and `ConflictPaths` returns the paths that a land conflict names.
- `Land` serves a `merge` client and a `write` client. Its time limit is 35 seconds (`DefaultLandBudget`), which is above the 30 seconds that the server waits, unless `WithBudget` sets another.
- `SwapRef` and `DeleteRef` serve a `node` client and a `merge` client. `SetProtectedRefs`, `ProtectedRefs`, `Events`, `CreateSubscription`, `Subscriptions` and `DeleteSubscription` serve an `operator` client.

Package `testserver` runs the real server in the process of a test, on a fresh schema of the PostgreSQL database that the caller names (or `MOTTE_TEST_DATABASE_URL`) and a temporary filesystem bucket. It skips the test when no DSN is set. It returns the base URL, the pool of its schema (for a test that sets up a state no route can make), the metadata store, the bucket and the verifier (for a test that runs a second replica on the same state), the control-plane and node signers, and helpers that mint operator, node, read, write, import and merge grants. It does not run the delivery worker. A land needs no worker: a request starts the leader in the process of the server.

## Observability

- Traces: OTLP, with the endpoint from the environment and no vendor in the code. Span names start with `vcs.serve.` and end with the route, such as `vcs.serve.put.refs`. Requests accept W3C trace headers.
- Metrics: Prometheus at `/metrics`, with names that start with `motte_`: `motte_http_requests_total` and `motte_http_request_duration_seconds`, by route.
- Logs: JSON. A log line of the service never holds a grant, a path or an object body. A log line of the node daemon never holds a grant, and it carries the error text of its own process, which can name a node path.
- Instrumentation is additive. It never changes behavior or errors.

## Deployment

`images/motte/Dockerfile` builds the service image for `linux/amd64` and `linux/arm64`. It holds a static `cmd/motte` binary on a distroless non-root base that the Dockerfile pins by digest. `go run ./internal/release/cmd/release image` builds it, and the `motte-vfsd` image (see Release entry points). The workflow `image.yml` is one caller of that command: it builds both images on a pull request, and on a tag `v*` it pushes them.

`charts/motte` installs a `Deployment` (2 replicas by default), a `ServiceAccount` and a `Service`. The pod runs as non-root with a read-only root file system. The probes use `/health` and `/ready`, and `/metrics` is on the service port. Values set:

- The image (digest or tag), `replicas`, `resources` (default request 1536Mi of memory, which covers the bound in Limits and memory), node selector, tolerations and affinity.
- `serviceAccount.create` (default `true`), `serviceAccount.name` and `serviceAccount.annotations`, for workload identity. With `create` false the chart makes no `ServiceAccount` and the pod uses the account that `serviceAccount.name` names, or the release name when it is empty.
- `listenPort` (default 8080), the container port. It sets `MOTTE_LISTEN`.
- `prometheusAnnotations` (default `true`), which adds the `prometheus.io` scrape annotations for `/metrics`, and `podAnnotations`, more annotations for the pod.
- `service.type` (default `ClusterIP`), `service.loadBalancerIP` and `service.annotations`, so a caller can ask for an internal load balancer.
- `database.dsnSecret`, the name and key of an existing Secret that holds the PostgreSQL DSN.
- `bucket.kind` and `bucket.url`. For `s3`, `bucket.url` is the bucket name, and `bucket.s3.endpoint`, `bucket.s3.region` and `bucket.s3.pathStyle` become `MOTTE_S3_ENDPOINT`, `MOTTE_S3_REGION` and `MOTTE_S3_PATH_STYLE`. The values hold no S3 credential: the AWS SDK default chain reads it from `extraEnv` (a `secretKeyRef`), or from a role that `serviceAccount.annotations` grants. An `fs` bucket is an `emptyDir` volume, for tests only, and the chart rejects it with more than 1 replica.
- `trustedKeys`, a list of `{kid, key, scopes}`, `trustedKeysConfigMap`, an existing ConfigMap that holds the JSON list, or `trustedKeysSecret`, the name and key of an existing Secret that holds it. The Deployment sets `MOTTE_TRUSTED_KEYS` from the one source that is set. The chart fails to render when none or more than one is set.
- `otlpEndpoint`, optional, and `extraEnv`.

The chart creates no Secret and no namespace. `image.repository` has no default: a release sets it, and the chart fails to render when it is empty. A release needs `database.dsnSecret.name`, `bucket.url` and exactly one source of trusted keys. CI runs `helm lint` and `helm template` over the files in `charts/motte/ci`.

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

### Release entry points

The release of an artifact does not depend on a CI provider. The command `go run ./internal/release/cmd/release` is the only code that builds or publishes a release artifact. A CI workflow is a caller: it sets inputs and runs the command, and it holds no build logic. The command needs `docker` with `buildx`, and `gh` only for the `github` destination.

| Command | Effect |
|---|---|
| `image -name N -repo R [-version V] [-push] [-platforms P] [-file F] [-build-arg K=V] [-dry-run]` | Runs `docker buildx build` of `images/N/Dockerfile` (or `F`) for `P` (default `linux/amd64,linux/arm64`). `-version` must be semver. It tags `R:X.Y.Z` and, for a version that is not a prerelease, `R:X.Y`. Without `-push` it only builds, and it loads the image when `P` names one platform. With `-push` it needs `-version`, adds provenance (`mode=max`) and SBOM attestations, and pushes. |
| `jj -out DIR [-arch amd64\|arm64] [-src jj] [-dry-run]` | Builds `jj` in the pinned Debian bookworm Rust image for `linux/ARCH`, runs `jj --help` in the pinned Debian bookworm base image, and writes `DIR/jj-linux-ARCH` and `DIR/jj-linux-ARCH.sha256`. |
| `publish -dir DIR -dest D [-tag T] [-dry-run]` | Sends every file of `DIR` to `D`. `github` attaches them to the release of `T` through `gh`, and creates the release when it is missing. `dir=PATH` copies them to `PATH`. Each destination is an implementation of `publish.Publisher`. A destination that the command does not know is sent by the caller from `DIR`, which holds plain files. |

`REGISTRY_USERNAME` and `REGISTRY_PASSWORD` make `image -push` run `docker login` for the registry host of `R`. When `REGISTRY_PASSWORD` is unset, the caller has signed in already. The registry host and path are inputs, never literals in code or in a chart default.

GitHub is one caller. The workflow `image.yml` reads the repository variables `IMAGE_REGISTRY` (default `ghcr.io`), `IMAGE_NAMESPACE` (default the repository owner, lowercased) and `REGISTRY_USERNAME` (default the actor), and the secret `REGISTRY_PASSWORD` (default `GITHUB_TOKEN`). With none set it pushes `ghcr.io/<owner>/motte` and `ghcr.io/<owner>/motte-vfsd`. The workflow `jj-release.yml` builds each architecture on a native runner and publishes to the destination in the variable `JJ_RELEASE_DEST` (default `github`).

A consumer of the `jj` binary takes the download base URL as a setting and fetches `<base>/jj-linux-<arch>` and `<base>/jj-linux-<arch>.sha256`. The release does not fix the base. For the `github` destination it is `https://github.com/<owner>/<repo>/releases/download/<tag>`.

### Chart and image

`charts/motte-vfsd` installs the `CSIDriver` and the DaemonSet with the plugin container and the kubelet registrar. Values set the image (digest or tag), the store URL `apiURL`, the Secret of the grant key, `allowedNamespaces` (required, no default), `driverName`, `kubeletDir`, `stateDir`, `runtimeClassName`, node selector, node affinity expressions, tolerations, resources and extra environment. The release namespace needs the privileged Pod Security level. The chart creates no Secret and no namespace. `image.repository` has no default: a release sets it. `images/motte-vfsd/Dockerfile` builds the image for `linux/amd64` and `linux/arm64`. It holds the daemon and `git`. The workflow `image.yml` builds both on a pull request and pushes both on a tag `v*`, through the command in Release entry points.

### Tests

The packages under `internal/vfs` pin the daemon over an in-memory upper directory, the plugin over a fake of the mount seam, the control protocol fixtures against the real service, and `HTTPStore` against the real service. `internal/vfs/upperdir` tests `Scan` against a model of overlayfs and, as root, against a real overlay. No Go test mounts FUSE. The harness under `internal/vfs/testdata` (`vfsfuzz`, `overlaycheck`, `overlaykit`) checks a real overlay over a running daemon and needs root, `/dev/fuse` and overlayfs. CI vets the tools and runs the tests of `overlaykit`.
