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
| jj backend | The jj crate that talks to the store plane. | M5 |

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
