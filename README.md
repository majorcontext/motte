# motte

motte is a version control store and a set of virtual workspaces for coding agents. It is built on [jj](https://github.com/jj-vcs/jj).

Status: early. The repository holds milestone M1: the specification, the grant format, the wire types and the store primitives. The store service, the client, the workspace daemon and the jj backend follow.

Run it with no other service: `go test ./...` runs the tests that need no database, and the section Run locally of `docs/architecture.md` starts the service on SQLite and a directory. The metadata store and the object store are interfaces (`meta`, `bucket`) with PostgreSQL, SQLite, Cloud Storage and filesystem backends.

Read `docs/architecture.md` for the design. Read `AGENTS.md` before you change the repository.

License: MIT. The `jj/` crate is Apache-2.0, because it adapts code from jj-lib.
