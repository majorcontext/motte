# motte-jj agent instructions

Some runners load only this file. Read the [repository instructions](../AGENTS.md) too. The protocol and the identity rules are in `docs/architecture.md`, "jj backend".

## Rules

- Keep the crate to four shims: `Backend`, `OpStore`, `OpHeadsStore` and `WorkingCopy`. The store names are written into `.jj/repo/*/type`; a rename is a migration.
- Talk only to the local `motte-vfsd` channel that `MOTTE_VFS_SOCK` names. The one network client is the plain-HTTP OTLP trace exporter, active only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- After a dependency change, `cargo tree` must show no `rustls` or `openssl`. Put no path, file content or credential in a span.
- Keep commit IDs equal to the IDs of stock `GitBackend::write_commit`. `tests/differential.rs` proves it.
- Run from `jj/`: `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --locked`.
- `MOTTE_DIFF_COMMITS=5000` runs the larger differential corpus. Run it before a jj pin change.
- Build the binary with `cargo build --release --locked --bin jj`. The binary is named `jj` because it is jj with the `jj motte` subcommand.

## Pin policy

- Pin `jj-lib` and `jj-cli` to one exact version in `Cargo.toml`. Commit `Cargo.lock` and pass `--locked`.
- Move jj in one change: `Cargo.toml` and `Cargo.lock`.
- Keep `gix` at the version that `jj-lib` resolves.
