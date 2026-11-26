# motte-jj agent instructions

Some runners load only this file. Read the [repository instructions](../AGENTS.md) too. The protocol and the identity rules are in `docs/architecture.md`, "jj backend".

## Rules

- Keep the crate to four shims: `Backend`, `OpStore`, `OpHeadsStore` and `WorkingCopy`. The store names are written into `.jj/repo/*/type`; a rename is a migration.
- The command process talks only to the local `motte-vfsd` channel that `MOTTE_VFS_SOCK` or the file `store/ctl_sock` names. It opens no network connection. The one network client is the trace export mode (`jj/src/trace_export.rs`), a detached copy of the binary that runs only when an OTLP endpoint is set.
- `rustls` is allowed for the trace export mode only. After a dependency change, `cargo tree` must show no `openssl`. Put no path, file content or credential in a span.
- Keep commit IDs equal to the IDs of stock `GitBackend::write_commit`. `tests/differential.rs` proves it.
- Keep the fake daemon in `tests/common/mod.rs` equal to the daemon. `tests/fixtures.rs` replays `testdata/ctl` against it. Change a request or an answer in `jj/src/ctl/wire.rs` and in the fixtures together.
- Keep every function at 80 lines or fewer: `clippy::too_many_lines` fails the check.
- Run from `jj/`: `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --locked`.
- `MOTTE_DIFF_COMMITS=5000` runs the larger differential corpus. Run it before a jj pin change.
- Build the binary with `cargo build --release --locked --bin jj`. The binary is named `jj` because it is jj with the `jj motte` subcommand.

## Pin policy

- Pin `jj-lib` and `jj-cli` to one exact version in `Cargo.toml`. Commit `Cargo.lock` and pass `--locked`.
- Move jj in one change: `Cargo.toml` and `Cargo.lock`.
- Keep `gix` at the version that `jj-lib` resolves.
