//! `jj motte` init, flush, sync, restore and attach, driven through the built binary against the
//! fake daemon that `tests/fixtures.rs` checks against the contract. Each test names the failure
//! it catches.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use common::{FakeDaemon, Fixture, Workspace, write_tree};
use serde_json::Value;

fn files() -> BTreeMap<&'static str, &'static str> {
    [("a", "1"), ("b", "2")].into()
}

fn flush(f: &Fixture, ws: &Workspace) -> Value {
    let out = f.jj_env(&ws.mount, &["motte", "flush"], &[]);
    serde_json::from_str(out.lines().last().unwrap()).unwrap_or_else(|e| panic!("{e}: {out}"))
}

fn sock(f: &Fixture) -> String {
    f.daemon.path().to_str().unwrap().to_owned()
}

/// A second mount of the files, as a new box would have.
fn mount_of(f: &Fixture, name: &str) -> PathBuf {
    let mount = f.dir.path().join(name);
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &files());
    mount
}

#[test]
fn a_flush_uploads_the_op_log_swaps_the_heads_and_reports_the_workspace() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let out = flush(&f, &ws);
    let op = out["op"].as_str().unwrap();
    let st = f.daemon.state.lock().unwrap();
    let (generation, heads) = st.opheads.clone().unwrap();
    assert_eq!(heads, [op], "the remote heads are the local head");
    assert_eq!(
        out["generation"], generation,
        "the report carries the swap generation"
    );
    assert!(
        st.op_objects
            .contains_key(&("operations".into(), op.into())),
        "the op was uploaded"
    );
    assert_eq!(st.reports.len(), 1, "one report");
    assert_eq!(st.reports[0].0, op);
    assert_eq!(out["reported"], true);
}

#[test]
fn a_sync_drains_an_op_that_a_failed_update_left_behind() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let first = flush(&f, &ws)["op"].as_str().unwrap().to_owned();
    f.daemon
        .state
        .lock()
        .unwrap()
        .failing
        .insert("opheads".into(), "store_unavailable".into());
    let (ok, text) = f.jj_try(&ws.mount, &["describe", "-m", "y"], &[]);
    assert!(ok, "a store failure must not fail the command: {text}");
    let marker = ws.jj_root.join(".jj/repo/motte_unsynced");
    assert!(
        marker.exists(),
        "the failed update left no motte_unsynced mark"
    );
    f.daemon.state.lock().unwrap().failing.clear();
    let head = f.jj(
        &ws.mount,
        &["op", "log", "-n1", "--no-graph", "-T", "id.short(12)"],
    );
    let at = format!("--at-operation={head}");
    let out = f.jj(&ws.mount, &[&at, "motte", "sync", "--if-unsynced"]);
    assert!(out.contains("\"drained\":true"), "{out}");
    assert!(!marker.exists(), "a drain clears the mark");
    let heads = f.daemon.state.lock().unwrap().opheads.clone().unwrap().1;
    assert_ne!(heads, [first], "the remote heads still lag");
    let before = f.daemon.ops("opheads");
    let out = f.jj(&ws.mount, &[&at, "motte", "sync", "--if-unsynced"]);
    assert!(out.contains("\"drained\":false"), "{out}");
    assert_eq!(
        f.daemon.ops("opheads"),
        before,
        "a clean workspace made a daemon call"
    );
}

#[test]
fn a_restore_rebuilds_the_workspace_from_the_op_log_in_the_store() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let out = flush(&f, &ws);
    let (mount, root) = (mount_of(&f, "mount2"), f.dir.path().join("root2"));
    let args = restore_args(&f, &ws, &out, &mount, &root);
    let text = f.jj_env(
        &mount,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        &[],
    );
    assert!(
        text.contains("Restored motte workspace at operation"),
        "{text}"
    );
    let st = f.jj_env(&mount, &["st"], &[]);
    assert!(st.contains("The working copy has no changes."), "{st}");
}

/// Catches a restore that reads the op log and the commits of the index one after another: each
/// object names the next, so without the restore list of the flush a restore costs one daemon
/// round trip for each operation, view and commit.
#[test]
fn a_restore_reads_the_op_log_and_the_commits_at_once() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    for _ in 0..12 {
        f.jj(&ws.mount, &["new"]);
    }
    let out = flush(&f, &ws);
    let (mount, root) = (mount_of(&f, "mount2"), f.dir.path().join("root2"));
    let args = restore_args(&f, &ws, &out, &mount, &root);
    f.daemon.state.lock().unwrap().read_delay_ms = 20;
    let text = f.jj_env(
        &mount,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        &[],
    );
    assert!(
        text.contains("Restored motte workspace at operation"),
        "{text}"
    );
    let peaks = f.daemon.state.lock().unwrap().reads_peak.clone();
    for op in ["get", "getop"] {
        let peak = peaks.get(op).copied().unwrap_or(0);
        assert!(
            peak >= 4,
            "at most {peak} {op} reads were in flight at once"
        );
    }
    f.daemon.state.lock().unwrap().read_delay_ms = 0;
    let log = f.jj_env(
        &mount,
        &["log", "--no-graph", "-T", "commit_id ++ \"\\n\""],
        &[],
    );
    assert_eq!(log.lines().count(), 15, "{log}");
}

/// Catches a restore that reads on more connections than the caller allowed: a caller lowers the
/// width when many workspaces wake at once.
#[test]
fn a_restore_reads_on_no_more_connections_than_its_prefetch_width() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    for _ in 0..6 {
        f.jj(&ws.mount, &["new"]);
    }
    let out = flush(&f, &ws);
    let (mount, root) = (mount_of(&f, "mount2"), f.dir.path().join("root2"));
    let mut args = restore_args(&f, &ws, &out, &mount, &root);
    args.extend(["--prefetch-width".to_owned(), "1".to_owned()]);
    f.daemon.state.lock().unwrap().read_delay_ms = 20;
    f.jj_env(
        &mount,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        &[],
    );
    let all = f.daemon.state.lock().unwrap().reads_peak_all;
    assert_eq!(all, 1, "{all} reads were in flight at once");
}

fn restore_args(
    f: &Fixture,
    ws: &Workspace,
    out: &Value,
    mount: &std::path::Path,
    root: &std::path::Path,
) -> Vec<String> {
    [
        "motte",
        "restore",
        "--sock",
        &sock(f),
        "--mount",
        mount.to_str().unwrap(),
        "--jj-root",
        root.to_str().unwrap(),
        "--commit",
        out["commit"].as_str().unwrap(),
        "--op",
        out["op"].as_str().unwrap(),
        "--base",
        &ws.commit,
    ]
    .map(str::to_owned)
    .to_vec()
}

#[test]
fn a_restore_refuses_op_bytes_that_do_not_hash_to_their_id() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let out = flush(&f, &ws);
    let op = out["op"].as_str().unwrap().to_owned();
    {
        let mut st = f.daemon.state.lock().unwrap();
        let other = st
            .op_objects
            .iter()
            .find(|((kind, id), _)| kind == "operations" && *id != op)
            .map(|(_, data)| data.clone())
            .expect("the log holds another operation");
        st.op_objects
            .insert(("operations".into(), op.clone()), other);
    }
    let (mount, root) = (mount_of(&f, "mount2"), f.dir.path().join("root2"));
    let args = restore_args(&f, &ws, &out, &mount, &root);
    let (ok, text) = f.jj_try(
        &mount,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        &[],
    );
    assert!(!ok && text.contains("are not operation"), "{text}");
}

#[test]
fn an_attach_links_the_jj_dir_into_a_new_mount_of_the_flushed_commit() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let out = flush(&f, &ws);
    let mount = mount_of(&f, "mount3");
    let args = [
        "motte",
        "attach",
        "--sock",
        &sock(&f),
        "--mount",
        mount.to_str().unwrap(),
        "--jj-root",
        ws.jj_root.to_str().unwrap(),
        "--commit",
        out["commit"].as_str().unwrap(),
        "--op",
        out["op"].as_str().unwrap(),
    ];
    let text = f.jj_env(&mount, &args, &[]);
    assert!(
        text.contains("Attached motte workspace at operation"),
        "{text}"
    );
    assert!(
        std::fs::symlink_metadata(mount.join(".jj"))
            .unwrap()
            .is_symlink()
    );
    let st = f.jj_env(&mount, &["st"], &[]);
    assert!(st.contains("The working copy has no changes."), "{st}");
}

#[test]
fn an_attach_of_a_commit_that_no_flush_named_is_refused() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    let out = flush(&f, &ws);
    let mount = mount_of(&f, "mount3");
    let args = [
        "motte",
        "attach",
        "--sock",
        &sock(&f),
        "--mount",
        mount.to_str().unwrap(),
        "--jj-root",
        ws.jj_root.to_str().unwrap(),
        "--commit",
        &ws.commit,
        "--op",
        out["op"].as_str().unwrap(),
    ];
    let (ok, text) = f.jj_try(&mount, &args, &[]);
    assert!(!ok && text.contains("no flush named"), "{text}");
}

#[test]
fn an_init_on_an_object_that_is_no_commit_fails_with_a_message_and_no_panic() {
    let f = Fixture::with(FakeDaemon::full());
    let blob = common::put(&f.daemon, "blob", b"x");
    let (mount, root) = (mount_of(&f, "mount2"), f.dir.path().join("root2"));
    let args = [
        "motte",
        "init",
        "--sock",
        &sock(&f),
        "--commit",
        &blob,
        "--jj-root",
        root.to_str().unwrap(),
    ];
    let (ok, text) = f.jj_try(&mount, &args, &[]);
    assert!(
        !ok && !text.contains("panicked") && text.contains("tree line"),
        "{text}"
    );
}

#[test]
fn a_snapshot_of_a_file_over_16_mib_names_the_file_and_the_limit() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    std::fs::write(ws.mount.join("big"), vec![b'x'; (16 << 20) + 1]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(120));
    f.daemon.state.lock().unwrap().entries =
        vec![serde_json::json!({"path": "big", "kind": "file"})];
    let args = ["--config", "snapshot.max-new-file-size=100000000", "st"];
    let (ok, text) = f.jj_try(&ws.mount, &args, &[]);
    assert!(
        !ok && text.contains("big is 16777217 bytes") && text.contains("16 MiB"),
        "{text}"
    );
}

#[test]
fn a_snapshot_reads_the_remote_op_heads_once_and_swaps_them_once() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    flush(&f, &ws);
    std::thread::sleep(std::time::Duration::from_millis(120));
    std::fs::write(ws.mount.join("a"), "one\n").unwrap();
    f.daemon.state.lock().unwrap().entries = vec![serde_json::json!({"path": "a", "kind": "file"})];
    let before = f.daemon.ops("opheads");
    let out = f.jj_env(&ws.mount, &["motte", "snapshot"], &[]);
    let report: Value = serde_json::from_str(out.lines().last().unwrap()).unwrap();
    assert_eq!(report["snapshot"], true, "{out}");
    assert_eq!(f.daemon.ops("opheads") - before, 2, "one read, one swap");
}

#[test]
fn a_snapshot_whose_blobs_are_not_stored_saves_no_stamp_for_them() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    flush(&f, &ws);
    std::thread::sleep(std::time::Duration::from_millis(120));
    std::fs::write(ws.mount.join("a"), "one\n").unwrap();
    {
        let mut st = f.daemon.state.lock().unwrap();
        st.entries = vec![serde_json::json!({"path": "a", "kind": "file"})];
        st.failing.insert("putbatch".into(), "internal".into());
    }
    let (ok, text) = f.jj_try(&ws.mount, &["motte", "snapshot"], &[]);
    let cache = f
        .dir
        .path()
        .join("jjroot/.jj/working_copy/motte_wc_cache.json");
    assert!(!ok && !cache.exists(), "{text}");
}

#[test]
fn a_snapshot_stores_its_blobs_trees_and_commit_in_one_call() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = f.workspace(&files(), &[]);
    flush(&f, &ws);
    std::thread::sleep(std::time::Duration::from_millis(120));
    std::fs::write(ws.mount.join("a"), "one\n").unwrap();
    f.daemon.state.lock().unwrap().entries = vec![serde_json::json!({"path": "a", "kind": "file"})];
    let calls = || f.daemon.ops("put") + f.daemon.ops("putbatch");
    let before = calls();
    let out = f.jj_env(&ws.mount, &["motte", "snapshot"], &[]);
    assert!(out.contains("\"snapshot\":true"), "{out}");
    assert_eq!(calls() - before, 1, "one putbatch for the whole snapshot");
}
