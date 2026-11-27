//! The crate against a fake daemon: batched blob puts, and the `jj` binary on a mount.

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use common::{FakeDaemon, Fixture, write_tree};
use gix::objs::Kind;
use jj_lib::object_id::ObjectId as _;
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, ObjectStore, hash_object};
use motte_jj::stores::{clear_drained, merge_remote, mirror, put_ops, read_heads, update_unsynced};

fn blobs(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("blob {i}\n").into_bytes()).collect()
}

#[test]
fn put_many_sends_one_request_and_skips_known_objects() {
    let d = FakeDaemon::start(true);
    let c = CtlClient::new(d.path());
    let data = blobs(100);
    let objs: Vec<_> = data.iter().map(|b| (Kind::Blob, b.as_slice())).collect();
    let ids = c.put_many(&objs).unwrap();
    for (id, b) in ids.iter().zip(&data) {
        assert_eq!(*id, hash_object(Kind::Blob, b));
    }
    assert_eq!((d.ops("putbatch"), d.ops("put")), (1, 0));
    assert_eq!(d.state.lock().unwrap().objects.len(), 100);
    c.put_many(&objs).unwrap();
    assert_eq!(d.ops("putbatch"), 1, "known objects are not sent again");
}

#[test]
fn put_many_falls_back_to_put_on_a_daemon_without_putbatch() {
    let d = FakeDaemon::start(false);
    let c = CtlClient::new(d.path());
    let data = blobs(5);
    let objs: Vec<_> = data.iter().map(|b| (Kind::Blob, b.as_slice())).collect();
    c.put_many(&objs).unwrap();
    assert_eq!((d.ops("putbatch"), d.ops("put")), (1, 5));
    let more = [(Kind::Blob, b"another".as_slice())];
    c.put_many(&more).unwrap();
    assert_eq!(d.ops("putbatch"), 1, "the missing op is remembered");
    assert_eq!(d.state.lock().unwrap().objects.len(), 6);
}

#[test]
fn get_rejects_bytes_that_do_not_hash_to_the_id() {
    let d = FakeDaemon::start(true);
    let id = hash_object(Kind::Blob, b"wanted");
    d.state
        .lock()
        .unwrap()
        .objects
        .insert(id.to_string(), ("blob".into(), b"other".to_vec()));
    assert!(CtlClient::new(d.path()).get(&id).is_err());
}

#[test]
fn jj_snapshots_the_dirty_set_and_new_writes_only_the_changed_files() {
    let f = Fixture::new();
    let a: BTreeMap<&str, &str> = [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")].into();
    let ta = f.tree(&a);
    let ca = f.commit(ta.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = ta.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &a);
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            f.daemon.path().to_str().unwrap(),
            "--commit",
            &ca,
            "--jj-root",
            jj_root.to_str().unwrap(),
        ],
    );
    assert!(
        std::fs::symlink_metadata(mount.join(".jj"))
            .unwrap()
            .is_symlink()
    );
    assert!(
        f.jj(&mount, &["st"])
            .contains("The working copy has no changes.")
    );

    std::fs::write(mount.join("b"), "two").unwrap();
    std::fs::write(mount.join("e"), "5").unwrap();
    std::fs::remove_file(mount.join("d")).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(120));
    f.daemon.state.lock().unwrap().entries.extend([
        serde_json::json!({"path": "b", "kind": "file"}),
        serde_json::json!({"path": "e", "kind": "file"}),
        serde_json::json!({"path": "d", "kind": "deleted"}),
    ]);
    let before = f.daemon.ops("putbatch") + f.daemon.ops("put");
    let st = f.jj(&mount, &["st"]);
    assert!(
        st.contains("M b") && st.contains("A e") && st.contains("D d"),
        "{st}"
    );
    assert!(f.daemon.ops("putbatch") + f.daemon.ops("put") > before);
    assert!(
        jj_root
            .join(".jj/working_copy/motte_wc_cache.json")
            .exists(),
        "the snapshot leaves its file cache"
    );
    f.jj(&mount, &["commit", "-m", "x"]);
    let out = f.jj(&mount, &["new", &ca]);
    assert!(
        out.contains("Added 1 files, modified 1 files, removed 1 files"),
        "{out}"
    );
    for (name, body) in &a {
        assert_eq!(&std::fs::read_to_string(mount.join(name)).unwrap(), body);
    }
    assert!(!mount.join("e").exists());
}

/// Runs `jj` in `cwd` against the fake daemon and returns its status and output.
fn jj_status(f: &Fixture, cwd: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_jj"))
        .args(args)
        .current_dir(cwd)
        .env("HOME", f.dir.path())
        .env("XDG_CONFIG_HOME", f.dir.path())
        .env("JJ_CONFIG", f.dir.path().join("none.toml"))
        .env("JJ_USER", "t")
        .env("JJ_EMAIL", "t@example.com")
        .env("MOTTE_VFS_SOCK", f.daemon.path())
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn a_daemon_without_overlay_mode_is_refused_with_a_clear_message() {
    let f = Fixture::new();
    f.daemon.state.lock().unwrap().token.clear();
    let (ok, text) = jj_status(&f, f.dir.path(), &["motte", "dirty"]);
    assert!(
        !ok && text.contains("does not run in overlay mode"),
        "{text}"
    );
}

#[test]
fn a_state_file_with_unknown_fields_still_loads() {
    let f = Fixture::new();
    let a: BTreeMap<&str, &str> = [("a", "1")].into();
    let ta = f.tree(&a);
    let ca = f.commit(ta.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = ta.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &a);
    let sock = f.daemon.path().to_str().unwrap();
    let root = jj_root.to_str().unwrap();
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            sock,
            "--commit",
            &ca,
            "--jj-root",
            root,
        ],
    );
    let state = jj_root.join(".jj/working_copy/motte_wc.json");
    let mut doc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    doc["generation"] = 7.into();
    doc["rescan"] = serde_json::json!(["a"]);
    doc["unrebased"] = 3.into();
    doc["overlay"] = false.into();
    std::fs::write(&state, serde_json::to_vec(&doc).unwrap()).unwrap();
    assert!(
        f.jj(&mount, &["st"])
            .contains("The working copy has no changes.")
    );
}

/// Accepts OTLP/HTTP posts and sends (path, body) of each. The command exports after it exits, so a
/// test waits on the channel.
type Posts = std::sync::mpsc::Receiver<(String, Vec<u8>)>;

fn fake_collector() -> (String, Posts) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let (tx, got) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for conn in l.incoming() {
            let Ok(mut conn) = conn else { return };
            let mut r = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            let path = line.split(' ').nth(1).unwrap_or("").to_owned();
            let mut len = 0;
            loop {
                line.clear();
                r.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; len];
            r.read_exact(&mut body).unwrap();
            tx.send((path, body)).unwrap();
            conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        }
    });
    (url, got)
}

#[test]
fn jj_sends_traceparent_and_exports_spans() {
    let f = Fixture::new();
    let a: BTreeMap<&str, &str> = [("a", "1")].into();
    let ta = f.tree(&a);
    let ca = f.commit(ta.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = ta.hex();
    let mount = f.dir.path().join("mount");
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &a);
    let sock = f.daemon.path().to_str().unwrap();
    let jj_root = f.dir.path().join("jjroot");
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            sock,
            "--commit",
            &ca,
            "--jj-root",
            jj_root.to_str().unwrap(),
        ],
    );

    let (url, got) = fake_collector();
    let trace_id = "0af7651916cd43dd8448eb211c80319c";
    let parent = format!("00-{trace_id}-b7ad6b7169203331-01");
    f.jj_env(
        &mount,
        &["st"],
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", &url),
            ("TRACEPARENT", &parent),
        ],
    );

    let tps = f.daemon.state.lock().unwrap().traceparents.clone();
    assert!(
        tps.iter()
            .any(|t| t.starts_with(&format!("00-{trace_id}-"))),
        "{tps:?}"
    );
    let (path, body) = got.recv().expect("no OTLP request");
    assert_eq!(path, "/v1/traces");
    for name in [&b"jj.command"[..], b"jj.snapshot"] {
        assert!(body.windows(name.len()).any(|w| w == name));
    }
}

#[test]
fn an_unfinished_snapshot_leaves_a_conflicted_working_copy_to_the_next_snapshot() {
    let f = Fixture::new();
    let a: BTreeMap<&str, &str> = [("b", "2\n"), ("c", "0\n"), ("p", "p\n")].into();
    let ta = f.tree(&a);
    let ca = f.commit(ta.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = ta.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &a);
    let sock = f.daemon.path().to_str().unwrap();
    let root = jj_root.to_str().unwrap();
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            sock,
            "--commit",
            &ca,
            "--jj-root",
            root,
        ],
    );
    let set = |e: Vec<serde_json::Value>| f.daemon.state.lock().unwrap().entries = e;
    for side in ["left", "right"] {
        f.jj(&mount, &["new", &ca]);
        std::fs::write(mount.join("c"), format!("{side}\n")).unwrap();
        set(vec![serde_json::json!({"path": "c", "kind": "file"})]);
        f.jj(&mount, &["commit", "-m", side]);
    }
    f.jj(
        &mount,
        &[
            "new",
            "description(substring:left)",
            "description(substring:right)",
        ],
    );

    // An unfinished snapshot resolves `c` and deletes `p`; the working copy keeps the conflicted tree.
    std::fs::write(mount.join("c"), "resolved\n").unwrap();
    std::fs::remove_file(mount.join("p")).unwrap();
    set(vec![
        serde_json::json!({"path": "c", "kind": "file"}),
        serde_json::json!({"path": "p", "kind": "deleted"}),
    ]);
    f.jj(&mount, &["--no-integrate-operation", "st"]);
    let st = f.jj(&mount, &["st"]);
    assert!(st.contains("D p"), "{st}");
}

/// A workspace on `{a, b, c}` whose view modifies `b`, adds `e` and deletes `c`, as the daemon
/// reports them in `changes`.
fn changed_workspace(f: &Fixture) -> (std::path::PathBuf, String) {
    let a: BTreeMap<&str, &str> = [("a", "1\n"), ("b", "2\n"), ("c", "3\n")].into();
    let ta = f.tree(&a);
    let ca = f.commit(ta.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = ta.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    write_tree(&mount, &a);
    let sock = f.daemon.path().to_str().unwrap();
    let root = jj_root.to_str().unwrap();
    let init = [
        "motte",
        "init",
        "--sock",
        sock,
        "--commit",
        &ca,
        "--jj-root",
        root,
    ];
    f.jj(&mount, &init);
    std::fs::write(mount.join("b"), "two\n").unwrap();
    std::fs::write(mount.join("e"), "5\n").unwrap();
    std::fs::remove_file(mount.join("c")).unwrap();
    let sha = |s: &str| hash_object(Kind::Blob, s.as_bytes()).to_string();
    f.daemon.state.lock().unwrap().changes = vec![
        serde_json::json!({"path": "b", "status": "modified", "old_sha": sha("2\n"), "old_mode": 0o100644, "new_sha": sha("two\n"), "new_mode": 0o100644, "size": 4}),
        serde_json::json!({"path": "c", "status": "deleted", "old_sha": sha("3\n"), "old_mode": 0o100644}),
        serde_json::json!({"path": "e", "status": "added", "new_sha": sha("5\n"), "new_mode": 0o100644, "size": 2}),
    ];
    (mount, ca)
}

/// Runs the changes command as harness does: the argv of the repo config and the request in the
/// environment.
fn changes(f: &Fixture, mount: &Path, scope: &str, base: &str, cap: &str) -> serde_json::Value {
    let env = [
        ("HARNESS_CHANGES_SCOPE", scope),
        ("HARNESS_CHANGES_BASE", base),
        ("HARNESS_CHANGES_PATCH_BYTES", cap),
    ];
    let out = f.jj_env(mount, &["motte", "changes", "--json"], &env);
    serde_json::from_str(out.lines().next().unwrap_or_default())
        .unwrap_or_else(|e| panic!("{e}: {out}"))
}

#[test]
fn changes_diffs_the_daemon_view_without_a_snapshot_or_an_operation() {
    let f = Fixture::new();
    let (mount, ca) = changed_workspace(&f);
    let repo = f.dir.path().join("jjroot/.jj/repo");
    let ops = || {
        std::fs::read_dir(repo.join("op_store/operations"))
            .unwrap()
            .count()
    };
    let (ops_before, writes_before) = (ops(), f.daemon.ops("put") + f.daemon.ops("putbatch"));

    let got = changes(&f, &mount, "uncommitted", &ca, "");
    let want_files = serde_json::json!([
        {"path": "b", "status": "modified", "additions": 1, "deletions": 1, "binary": false, "large": false},
        {"path": "c", "status": "deleted", "additions": 0, "deletions": 1, "binary": false, "large": false},
        {"path": "e", "status": "added", "additions": 1, "deletions": 0, "binary": false, "large": false},
    ]);
    let want_patch = "diff --git a/b b/b\nindex 0cfbf08..f719efd 100644\n--- a/b\n+++ b/b\n@@ -1 +1 @@\n-2\n+two\n\
        diff --git a/c b/c\ndeleted file mode 100644\nindex 00750ed..0000000\n--- a/c\n+++ /dev/null\n@@ -1 +0,0 @@\n-3\n\
        diff --git a/e b/e\nnew file mode 100644\nindex 0000000..7ed6ff8\n--- /dev/null\n+++ b/e\n@@ -0,0 +1 @@\n+5\n";
    assert_eq!(got["files"], want_files, "{got}");
    assert_eq!(got["patch"], want_patch, "{got}");
    assert_eq!(
        (&got["engine"], &got["truncated"]),
        (&"jj".into(), &false.into())
    );

    let branch = changes(&f, &mount, "branch", &ca, "");
    assert_eq!(branch["files"], want_files);
    assert_eq!(
        (
            ops(),
            f.daemon.ops("put") + f.daemon.ops("putbatch"),
            f.daemon.ops("snapshot")
        ),
        (ops_before, writes_before, 0)
    );
}

#[test]
fn changes_cuts_the_patch_at_the_cap_and_keeps_every_file() {
    let f = Fixture::new();
    let (mount, _) = changed_workspace(&f);
    let got = changes(&f, &mount, "uncommitted", "", "100");
    assert_eq!(got["files"].as_array().unwrap().len(), 3);
    assert_eq!(
        got["patch"],
        "diff --git a/b b/b\nindex 0cfbf08..f719efd 100644\n--- a/b\n+++ b/b\n@@ -1 +1 @@\n-2\n+two\n"
    );
    assert_eq!(got["truncated"], true);
}

fn paths(got: &serde_json::Value) -> Vec<&str> {
    got["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap())
        .collect()
}

#[test]
fn changes_leaves_out_a_new_file_that_jj_would_not_track() {
    let f = Fixture::new();
    let (mount, _) = changed_workspace(&f);
    std::fs::write(mount.join(".gitignore"), "node_modules/\n").unwrap();
    std::fs::create_dir(mount.join("node_modules")).unwrap();
    std::fs::write(mount.join("node_modules/x.js"), "x\n").unwrap();
    f.daemon.state.lock().unwrap().changes.extend([
        serde_json::json!({"path": ".gitignore", "status": "added", "new_mode": 0o100644, "size": 14}),
        serde_json::json!({"path": "node_modules/x.js", "status": "added", "new_mode": 0o100644, "size": 2}),
    ]);
    let got = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(paths(&got), [".gitignore", "b", "c", "e"], "{got}");
}

#[test]
fn changes_takes_the_old_side_from_the_target_when_it_differs_from_the_mount_base() {
    let f = Fixture::new();
    let (mount, ca) = changed_workspace(&f);
    f.daemon.state.lock().unwrap().entries = vec![
        serde_json::json!({"path": "b", "kind": "file"}),
        serde_json::json!({"path": "e", "kind": "file"}),
        serde_json::json!({"path": "c", "kind": "deleted"}),
    ];
    f.jj(&mount, &["commit", "-m", "x"]);
    std::fs::write(mount.join("b"), "three\n").unwrap();
    let sha = |s: &str| hash_object(Kind::Blob, s.as_bytes()).to_string();
    let committed = f.jj(
        &mount,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    let mut st = f.daemon.state.lock().unwrap();
    let raw = st.objects[committed.trim()].1.clone();
    st.base_tree = String::from_utf8(raw[5..45].to_vec()).unwrap();
    st.entries = vec![serde_json::json!({"path": "b", "kind": "file"})];
    drop(st);
    f.daemon.state.lock().unwrap().changes = vec![
        serde_json::json!({"path": "b", "status": "modified", "old_sha": sha("two\n"), "old_mode": 0o100644, "old_size": 4, "new_sha": sha("three\n"), "new_mode": 0o100644, "size": 6}),
    ];
    let branch = changes(&f, &mount, "branch", &ca, "");
    assert_eq!(paths(&branch), ["b", "c", "e"], "{branch}");
    assert!(
        branch["patch"].as_str().unwrap().contains("-2\n+three\n"),
        "{branch}"
    );
    let uncommitted = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(paths(&uncommitted), ["b"], "{uncommitted}");
    assert!(
        uncommitted["patch"]
            .as_str()
            .unwrap()
            .contains("-two\n+three\n"),
        "{uncommitted}"
    );
}

#[test]
fn changes_keeps_a_file_the_working_copy_tracks_when_an_ignore_rule_matches_it() {
    let f = Fixture::new();
    let (mount, _) = changed_workspace(&f);
    f.daemon.state.lock().unwrap().entries = vec![
        serde_json::json!({"path": "b", "kind": "file"}),
        serde_json::json!({"path": "e", "kind": "file"}),
        serde_json::json!({"path": "c", "kind": "deleted"}),
    ];
    f.jj(&mount, &["st"]);
    std::fs::write(mount.join(".gitignore"), "e\n").unwrap();
    let mut st = f.daemon.state.lock().unwrap();
    st.changes.push(serde_json::json!({"path": ".gitignore", "status": "added", "new_mode": 0o100644, "size": 2}));
    drop(st);
    let got = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(paths(&got), [".gitignore", "b", "c", "e"], "{got}");
}

#[test]
fn changes_applies_the_new_file_rules_where_the_working_copy_holds_a_directory() {
    let f = Fixture::new();
    let (mount, _) = changed_workspace(&f);
    std::fs::create_dir(mount.join("d")).unwrap();
    std::fs::write(mount.join("d/x"), "x\n").unwrap();
    f.daemon.state.lock().unwrap().entries =
        vec![serde_json::json!({"path": "d/x", "kind": "file"})];
    f.jj(&mount, &["st"]);
    std::fs::remove_dir_all(mount.join("d")).unwrap();
    std::fs::write(mount.join("d"), "now a file\n").unwrap();
    std::fs::write(mount.join(".gitignore"), "d\n").unwrap();
    let mut st = f.daemon.state.lock().unwrap();
    st.changes.extend([
        serde_json::json!({"path": ".gitignore", "status": "added", "new_mode": 0o100644, "size": 2}),
        serde_json::json!({"path": "d", "status": "added", "new_mode": 0o100644, "size": 11}),
    ]);
    drop(st);
    let got = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(paths(&got), [".gitignore", "b", "c", "e"], "{got}");
}

#[test]
fn changes_leaves_out_a_large_file_the_daemon_did_not_hash_when_it_equals_the_target() {
    let f = Fixture::new();
    let big = "x".repeat(3 << 20);
    let t = f.tree(&[("big", big.as_str())].into());
    let c = f.commit(t.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = t.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    std::fs::write(mount.join("big"), &big).unwrap();
    let sock = f.daemon.path().to_str().unwrap();
    let root = jj_root.to_str().unwrap();
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            sock,
            "--commit",
            &c,
            "--jj-root",
            root,
        ],
    );
    let sha = hash_object(Kind::Blob, big.as_bytes()).to_string();
    f.daemon.state.lock().unwrap().changes = vec![
        serde_json::json!({"path": "big", "status": "modified", "old_sha": sha, "old_mode": 0o100644, "old_size": big.len(), "new_mode": 0o100644, "size": big.len()}),
    ];
    let got = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(got["files"], serde_json::json!([]), "{got}");
}

/// The read commands of the harness jj engine, with the global arguments that it passes.
#[test]
fn harness_reads_run_at_an_operation_without_a_snapshot() {
    let f = Fixture::new();
    let (mount, _) = changed_workspace(&f);
    let repo = f.dir.path().join("jjroot/.jj/repo");
    let op = std::fs::read_dir(repo.join("op_heads/heads"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let at = format!("--at-operation={}", op.file_name().to_string_lossy());
    let m = mount.to_str().unwrap();
    let global = [
        "--ignore-working-copy",
        "-R",
        m,
        &at,
        "--no-pager",
        "--color=never",
        "--quiet",
        "--config=ui.paginate=never",
        "--config=ui.color=never",
        "--config=ui.progress=false",
    ];
    let config = r#"name ++ " " ++ json(value) ++ "\n""#;
    let reads: [&[&str]; 7] = [
        &["log", "-r", "parents(@)", "--no-graph", "-T", "commit_id"],
        &[
            "log",
            "-r",
            "latest(fork_point(parents(@) | trunk()))",
            "--no-graph",
            "-T",
            "commit_id",
        ],
        &[
            "config",
            "list",
            "--repo",
            "harness.changes-command",
            "-T",
            config,
        ],
        &["file", "list", "-r", "@"],
        &["op", "log", "--limit", "1", "--no-graph"],
        &["bookmark", "list"],
        &["diff", "--summary", "-r", "@"],
    ];
    let before = std::fs::read_dir(repo.join("op_store/operations"))
        .unwrap()
        .count();
    let mut out = vec![];
    for r in reads {
        out.push(f.jj(&mount, &[&global[..], r].concat()));
    }
    assert!(
        out[2].contains(r#"harness.changes-command ["jj","motte","changes","--json"]"#),
        "{}",
        out[2]
    );
    assert!(repo.join("config-id").exists() || !repo.join("config.toml").exists());
    let after = std::fs::read_dir(repo.join("op_store/operations"))
        .unwrap()
        .count();
    assert_eq!((after, f.daemon.ops("snapshot")), (before, 0));
}

#[test]
fn changes_lists_a_file_that_grew_past_the_large_limit_from_its_target_bytes() {
    let f = Fixture::new();
    let big = "x".repeat((2 << 20) + 1);
    let t = f.tree(&[("big", big.as_str())].into());
    let c = f.commit(t.clone(), 1);
    f.daemon.state.lock().unwrap().base_tree = t.hex();
    let mount = f.dir.path().join("mount");
    let jj_root = f.dir.path().join("jjroot");
    std::fs::create_dir(&mount).unwrap();
    std::fs::write(mount.join("big"), &big).unwrap();
    let sock = f.daemon.path().to_str().unwrap();
    let root = jj_root.to_str().unwrap();
    f.jj(
        &mount,
        &[
            "motte",
            "init",
            "--sock",
            sock,
            "--commit",
            &c,
            "--jj-root",
            root,
        ],
    );
    std::fs::write(mount.join("big"), format!("{big}tail")).unwrap();
    let sha = hash_object(Kind::Blob, big.as_bytes()).to_string();
    f.daemon.state.lock().unwrap().changes = vec![
        serde_json::json!({"path": "big", "status": "modified", "old_sha": sha, "old_mode": 0o100644, "old_size": big.len(), "new_mode": 0o100644, "size": 2}),
    ];
    let got = changes(&f, &mount, "uncommitted", "", "");
    assert_eq!(got["files"][0]["path"], "big", "{got}");
    assert_eq!(got["files"][0]["large"], true, "{got}");
}

#[test]
fn the_objects_of_an_upload_go_up_at_once() {
    let d = FakeDaemon::full();
    d.state.lock().unwrap().hold_putops = 4;
    let c = CtlClient::new(d.path());
    let repo = tempfile::tempdir().unwrap();
    for kind in ["views", "operations"] {
        std::fs::create_dir_all(repo.path().join("op_store").join(kind)).unwrap();
        for name in ["a1", "b2"] {
            std::fs::write(repo.path().join("op_store").join(kind).join(name), b"x").unwrap();
        }
    }
    let pending = [
        ("a1".to_owned(), "a1".to_owned()),
        ("b2".to_owned(), "b2".to_owned()),
    ];
    put_ops(&c, repo.path(), &pending, None).unwrap();
    assert_eq!(d.state.lock().unwrap().putops_peak, 4);
}

#[test]
fn a_tree_and_its_commit_go_up_in_one_batch_at_the_flush() {
    let f = Fixture::new();
    let before = f.daemon.ops("put") + f.daemon.ops("putbatch");
    let a: BTreeMap<&str, &str> = [("a", "1"), ("b", "2")].into();
    let tree = f.tree(&a);
    f.commit(tree, 1);
    let after = f.daemon.ops("put") + f.daemon.ops("putbatch");
    assert_eq!(
        after - before,
        2,
        "one batch for the tree, one for the commit"
    );
}

#[test]
fn a_step_runs_only_after_the_send_of_its_own_batch() {
    use motte_jj::ctl::ObjectStore as _;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let d = FakeDaemon::full();
    d.state.lock().unwrap().delay_putbatch_ms = 300;
    let c = std::sync::Arc::new(CtlClient::new(d.path()));
    c.put_deferred(gix::objs::Kind::Blob, b"x").unwrap();
    let flushing = {
        let c = c.clone();
        std::thread::spawn(move || c.flush_deferred().unwrap())
    };
    while d.ops("putbatch") == 0 {
        std::thread::yield_now();
    }
    c.put_deferred(gix::objs::Kind::Blob, b"y").unwrap();
    let ran = std::sync::Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    c.after_flush(move || flag.store(true, SeqCst));
    flushing.join().unwrap();
    assert!(!ran.load(SeqCst), "the first send does not hold the blob y");
    c.flush_deferred().unwrap();
    assert!(ran.load(SeqCst));
}

#[test]
fn a_step_does_not_run_after_a_failed_send_and_runs_after_the_retry() {
    use motte_jj::ctl::ObjectStore as _;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let d = FakeDaemon::full();
    d.state
        .lock()
        .unwrap()
        .failing
        .insert("putbatch".into(), "internal".into());
    let c = CtlClient::new(d.path());
    c.put_deferred(gix::objs::Kind::Blob, b"x").unwrap();
    let ran = std::sync::Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    c.after_flush(move || flag.store(true, SeqCst));
    assert!(c.flush_deferred().is_err());
    assert!(!ran.load(SeqCst));
    d.state.lock().unwrap().failing.clear();
    c.flush_deferred().unwrap();
    assert!(ran.load(SeqCst));
}

#[test]
fn a_worker_past_its_deadline_makes_no_daemon_call() {
    let d = FakeDaemon::start(true);
    let c = CtlClient::new(d.path());
    let repo = tempfile::tempdir().unwrap();
    for kind in ["views", "operations"] {
        std::fs::create_dir_all(repo.path().join("op_store").join(kind)).unwrap();
        std::fs::write(repo.path().join("op_store").join(kind).join("x"), b"x").unwrap();
    }
    let late = std::time::Instant::now();
    let pending = [("x".to_owned(), "x".to_owned())];
    let puts = put_ops(&c, repo.path(), &pending, Some(late));
    let merge = merge_remote(
        &c,
        repo.path(),
        || Ok(vec!["head".to_owned()]),
        |_| Ok(()),
        late,
    );
    let swap = mirror(&c, (1, vec![]), &[], &["head".to_owned()], late);
    let calls = (d.ops("putop"), d.ops("opheads"));
    let spent = |e: Option<motte_jj::ctl::StoreError>| {
        e.is_some_and(|e| e.to_string().contains("budget ran out"))
    };
    assert!(spent(puts.err()) && spent(merge.err()) && spent(swap.err()) && calls == (0, 0));
    let soon = std::time::Instant::now() + std::time::Duration::from_millis(50);
    let slow = |_: &[String]| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        Ok(())
    };
    let during = merge_remote(&c, repo.path(), || Ok(vec!["head".to_owned()]), slow, soon);
    assert!(spent(during.err()) && d.ops("opheads") == 0);
}

#[test]
fn a_drain_rereads_the_local_heads_after_the_remote_read() {
    let d = FakeDaemon::start(true);
    let c = CtlClient::new(d.path());
    let repo = tempfile::tempdir().unwrap();
    let ops = repo.path().join("op_store").join("operations");
    std::fs::create_dir_all(&ops).unwrap();
    for op in ["a", "b"] {
        std::fs::write(ops.join(op), b"x").unwrap();
    }
    d.state.lock().unwrap().opheads = Some((1, vec!["b".to_owned()]));
    let mut reads = 0;
    let local = || {
        reads += 1;
        Ok(vec![(if reads == 1 { "a" } else { "b" }).to_owned()])
    };
    let mut uploads = vec![];
    let upload = |heads: &[String]| {
        let generation = d.state.lock().unwrap().opheads.clone().unwrap().0;
        uploads.push((heads.to_vec(), generation));
        Ok(())
    };
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let swapped = merge_remote(&c, repo.path(), local, upload, until).unwrap();
    assert_eq!(c.opheads(None).unwrap().1, ["b"]);
    assert_eq!(swapped, ["b"]);
    assert_eq!(
        uploads,
        [(vec!["a".to_owned()], 1), (vec!["b".to_owned()], 1)]
    );
}

#[test]
fn a_drain_keeps_the_marks_it_did_not_swap_in() {
    let repo = tempfile::tempdir().unwrap();
    let heads = repo.path().join("op_heads").join("heads");
    std::fs::create_dir_all(&heads).unwrap();
    for h in ["a", "b", "c"] {
        std::fs::write(heads.join(h), b"").unwrap();
    }
    let marks = repo.path().join("motte_unsynced");
    std::fs::write(&marks, "a\nb\ngone\n").unwrap();
    clear_drained(repo.path(), &["a".to_owned(), "c".to_owned()]);
    assert_eq!(std::fs::read_to_string(&marks).unwrap(), "b\n");
    clear_drained(repo.path(), &["b".to_owned()]);
    assert!(!marks.exists());
    std::fs::write(&marks, "b\n").unwrap();
    std::fs::remove_dir_all(repo.path().join("op_heads")).unwrap();
    clear_drained(repo.path(), &["b".to_owned()]);
    let d = FakeDaemon::start(true);
    let (c, until) = (
        CtlClient::new(d.path()),
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    );
    let merge = merge_remote(
        &c,
        repo.path(),
        || read_heads(repo.path()),
        |_| Ok(()),
        until,
    );
    assert!(marks.exists() && merge.is_err() && d.ops("opheads") == 0);
}

#[test]
fn concurrent_edits_of_the_marker_lose_no_mark() {
    let repo = tempfile::tempdir().unwrap();
    let heads = repo.path().join("op_heads").join("heads");
    std::fs::create_dir_all(&heads).unwrap();
    let names = |p: char| {
        (0..200)
            .map(move |i| format!("{p}{i:03x}"))
            .collect::<Vec<_>>()
    };
    for h in names('a').into_iter().chain(names('b')) {
        std::fs::write(heads.join(h), b"").unwrap();
    }
    std::fs::write(heads.join("ff"), b"").unwrap();
    let dir = repo.path();
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        let writers: Vec<_> = ['a', 'b', 'c']
            .map(|p| {
                let heads = &heads;
                s.spawn(move || {
                    for h in names(p) {
                        if p == 'c' {
                            std::fs::write(heads.join(&h), b"").unwrap();
                        }
                        update_unsynced(dir, &[], Some(&h));
                    }
                })
            })
            .into();
        s.spawn(|| {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                clear_drained(dir, &["ff".to_owned()]);
            }
        });
        for w in writers {
            w.join().unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    let marks = std::fs::read_to_string(repo.path().join("motte_unsynced")).unwrap();
    let missing: Vec<_> = names('a')
        .into_iter()
        .chain(names('b'))
        .chain(names('c'))
        .filter(|h| !marks.lines().any(|l| l == h))
        .collect();
    assert!(missing.is_empty(), "lost marks: {missing:?}");
}

#[test]
fn refs_create_read_list_swap_and_delete_through_the_daemon() {
    use motte_jj::ctl::{Code, RefInfo, RefSwap};
    let d = FakeDaemon::start(true);
    let c = CtlClient::new(d.path());
    let (a, b) = ("a".repeat(40), "b".repeat(40));
    let at = |n: &str, commit: &str, generation| RefInfo {
        name: n.to_owned(),
        commit: commit.to_owned(),
        generation,
    };
    let RefSwap::Swapped(Some(main)) = c.swap_ref("heads/main", 0, &a, false).unwrap() else {
        panic!("create lost");
    };
    assert_eq!(main, at("heads/main", &a, 1));
    assert_eq!(c.get_ref("heads/main").unwrap(), main);
    assert_eq!(c.refs().unwrap(), std::slice::from_ref(&main));
    let absent = c.get_ref("heads/none").unwrap_err().to_string();
    assert!(absent.contains("vcs_not_found"), "{absent}");
    assert_eq!(
        c.swap_ref("heads/main", 7, &b, false).unwrap(),
        RefSwap::Conflict(Some(main.clone()))
    );
    assert_eq!(
        c.swap_ref("heads/new", 3, &b, false).unwrap(),
        RefSwap::Conflict(None)
    );
    let swapped = c.swap_ref("heads/main", 1, &b, false).unwrap();
    assert_eq!(swapped, RefSwap::Swapped(Some(at("heads/main", &b, 2))));
    assert_eq!(
        c.swap_ref("heads/main", 1, "", true).unwrap(),
        RefSwap::Conflict(Some(at("heads/main", &b, 2)))
    );
    assert_eq!(
        c.swap_ref("heads/main", 2, "", true).unwrap(),
        RefSwap::Swapped(None)
    );
    assert!(c.refs().unwrap().is_empty());
    d.state
        .lock()
        .unwrap()
        .failing
        .insert("swapref".into(), "vcs_claim_lost".into());
    let lost = c.swap_ref("heads/new", 0, &b, false);
    assert!(matches!(lost, Err(e) if e.is(&Code::ClaimLost)));
}

#[test]
fn init_names_the_origin_branch_as_a_bookmark_a_tracked_remote_bookmark_and_trunk() {
    for branch in [
        Some("main"),
        Some("release/1"),
        Some("feature/quote\"name"),
        Some("a\u{a0}b\u{3000}c"),
        None,
    ] {
        let f = Fixture::new();
        let a: BTreeMap<&str, &str> = [("a", "1")].into();
        let ta = f.tree(&a);
        let ca = f.commit(ta.clone(), 1);
        f.daemon.state.lock().unwrap().base_tree = ta.hex();
        let mount = f.dir.path().join("mount");
        let jj_root = f.dir.path().join("jjroot");
        std::fs::create_dir(&mount).unwrap();
        write_tree(&mount, &a);
        let mut args = vec![
            "motte",
            "init",
            "--sock",
            f.daemon.path().to_str().unwrap(),
            "--commit",
            &ca,
            "--jj-root",
            jj_root.to_str().unwrap(),
        ];
        args.extend(branch.into_iter().flat_map(|b| ["--origin-branch", b]));
        f.jj(&mount, &args);
        let template = r#"separate("@", stringify(name), remote) ++ if(tracked, " tracked") ++ " " ++ normal_target.commit_id() ++ "\n""#;
        let list = f.jj(
            &mount,
            &["bookmark", "list", "--all-remotes", "-T", template],
        );
        let want = branch.map_or(String::new(), |b| {
            format!("{b} {ca}\n{b}@origin tracked {ca}\n")
        });
        assert_eq!(list, want);
        if branch.is_some() {
            let trunk = f.jj(
                &mount,
                &["log", "--no-graph", "-r", "trunk()", "-T", "commit_id"],
            );
            assert_eq!(trunk, ca);
        }
    }
}

fn shallow_backend(dir: &Path) -> NativeBackend {
    let d = FakeDaemon::start(true);
    let store: Arc<dyn ObjectStore> = Arc::new(CtlClient::new(d.path()));
    NativeBackend::new(store, dir, true).unwrap()
}

fn shallow_ids(range: std::ops::Range<u8>) -> Vec<jj_lib::backend::CommitId> {
    range
        .map(|i| jj_lib::backend::CommitId::from_bytes(&[i; 20]))
        .collect()
}

fn shallow_lines(dir: &Path) -> usize {
    std::fs::read_to_string(dir.join("shallow"))
        .unwrap()
        .lines()
        .count()
}

#[test]
fn concurrent_backends_add_shallow_roots_without_losing_any() {
    let dir = tempfile::tempdir().unwrap();
    let a = shallow_backend(dir.path());
    let b = shallow_backend(dir.path());
    std::thread::scope(|s| {
        s.spawn(|| (0..30u8).for_each(|i| a.add_shallow(&shallow_ids(i..i + 1)).unwrap()));
        s.spawn(|| (100..130u8).for_each(|i| b.add_shallow(&shallow_ids(i..i + 1)).unwrap()));
    });
    assert_eq!(shallow_lines(dir.path()), 60);
}

#[test]
fn failed_shallow_publish_keeps_the_file_and_the_memory_set() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let a = shallow_backend(dir.path());
    a.add_shallow(&shallow_ids(1..3)).unwrap();
    let before = std::fs::read(dir.path().join("shallow")).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = std::fs::File::create(dir.path().join("probe"));
    let result = a.add_shallow(&shallow_ids(5..6));
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    if probe.is_ok() {
        return;
    }
    assert!(result.is_err());
    assert_eq!(std::fs::read(dir.path().join("shallow")).unwrap(), before);
}
