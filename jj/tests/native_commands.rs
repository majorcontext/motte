//! `jj motte push`, `fetch` and `push --delete` of a native repository, driven through the built
//! binary against the fake daemon that `tests/fixtures.rs` checks against the contract. Each test
//! names the failure it catches.

mod common;

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;

use common::{FakeDaemon, Fixture, UNSET, Workspace, collector};
use gix::objs::Kind;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use motte_jj::ctl::hash_object;

const NATIVE: [&str; 5] = [
    "--origin-native",
    "--origin-default-branch",
    "main",
    "--origin-branch",
    "main",
];

/// The body of each OTLP post. The command exports after it exits, so a test waits on the channel.
const OUTCOMES: [&str; 5] = ["refused", "stale", "rejected", "network", "claim_lost"];

/// Runs a push or fetch and returns its text and the one outcome that its span carries: `ok` when
/// it succeeded.
fn run(f: &Fixture, ws: &Workspace, args: &[&str]) -> (bool, String, String) {
    let (url, posts) = collector();
    let env = [("OTEL_EXPORTER_OTLP_ENDPOINT", url.as_str())];
    let (ok, text) = f.jj_try(&ws.mount, args, &env);
    let body = posts.recv().expect("no OTLP request");
    let seen: Vec<&str> = OUTCOMES
        .into_iter()
        .filter(|o| body.windows(o.len()).any(|w| w == o.as_bytes()))
        .collect();
    assert!(seen.len() <= 1, "{args:?}: outcomes {seen:?}");
    let outcome = seen.first().map_or("ok", |o| *o);
    assert_eq!(ok, outcome == "ok", "{args:?}: {outcome}: {text}");
    (ok, text, outcome.to_owned())
}

fn files() -> BTreeMap<&'static str, &'static str> {
    [("a", "1"), ("b", "2")].into()
}

/// A native workspace on a store that holds the start commit as the ref `main`.
fn native(f: &Fixture) -> Workspace {
    let ws = f.workspace(&files(), &NATIVE);
    let mut st = f.daemon.state.lock().unwrap();
    st.ref_counter = 1;
    st.refs.insert("main".into(), (ws.commit.clone(), 1));
    drop(st);
    ws
}

/// Changes the file `a` in the mount and in the dirty set of the daemon, so the next snapshot
/// writes a new blob.
fn edit(f: &Fixture, ws: &Workspace, body: &str) {
    std::fs::write(ws.mount.join("a"), body).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(120));
    f.daemon.state.lock().unwrap().entries = vec![serde_json::json!({"path": "a", "kind": "file"})];
}

/// Makes the bookmark `name` on a new commit with a new blob, and returns the commit.
fn bookmark(f: &Fixture, ws: &Workspace, name: &str, body: &str) -> String {
    edit(f, ws, body);
    f.jj(&ws.mount, &["describe", "-m", name]);
    f.jj(&ws.mount, &["bookmark", "create", name, "-r", "@"]);
    f.jj(
        &ws.mount,
        &["log", "-r", name, "--no-graph", "-T", "commit_id"],
    )
}

fn ops(f: &Fixture) -> Vec<String> {
    f.daemon.state.lock().unwrap().log.clone()
}

fn position(log: &[String], ops: &[&str], last: bool) -> usize {
    let mut found = log
        .iter()
        .enumerate()
        .filter(|(_, o)| ops.contains(&o.as_str()));
    let hit = if last {
        found.next_back()
    } else {
        found.next()
    };
    hit.unwrap_or_else(|| panic!("no {ops:?} in {log:?}")).0
}

#[test]
fn a_native_push_probes_the_claim_around_its_checks_and_swaps_last() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let commit = bookmark(&f, &ws, "feature", "one");
    f.daemon.state.lock().unwrap().log.clear();
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "ok", "{text}");
    let log = ops(&f);
    let swap = position(&log, &["swapref"], false);
    let before = &log[..swap];
    let (first_probe, read) = (
        position(before, &["opheads"], false),
        position(before, &["refs"], false),
    );
    let checked = position(before, &["has", "get", "getbatch"], true);
    let second_probe = position(before, &["opheads"], true);
    assert!(
        first_probe < read && read < checked && checked < second_probe && second_probe < swap,
        "a swap that does not come last, after both claim probes and the object checks: {log:?}"
    );
    let st = f.daemon.state.lock().unwrap();
    assert_eq!(st.refs["feature"].0, commit, "the ref is at the bookmark");
    drop(st);
    let listed = f.jj(&ws.mount, &["bookmark", "list", "-a", "feature"]);
    assert!(
        listed.contains("@origin: "),
        "feature@origin is recorded: {listed}"
    );
}

#[test]
fn a_native_push_of_a_ref_that_origin_moved_is_stale_and_sends_no_swap() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("feature".into(), (ws.commit.clone(), 5));
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "stale", "{text}");
    assert!(
        text.contains("origin moved feature; run jj motte fetch first"),
        "{text}"
    );
    assert_eq!(f.daemon.ops("swapref"), 0, "a stale push sent a swap");
}

#[test]
fn a_native_push_that_loses_its_claim_before_the_swap_sends_no_swap() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    f.daemon.state.lock().unwrap().fail_after =
        Some(("has".into(), "opheads".into(), "vcs_claim_lost".into()));
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "claim_lost", "{text}");
    assert!(
        text.contains("lost its claim on the op log; nothing was pushed"),
        "{text}"
    );
    assert_eq!(
        f.daemon.ops("swapref"),
        0,
        "a push without its claim sent a swap"
    );
}

#[test]
fn a_native_push_whose_blob_the_store_lacks_stops_as_network() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    let blob = hash_object(Kind::Blob, b"one").to_string();
    f.daemon.state.lock().unwrap().objects.remove(&blob);
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "network", "{text}");
    assert_eq!(f.daemon.ops("swapref"), 0);
}

#[test]
fn a_native_push_of_a_missing_bookmark_is_refused() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "nothing"]);
    assert_eq!(outcome, "refused", "{text}");
}

#[test]
fn a_swap_whose_answer_is_lost_is_read_back_and_never_sent_twice() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let commit = bookmark(&f, &ws, "feature", "one");
    f.daemon.state.lock().unwrap().lose_answer_of = Some("swapref".into());
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(
        outcome, "ok",
        "a swap that landed was reported as stale: {text}"
    );
    assert_eq!(f.daemon.ops("swapref"), 1, "a swap was replayed");
    assert_eq!(f.daemon.state.lock().unwrap().refs["feature"].0, commit);
}

#[test]
fn a_native_delete_removes_the_ref_and_forgets_origin_but_keeps_the_bookmark() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    run(&f, &ws, &["motte", "push", "-b", "feature"]);
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "--delete", "feature"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(!f.daemon.state.lock().unwrap().refs.contains_key("feature"));
    let listed = f.jj(&ws.mount, &["bookmark", "list", "-a", "feature"]);
    assert!(
        listed.contains("feature:") && !listed.contains("@origin"),
        "{listed}"
    );
}

#[test]
fn a_native_delete_of_a_ref_that_moved_is_stale_and_sends_no_swap() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    run(&f, &ws, &["motte", "push", "-b", "feature"]);
    let swaps = f.daemon.ops("swapref");
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("feature".into(), (ws.commit.clone(), 9));
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "--delete", "feature"]);
    assert_eq!(outcome, "stale", "{text}");
    assert_eq!(f.daemon.ops("swapref"), swaps, "a stale delete sent a swap");
}

#[test]
fn a_native_fetch_imports_the_commits_of_a_new_ref_and_records_it() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let tree = f.tree(&[("a", "1"), ("b", "2"), ("c", "3")].into());
    let parent = CommitId::try_from_hex(&ws.commit).unwrap();
    let topic = f.commit_on(&[parent], tree, 2);
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("topic".into(), (topic.clone(), 2));
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch", "-b", "topic"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(text.contains("topic: new ->"), "{text}");
    let got = f.jj(
        &ws.mount,
        &["log", "-r", "topic@origin", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(got, topic);
}

#[test]
fn a_native_fetch_without_names_takes_at_most_100_bookmarks_and_the_default_first() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    {
        let mut st = f.daemon.state.lock().unwrap();
        for i in 0..150 {
            st.refs
                .insert(format!("b{i:03}"), (ws.commit.clone(), 2 + i));
        }
    }
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(text.contains("has 151 bookmarks; fetching 100"), "{text}");
    let listed = f.jj(
        &ws.mount,
        &[
            "bookmark",
            "list",
            "-a",
            "-T",
            "name ++ \"@\" ++ remote ++ \"\\n\"",
        ],
    );
    let remote: Vec<&str> = listed.lines().filter(|l| l.ends_with("@origin")).collect();
    assert_eq!(remote.len(), 100, "{listed}");
    assert!(
        remote.contains(&"main@origin"),
        "the default branch comes first"
    );
    assert!(
        remote.contains(&"b098@origin") && !remote.contains(&"b099@origin"),
        "name order"
    );
}

/// Catches a fetch without names that takes a tag as a bookmark: the store keeps a tag under its
/// full ref name, and an annotated tag names no commit, so the fetch refused every bookmark.
#[test]
fn a_native_fetch_without_names_skips_refs_that_are_not_branches() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let tree = f.tree(&files());
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("refs/tags/v1".into(), (tree.hex(), 2));
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(!text.contains("refs/tags/v1"), "{text}");
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch", "-b", "refs/tags/v1"]);
    assert_eq!(outcome, "refused", "{text}");
}

/// Lands `name` the way ward does when main moved: main gets a commit of its own, and the commit of
/// `name` is rebased onto it with its change ID and the file `a` set to `body`.
fn land_rebased(f: &Fixture, ws: &Workspace, name: &str, body: &str) -> String {
    let change = f.jj(
        &ws.mount,
        &[
            "log",
            "-r",
            name,
            "--no-graph",
            "-T",
            "change_id.normal_hex()",
        ],
    );
    let base = CommitId::try_from_hex(&ws.commit).unwrap();
    let moved = f.commit_on(
        &[base],
        f.tree(&[("a", "1"), ("b", "2"), ("c", "3")].into()),
        7,
    );
    let landed = f.commit_with_change(
        &[CommitId::try_from_hex(&moved).unwrap()],
        f.tree(&[("a", body), ("b", "2"), ("c", "3")].into()),
        jj_lib::backend::ChangeId::try_from_hex(&change).unwrap(),
        &format!("{name}\n"),
    );
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("main".into(), (landed.clone(), 3));
    landed
}

/// Catches a fetch that leaves the pushed commit beside the commit that landed it: a land that
/// rebases keeps the change ID, so both show as divergent until the local one is hidden.
#[test]
fn a_native_fetch_hides_the_local_commit_that_landed() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "topic", "x");
    f.jj(&ws.mount, &["new"]);
    let landed = land_rebased(&f, &ws, "topic", "x");
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch", "-b", "main"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(
        text.contains(&format!("landed as {}", &landed[..12])),
        "{text}"
    );
    let parent = f.jj(
        &ws.mount,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(
        parent, landed,
        "the working copy moves onto the landed commit"
    );
    let topic = f.jj(
        &ws.mount,
        &["log", "-r", "topic", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(topic, landed, "the bookmark follows the rewrite");
}

/// Catches a fetch that hides a local commit with edits that the land does not hold.
#[test]
fn a_native_fetch_keeps_a_local_commit_that_differs_from_the_landed_one() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let local = bookmark(&f, &ws, "topic", "x");
    f.jj(&ws.mount, &["new"]);
    land_rebased(&f, &ws, "topic", "y");
    let (_, text, outcome) = run(&f, &ws, &["motte", "fetch", "-b", "main"]);
    assert_eq!(outcome, "ok", "{text}");
    assert!(!text.contains("landed as"), "{text}");
    let parent = f.jj(
        &ws.mount,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(parent, local, "the amended local commit stays");
}

/// Catches a fetch that hides a local merge: its delta over the merged parents is empty, so a
/// landed commit without the second parent would match it and drop what that parent brings.
#[test]
fn a_native_fetch_keeps_a_local_merge() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "x", "x");
    f.jj(&ws.mount, &["new", &ws.commit]);
    bookmark(&f, &ws, "o", "o");
    f.jj(&ws.mount, &["new", "x", "o", "-m", "e"]);
    f.jj(&ws.mount, &["bookmark", "create", "e", "-r", "@"]);
    f.jj(&ws.mount, &["new"]);
    land_rebased(&f, &ws, "e", "1");
    let (_, text, _) = run(&f, &ws, &["motte", "fetch", "-b", "main"]);
    assert!(!text.contains("landed as"), "{text}");
}

/// Catches a fetch that rewrites an immutable descendant of the local commit.
#[test]
fn a_native_fetch_keeps_a_local_commit_with_an_immutable_descendant() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "topic", "x");
    f.jj(&ws.mount, &["new", "-m", "dd"]);
    f.jj(&ws.mount, &["new"]);
    land_rebased(&f, &ws, "topic", "x");
    let config = "revset-aliases.\"immutable_heads()\"=\"description(substring:dd)\"";
    let (_, text, _) = run(
        &f,
        &ws,
        &["--config", config, "motte", "fetch", "-b", "main"],
    );
    assert!(!text.contains("landed as"), "{text}");
}

/// Catches a conflicted flush whose mount commit names a tree that the store lacks, and a
/// rematerialize that does not store it again.
#[test]
fn a_flush_of_a_conflicted_working_copy_stores_every_object_of_its_mount_commit() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "x", "x");
    f.jj(&ws.mount, &["new", &ws.commit]);
    bookmark(&f, &ws, "o", "o");
    f.jj(&ws.mount, &["new", "x", "o"]);
    let out = f.jj(&ws.mount, &["motte", "flush"]);
    let commit = &out.rsplit("\"commit\":\"").next().unwrap()[..40];
    let tree = walk_stored(&f, commit);
    f.daemon.state.lock().unwrap().objects.remove(&tree);
    f.jj(&ws.mount, &["motte", "rematerialize", "--commit", commit]);
    walk_stored(&f, commit);
}

/// Requires every object that `commit` reaches in the store of the daemon, and returns its tree.
fn walk_stored(f: &Fixture, commit: &str) -> String {
    let st = f.daemon.state.lock().unwrap();
    let root = String::from_utf8_lossy(&st.objects[commit].1[5..45]).into_owned();
    let mut todo = vec![root.clone()];
    while let Some(id) = todo.pop() {
        let (kind, data) = st.objects.get(&id).expect("the store holds the object");
        if kind == "tree" {
            let tree = gix::objs::TreeRef::from_bytes(data, gix::hash::Kind::Sha1).unwrap();
            todo.extend(tree.entries.iter().map(|e| e.oid.to_string()));
        }
    }
    root
}

/// Catches a snapshot that takes the files of a checkout that failed part of the way for edits.
#[test]
fn a_snapshot_over_a_checkout_that_did_not_finish_stops() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "x", "2");
    f.jj(&ws.mount, &["new", &ws.commit]);
    std::fs::set_permissions(&ws.mount, PermissionsExt::from_mode(0o555)).unwrap();
    assert!(!f.jj_try(&ws.mount, &["edit", "x"], &[]).0);
    f.jj(&ws.mount, &["undo", "--ignore-working-copy"]);
    let (ok, text) = f.jj_try(&ws.mount, &["status"], &[]);
    std::fs::set_permissions(&ws.mount, PermissionsExt::from_mode(0o755)).unwrap();
    assert!(!ok && text.contains("update-stale"), "{text}");
}

#[test]
fn a_native_push_of_more_than_1000_new_commits_is_refused() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let tree = f.tree(&files());
    let mut parent = CommitId::try_from_hex(&ws.commit).unwrap();
    let mut tip = String::new();
    for seed in 2..1002 {
        tip = f.commit_on(std::slice::from_ref(&parent), tree.clone(), seed);
        parent = CommitId::try_from_hex(&tip).unwrap();
    }
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("chain".into(), (tip.clone(), 2));
    run(&f, &ws, &["motte", "fetch", "-b", "chain"]);
    f.jj(
        &ws.mount,
        &["bookmark", "forget", "--include-remotes", "chain"],
    );
    f.jj(&ws.mount, &["new", &tip, "-m", "top"]);
    f.jj(&ws.mount, &["bookmark", "create", "big", "-r", "@"]);
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "big"]);
    assert_eq!(outcome, "refused", "{text}");
    assert!(text.contains("more than 1000 new commits"), "{text}");
    assert_eq!(f.daemon.ops("swapref"), 0);
}

#[test]
fn a_released_daemon_without_codes_gets_the_same_outcomes() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    bookmark(&f, &ws, "feature", "one");
    f.daemon.state.lock().unwrap().codeless = true;
    f.daemon
        .state
        .lock()
        .unwrap()
        .refs
        .insert("feature".into(), (ws.commit.clone(), 5));
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "stale", "{text}");
    f.daemon
        .state
        .lock()
        .unwrap()
        .failing
        .insert("opheads".into(), "vcs_claim_lost".into());
    let (_, text, outcome) = run(&f, &ws, &["motte", "push", "-b", "feature"]);
    assert_eq!(outcome, "claim_lost", "{text}");
}

#[test]
fn the_socket_comes_from_the_store_when_the_variable_is_unset() {
    let f = Fixture::with(FakeDaemon::full());
    let ws = native(&f);
    let out = f.jj_env(&ws.mount, &["st"], &[("MOTTE_VFS_SOCK", UNSET)]);
    assert!(out.contains("The working copy has no changes."), "{out}");
}
