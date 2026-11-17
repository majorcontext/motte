//! `jj motte push` and `fetch` to a git origin, driven through the built binary against the fake
//! daemon and a bare git repository that stands for origin. Each test names the failure it
//! catches.

mod common;

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use common::{FakeDaemon, Fixture, Workspace};
use jj_lib::backend::CommitId;

const URL: &str = "https://origin.test/r.git";

fn files() -> BTreeMap<&'static str, &'static str> {
    [("a", "1"), ("b", "2")].into()
}

fn git(dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> String {
    let mut child = Command::new("git")
        .arg("--git-dir")
        .arg(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.unwrap_or_default())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// Origin: a bare repository with every object of the fake daemon, and `main` at `commit`.
struct Origin {
    dir: PathBuf,
}

impl Origin {
    fn new(f: &Fixture, commit: &str) -> Self {
        let dir = f.dir.path().join("origin.git");
        git(
            &dir,
            &["init", "--bare", "--quiet", dir.to_str().unwrap()],
            None,
        );
        git(&dir, &["config", "uploadpack.allowFilter", "true"], None);
        git(
            &dir,
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
            None,
        );
        let origin = Self { dir };
        origin.take_objects(f);
        git(
            &origin.dir,
            &["update-ref", "refs/heads/main", commit],
            None,
        );
        git(
            &origin.dir,
            &["symbolic-ref", "HEAD", "refs/heads/main"],
            None,
        );
        origin
    }

    /// Copies the objects of the daemon that origin lacks.
    fn take_objects(&self, f: &Fixture) {
        let objects = f.daemon.state.lock().unwrap().objects.clone();
        for (_, (kind, data)) in objects {
            let args = ["hash-object", "-w", "--literally", "-t", &kind, "--stdin"];
            git(&self.dir, &args, Some(&data));
        }
    }

    fn tip(&self, name: &str) -> String {
        git(
            &self.dir,
            &["rev-parse", &format!("refs/heads/{name}")],
            None,
        )
    }

    /// The `jj` environment that sends the URL of origin to this repository.
    fn env(&self) -> Vec<(String, String)> {
        let key = format!("url.file://{}.insteadOf", self.dir.display());
        vec![
            ("GIT_CONFIG_COUNT".into(), "1".into()),
            ("GIT_CONFIG_KEY_0".into(), key),
            ("GIT_CONFIG_VALUE_0".into(), URL.into()),
        ]
    }
}

struct Setup {
    f: Fixture,
    ws: Workspace,
    origin: Origin,
}

impl Setup {
    fn new() -> Self {
        let f = Fixture::with(FakeDaemon::full());
        let ws = f.workspace(&files(), &["--origin-url", URL, "--origin-branch", "main"]);
        let origin = Origin::new(&f, &ws.commit);
        Self { f, ws, origin }
    }

    /// Runs `jj` with origin reachable.
    fn jj(&self, args: &[&str]) -> (bool, String) {
        let env = self.origin.env();
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        self.f.jj_try(&self.ws.mount, args, &env)
    }

    /// Makes the bookmark `name` on a new commit that adds the file `body`. A file that no base
    /// holds at its path keeps `git push` from reading a blob that the transport lacks.
    fn bookmark(&self, name: &str, body: &str) -> String {
        std::fs::write(self.ws.mount.join(body), body).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(120));
        self.f.daemon.state.lock().unwrap().entries =
            vec![serde_json::json!({"path": body, "kind": "file"})];
        let (m, f) = (&self.ws.mount, &self.f);
        f.jj(m, &["describe", "-m", name]);
        f.jj(m, &["bookmark", "set", name, "-r", "@"]);
        f.jj(m, &["log", "-r", name, "--no-graph", "-T", "commit_id"])
    }
}

#[test]
fn a_git_push_sends_the_new_commit_to_origin_and_records_the_bookmark() {
    let s = Setup::new();
    let commit = s.bookmark("feature", "one");
    let (ok, text) = s.jj(&["motte", "push", "-b", "feature"]);
    assert!(ok, "{text}");
    assert_eq!(s.origin.tip("feature"), commit, "origin holds the commit");
    let listed = s.f.jj(&s.ws.mount, &["bookmark", "list", "-a", "feature"]);
    assert!(listed.contains("@origin: "), "{listed}");
}

#[test]
fn a_git_push_over_a_branch_that_origin_moved_is_stale() {
    let s = Setup::new();
    s.bookmark("feature", "one");
    assert!(s.jj(&["motte", "push", "-b", "feature"]).0);
    git(
        &s.origin.dir,
        &["update-ref", "refs/heads/feature", &s.ws.commit],
        None,
    );
    s.bookmark("feature", "two");
    let (ok, text) = s.jj(&["motte", "push", "-b", "feature"]);
    assert!(!ok && text.contains("origin moved feature"), "{text}");
    assert_eq!(
        s.origin.tip("feature"),
        s.ws.commit,
        "a stale push overwrote origin"
    );
}

#[test]
fn a_git_push_that_rewrites_the_default_branch_is_refused() {
    let s = Setup::new();
    s.bookmark("feature", "one");
    s.f.jj(&s.ws.mount, &["new", "root()", "-m", "other"]);
    s.f.jj(
        &s.ws.mount,
        &["bookmark", "set", "main", "-r", "@", "--allow-backwards"],
    );
    let (ok, text) = s.jj(&["motte", "push", "-b", "main"]);
    assert!(
        !ok && text.contains("refusing to rewrite main, the default branch of origin"),
        "{text}"
    );
    assert_eq!(s.origin.tip("main"), s.ws.commit);
}

#[test]
fn a_git_push_that_origin_rejects_is_not_called_stale() {
    let s = Setup::new();
    s.bookmark("feature", "one");
    let hook = s.origin.dir.join("hooks/pre-receive");
    std::fs::write(&hook, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&hook, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let (ok, text) = s.jj(&["motte", "push", "-b", "feature"]);
    assert!(!ok && text.contains("origin rejected feature"), "{text}");
}

#[test]
fn a_git_push_to_an_origin_that_cannot_be_reached_is_a_network_failure() {
    let s = Setup::new();
    s.bookmark("feature", "one");
    let env = [
        ("GIT_CONFIG_COUNT", "1"),
        (
            "GIT_CONFIG_KEY_0",
            "url.file:///nonexistent/origin.git.insteadOf",
        ),
        ("GIT_CONFIG_VALUE_0", URL),
    ];
    let (ok, text) =
        s.f.jj_try(&s.ws.mount, &["motte", "push", "-b", "feature"], &env);
    assert!(!ok && text.contains("git ls-remote failed"), "{text}");
}

#[test]
fn a_git_fetch_writes_the_new_commits_to_the_daemon_and_records_the_bookmark() {
    let s = Setup::new();
    let held: Vec<String> =
        s.f.daemon
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .cloned()
            .collect();
    let tree = s.f.tree(&[("a", "1"), ("b", "2"), ("c", "3")].into());
    let parent = CommitId::try_from_hex(&s.ws.commit).unwrap();
    let topic = s.f.commit_on(&[parent], tree, 2);
    s.origin.take_objects(&s.f);
    git(
        &s.origin.dir,
        &["update-ref", "refs/heads/topic", &topic],
        None,
    );
    s.f.daemon
        .state
        .lock()
        .unwrap()
        .objects
        .retain(|sha, _| held.contains(sha));
    let (ok, text) = s.jj(&["motte", "fetch", "-b", "topic"]);
    assert!(ok && text.contains("topic: new ->"), "{text}");
    assert!(
        s.f.daemon
            .state
            .lock()
            .unwrap()
            .objects
            .contains_key(&topic),
        "the daemon lacks the commit"
    );
    let got = s.f.jj(
        &s.ws.mount,
        &["log", "-r", "topic@origin", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(got, topic);
}

#[test]
fn a_git_fetch_without_names_takes_at_most_100_bookmarks() {
    let s = Setup::new();
    let lines: String = (0..150)
        .map(|i| format!("{} refs/heads/b{i:03}\n", s.ws.commit))
        .collect();
    std::fs::write(s.origin.dir.join("packed-refs"), lines).unwrap();
    let (ok, text) = s.jj(&["motte", "fetch"]);
    assert!(
        ok && text.contains("has 151 bookmarks; fetching 100"),
        "{text}"
    );
    let listed = s.f.jj(
        &s.ws.mount,
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
}

#[test]
fn a_git_push_that_loses_its_claim_sends_nothing_to_origin() {
    let s = Setup::new();
    s.bookmark("feature", "one");
    s.f.daemon
        .state
        .lock()
        .unwrap()
        .failing
        .insert("opheads".into(), "vcs_claim_lost".into());
    let (ok, text) = s.jj(&["motte", "push", "-b", "feature"]);
    assert!(
        !ok && text.contains("lost its claim on the op log; nothing was pushed"),
        "{text}"
    );
    let refs = git(&s.origin.dir, &["for-each-ref", "refs/heads/feature"], None);
    assert!(
        refs.is_empty(),
        "a push without its claim reached origin: {refs}"
    );
}

#[test]
fn a_git_fetch_whose_object_writes_lose_the_claim_ends_as_claim_lost_not_network() {
    let s = Setup::new();
    let tree = s.f.tree(&[("a", "1"), ("b", "2"), ("c", "3")].into());
    let parent = CommitId::try_from_hex(&s.ws.commit).unwrap();
    let topic = s.f.commit_on(&[parent], tree, 2);
    s.origin.take_objects(&s.f);
    git(
        &s.origin.dir,
        &["update-ref", "refs/heads/topic", &topic],
        None,
    );
    s.f.daemon.state.lock().unwrap().objects.remove(&topic);
    s.f.daemon
        .state
        .lock()
        .unwrap()
        .failing
        .insert("putbatch".into(), "vcs_claim_lost".into());
    let (ok, text) = s.jj(&["motte", "fetch", "-b", "topic"]);
    assert!(
        !ok && text.contains("lost its claim on the op log; nothing was fetched"),
        "{text}"
    );
}
