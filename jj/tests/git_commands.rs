//! `jj motte push` and `fetch` to a git origin, driven through the built binary against the fake
//! daemon and a bare git repository that stands for origin. Each test names the failure it
//! catches.

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
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

/// Serves the bare repositories under `root` over smart HTTP with `git http-backend`, as a git
/// host does. Returns the base URL. The thread ends with the test process.
fn serve_http(root: &Path) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let root = root.to_owned();
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let root = root.clone();
            std::thread::spawn(move || cgi(conn, &root));
        }
    });
    url
}

fn cgi(conn: TcpStream, root: &Path) {
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut parts = line.split_whitespace();
    let (method, target) = (parts.next().unwrap(), parts.next().unwrap());
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut cmd = Command::new("git");
    cmd.arg("http-backend")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REMOTE_USER", "t")
        .env("REQUEST_METHOD", method)
        .env("PATH_INFO", path)
        .env("QUERY_STRING", query);
    let mut length = 0;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        let Some((name, value)) = header.trim().split_once(':') else {
            break;
        };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse().unwrap(),
            "content-type" => {
                cmd.env("CONTENT_TYPE", value);
            }
            "content-encoding" => {
                cmd.env("HTTP_CONTENT_ENCODING", value);
            }
            "git-protocol" => {
                cmd.env("GIT_PROTOCOL", value);
            }
            _ => {}
        }
    }
    cmd.env("CONTENT_LENGTH", length.to_string());
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let writer = std::thread::spawn(move || stdin.write_all(&body));
    let out = child.wait_with_output().unwrap().stdout;
    drop(writer.join());
    let split = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&out[..split]);
    let status = head
        .lines()
        .find_map(|l| l.strip_prefix("Status: "))
        .unwrap_or("200 OK");
    let mut conn = conn;
    let mut resp = format!("HTTP/1.1 {status}\r\n").into_bytes();
    for l in head.lines().filter(|l| !l.starts_with("Status:")) {
        resp.extend_from_slice(format!("{l}\r\n").as_bytes());
    }
    let payload = &out[split + 4..];
    resp.extend_from_slice(
        format!(
            "Content-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        )
        .as_bytes(),
    );
    resp.extend_from_slice(payload);
    drop(conn.write_all(&resp));
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

    /// Makes the bookmark `name` on a new commit that changes the existing file `a`.
    fn bookmark(&self, name: &str, body: &str) -> String {
        std::fs::write(self.ws.mount.join("a"), body).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(120));
        self.f.daemon.state.lock().unwrap().entries =
            vec![serde_json::json!({"path": "a", "kind": "file"})];
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
fn a_git_push_over_smart_http_edits_a_file_without_the_old_blob() {
    let s = Setup::new();
    git(&s.origin.dir, &["config", "http.receivepack", "true"], None);
    let base = serve_http(s.f.dir.path());
    let commit = s.bookmark("feature", "one");
    let key = format!("url.{base}/origin.git.insteadOf");
    let env = [
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_KEY_0", key.as_str()),
        ("GIT_CONFIG_VALUE_0", URL),
    ];
    let (ok, text) =
        s.f.jj_try(&s.ws.mount, &["motte", "push", "-b", "feature"], &env);
    assert!(ok, "{text}");
    assert_eq!(s.origin.tip("feature"), commit, "origin holds the commit");
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
