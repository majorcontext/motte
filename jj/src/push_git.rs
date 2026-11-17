use std::collections::HashMap;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use gix::odb::pack::Find as _;

use crate::push::{Fail, Outcome, open_odb};

const TIMEOUT: Duration = Duration::from_secs(600);

const DENIED: [&str; 6] = [
    "error: 403",
    "error: 401",
    "Authentication failed",
    "could not read Username",
    "Permission to ",
    "denied to",
];

/// The workspace-local bare repo that carries objects to origin. The git binary is the only network
/// path of the crate; every step shares one deadline.
pub struct Git {
    dir: PathBuf,
    deadline: Instant,
}

pub struct Finished {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

pub struct Remote {
    pub default_branch: Option<String>,
    pub tips: HashMap<String, String>,
}

/// One ref to update: the bookmark, its new commit and the commit that origin must hold now.
pub struct Update {
    pub name: String,
    pub sha: String,
    pub lease: Option<String>,
}

impl Git {
    pub fn open(repo_path: &Path, url: &str) -> Result<Self, Fail> {
        let git = Self {
            dir: repo_path.join("motte_git"),
            deadline: Instant::now() + TIMEOUT,
        };
        if !git.dir.join("HEAD").exists() {
            let dir = git.dir.to_string_lossy().into_owned();
            let init = ["init", "--bare", "--quiet", &dir].map(str::to_owned);
            git.checked("init", git.exec(&init, false)?)?;
        }
        for (key, value) in [("remote.origin.url", url), ("gc.auto", "0")] {
            git.remote(
                "config",
                &["config".into(), key.into(), value.into()],
                false,
            )?;
        }
        Ok(git)
    }

    pub fn objects_dir(&self) -> PathBuf {
        self.dir.join("objects")
    }

    pub fn ls_remote(&self, names: &[String]) -> Result<Remote, Fail> {
        let mut args = vec!["ls-remote".to_owned(), "--symref".into(), "origin".into()];
        args.push("HEAD".into());
        if names.is_empty() {
            args.push("refs/heads/*".into());
        }
        args.extend(names.iter().map(|n| format!("refs/heads/{n}")));
        let out = self.remote("ls-remote", &args, false)?;
        let mut remote = Remote {
            default_branch: None,
            tips: HashMap::new(),
        };
        for line in out.stdout.lines() {
            let Some((left, name)) = line.split_once('\t') else {
                continue;
            };
            if let Some(target) = left.strip_prefix("ref: ") {
                if name == "HEAD" {
                    remote.default_branch = target.strip_prefix("refs/heads/").map(str::to_owned);
                }
            } else if let Some(branch) = name.strip_prefix("refs/heads/") {
                remote.tips.insert(branch.to_owned(), left.to_owned());
            }
        }
        Ok(remote)
    }

    /// Fetches the commits and trees (no blobs) of `shas`. An origin that refuses wants by ID gets
    /// the heads named in `branches` instead, at depth 20, 200 and 1,000 and then in full, until
    /// every sha is held: a base that is no tip lies below the boundary of a shallow fetch.
    pub fn fetch(&self, shas: &[String], branches: &[String]) -> Result<(), Fail> {
        let base = ["fetch", "--no-tags", "--filter=blob:none"].map(str::to_owned);
        let by_id = [&base[..], &["--depth=1".into(), "origin".into()], shas].concat();
        match self.remote("fetch", &by_id, false) {
            Err(f) if f.outcome == Outcome::Network && !branches.is_empty() => {
                let heads = branches
                    .iter()
                    .map(|b| format!("+refs/heads/{b}:refs/motte/base/{b}"));
                for depth in [Some(20), Some(200), Some(1000), None] {
                    let flag = match depth {
                        Some(n) => format!("--depth={n}"),
                        None if self.dir.join("shallow").exists() => "--unshallow".into(),
                        None => break,
                    };
                    let args = [&base[..], &[flag, "origin".into()]]
                        .concat()
                        .into_iter()
                        .chain(heads.clone())
                        .collect::<Vec<_>>();
                    self.remote("fetch", &args, false)?;
                    if self.has_all(shas) {
                        break;
                    }
                }
                Ok(())
            }
            other => other.map(|_| ()),
        }
    }

    /// Whether the object database of the transport holds every commit of `shas`.
    pub fn has_all(&self, shas: &[String]) -> bool {
        open_odb(self).is_ok_and(|odb| {
            shas.iter()
                .all(|s| gix::ObjectId::from_hex(s.as_bytes()).is_ok_and(|id| odb.contains(&id)))
        })
    }

    /// Deletes every ref under `prefix` and returns how many there were.
    fn clear_refs(&self, prefix: &str) -> Result<usize, Fail> {
        let list = ["for-each-ref", "--format=%(refname)", prefix].map(str::to_owned);
        let out = self.remote("for-each-ref", &list, false)?;
        for name in out.stdout.lines() {
            let args = ["update-ref", "-d", name].map(str::to_owned);
            self.remote("update-ref", &args, false)?;
        }
        Ok(out.stdout.lines().count())
    }

    /// Fetches the heads `names` with `depth` commits of history each into `refs/motte/origin/`.
    /// The refs of earlier fetches go first: a name that became a directory cannot lock its ref.
    /// A filtered base fetch of a push leaves blobs out of commits that origin then treats as
    /// held, so `--refetch --no-filter` follows the removal of its refs.
    pub fn fetch_heads(&self, names: &[String], depth: usize) -> Result<(), Fail> {
        self.clear_refs("refs/motte/origin/")?;
        let filtered = self.clear_refs("refs/motte/base/")? > 0;
        let mut args = ["fetch", "--no-tags", "--no-filter"]
            .map(str::to_owned)
            .to_vec();
        if filtered {
            args.push("--refetch".into());
        }
        args.push(format!("--depth={depth}"));
        args.push("origin".into());
        args.extend(
            names
                .iter()
                .map(|n| format!("+refs/heads/{n}:refs/motte/origin/{n}")),
        );
        self.remote("fetch", &args, false).map(|_| ())
    }

    /// The commit that each head fetched by `fetch_heads` has now.
    pub fn fetched(&self) -> Result<HashMap<String, String>, Fail> {
        let args = [
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/motte/origin/",
        ]
        .map(str::to_owned);
        let out = self.remote("for-each-ref", &args, false)?;
        Ok(out
            .stdout
            .lines()
            .filter_map(|l| l.split_once(" refs/motte/origin/"))
            .map(|(sha, name)| (name.to_owned(), sha.to_owned()))
            .collect())
    }

    /// Pushes all updates atomically. `push.negotiate` makes git find the commits that origin
    /// already has, so a base that is not a branch tip stays out of the pack.
    pub fn push(&self, updates: &[Update]) -> Result<Finished, Fail> {
        let mut args = [
            "-c",
            "push.negotiate=true",
            "push",
            "--porcelain",
            "--atomic",
        ]
        .map(str::to_owned)
        .to_vec();
        for u in updates {
            args.push(format!(
                "--force-with-lease=refs/heads/{}:{}",
                u.name,
                u.lease.as_deref().unwrap_or("")
            ));
        }
        args.push("origin".into());
        args.extend(
            updates
                .iter()
                .map(|u| format!("{}:refs/heads/{}", u.sha, u.name)),
        );
        self.run(&args, true)
    }

    fn remote(&self, step: &str, args: &[String], relay: bool) -> Result<Finished, Fail> {
        self.checked(step, self.run(args, relay)?)
    }

    fn checked(&self, step: &str, f: Finished) -> Result<Finished, Fail> {
        if f.ok { Ok(f) } else { Err(failure(step, &f)) }
    }

    fn run(&self, args: &[String], relay: bool) -> Result<Finished, Fail> {
        let mut full = vec![
            "--git-dir".to_owned(),
            self.dir.to_string_lossy().into_owned(),
        ];
        full.extend_from_slice(args);
        self.exec(&full, relay)
    }

    fn exec(&self, args: &[String], relay: bool) -> Result<Finished, Fail> {
        let mut child = Command::new("git")
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_NO_LAZY_FETCH", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .map_err(|e| Fail::new(Outcome::Network, format!("cannot run git: {e}")))?;
        let mut stdout = child.stdout.take().expect("piped");
        let stderr = child.stderr.take().expect("piped");
        let out = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout.read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        });
        let err = thread::spawn(move || {
            let (mut reader, mut text) = (BufReader::new(stderr), Vec::new());
            let mut line = Vec::new();
            while reader.read_until(b'\n', &mut line).is_ok_and(|n| n > 0) {
                if relay {
                    let _ = std::io::stderr().write_all(&line);
                }
                text.extend_from_slice(&line);
                line.clear();
            }
            String::from_utf8_lossy(&text).into_owned()
        });
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) if Instant::now() < self.deadline => {
                    thread::sleep(Duration::from_millis(20))
                }
                _ => {
                    // SAFETY: the child leads its own process group.
                    unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
                    let _ = child.wait();
                    break None;
                }
            }
        };
        let (stdout, stderr) = (
            out.join().unwrap_or_default(),
            err.join().unwrap_or_default(),
        );
        match status {
            Some(status) => Ok(Finished {
                ok: status.success(),
                stdout,
                stderr,
            }),
            None => Err(Fail::new(
                Outcome::Network,
                "git timed out after 10 minutes",
            )),
        }
    }
}

/// Maps a failed git step to the message the user reads. The stderr text decides whether origin
/// refused the caller or the network failed.
pub fn failure(step: &str, f: &Finished) -> Fail {
    if DENIED.iter().any(|d| f.stderr.contains(d)) {
        let what = if step == "push" {
            "origin refused the push"
        } else {
            "origin refused access"
        };
        return Fail::new(Outcome::Rejected, what);
    }
    let last = f
        .stderr
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("no output");
    Fail::new(Outcome::Network, format!("git {step} failed: {last}"))
}
