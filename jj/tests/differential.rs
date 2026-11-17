//! Differential test. Every object written through `NativeBackend` must have the same bytes (and
//! therefore the same id) as the one `GitBackend` writes for the same inputs.
//!
//! Env: MOTTE_DIFF_COMMITS (default 300), MOTTE_DIFF_SEED, MOTTE_CTL (a live daemon instead of the fake one).

mod common;

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;

use common::MemStore;
use gix::objs::Kind;
use jj_lib::backend::{
    Backend, ChangeId, Commit, CommitId, CopyId, FileId, MillisSinceEpoch, Signature, SigningFn,
    SymlinkId, Timestamp, Tree, TreeId, TreeValue,
};
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::git_backend::GitBackend;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::{RepoPath, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, ObjectStore, hash_object};
use pollster::block_on;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

fn settings(write_change_id_header: bool) -> UserSettings {
    let mut config = StackedConfig::with_defaults();
    let toml = format!("git.write-change-id-header = {write_change_id_header}\n");
    config.add_layer(ConfigLayer::parse(ConfigSource::User, &toml).unwrap());
    UserSettings::from_config_and_home_dir(config, None).unwrap()
}

struct Pair {
    git: GitBackend,
    native: NativeBackend,
    store: Arc<dyn ObjectStore>,
    mem: Option<Arc<MemStore>>,
    git_dir: std::path::PathBuf,
    _tmp: tempfile::TempDir,
    wrote: Vec<gix::ObjectId>,
}

impl Pair {
    fn new(header: bool, ctl: Option<&str>) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let gpath = tmp.path().join("g");
        std::fs::create_dir(&gpath).unwrap();
        let npath = tmp.path().join("n");
        std::fs::create_dir(&npath).unwrap();
        let s = settings(header);
        let git = GitBackend::init_internal(
            &s.git_backend_settings().unwrap(),
            &gpath,
            gix::hash::Kind::Sha1,
        )
        .unwrap();
        let (store, mem): (Arc<dyn ObjectStore>, Option<Arc<MemStore>>) = match ctl {
            Some(sock) => (Arc::new(CtlClient::new(sock)), None),
            None => {
                let m = Arc::new(MemStore::default());
                (m.clone(), Some(m))
            }
        };
        let native = NativeBackend::new(store.clone(), &npath, header).unwrap();
        let git_dir = gpath.join("git");
        Self {
            git,
            native,
            store,
            mem,
            git_dir,
            _tmp: tmp,
            wrote: vec![],
        }
    }

    fn file(&mut self, data: &[u8]) -> FileId {
        let p = RepoPath::root();
        let a = block_on(self.git.write_file(p, &mut &data[..])).unwrap();
        let b = block_on(self.native.write_file(p, &mut &data[..])).unwrap();
        assert_eq!(a, b, "file id mismatch");
        self.wrote
            .push(gix::ObjectId::from_bytes_or_panic(a.as_bytes()));
        a
    }

    fn symlink(&mut self, target: &str) -> SymlinkId {
        let p = RepoPath::root();
        let a = block_on(self.git.write_symlink(p, target)).unwrap();
        let b = block_on(self.native.write_symlink(p, target)).unwrap();
        assert_eq!(a, b, "symlink id mismatch");
        self.wrote
            .push(gix::ObjectId::from_bytes_or_panic(a.as_bytes()));
        a
    }

    fn tree(&mut self, t: &Tree) -> TreeId {
        let p = RepoPath::root();
        let a = block_on(self.git.write_tree(p, t)).unwrap();
        let b = block_on(self.native.write_tree(p, t)).unwrap();
        assert_eq!(a, b, "tree id mismatch for {:?}", t);
        let ra = block_on(self.git.read_tree(p, &a)).unwrap();
        let rb = block_on(self.native.read_tree(p, &b)).unwrap();
        assert_eq!(ra, rb, "read_tree mismatch");
        assert_eq!(&ra, t, "read_tree does not round-trip");
        self.wrote
            .push(gix::ObjectId::from_bytes_or_panic(a.as_bytes()));
        a
    }

    fn commit(
        &mut self,
        c: Commit,
        sign: bool,
    ) -> (Result<CommitId, ()>, Result<CommitId, ()>, Option<Commit>) {
        let mut signer_a = fake_signer();
        let mut signer_b = fake_signer();
        let sa: Option<&mut SigningFn> = if sign { Some(&mut signer_a) } else { None };
        let sb: Option<&mut SigningFn> = if sign { Some(&mut signer_b) } else { None };
        let ra = block_on(self.git.write_commit(c.clone(), sa));
        let rb = block_on(self.native.write_commit(c, sb));
        match (ra, rb) {
            (Ok((ia, ca)), Ok((ib, cb))) => {
                assert_eq!(ia, ib, "commit id mismatch");
                assert_eq!(ca, cb, "returned commit mismatch");
                self.wrote
                    .push(gix::ObjectId::from_bytes_or_panic(ia.as_bytes()));
                (Ok(ia), Ok(ib), Some(ca))
            }
            (Err(_), Err(_)) => (Err(()), Err(()), None),
            (a, b) => panic!(
                "write_commit result mismatch: git={:?} native={:?}",
                a.map(|x| x.0),
                b.map(|x| x.0)
            ),
        }
    }
}

fn fake_signer() -> Box<SigningFn<'static>> {
    Box::new(|data: &[u8]| {
        let h = hash_object(Kind::Blob, data);
        Ok(
            format!(
                "-----BEGIN FAKE SIGNATURE-----\n\n{h}\nline two\n-----END FAKE SIGNATURE-----"
            )
            .into_bytes(),
        )
    })
}

const NAMES: &[&str] = &[
    "a",
    "a.b",
    "a-b",
    "a b",
    "a_b",
    "a0",
    "A",
    "Z",
    "z",
    "\u{e4}",
    "\u{1f600}",
    "b",
    "ab",
    "a+b",
    "a=b",
    "README",
    "Makefile",
    ".gitignore",
    ".jjconflict-base-0",
    ".jjconflict-side-1",
    "JJ-CONFLICT-README",
    ".jjconflictx",
    "-",
    "~",
    "0",
    "1",
    "10",
    "2",
    "foo.rs",
    "foo",
    "foo.",
    "foo-bar",
    "foo_bar",
    "foo bar",
    "caf\u{e9}",
    "cafe\u{301}",
];

fn name(rng: &mut ChaCha8Rng) -> RepoPathComponentBuf {
    let s = if rng.random_bool(0.7) {
        NAMES[rng.random_range(0..NAMES.len())].to_owned()
    } else {
        let n = rng.random_range(1..8);
        (0..n)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect()
    };
    RepoPathComponentBuf::new(s).unwrap()
}

fn text(rng: &mut ChaCha8Rng) -> String {
    const PIECES: &[&str] = &[
        "fix",
        "bug",
        "\u{1f980}",
        "caf\u{e9}",
        "\u{65e5}\u{672c}\u{8a9e}",
        "line",
        " ",
        "  ",
        "\n",
        "\n\n",
        "\r\n",
        "\t",
        "Signed-off-by: x <y@z>",
        "# heading",
        "tab\there",
        "\u{200b}",
    ];
    let mut s = String::new();
    for _ in 0..rng.random_range(0..12) {
        s.push_str(PIECES[rng.random_range(0..PIECES.len())]);
    }
    match rng.random_range(0..4) {
        0 => s.push('\n'),
        1 => s.push_str("\n\n"),
        _ => {}
    }
    s
}

fn bytes(rng: &mut ChaCha8Rng) -> Vec<u8> {
    match rng.random_range(0..8) {
        0 => vec![],
        1 => text(rng).into_bytes(),
        2 => (0..rng.random_range(1..300))
            .map(|_| rng.random::<u8>())
            .collect(),
        3 => vec![0u8; rng.random_range(1..70_000)],
        4 => b"\n".to_vec(),
        _ => format!("{}\n", text(rng)).into_bytes(),
    }
}

fn signature(rng: &mut ChaCha8Rng) -> Signature {
    const NAMEZ: &[&str] = &[
        "",
        "Alice",
        "Bob B.",
        "\u{e9}milie",
        "\u{1f980} Crab",
        "J\u{f6}rg <x>",
        "a b c",
    ];
    const MAILS: &[&str] = &[
        "",
        "a@x.org",
        "b+tag@example.com",
        "\u{e9}@x.io",
        "weird@@x",
    ];
    let tz = [-720, -480, -210, 0, 60, 330, 345, 525, 840][rng.random_range(0..9)];
    let ms = match rng.random_range(0..6) {
        0 => rng.random_range(-5_000_000_000i64..0),
        1 => 0,
        2 => 1_700_000_000_123,
        _ => rng.random_range(0..2_000_000_000_000i64),
    };
    Signature {
        name: NAMEZ[rng.random_range(0..NAMEZ.len())].to_owned(),
        email: MAILS[rng.random_range(0..MAILS.len())].to_owned(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(ms),
            tz_offset: tz,
        },
    }
}

struct Gen {
    rng: ChaCha8Rng,
    files: Vec<FileId>,
    links: Vec<SymlinkId>,
    trees: Vec<TreeId>,
    commits: Vec<CommitId>,
    conflicted: usize,
    signed: usize,
    merges: usize,
    dup_attempts: usize,
    bumped: usize,
}

impl Gen {
    fn tree(&mut self, p: &mut Pair, depth: usize) -> TreeId {
        let n = self.rng.random_range(0..7);
        let mut entries: BTreeMap<RepoPathComponentBuf, TreeValue> = BTreeMap::new();
        for _ in 0..n {
            let nm = name(&mut self.rng);
            let v = match self.rng.random_range(0..10) {
                0..=3 => {
                    let id = if self.files.is_empty() || self.rng.random_bool(0.6) {
                        let b = bytes(&mut self.rng);
                        let id = p.file(&b);
                        self.files.push(id.clone());
                        id
                    } else {
                        self.files[self.rng.random_range(0..self.files.len())].clone()
                    };
                    TreeValue::File {
                        id,
                        executable: self.rng.random_bool(0.3),
                        copy_id: CopyId::placeholder(),
                    }
                }
                4 => {
                    let t = if self.links.is_empty() || self.rng.random_bool(0.5) {
                        let t = format!(
                            "../{}/{}",
                            text(&mut self.rng).replace('\n', ""),
                            self.rng.random_range(0..9)
                        );
                        let id = p.symlink(&t);
                        self.links.push(id.clone());
                        id
                    } else {
                        self.links[self.rng.random_range(0..self.links.len())].clone()
                    };
                    TreeValue::Symlink(t)
                }
                5 if self.rng.random_bool(0.2) => {
                    let mut raw = [0u8; 20];
                    self.rng.fill_bytes(&mut raw);
                    TreeValue::GitSubmodule(CommitId::from_bytes(&raw))
                }
                _ if depth < 3 => TreeValue::Tree(self.tree(p, depth + 1)),
                _ => continue,
            };
            // a subtree must be non-empty in a real tree; skip empty directories
            if let TreeValue::Tree(id) = &v
                && id == p.native.empty_tree_id()
            {
                continue;
            }
            entries.insert(nm, v);
        }
        let t = Tree::from_sorted_entries(entries.into_iter().collect());
        let id = p.tree(&t);
        self.trees.push(id.clone());
        id
    }

    fn root_tree(&mut self, p: &mut Pair, conflict: bool) -> Merge<TreeId> {
        if !conflict {
            if self.rng.random_bool(0.05) {
                return Merge::resolved(p.native.empty_tree_id().clone());
            }
            return Merge::resolved(self.tree(p, 0));
        }
        let sides = [3, 3, 3, 5, 7][self.rng.random_range(0..5)];
        let ids: Vec<TreeId> = (0..sides)
            .map(|i| {
                if i > 0 && self.rng.random_bool(0.15) {
                    p.native.empty_tree_id().clone()
                } else {
                    self.tree(p, 0)
                }
            })
            .collect();
        Merge::from_vec(ids)
    }

    fn commit(&mut self, p: &mut Pair, header: bool) {
        let conflict = self.rng.random_bool(0.1);
        let root_tree = self.root_tree(p, conflict);
        let conflict_labels = if conflict && self.rng.random_bool(0.6) {
            Merge::from_vec(
                (0..root_tree.iter().count())
                    .map(|i| format!("side {i} \u{2013} {}", self.rng.random_range(0..99)))
                    .collect::<Vec<_>>(),
            )
        } else {
            Merge::resolved(String::new())
        };
        let root = p.native.root_commit_id().clone();
        let parents = if self.commits.is_empty() || self.rng.random_bool(0.04) {
            vec![root]
        } else {
            let n = [1, 1, 1, 1, 2, 2, 3][self.rng.random_range(0..7)];
            let mut v: Vec<CommitId> = Vec::new();
            for _ in 0..n {
                let c = self.commits[self.rng.random_range(0..self.commits.len())].clone();
                if !v.contains(&c) {
                    v.push(c);
                }
            }
            if v.len() > 1 {
                self.merges += 1;
            }
            v
        };
        let mut change = [0u8; 16];
        self.rng.fill_bytes(&mut change);
        let preds: Vec<CommitId> = (0..self.rng.random_range(0..3))
            .filter_map(|_| self.commits.choose(&mut self.rng).cloned())
            .collect();
        let c = Commit {
            parents,
            predecessors: preds,
            root_tree,
            conflict_labels,
            change_id: ChangeId::from_bytes(&change),
            description: text(&mut self.rng),
            author: signature(&mut self.rng),
            committer: signature(&mut self.rng),
            secure_sig: None,
        };
        let sign = self.rng.random_bool(0.1);
        let mut c = c;
        if sign {
            // gix rejects '<' in signatures; GitBackend then panics on the signing path
            for s in [&mut c.author, &mut c.committer] {
                s.name = s.name.replace(['<', '>'], "_");
            }
        }
        let is_conflict = !c.root_tree.is_resolved();
        if let (Ok(id), _, Some(rc)) = p.commit(c.clone(), sign) {
            self.bumped += (rc.committer.timestamp.timestamp.0.div_euclid(1000)
                != c.committer.timestamp.timestamp.0.div_euclid(1000))
                as usize;
            self.commits.push(id);
            self.conflicted += is_conflict as usize;
            self.signed += sign as usize;
            if !header {
                // Same content, different change id: with no change-id header the git commit id
                // is identical, exercising the extras collision (timestamp bump) path.
                let mut d = c;
                self.rng.fill_bytes(&mut change);
                d.change_id = ChangeId::from_bytes(&change);
                self.dup_attempts += 1;
                if let (Ok(id2), _, Some(rc)) = p.commit(d.clone(), sign) {
                    self.bumped += (rc.committer.timestamp.timestamp.0.div_euclid(1000)
                        != d.committer.timestamp.timestamp.0.div_euclid(1000))
                        as usize;
                    self.commits.push(id2);
                }
            }
        }
    }
}

/// The IDs of every object in the git repo of the pair.
fn git_object_ids(p: &Pair) -> Vec<gix::ObjectId> {
    let listing = Command::new("git")
        .args([
            "-C",
            p.git_dir.to_str().unwrap(),
            "cat-file",
            "--batch-all-objects",
            "--batch-check",
        ])
        .output()
        .unwrap();
    assert!(listing.status.success());
    let git_ids: Vec<gix::ObjectId> = String::from_utf8(listing.stdout)
        .unwrap()
        .lines()
        .map(|l| gix::ObjectId::from_hex(l.split(' ').next().unwrap().as_bytes()).unwrap())
        .collect();
    git_ids
}

/// Compares the kind and bytes of every git object with the native store; returns the count.
fn compare_objects(p: &Pair, git_ids: &[gix::ObjectId]) -> usize {
    let repo = p.git.git_repo();
    let mut compared = 0;
    for gid in git_ids {
        let obj = repo.find_object(*gid).unwrap();
        let (kind, data) = p
            .store
            .get(gid)
            .unwrap_or_else(|e| panic!("native store lacks git object {gid}: {e}"));
        assert_eq!(kind, obj.kind, "kind mismatch {gid}");
        assert_eq!(data, obj.data, "bytes mismatch {gid}");
        compared += 1;
    }
    if let Some(mem) = &p.mem {
        assert_eq!(
            mem.len(),
            git_ids.len(),
            "object count differs (native {} vs git {})",
            mem.len(),
            git_ids.len()
        );
    } else {
        for id in &p.wrote {
            assert!(git_ids.contains(id));
        }
    }
    compared
}

/// Reads with a native backend the objects that git wrote.
fn cross_read(p: &Pair, g: &Gen, git_ids: &[gix::ObjectId], header: bool) {
    let repo = p.git.git_repo();
    let mem2 = Arc::new(MemStore::default());
    for gid in git_ids {
        let obj = repo.find_object(*gid).unwrap();
        mem2.put(obj.kind, &obj.data).unwrap();
    }
    let tmp2 = tempfile::tempdir().unwrap();
    let cross = NativeBackend::new(mem2, tmp2.path(), header).unwrap();
    for id in &g.commits {
        let mut a = block_on(p.git.read_commit(id)).unwrap();
        let b = block_on(cross.read_commit(id)).unwrap();
        a.predecessors.clear(); // predecessors live in the side table, not the object
        let mut b2 = b;
        if !header {
            b2.change_id = a.change_id.clone(); // synthetic when no header and no side table
        }
        assert_eq!(a, b2, "cross read mismatch {id}");
    }
}

fn run(header: bool, ctl: Option<&str>, n: usize, seed: u64) {
    let mut p = Pair::new(header, ctl);
    let mut g = Gen {
        rng: ChaCha8Rng::seed_from_u64(seed),
        files: vec![],
        links: vec![],
        trees: vec![],
        commits: vec![],
        conflicted: 0,
        signed: 0,
        merges: 0,
        dup_attempts: 0,
        bumped: 0,
    };
    // empty tree and error cases
    let empty = block_on(p.git.write_tree(RepoPath::root(), &Tree::default())).unwrap();
    assert_eq!(
        empty,
        block_on(p.native.write_tree(RepoPath::root(), &Tree::default())).unwrap()
    );
    assert_eq!(&empty, p.native.empty_tree_id());
    while g.commits.len() < n {
        g.commit(&mut p, header);
    }
    // parity: root + other parent, and no parents
    let root = p.native.root_commit_id().clone();
    let base = Commit {
        parents: vec![root.clone(), g.commits[0].clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(empty.clone()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::from_bytes(&[7; 16]),
        description: String::new(),
        author: signature(&mut g.rng),
        committer: signature(&mut g.rng),
        secure_sig: None,
    };
    // commit() panics when the two backends disagree
    let _ = p.commit(base.clone(), false);
    let mut no_parents = base;
    no_parents.parents = vec![];
    assert!(matches!(
        p.commit(no_parents, false),
        (Err(()), Err(()), None)
    ));

    // read parity for every commit (both backends read what they wrote)
    for id in &g.commits {
        let a = block_on(p.git.read_commit(id)).unwrap();
        let b = block_on(p.native.read_commit(id)).unwrap();
        assert_eq!(a, b, "read_commit mismatch for {id}");
    }
    let git_ids = git_object_ids(&p);
    let compared = compare_objects(&p, &git_ids);
    let fsck = Command::new("git")
        .args(["-C", p.git_dir.to_str().unwrap(), "fsck", "--no-dangling"])
        .output()
        .unwrap();
    let fsck_err = String::from_utf8_lossy(&fsck.stderr).into_owned();
    println!(
        "header={header} ctl={} commits={} trees={} files={} conflicted={} signed={} merges={} dup_attempts={} committer_ts_bumped={} objects_compared={} git_fsck_ok={} {}",
        ctl.is_some(),
        g.commits.len(),
        g.trees.len(),
        g.files.len(),
        g.conflicted,
        g.signed,
        g.merges,
        g.dup_attempts,
        g.bumped,
        compared,
        fsck.status.success(),
        fsck_err.lines().take(3).collect::<Vec<_>>().join(" | ")
    );
    assert!(
        g.conflicted >= n / 30,
        "need >= {} conflicted trees, got {}",
        n / 30,
        g.conflicted
    );
    cross_read(&p, &g, &git_ids, header);
}

#[test]
fn differential_header_on() {
    let n = std::env::var("MOTTE_DIFF_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let seed = std::env::var("MOTTE_DIFF_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    run(true, None, n, seed);
}

#[test]
fn differential_header_off() {
    let n = std::env::var("MOTTE_DIFF_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let seed = std::env::var("MOTTE_DIFF_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    run(false, None, n, seed);
}

#[test]
fn differential_over_daemon() {
    let fake = common::FakeDaemon::start(true);
    let sock = std::env::var("MOTTE_CTL").unwrap_or_else(|_| fake.path().display().to_string());
    let n = std::env::var("MOTTE_DIFF_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    run(true, Some(&sock), n, 3);
}
