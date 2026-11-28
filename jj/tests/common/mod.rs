//! A fake `motte-vfsd` control socket in overlay mode: an in-memory store plus a settable dirty
//! set. `tests/fixtures.rs` replays `testdata/ctl/*.json` against it, so it answers as the daemon
//! does.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use gix::objs::Kind;
use jj_lib::backend::{
    Backend, ChangeId, Commit, CommitId, CopyId, MillisSinceEpoch, Signature, Timestamp, Tree,
    TreeId, TreeValue,
};
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::{RepoPath, RepoPathComponentBuf};
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::CtlClient;
use motte_jj::ctl::{MAX_OBJECT_BYTES, ObjectStore, StoreError, StoreResult, hash_object};
use pollster::block_on;
use serde_json::{Value, json};

/// The `env` value that removes a variable.
pub const UNSET: &str = "\0unset";

/// The daemon limit on one request line.
pub const MAX_LINE_BYTES: usize = 64 << 20;

#[derive(Default)]
pub struct State {
    pub objects: HashMap<String, (String, Vec<u8>)>,
    pub entries: Vec<Value>,
    pub changes: Vec<Value>,
    /// The tree the mount shows; `dirty`, `token` and `changes` report it.
    pub base_tree: String,
    /// The change token; empty plays a daemon that is not in overlay mode: `token` answers
    /// `unknown_op` and `dirty` carries no token.
    pub token: String,
    pub ops: HashMap<String, u64>,
    pub traceparents: Vec<String>,
    /// The generation and heads of the remote op heads; unset answers `unknown_op`.
    pub opheads: Option<(i64, Vec<String>)>,
    /// The refs by name as (commit, generation); `ref_counter` is the last generation handed out.
    pub refs: BTreeMap<String, (String, i64)>,
    pub ref_counter: i64,
    /// The objects of the op log by (type, id).
    pub op_objects: HashMap<(String, String), Vec<u8>>,
    /// The reports of the `workspace` op, as (head operation, commit, generation).
    pub reports: Vec<(String, String, i64)>,
    /// Ops that answer `unknown_op`, like a daemon that lacks them.
    pub unsupported: BTreeSet<String>,
    /// Answers every error without a `code`, like a daemon that sends none.
    pub codeless: bool,
    /// The op whose next request takes effect and gets no answer: the connection closes.
    pub lose_answer_of: Option<String>,
    /// The op whose next request is dropped before it takes effect: the connection closes.
    pub drop_request_of: Option<String>,
    /// Ops that are answered with an error of the given code.
    pub failing: BTreeMap<String, String>,
    /// After a request for the first op, the second op fails with the third, a code.
    pub fail_after: Option<(String, String, String)>,
    /// Every request op, in order.
    pub log: Vec<String>,
    /// The longest request line that is answered; a longer one gets `line_too_long`.
    pub max_line: usize,
    /// The count of request lines received.
    pub lines: u64,
    /// A `putop` waits up to a second until this many are in flight, then answers.
    pub hold_putops: usize,
    pub putops_inflight: usize,
    /// The most `putop` requests in flight at once.
    pub putops_peak: usize,
    /// A `putbatch` answers this many milliseconds late, and counts as answered after the wait.
    pub delay_putbatch_ms: u64,
    pub putbatches_answered: usize,
    /// Each `get`, `getop` and `getbatch` answers this many milliseconds late, outside the state
    /// lock, as a daemon that reads every object over the network does.
    pub read_delay_ms: u64,
    pub reads_inflight: HashMap<String, usize>,
    /// The most delayed reads of each op in flight at once.
    pub reads_peak: HashMap<String, usize>,
}

pub struct FakeDaemon {
    pub state: Arc<Mutex<State>>,
    pub sock: PathBuf,
    _dir: tempfile::TempDir,
}

impl FakeDaemon {
    /// A daemon with the store ops of a minimal one: no op log, op heads or `stats`.
    /// `batch: false` answers `putbatch` with `unknown_op` as well.
    pub fn start(batch: bool) -> Self {
        let mut unsupported: BTreeSet<String> = ["putop", "getop", "workspace", "stats"]
            .map(str::to_owned)
            .into();
        if !batch {
            unsupported.insert("putbatch".to_owned());
        }
        Self::with(State {
            unsupported,
            ..State::default()
        })
    }

    /// A daemon that answers every op of the protocol.
    pub fn full() -> Self {
        Self::with(State {
            opheads: Some((0, vec![])),
            ..State::default()
        })
    }

    fn with(mut state: State) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("ctl.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        state.token = "b0:1".into();
        state.max_line = MAX_LINE_BYTES;
        let state = Arc::new(Mutex::new(state));
        let st = state.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { return };
                let st = st.clone();
                std::thread::spawn(move || serve(conn, st));
            }
        });
        Self {
            state,
            sock,
            _dir: dir,
        }
    }

    pub fn ops(&self, op: &str) -> u64 {
        self.state.lock().unwrap().ops.get(op).copied().unwrap_or(0)
    }

    pub fn path(&self) -> &Path {
        &self.sock
    }
}

type Failure = (&'static str, String);
type Answer = Result<Value, Failure>;

/// An error answer. The text holds the code, as the daemon writes it.
fn fail(st: &State, code: &str, err: &str) -> Value {
    let text = if code == "unknown_op" {
        err.to_owned()
    } else {
        format!("{err} ({code})")
    };
    if st.codeless {
        json!({"err": text})
    } else {
        json!({"err": text, "code": code})
    }
}

fn is_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_ref_name(n: &str) -> bool {
    !n.is_empty() && n.split('/').all(|s| !s.is_empty() && s != "." && s != "..")
}

fn kind_of(s: &str) -> Option<Kind> {
    Some(match s {
        "blob" => Kind::Blob,
        "tree" => Kind::Tree,
        "commit" => Kind::Commit,
        "tag" => Kind::Tag,
        _ => return None,
    })
}

fn invalid(what: &str) -> Failure {
    ("invalid_request", what.to_owned())
}

fn not_found() -> Failure {
    ("vcs_not_found", "object not found".to_owned())
}

fn decode(v: &Value, what: &str) -> Result<Vec<u8>, Failure> {
    v.as_str()
        .and_then(|s| B64.decode(s).ok())
        .ok_or_else(|| invalid(what))
}

fn sha_list(rq: &Value) -> Result<Vec<&str>, Failure> {
    let list = rq["shas"].as_array().ok_or_else(|| invalid("no shas"))?;
    if list.len() > 4096 {
        return Err(invalid("more than 4096 shas"));
    }
    list.iter()
        .map(|s| {
            s.as_str()
                .filter(|s| is_sha(s))
                .ok_or_else(|| invalid("bad sha"))
        })
        .collect()
}

/// Checks and stores one object, returning its SHA.
fn store(st: &mut State, kind: &Value, data: &Value) -> Result<String, Failure> {
    let kind = kind
        .as_str()
        .filter(|k| kind_of(k).is_some())
        .ok_or_else(|| invalid("bad type"))?;
    let data = decode(data, "data is not base64")?;
    if data.len() as u64 > MAX_OBJECT_BYTES {
        return Err(("vcs_object_too_large", "object too large".to_owned()));
    }
    let sha = hash_object(kind_of(kind).unwrap(), &data).to_string();
    st.objects.insert(sha.clone(), (kind.to_owned(), data));
    Ok(sha)
}

fn answer(st: &mut State, rq: &Value) -> Answer {
    let op = rq["op"].as_str().unwrap_or("");
    if let Some(code) = st.failing.get(op) {
        let code: &'static str = Box::leak(code.clone().into_boxed_str());
        return Err((code, "the daemon is set to fail".to_owned()));
    }
    match op {
        "get" => {
            let sha = rq["sha"].as_str().filter(|s| is_sha(s));
            let (k, d) = st
                .objects
                .get(sha.ok_or_else(|| invalid("bad sha"))?)
                .ok_or_else(not_found)?;
            Ok(json!({"type": k, "data": B64.encode(d)}))
        }
        "put" => Ok(json!({"sha": store(st, &rq["type"], &rq["data"])?})),
        "putbatch" => putbatch(st, rq),
        "has" => {
            let present: Vec<bool> = sha_list(rq)?
                .iter()
                .map(|s| st.objects.contains_key(*s))
                .collect();
            Ok(json!({"present": present}))
        }
        "getbatch" => getbatch(st, rq),
        "opheads" => opheads(st, rq),
        "refs" | "swapref" => refs(st, rq),
        "putop" | "getop" => op_object(st, rq),
        "workspace" => workspace(st, rq),
        "dirty" => Ok(
            json!({"boot": "b0", "token": st.token, "base_tree": st.base_tree, "entries": st.entries}),
        ),
        "token" if !st.token.is_empty() => {
            Ok(json!({"boot": "b0", "token": st.token, "base_tree": st.base_tree}))
        }
        "changes" => Ok(
            json!({"boot": "b0", "token": st.token, "base_tree": st.base_tree, "changes": st.changes}),
        ),
        "stats" => Ok(json!({"blob_hits": 0, "blob_misses": 0, "tree_calls": 0})),
        _ => Err(("unknown_op", "unknown op".to_owned())),
    }
}

fn putbatch(st: &mut State, rq: &Value) -> Answer {
    let list = rq["objects"].as_array().filter(|l| l.len() <= 4096);
    let list = list.ok_or_else(|| invalid("bad objects"))?;
    for o in list {
        let kind = o["type"].as_str().and_then(kind_of);
        if kind.is_none() || decode(&o["data"], "data").is_err() {
            return Err(invalid("bad object"));
        }
    }
    let mut shas = vec![];
    for o in list {
        shas.push(store(st, &o["type"], &o["data"])?);
    }
    Ok(json!({"shas": shas}))
}

fn workspace(st: &mut State, rq: &Value) -> Answer {
    let sha = rq["sha"].as_str().unwrap_or("");
    let commit = rq["commit"].as_str().unwrap_or("");
    let generation = rq["generation"].as_i64().unwrap_or(0);
    if !is_hex(sha) || !is_sha(commit) {
        return Err(invalid("bad workspace report"));
    }
    if generation < 1 {
        return Err(("vcs_object_invalid", "generation below 1".to_owned()));
    }
    st.reports
        .push((sha.to_owned(), commit.to_owned(), generation));
    Ok(json!({}))
}

fn getbatch(st: &mut State, rq: &Value) -> Answer {
    let max = rq["max_size"].as_u64();
    let (mut datas, mut large, mut total) = (vec![], vec![], 0u64);
    for (i, sha) in sha_list(rq)?.into_iter().enumerate() {
        let (_, d) = st.objects.get(sha).ok_or_else(not_found)?;
        if max.is_some_and(|m| d.len() as u64 > m) {
            large.push(i);
            datas.push(String::new());
            continue;
        }
        total += d.len() as u64;
        datas.push(B64.encode(d));
    }
    if total > MAX_OBJECT_BYTES {
        return Err(("vcs_object_too_large", "answer too large".to_owned()));
    }
    Ok(json!({"datas": datas, "large": large}))
}

fn opheads(st: &mut State, rq: &Value) -> Answer {
    let Some((generation, heads)) = st.opheads.clone() else {
        return Err(("unknown_op", "unknown op".to_owned()));
    };
    let swapped: Vec<String> = rq["heads"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|h| h.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    match rq["expect"].as_i64() {
        Some(e) if !swapped.is_empty() => {
            if e != generation {
                return Err(("vcs_generation_conflict", "the generation moved".to_owned()));
            }
            st.opheads = Some((generation + 1, swapped.clone()));
            Ok(json!({"gen": generation + 1, "heads": swapped}))
        }
        _ => Ok(json!({"gen": generation, "heads": heads})),
    }
}

fn op_object(st: &mut State, rq: &Value) -> Answer {
    let kind = rq["type"]
        .as_str()
        .filter(|k| ["operations", "views"].contains(k));
    let sha = rq["sha"].as_str().filter(|s| is_hex(s));
    let (Some(kind), Some(sha)) = (kind, sha) else {
        return Err(invalid("bad op object name"));
    };
    let key = (kind.to_owned(), sha.to_owned());
    if rq["op"] == "putop" {
        let data = decode(&rq["data"], "data is not base64")?;
        st.op_objects.insert(key, data);
        return Ok(json!({}));
    }
    let data = st.op_objects.get(&key).ok_or_else(not_found)?;
    Ok(json!({"data": B64.encode(data)}))
}

fn refs(st: &mut State, rq: &Value) -> Answer {
    let name = rq["name"].as_str().unwrap_or_default();
    let view = |n: &str, r: &(String, i64)| json!({"name": n, "commit": r.0, "generation": r.1});
    if rq["op"] == "refs" {
        if !name.is_empty() && !valid_ref_name(name) {
            return Err(invalid("bad ref name"));
        }
        if name.is_empty() {
            let all: Vec<Value> = st.refs.iter().map(|(n, r)| view(n, r)).collect();
            return Ok(json!({ "refs": all }));
        }
        let found = st.refs.get(name).ok_or_else(not_found)?;
        return Ok(json!({"ref": view(name, found)}));
    }
    let delete = rq["delete"].as_bool().unwrap_or(false);
    let expect = rq["expect"].as_i64().unwrap_or(0);
    let commit = rq["commit"].as_str().unwrap_or("");
    let bad = !valid_ref_name(name) || expect < 0 || (delete && expect < 1);
    if bad || (!delete && !is_sha(commit)) {
        return Err(invalid("bad swapref"));
    }
    let stored = st.refs.get(name).cloned();
    if stored.as_ref().map_or(0, |r| r.1) != expect {
        let mut rs = fail(st, "vcs_generation_conflict", "the ref moved");
        if let Some(r) = &stored {
            rs["ref"] = view(name, r);
        }
        return Ok(rs);
    }
    if delete {
        st.refs.remove(name);
        return Ok(json!({}));
    }
    st.ref_counter += 1;
    let next = (commit.to_owned(), st.ref_counter);
    st.refs.insert(name.to_owned(), next.clone());
    Ok(json!({"ref": view(name, &next)}))
}

fn serve(conn: UnixStream, state: Arc<Mutex<State>>) {
    let mut w = conn.try_clone().unwrap();
    for line in BufReader::new(conn).lines() {
        let Ok(line) = line else { return };
        let mut st = state.lock().unwrap();
        st.lines += 1;
        let dropped = st.drop_request_of.clone().filter(|op| {
            serde_json::from_str::<Value>(&line).is_ok_and(|v| v["op"] == op.as_str())
        });
        if dropped.is_some() {
            st.drop_request_of = None;
            return;
        }
        let rs = reply(&mut st, &line);
        let read = serde_json::from_str::<Value>(&line)
            .ok()
            .and_then(|v| v["op"].as_str().map(str::to_owned))
            .filter(|op| matches!(op.as_str(), "get" | "getop" | "getbatch"));
        if let Some(op) = read.filter(|_| st.read_delay_ms > 0) {
            let delay = std::time::Duration::from_millis(st.read_delay_ms);
            let n = *st
                .reads_inflight
                .entry(op.clone())
                .and_modify(|n| *n += 1)
                .or_insert(1);
            let peak = st.reads_peak.entry(op.clone()).or_default();
            *peak = (*peak).max(n);
            drop(st);
            std::thread::sleep(delay);
            st = state.lock().unwrap();
            *st.reads_inflight.get_mut(&op).unwrap() -= 1;
        }
        let lost = st.lose_answer_of.clone().filter(|op| {
            serde_json::from_str::<Value>(&line).is_ok_and(|v| v["op"] == op.as_str())
        });
        if lost.is_some() {
            st.lose_answer_of = None;
            return;
        }
        let held = st.hold_putops > 0
            && serde_json::from_str::<Value>(&line).is_ok_and(|v| v["op"] == "putop");
        if held {
            st.putops_inflight += 1;
            st.putops_peak = st.putops_peak.max(st.putops_inflight);
            let give_up = std::time::Instant::now() + std::time::Duration::from_secs(1);
            while st.putops_inflight < st.hold_putops && std::time::Instant::now() < give_up {
                drop(st);
                std::thread::yield_now();
                st = state.lock().unwrap();
            }
            st.putops_inflight -= 1;
        }
        let delay = st.delay_putbatch_ms > 0
            && serde_json::from_str::<Value>(&line).is_ok_and(|v| v["op"] == "putbatch");
        if delay {
            let ms = st.delay_putbatch_ms;
            drop(st);
            std::thread::sleep(std::time::Duration::from_millis(ms));
            state.lock().unwrap().putbatches_answered += 1;
            st = state.lock().unwrap();
        }
        drop(st);
        let mut out = serde_json::to_vec(&rs).unwrap();
        out.push(b'\n');
        if w.write_all(&out).is_err() {
            return;
        }
    }
}

fn reply(st: &mut State, line: &str) -> Value {
    if line.len() > st.max_line {
        return fail(st, "line_too_long", "the request line is too long");
    }
    let Ok(rq) = serde_json::from_str::<Value>(line) else {
        return fail(st, "invalid_request", "not JSON");
    };
    let op = rq["op"].as_str().unwrap_or("").to_owned();
    *st.ops.entry(op.clone()).or_default() += 1;
    st.log.push(op.clone());
    if let Some((_, fails, code)) = st.fail_after.clone().filter(|(after, _, _)| *after == op) {
        st.failing.insert(fails, code);
    }
    if let Some(tp) = rq["traceparent"].as_str() {
        st.traceparents.push(tp.to_owned());
    }
    let result = if st.unsupported.contains(&op) {
        Err(("unknown_op", "unknown op".to_owned()))
    } else {
        answer(st, &rq)
    };
    match result {
        Ok(mut v) => {
            if st.codeless
                && let Some(o) = v.as_object_mut()
            {
                o.remove("code");
            }
            v
        }
        Err((code, err)) => fail(st, code, &err),
    }
}

/// An in-memory object store.
#[derive(Debug, Default)]
pub struct MemStore {
    objects: Mutex<HashMap<gix::ObjectId, (Kind, Vec<u8>)>>,
}

impl MemStore {
    pub fn len(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all(&self) -> HashMap<gix::ObjectId, (Kind, Vec<u8>)> {
        self.objects.lock().unwrap().clone()
    }
}

impl ObjectStore for MemStore {
    fn get(&self, id: &gix::ObjectId) -> StoreResult<(Kind, Vec<u8>)> {
        let found = self.objects.lock().unwrap().get(id).cloned();
        found.ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    fn put(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId> {
        let id = hash_object(kind, data);
        let mut objects = self.objects.lock().unwrap();
        objects.entry(id).or_insert_with(|| (kind, data.to_vec()));
        Ok(id)
    }
}
pub struct Fixture {
    pub daemon: FakeDaemon,
    pub backend: NativeBackend,
    pub dir: tempfile::TempDir,
}

impl Fixture {
    pub fn new() -> Self {
        Self::with(FakeDaemon::start(true))
    }

    pub fn with(daemon: FakeDaemon) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(CtlClient::new(daemon.path()));
        let backend = NativeBackend::new(store, dir.path(), true).unwrap();
        Self {
            daemon,
            backend,
            dir,
        }
    }

    pub fn tree(&self, files: &BTreeMap<&str, &str>) -> TreeId {
        let entries = files
            .iter()
            .map(|(name, body)| {
                let id = block_on(
                    self.backend
                        .write_file(RepoPath::root(), &mut body.as_bytes()),
                )
                .unwrap();
                (
                    RepoPathComponentBuf::new(*name).unwrap(),
                    TreeValue::File {
                        id,
                        executable: false,
                        copy_id: CopyId::placeholder(),
                    },
                )
            })
            .collect();
        let t = Tree::from_sorted_entries(entries);
        let id = block_on(self.backend.write_tree(RepoPath::root(), &t)).unwrap();
        self.backend.object_store().flush_deferred().unwrap();
        id
    }

    pub fn commit(&self, tree: TreeId, seed: u8) -> String {
        self.commit_on(
            &[self.backend.root_commit_id().clone()],
            tree,
            u32::from(seed),
        )
    }

    /// Writes a commit with the given parents, and returns its ID.
    pub fn commit_on(&self, parents: &[CommitId], tree: TreeId, seed: u32) -> String {
        let change_id = ChangeId::from_bytes(
            &[
                seed.to_le_bytes(),
                seed.to_le_bytes(),
                seed.to_le_bytes(),
                seed.to_le_bytes(),
            ]
            .concat(),
        );
        self.commit_with_change(parents, tree, change_id, &format!("commit {seed}\n"))
    }

    /// Writes a commit with the given parents and change ID, as a land that rebases a commit does,
    /// and returns its ID.
    pub fn commit_with_change(
        &self,
        parents: &[CommitId],
        tree: TreeId,
        change_id: ChangeId,
        description: &str,
    ) -> String {
        let sig = Signature {
            name: "t".into(),
            email: "t@example.com".into(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(1_700_000_000_000),
                tz_offset: 0,
            },
        };
        let c = Commit {
            parents: parents.to_vec(),
            predecessors: vec![],
            root_tree: Merge::resolved(tree),
            conflict_labels: Merge::resolved(String::new()),
            change_id,
            description: description.to_owned(),
            author: sig.clone(),
            committer: sig,
            secure_sig: None,
        };
        let (id, _) = block_on(self.backend.write_commit(c, None)).unwrap();
        self.backend.object_store().flush_deferred().unwrap();
        id.hex()
    }

    pub fn jj(&self, cwd: &Path, args: &[&str]) -> String {
        self.jj_env(cwd, args, &[])
    }

    pub fn jj_env(&self, cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> String {
        let (ok, text) = self.jj_try(cwd, args, env);
        assert!(ok, "jj {args:?} failed:\n{text}");
        text
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

pub fn write_tree(dir: &Path, files: &BTreeMap<&str, &str>) {
    for (name, body) in files {
        std::fs::write(dir.join(name), body).unwrap();
    }
}

impl Fixture {
    /// Runs `jj` and returns whether it succeeded and what it printed. An `env` value of `UNSET`
    /// removes the variable.
    pub fn jj_try(&self, cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> (bool, String) {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jj"));
        cmd.args(args)
            .current_dir(cwd)
            .env("HOME", self.dir.path())
            .env("XDG_CONFIG_HOME", self.dir.path())
            .env("JJ_CONFIG", self.dir.path().join("none.toml"))
            .env("JJ_USER", "t")
            .env("JJ_EMAIL", "t@example.com")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .env("MOTTE_VFS_SOCK", self.daemon.path());
        for (k, v) in env {
            if *v == UNSET {
                cmd.env_remove(k);
            } else {
                cmd.env(k, v);
            }
        }
        let out = cmd.output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    }

    /// Makes a mount directory holding `files`, a daemon that shows their tree, and a workspace
    /// initialized on a commit of that tree with `extra` arguments of `jj motte init`.
    pub fn workspace(&self, files: &BTreeMap<&str, &str>, extra: &[&str]) -> Workspace {
        let tree = self.tree(files);
        let commit = self.commit(tree.clone(), 1);
        self.daemon.state.lock().unwrap().base_tree = tree.hex();
        let mount = self.dir.path().join("mount");
        let jj_root = self.dir.path().join("jjroot");
        std::fs::create_dir(&mount).unwrap();
        write_tree(&mount, files);
        let mut args = vec![
            "motte",
            "init",
            "--sock",
            self.daemon.path().to_str().unwrap(),
            "--commit",
            &commit,
            "--jj-root",
            jj_root.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        self.jj(&mount, &args);
        Workspace {
            mount,
            jj_root,
            commit,
        }
    }
}

pub struct Workspace {
    pub mount: PathBuf,
    pub jj_root: PathBuf,
    pub commit: String,
}

/// Stores an object in the fake daemon and returns its SHA.
pub fn put(d: &FakeDaemon, kind: &str, data: &[u8]) -> String {
    let sha = hash_object(kind_of(kind).unwrap(), data).to_string();
    let mut st = d.state.lock().unwrap();
    st.objects
        .insert(sha.clone(), (kind.to_owned(), data.to_vec()));
    sha
}
