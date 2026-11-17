//! Client for the `motte-vfsd` control socket (one JSON object per line) and the object-store
//! abstraction the native backend writes git objects through.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use gix::objs::Kind;
use serde::Deserialize;
use serde_json::{Value, json};

pub const HASH_KIND: gix::hash::Kind = gix::hash::Kind::Sha1;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object {0} not found")]
    NotFound(String),
    #[error("control socket {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon error: {0}")]
    Daemon(String),
    #[error("bad daemon response: {0}")]
    Protocol(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// One ref of the native repo of the workspace. `generation` is never reused.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RefInfo {
    pub name: String,
    pub commit: String,
    pub generation: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RefSwap {
    /// The new ref, or `None` after a delete.
    Swapped(Option<RefInfo>),
    /// The stored generation differed; carries the stored ref, `None` when there is none.
    Conflict(Option<RefInfo>),
}

fn ref_reply(v: Value) -> StoreResult<RefInfo> {
    let r = v
        .get("ref")
        .cloned()
        .ok_or_else(|| StoreError::Protocol("ref answer without ref".into()))?;
    serde_json::from_value(r).map_err(|e| StoreError::Protocol(e.to_string()))
}

/// Raw git object storage (payloads without the `<type> <len>\0` header).
pub trait ObjectStore: Send + Sync + Debug {
    fn get(&self, id: &gix::ObjectId) -> StoreResult<(Kind, Vec<u8>)>;
    fn put(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId>;
    /// Stores several objects; a daemon that supports it takes them in one round trip.
    fn put_many(&self, objects: &[(Kind, &[u8])]) -> StoreResult<Vec<gix::ObjectId>> {
        objects.iter().map(|(k, d)| self.put(*k, d)).collect()
    }
}

pub fn hash_object(kind: Kind, data: &[u8]) -> gix::ObjectId {
    gix::objs::compute_hash(HASH_KIND, kind, data).expect("sha1 hashing cannot fail")
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Blob => "blob",
        Kind::Tree => "tree",
        Kind::Commit => "commit",
        Kind::Tag => "tag",
    }
}

fn parse_kind(s: &str) -> StoreResult<Kind> {
    Ok(match s {
        "blob" => Kind::Blob,
        "tree" => Kind::Tree,
        "commit" => Kind::Commit,
        "tag" => Kind::Tag,
        other => return Err(StoreError::Protocol(format!("object type {other:?}"))),
    })
}

/// In-memory store, used by tests.
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
        self.objects
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }
    fn put(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId> {
        let id = hash_object(kind, data);
        self.objects
            .lock()
            .unwrap()
            .entry(id)
            .or_insert_with(|| (kind, data.to_vec()));
        Ok(id)
    }
}

struct Conn {
    w: UnixStream,
    r: BufReader<UnixStream>,
}

/// Counters for the end-to-end report.
#[derive(Debug, Default, Clone)]
pub struct CtlStats {
    pub gets: u64,
    pub puts: u64,
    pub batches: u64,
    pub put_skipped: u64,
}

/// Client for the daemon control socket.
#[derive(Debug)]
pub struct CtlClient {
    path: PathBuf,
    idle: Mutex<Vec<Conn>>,
    known: Mutex<HashSet<gix::ObjectId>>,
    stats: Mutex<CtlStats>,
    no_batch: AtomicBool,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Conn")
    }
}

#[derive(Debug, Deserialize)]
pub struct DirtyEntry {
    pub path: String,
    pub kind: String,
    #[serde(default)]
    pub exec: bool,
    #[serde(default)]
    pub opaque: bool,
}

#[derive(Debug, Deserialize)]
pub struct DirtyReply {
    /// Names the daemon process; a token of another boot id equals no token of this one.
    #[serde(default)]
    pub boot: String,
    #[serde(default)]
    pub base_tree: String,
    /// `<boot>:<fingerprint>` of the upper directory.
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub entries: Vec<DirtyEntry>,
}

/// What a client sees from a daemon that does not run in overlay mode.
const NOT_OVERLAY: &str = "the daemon does not run in overlay mode (it has no `token` op, or its `dirty` reply carries no token); this shim needs motte-vfsd with an upper directory";

/// The answer of the `token` op.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenReply {
    #[serde(default)]
    pub boot: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub base_tree: String,
}

impl TokenReply {
    /// An unsettled token equals no other token.
    pub fn settled(&self) -> bool {
        token_settled(&self.token)
    }
}

pub fn token_settled(token: &str) -> bool {
    !token.is_empty() && !token.contains(":unsettled.")
}

const GET_BATCH: usize = 64;
const HAS_BATCH: usize = 4096;

/// One file whose view content differs from the daemon's base tree. Modes are git modes; an
/// absent side has none.
#[derive(Debug, Deserialize)]
pub struct ChangeEntry {
    pub path: String,
    #[serde(default)]
    pub old_sha: Option<String>,
    #[serde(default)]
    pub old_mode: u32,
    #[serde(default)]
    pub old_size: u64,
    #[serde(default)]
    pub new_sha: Option<String>,
    #[serde(default)]
    pub new_mode: u32,
    #[serde(default)]
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct ChangesReply {
    #[serde(default)]
    pub base_tree: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub changes: Vec<ChangeEntry>,
}

impl CtlClient {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            idle: Mutex::new(Vec::new()),
            known: Mutex::new(HashSet::new()),
            stats: Mutex::new(CtlStats::default()),
            no_batch: AtomicBool::new(false),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn stats(&self) -> CtlStats {
        self.stats.lock().unwrap().clone()
    }

    fn io_err(&self, source: std::io::Error) -> StoreError {
        StoreError::Io {
            path: self.path.display().to_string(),
            source,
        }
    }

    /// Opens the control socket.
    fn connect(&self) -> StoreResult<Conn> {
        let w = UnixStream::connect(&self.path).map_err(|e| self.io_err(e))?;
        let r = w.try_clone().map_err(|e| self.io_err(e))?;
        Ok(Conn {
            w,
            r: BufReader::with_capacity(1 << 20, r),
        })
    }

    fn call_once(&self, conn: &mut Conn, line: &[u8]) -> StoreResult<Value> {
        conn.w.write_all(line).map_err(|e| self.io_err(e))?;
        let mut buf = String::new();
        let n = conn.r.read_line(&mut buf).map_err(|e| self.io_err(e))?;
        if n == 0 {
            return Err(self.io_err(std::io::ErrorKind::UnexpectedEof.into()));
        }
        serde_json::from_str(&buf).map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// Sends one request and returns the reply object; retries once on a broken connection.
    /// Connections are pooled so that concurrent callers do not serialize on one socket.
    pub fn call(&self, req: Value) -> StoreResult<Value> {
        self.send(req, true)
    }

    /// `call`, except that an `err` reply is returned as a reply when `fail_on_err` is false.
    fn send(&self, mut req: Value, fail_on_err: bool) -> StoreResult<Value> {
        if let Some(tp) = crate::trace::traceparent() {
            req["traceparent"] = json!(tp);
        }
        let mut line = serde_json::to_vec(&req).unwrap();
        line.push(b'\n');
        let mut last = None;
        for _ in 0..2 {
            let pooled = self.idle.lock().unwrap().pop();
            let mut conn = match pooled {
                Some(c) => c,
                None => self.connect()?,
            };
            match self.call_once(&mut conn, &line) {
                Ok(v) => {
                    self.idle.lock().unwrap().push(conn);
                    if let Some(e) = v.get("err").and_then(Value::as_str).filter(|_| fail_on_err) {
                        return Err(StoreError::Daemon(e.to_owned()));
                    }
                    return Ok(v);
                }
                Err(e @ StoreError::Io { .. }) => {
                    self.idle.lock().unwrap().clear();
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap())
    }

    /// The dirty set: every path where the upper directory differs from the base tree, with the
    /// change token of the walk that listed it.
    pub fn dirty(&self) -> StoreResult<DirtyReply> {
        let v = match self.call(json!({ "op": "dirty" })) {
            Err(StoreError::Daemon(m)) if m.contains("unknown op") => {
                return Err(StoreError::Protocol(NOT_OVERLAY.into()));
            }
            other => other?,
        };
        let reply: DirtyReply =
            serde_json::from_value(v).map_err(|e| StoreError::Protocol(e.to_string()))?;
        if reply.token.is_empty() {
            return Err(StoreError::Protocol(NOT_OVERLAY.into()));
        }
        Ok(reply)
    }

    /// The change token of the upper directory.
    pub fn token(&self) -> StoreResult<TokenReply> {
        let v = match self.call(json!({"op": "token"})) {
            Err(StoreError::Daemon(m)) if m.contains("unknown op") => {
                return Err(StoreError::Protocol(NOT_OVERLAY.into()));
            }
            other => other?,
        };
        serde_json::from_value(v).map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// Reads the remote op heads, or swaps them when `swap` names the expected generation and the
    /// new heads. Answers the generation and the heads.
    pub fn opheads(&self, swap: Option<(i64, &[String])>) -> StoreResult<(i64, Vec<String>)> {
        let mut req = json!({"op": "opheads"});
        if let Some((expect, heads)) = swap {
            req["expect"] = json!(expect);
            req["heads"] = json!(heads);
        }
        let v = self.call(req)?;
        let heads = v
            .get("heads")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|h| h.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok((v.get("gen").and_then(Value::as_i64).unwrap_or(0), heads))
    }

    /// Records the head operation and the mount commit of the workspace with the control plane. A daemon
    /// without the `workspace` op answers `false`; the answer `true` means the control plane took it.
    /// `generation` is the op-heads generation of the swap that set `op`, which orders the reports.
    pub fn set_workspace(&self, op: &str, commit: &str, generation: i64) -> StoreResult<bool> {
        match self
            .call(json!({"op": "workspace", "sha": op, "commit": commit, "generation": generation}))
        {
            Ok(_) => Ok(true),
            Err(StoreError::Daemon(m)) if m.contains("unknown op") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Lists the refs of the repo of the workspace.
    pub fn refs(&self) -> StoreResult<Vec<RefInfo>> {
        let v = self.call(json!({"op": "refs"}))?;
        serde_json::from_value(v.get("refs").cloned().unwrap_or_else(|| json!([])))
            .map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// Reads one ref; an absent ref fails as a daemon error that carries `vcs_not_found`.
    pub fn get_ref(&self, name: &str) -> StoreResult<RefInfo> {
        ref_reply(self.call(json!({"op": "refs", "name": name}))?)
    }

    /// Swaps a ref when its stored generation is `expect` (0 creates it), or deletes it with `delete`.
    /// A lost race is `RefSwap::Conflict` with the stored ref, so the caller retries from the winner.
    pub fn swap_ref(
        &self,
        name: &str,
        expect: i64,
        commit: &str,
        delete: bool,
    ) -> StoreResult<RefSwap> {
        let mut req = json!({"op": "swapref", "name": name, "expect": expect});
        if delete {
            req["delete"] = json!(true);
        } else {
            req["commit"] = json!(commit);
        }
        let v = self.send(req, false)?;
        let conflict = v.get("code").and_then(Value::as_str) == Some("vcs_generation_conflict");
        match v.get("err").and_then(Value::as_str) {
            Some(_) if conflict => Ok(RefSwap::Conflict(
                v.get("ref")
                    .map(|r| serde_json::from_value(r.clone()))
                    .transpose()
                    .map_err(|e| StoreError::Protocol(e.to_string()))?,
            )),
            Some(e) => Err(StoreError::Daemon(e.to_owned())),
            None if delete => Ok(RefSwap::Swapped(None)),
            None => ref_reply(v).map(|r| RefSwap::Swapped(Some(r))),
        }
    }

    /// Stores one object of the op log of the workspace; `kind` is "operations" or "views".
    pub fn putop(&self, kind: &str, id: &str, data: &[u8]) -> StoreResult<()> {
        self.call(json!({"op": "putop", "type": kind, "sha": id, "data": B64.encode(data)}))
            .map(|_| ())
    }

    pub fn getop(&self, kind: &str, id: &str) -> StoreResult<Vec<u8>> {
        let v = self.call(json!({"op": "getop", "type": kind, "sha": id}))?;
        let data = v
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::Protocol("getop answer without data".into()))?;
        B64.decode(data)
            .map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// The files of the view that differ from the daemon's base tree, read without a snapshot.
    pub fn changes(&self) -> StoreResult<ChangesReply> {
        let v = self.call(json!({"op": "changes"}))?;
        serde_json::from_value(v).map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// Reads blobs in batches of `GET_BATCH`, or one by one from a daemon without `getbatch`. A
    /// batch over the byte limit of a batch is split. A blob larger than `max_size`, or one that
    /// alone passes the limit, is `None`.
    pub fn get_blobs(
        &self,
        ids: &[gix::ObjectId],
        max_size: u64,
    ) -> StoreResult<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(GET_BATCH) {
            out.extend(self.get_batch(chunk, max_size)?);
        }
        Ok(out)
    }

    /// Asks the daemon which objects the store holds, without reading them. A daemon without the
    /// `has` op fails as a daemon error that contains "unknown op".
    pub fn has(&self, ids: &[gix::ObjectId]) -> StoreResult<Vec<bool>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(HAS_BATCH) {
            let shas: Vec<String> = chunk.iter().map(|i| i.to_string()).collect();
            let v = self.call(json!({"op": "has", "shas": shas}))?;
            let present: Vec<bool> = v
                .get("present")
                .map(|p| serde_json::from_value(p.clone()))
                .transpose()
                .map_err(|e| StoreError::Protocol(e.to_string()))?
                .unwrap_or_default();
            if present.len() != chunk.len() {
                return Err(StoreError::Protocol("has answered another count".into()));
            }
            out.extend(present);
        }
        Ok(out)
    }

    fn get_batch(&self, ids: &[gix::ObjectId], max_size: u64) -> StoreResult<Vec<Option<Vec<u8>>>> {
        let shas: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
        let req = json!({"op": "getbatch", "shas": shas, "max_size": max_size});
        let v = match self.call(req) {
            Err(StoreError::Daemon(m)) if m.contains("unknown op") => {
                return ids.iter().map(|id| self.get_one(id)).collect();
            }
            Err(StoreError::Daemon(m)) if m.contains("too_large") => {
                if ids.len() == 1 {
                    return Ok(vec![None]);
                }
                let (a, b) = ids.split_at(ids.len() / 2);
                let mut out = self.get_batch(a, max_size)?;
                out.extend(self.get_batch(b, max_size)?);
                return Ok(out);
            }
            other => other?,
        };
        let datas = v.get("datas").and_then(Value::as_array);
        if datas.map(Vec::len) != Some(ids.len()) {
            return Err(StoreError::Protocol(
                "getbatch answered another count".into(),
            ));
        }
        let large: HashSet<u64> = v
            .get("large")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        let mut out = Vec::with_capacity(ids.len());
        for (i, (id, d)) in ids.iter().zip(datas.into_iter().flatten()).enumerate() {
            if large.contains(&(i as u64)) {
                out.push(None);
                continue;
            }
            let data = B64
                .decode(d.as_str().unwrap_or(""))
                .map_err(|e| StoreError::Protocol(e.to_string()))?;
            if hash_object(Kind::Blob, &data) != *id {
                return Err(StoreError::Protocol(format!(
                    "daemon sent wrong bytes for {id}"
                )));
            }
            crate::trace::count(1, data.len() as u64);
            out.push(Some(data));
        }
        Ok(out)
    }

    fn get_one(&self, id: &gix::ObjectId) -> StoreResult<Option<Vec<u8>>> {
        match self.get(id) {
            Ok((_, data)) => Ok(Some(data)),
            Err(StoreError::Daemon(m)) if m.contains("too_large") => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl ObjectStore for CtlClient {
    fn get(&self, id: &gix::ObjectId) -> StoreResult<(Kind, Vec<u8>)> {
        self.stats.lock().unwrap().gets += 1;
        let v = match self.call(json!({"op": "get", "sha": id.to_string()})) {
            Err(StoreError::Daemon(m)) if m.contains("not found") => {
                return Err(StoreError::NotFound(id.to_string()));
            }
            other => other?,
        };
        let kind = parse_kind(v.get("type").and_then(Value::as_str).unwrap_or(""))?;
        let data = v.get("data").and_then(Value::as_str).unwrap_or("");
        let data = B64
            .decode(data)
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        if hash_object(kind, &data) != *id {
            return Err(StoreError::Protocol(format!(
                "daemon sent wrong bytes for {id}"
            )));
        }
        crate::trace::count(1, data.len() as u64);
        Ok((kind, data))
    }

    fn put(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId> {
        let id = hash_object(kind, data);
        if self.known.lock().unwrap().contains(&id) {
            self.stats.lock().unwrap().put_skipped += 1;
            return Ok(id);
        }
        self.stats.lock().unwrap().puts += 1;
        let v =
            self.call(json!({"op": "put", "type": kind_name(kind), "data": B64.encode(data)}))?;
        let sha = v.get("sha").and_then(Value::as_str).unwrap_or("");
        if id.to_hex() != sha {
            return Err(StoreError::Protocol(format!(
                "daemon sha {sha} != local {id}"
            )));
        }
        self.known.lock().unwrap().insert(id);
        crate::trace::count(1, data.len() as u64);
        Ok(id)
    }

    fn put_many(&self, objects: &[(Kind, &[u8])]) -> StoreResult<Vec<gix::ObjectId>> {
        let ids: Vec<_> = objects.iter().map(|(k, d)| hash_object(*k, d)).collect();
        if self.no_batch.load(Ordering::Relaxed) {
            return objects.iter().map(|(k, d)| self.put(*k, d)).collect();
        }
        let mut seen = HashSet::new();
        let missing: Vec<usize> = {
            let known = self.known.lock().unwrap();
            (0..ids.len())
                .filter(|&i| !known.contains(&ids[i]) && seen.insert(ids[i]))
                .collect()
        };
        self.stats.lock().unwrap().put_skipped += (ids.len() - missing.len()) as u64;
        if missing.is_empty() {
            return Ok(ids);
        }
        let list: Vec<Value> = missing
            .iter()
            .map(|&i| json!({"type": kind_name(objects[i].0), "data": B64.encode(objects[i].1)}))
            .collect();
        let v = match self.call(json!({"op": "putbatch", "objects": list})) {
            Err(StoreError::Daemon(m)) if m.contains("unknown op") => {
                self.no_batch.store(true, Ordering::Relaxed);
                return objects.iter().map(|(k, d)| self.put(*k, d)).collect();
            }
            other => other?,
        };
        let shas: Vec<&str> = v
            .get("shas")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if shas.len() != missing.len()
            || missing
                .iter()
                .zip(&shas)
                .any(|(&i, s)| ids[i].to_hex() != *s)
        {
            return Err(StoreError::Protocol(
                "putbatch shas differ from local hashes".into(),
            ));
        }
        {
            let mut stats = self.stats.lock().unwrap();
            stats.puts += missing.len() as u64;
            stats.batches += 1;
        }
        self.known
            .lock()
            .unwrap()
            .extend(missing.iter().map(|&i| ids[i]));
        crate::trace::count(
            missing.len() as u64,
            missing.iter().map(|&i| objects[i].1.len() as u64).sum(),
        );
        Ok(ids)
    }
}
