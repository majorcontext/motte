//! Client for the `motte-vfsd` control socket (one JSON object per line) and the object-store
//! abstraction the native backend writes git objects through.

mod wire;

use std::collections::{BTreeMap, HashSet};
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
use serde::de::DeserializeOwned;
use serde_json::Value;

pub use wire::{Code, MAX_OBJECT_BYTES, OpHeadsReply, Request, WireObject};

pub const HASH_KIND: gix::hash::Kind = gix::hash::Kind::Sha1;

/// The file in the store directory of a repo that names the control socket.
pub const SOCK_FILE: &str = "ctl_sock";

/// The control socket of a workspace: `MOTTE_VFS_SOCK`, else the path in the file `ctl_sock` of
/// the store directory.
pub fn socket_path(store_dir: &Path) -> std::io::Result<PathBuf> {
    if let Some(sock) = std::env::var_os("MOTTE_VFS_SOCK") {
        return Ok(sock.into());
    }
    let named = std::fs::read(store_dir.join(SOCK_FILE))?;
    Ok(String::from_utf8_lossy(&named).into_owned().into())
}

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
    /// The answer to a request that must not be sent twice never came, so the request may have
    /// taken effect.
    #[error("control socket {path}: no answer to a request that may have taken effect: {source}")]
    AnswerLost {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon error {}: {message}", .code.as_str())]
    Daemon { code: Code, message: String },
    /// A failure on the side of the shim, such as an unreadable local file.
    #[error("{0}")]
    Local(String),
    #[error("bad daemon response: {0}")]
    Protocol(String),
}

impl StoreError {
    /// The code of an error answer of the daemon.
    pub fn code(&self) -> Option<&Code> {
        match self {
            Self::Daemon { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn is(&self, code: &Code) -> bool {
        self.code() == Some(code)
    }
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

/// Raw git object storage (payloads without the `<type> <len>\0` header).
pub trait ObjectStore: Send + Sync + Debug {
    fn get(&self, id: &gix::ObjectId) -> StoreResult<(Kind, Vec<u8>)>;
    fn put(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId>;
    /// Hands an object over to be stored before the next `flush_deferred`; the default stores it now.
    fn put_deferred(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId> {
        self.put(kind, data)
    }
    /// Stores the objects that `put_deferred` took.
    fn flush_deferred(&self) -> StoreResult<()> {
        Ok(())
    }
    /// Stores several objects; a daemon that supports it takes them in one round trip.
    fn put_many(&self, objects: &[(Kind, &[u8])]) -> StoreResult<Vec<gix::ObjectId>> {
        objects.iter().map(|(k, d)| self.put(*k, d)).collect()
    }
}

/// The op heads that the last successful `opheads` call of this process saw for each socket, and
/// the read that `prefetch_opheads` made ahead of the swap.
type Seen = Mutex<BTreeMap<PathBuf, (i64, Vec<String>)>>;
static LAST_OPHEADS: Seen = Mutex::new(BTreeMap::new());
static PREFETCHED: Seen = Mutex::new(BTreeMap::new());

/// The objects that `put_deferred` holds for a daemon socket until `flush_deferred` sends them in
/// one request. A snapshot writes its blobs, trees and commit this way, and the op store flushes
/// before it writes the operation, so no local operation names an object that the daemon lacks.
///
/// The completion steps that `after_flush` registers live in the same value as the objects, and a
/// flush takes both under one lock. A step therefore runs only after the send of the batch that
/// holds the objects it was registered after, whichever thread buffers more objects meanwhile. A
/// failed send puts the objects and the steps back. One flush at a time for the process: a flush
/// that finds nothing to send must not run a step while another flush still uploads its objects.
type Held = Vec<(gix::ObjectId, Kind, Vec<u8>)>;
type Step = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Batch {
    held: Held,
    steps: Vec<Step>,
}

static DEFERRED: Mutex<BTreeMap<PathBuf, Batch>> = Mutex::new(BTreeMap::new());
static FLUSHING: Mutex<()> = Mutex::new(());

/// Drops what every socket defers, unsent. Only a caller that never lets a stored object name one
/// of them may call it.
pub fn discard_deferred() {
    DEFERRED.lock().unwrap().clear();
}

/// Sends what every socket still defers. A command runs it when it ends, so that no object that it
/// wrote dies with the process when the command wrote no operation, which is what sends them
/// otherwise. A failure is a warning, because the command has already reported its outcome.
pub fn flush_all_deferred() {
    let paths: Vec<PathBuf> = DEFERRED.lock().unwrap().keys().cloned().collect();
    for path in paths {
        if let Err(e) = CtlClient::new(&path).flush_deferred() {
            eprintln!("warning: cannot store the objects that this command wrote: {e}");
        }
    }
}

/// The most bytes and objects that one socket defers; past it an object goes to the daemon now.
const DEFER_BYTES: usize = 4 << 20;
const DEFER_OBJECTS: usize = 1024;

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

fn too_large(len: usize) -> StoreError {
    StoreError::Local(format!(
        "an object of {len} bytes passes the limit of {MAX_OBJECT_BYTES} bytes (16 MiB)"
    ))
}

fn decode(data: &str) -> StoreResult<Vec<u8>> {
    B64.decode(data)
        .map_err(|e| StoreError::Protocol(e.to_string()))
}

struct Conn {
    w: UnixStream,
    r: BufReader<UnixStream>,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Conn")
    }
}

/// Client for the daemon control socket.
#[derive(Debug)]
pub struct CtlClient {
    path: PathBuf,
    idle: Mutex<Vec<Conn>>,
    known: Mutex<HashSet<gix::ObjectId>>,
    no_batch: AtomicBool,
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
            no_batch: AtomicBool::new(false),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn io_err(&self, source: std::io::Error) -> StoreError {
        StoreError::Io {
            path: self.path.display().to_string(),
            source,
        }
    }

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

    /// Sends one request and returns the answer object, an error answer too. After a lost answer
    /// it sends an idempotent request once more, and fails a request that may have taken effect
    /// with `AnswerLost`. Connections are pooled so that concurrent callers do not serialize on
    /// one socket.
    fn exchange(&self, req: &Request) -> StoreResult<Value> {
        let line = wire::Line {
            request: req,
            traceparent: crate::trace::traceparent(),
        };
        let mut line = serde_json::to_vec(&line).expect("a request encodes");
        line.push(b'\n');
        let tries = if req.is_idempotent() { 2 } else { 1 };
        let mut last = None;
        for _ in 0..tries {
            let pooled = self.idle.lock().unwrap().pop();
            let mut conn = match pooled {
                Some(c) => c,
                None => self.connect()?,
            };
            match self.call_once(&mut conn, &line) {
                Ok(v) => {
                    self.idle.lock().unwrap().push(conn);
                    return Ok(v);
                }
                Err(StoreError::Io { path, source }) => {
                    self.idle.lock().unwrap().clear();
                    last = Some((path, source));
                }
                Err(e) => return Err(e),
            }
        }
        let (path, source) = last.expect("one try ran");
        Err(if req.is_idempotent() {
            StoreError::Io { path, source }
        } else {
            StoreError::AnswerLost { path, source }
        })
    }

    /// The failure that an error answer stands for.
    fn answered(v: &Value) -> Option<StoreError> {
        let message = v.get("err").and_then(Value::as_str)?;
        let code = v.get("code").and_then(Value::as_str);
        Some(StoreError::Daemon {
            code: Code::of_answer(code, message),
            message: message.to_owned(),
        })
    }

    /// Sends a request and returns the answer, or the error that an error answer stands for.
    pub fn call(&self, req: &Request) -> StoreResult<Value> {
        let v = self.exchange(req)?;
        match Self::answered(&v) {
            Some(e) => Err(e),
            None => Ok(v),
        }
    }

    fn call_as<T: DeserializeOwned>(&self, req: &Request) -> StoreResult<T> {
        serde_json::from_value(self.call(req)?).map_err(|e| StoreError::Protocol(e.to_string()))
    }

    /// The dirty set: every path where the upper directory differs from the base tree, with the
    /// change token of the walk that listed it.
    pub fn dirty(&self) -> StoreResult<DirtyReply> {
        let reply: DirtyReply = self.call_as(&Request::Dirty).map_err(not_overlay)?;
        if reply.token.is_empty() {
            return Err(StoreError::Protocol(NOT_OVERLAY.into()));
        }
        Ok(reply)
    }

    /// The change token of the upper directory.
    pub fn token(&self) -> StoreResult<TokenReply> {
        self.call_as(&Request::Token).map_err(not_overlay)
    }

    /// Reads the remote op heads, or swaps them when `swap` names the expected generation and the
    /// new heads. Answers the generation and the heads. A swap whose answer is lost is judged by
    /// a read: it landed when the generation moved on to exactly these heads, it is sent again
    /// when the generation is still `expect`, and it lost otherwise.
    pub fn opheads(&self, swap: Option<(i64, &[String])>) -> StoreResult<(i64, Vec<String>)> {
        let req = Request::OpHeads {
            expect: swap.map(|(e, _)| e),
            heads: swap.map(|(_, h)| h.to_vec()),
        };
        let reply: OpHeadsReply = match self.call_as(&req) {
            Err(StoreError::AnswerLost { .. }) => {
                let (expect, heads) = swap.expect("only a swap loses its answer");
                let now: OpHeadsReply = self.call_as(&Request::OpHeads {
                    expect: None,
                    heads: None,
                })?;
                if now.generation == expect {
                    self.call_as(&req)?
                } else if now.heads == heads {
                    now
                } else {
                    return Err(StoreError::Daemon {
                        code: Code::GenerationConflict,
                        message: format!("the op heads moved to generation {}", now.generation),
                    });
                }
            }
            other => other?,
        };
        LAST_OPHEADS
            .lock()
            .unwrap()
            .insert(self.path.clone(), (reply.generation, reply.heads.clone()));
        Ok((reply.generation, reply.heads))
    }

    /// Runs `step` once the batch that holds every object deferred so far is stored; a failed send
    /// runs nothing and keeps the step for the next flush.
    pub fn after_flush(&self, step: impl FnOnce() + Send + 'static) {
        DEFERRED
            .lock()
            .unwrap()
            .entry(self.path.clone())
            .or_default()
            .steps
            .push(Box::new(step));
    }

    /// The generation and heads that this process last read or swapped, if any.
    pub fn last_opheads(&self) -> Option<(i64, Vec<String>)> {
        LAST_OPHEADS.lock().unwrap().get(&self.path).cloned()
    }

    /// Reads the remote op heads on a thread, so that the swap of the next op-heads update finds
    /// them. A stale read only costs the swap its retry: the swap is a compare-and-swap.
    pub fn prefetch_opheads(&self) {
        let c = CtlClient::new(self.path.clone());
        std::thread::spawn(move || {
            if let Ok(seen) = c.opheads(None) {
                PREFETCHED.lock().unwrap().insert(c.path.clone(), seen);
            }
        });
    }

    /// The read of `prefetch_opheads`, once.
    pub fn take_prefetched_opheads(&self) -> Option<(i64, Vec<String>)> {
        PREFETCHED.lock().unwrap().remove(&self.path)
    }

    /// Records the head operation and the mount commit of the workspace with the control plane. A
    /// daemon that answers `unknown_op` has no control plane to tell, and the answer is `false`;
    /// `true` means the control plane took it. `generation` is the op-heads generation of the swap
    /// that set `op`, which orders the reports.
    pub fn set_workspace(&self, op: &str, commit: &str, generation: i64) -> StoreResult<bool> {
        let req = Request::Workspace {
            sha: op.to_owned(),
            commit: commit.to_owned(),
            generation,
        };
        match self.call(&req) {
            Ok(_) => Ok(true),
            Err(e) if e.is(&Code::UnknownOp) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Lists the refs of the repo of the workspace.
    pub fn refs(&self) -> StoreResult<Vec<RefInfo>> {
        Ok(self
            .call_as::<wire::RefsReply>(&Request::Refs { name: None })?
            .refs)
    }

    /// Reads one ref; an absent ref fails with the code `vcs_not_found`.
    pub fn get_ref(&self, name: &str) -> StoreResult<RefInfo> {
        let req = Request::Refs {
            name: Some(name.to_owned()),
        };
        Ok(self.call_as::<wire::RefReply>(&req)?.stored)
    }

    /// Swaps a ref when its stored generation is `expect` (0 creates it), or deletes it with
    /// `delete`. A lost race is `RefSwap::Conflict` with the stored ref, so the caller retries
    /// from the winner. A swap whose answer is lost is judged by reading the ref.
    pub fn swap_ref(
        &self,
        name: &str,
        expect: i64,
        commit: &str,
        delete: bool,
    ) -> StoreResult<RefSwap> {
        let req = Request::SwapRef {
            name: name.to_owned(),
            expect,
            commit: (!delete).then(|| commit.to_owned()),
            delete,
        };
        match self.swap_once(&req) {
            Err(StoreError::AnswerLost { .. }) => {}
            other => return other,
        }
        let stored = match self.get_ref(name) {
            Ok(r) => Some(r),
            Err(e) if e.is(&Code::NotFound) => None,
            Err(e) => return Err(e),
        };
        match &stored {
            None if delete => Ok(RefSwap::Swapped(None)),
            None if expect > 0 => Ok(RefSwap::Conflict(None)),
            Some(r) if !delete && r.commit == commit && r.generation > expect => {
                Ok(RefSwap::Swapped(stored))
            }
            Some(r) if r.generation != expect => Ok(RefSwap::Conflict(stored)),
            _ => self.swap_once(&req),
        }
    }

    fn swap_once(&self, req: &Request) -> StoreResult<RefSwap> {
        let v = self.exchange(req)?;
        let Some(e) = Self::answered(&v) else {
            let delete = matches!(req, Request::SwapRef { delete: true, .. });
            return Ok(RefSwap::Swapped(if delete {
                None
            } else {
                Some(parse::<wire::RefReply>(v)?.stored)
            }));
        };
        if !e.is(&Code::GenerationConflict) {
            return Err(e);
        }
        let stored = v
            .get("ref")
            .map(|r| serde_json::from_value(r.clone()))
            .transpose()
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        Ok(RefSwap::Conflict(stored))
    }

    /// Stores one object of the op log of the workspace; `kind` is "operations" or "views".
    pub fn putop(&self, kind: &str, id: &str, data: &[u8]) -> StoreResult<()> {
        self.call(&Request::PutOp {
            kind: kind.to_owned(),
            sha: id.to_owned(),
            data: B64.encode(data),
        })
        .map(|_| ())
    }

    /// Reads one object of the op log. The caller checks that the bytes belong to `id`.
    pub fn getop(&self, kind: &str, id: &str) -> StoreResult<Vec<u8>> {
        let req = Request::GetOp {
            kind: kind.to_owned(),
            sha: id.to_owned(),
        };
        decode(&self.call_as::<wire::Data>(&req)?.data)
    }

    /// The files of the view that differ from the daemon's base tree, read without a snapshot.
    pub fn changes(&self) -> StoreResult<ChangesReply> {
        self.call_as(&Request::Changes)
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
    /// `has` op fails with the code `unknown_op`.
    pub fn has(&self, ids: &[gix::ObjectId]) -> StoreResult<Vec<bool>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(HAS_BATCH) {
            let shas = chunk.iter().map(ToString::to_string).collect();
            let present = self
                .call_as::<wire::Present>(&Request::Has { shas })?
                .present;
            if present.len() != chunk.len() {
                return Err(StoreError::Protocol("has answered another count".into()));
            }
            out.extend(present);
        }
        Ok(out)
    }

    fn get_batch(&self, ids: &[gix::ObjectId], max_size: u64) -> StoreResult<Vec<Option<Vec<u8>>>> {
        let req = Request::GetBatch {
            shas: ids.iter().map(ToString::to_string).collect(),
            max_size: Some(max_size),
        };
        let reply: wire::Datas = match self.call_as(&req) {
            Err(e) if e.is(&Code::UnknownOp) => {
                return ids.iter().map(|id| self.get_one(id)).collect();
            }
            Err(e) if e.is(&Code::ObjectTooLarge) => {
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
        if reply.datas.len() != ids.len() {
            return Err(StoreError::Protocol(
                "getbatch answered another count".into(),
            ));
        }
        let mut out = Vec::with_capacity(ids.len());
        for (i, (id, d)) in ids.iter().zip(&reply.datas).enumerate() {
            if reply.large.contains(&(i as u64)) {
                out.push(None);
                continue;
            }
            let data = decode(d)?;
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
            Err(e) if e.is(&Code::ObjectTooLarge) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn parse<T: DeserializeOwned>(v: Value) -> StoreResult<T> {
    serde_json::from_value(v).map_err(|e| StoreError::Protocol(e.to_string()))
}

/// A daemon that cannot answer `dirty` or `token` has no upper directory.
fn not_overlay(e: StoreError) -> StoreError {
    if e.is(&Code::UnknownOp) {
        StoreError::Protocol(NOT_OVERLAY.into())
    } else {
        e
    }
}

impl ObjectStore for CtlClient {
    fn put_deferred(&self, kind: Kind, data: &[u8]) -> StoreResult<gix::ObjectId> {
        let id = hash_object(kind, data);
        if data.len() as u64 > MAX_OBJECT_BYTES {
            return Err(too_large(data.len()));
        }
        if self.no_batch.load(Ordering::Relaxed) || self.known.lock().unwrap().contains(&id) {
            return self.put(kind, data);
        }
        let mut all = DEFERRED.lock().unwrap();
        let held = &mut all.entry(self.path.clone()).or_default().held;
        let bytes: usize = held.iter().map(|(_, _, d)| d.len()).sum();
        if held.len() >= DEFER_OBJECTS || bytes + data.len() > DEFER_BYTES {
            drop(all);
            self.flush_deferred()?;
            return self.put(kind, data);
        }
        if !held.iter().any(|(h, _, _)| *h == id) {
            held.push((id, kind, data.to_vec()));
        }
        Ok(id)
    }

    fn flush_deferred(&self) -> StoreResult<()> {
        let _one_at_a_time = FLUSHING.lock().unwrap_or_else(|e| e.into_inner());
        let Batch { held, steps } = DEFERRED
            .lock()
            .unwrap()
            .remove(&self.path)
            .unwrap_or_default();
        if !held.is_empty() {
            let objects: Vec<(Kind, &[u8])> =
                held.iter().map(|(_, k, d)| (*k, d.as_slice())).collect();
            if let Err(e) = self.put_many(&objects) {
                let mut all = DEFERRED.lock().unwrap();
                let back = all.entry(self.path.clone()).or_default();
                back.held.extend(held);
                back.steps.extend(steps);
                return Err(e);
            }
        }
        steps.into_iter().for_each(|step| step());
        Ok(())
    }

    fn get(&self, id: &gix::ObjectId) -> StoreResult<(Kind, Vec<u8>)> {
        let held = DEFERRED.lock().unwrap().get(&self.path).and_then(|b| {
            b.held
                .iter()
                .find(|(i, _, _)| i == id)
                .map(|(_, k, d)| (*k, d.clone()))
        });
        if let Some(found) = held {
            return Ok(found);
        }
        let req = Request::Get {
            sha: id.to_string(),
        };
        let reply: wire::TypeData = match self.call_as(&req) {
            Err(e) if e.is(&Code::NotFound) => return Err(StoreError::NotFound(id.to_string())),
            other => other?,
        };
        let kind = parse_kind(&reply.kind)?;
        let data = decode(&reply.data)?;
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
            return Ok(id);
        }
        if data.len() as u64 > MAX_OBJECT_BYTES {
            return Err(too_large(data.len()));
        }
        let req = Request::Put {
            kind: kind_name(kind).to_owned(),
            data: B64.encode(data),
        };
        let sha = self.call_as::<wire::Sha>(&req)?.sha;
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
        if missing.is_empty() {
            return Ok(ids);
        }
        if let Some(&i) = missing
            .iter()
            .find(|&&i| objects[i].1.len() as u64 > MAX_OBJECT_BYTES)
        {
            return Err(too_large(objects[i].1.len()));
        }
        let list = missing
            .iter()
            .map(|&i| WireObject {
                kind: kind_name(objects[i].0).to_owned(),
                data: B64.encode(objects[i].1),
            })
            .collect();
        let reply = match self.call_as::<wire::Shas>(&Request::PutBatch { objects: list }) {
            Err(e) if e.is(&Code::UnknownOp) => {
                self.no_batch.store(true, Ordering::Relaxed);
                return objects.iter().map(|(k, d)| self.put(*k, d)).collect();
            }
            other => other?,
        };
        let shas = reply.shas;
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
