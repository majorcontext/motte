//! Op log stores: jj's simple file-based op store and op-heads store registered under motte names.
//! The local op heads are the truth of the writer; each update is mirrored to the remote op heads
//! through the daemon's compare-and-swap.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use jj_lib::backend::{BackendInitError, CommitId};
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::{
    OpStore, OpStoreResult, Operation, OperationId, RootOperationData, View, ViewId,
};
use jj_lib::repo::StoreFactories;
use jj_lib::simple_op_heads_store::SimpleOpHeadsStore;
use jj_lib::simple_op_store::SimpleOpStore;

use crate::backend::{BACKEND_NAME, NativeBackend};
use crate::ctl::{Code, CtlClient, StoreError, socket_path};

pub const OP_STORE_NAME: &str = "motte-op-store";
pub const OP_HEADS_NAME: &str = "motte-op-heads";

#[derive(Debug)]
pub struct MotteOpStore(SimpleOpStore);

impl MotteOpStore {
    pub fn init(path: &Path, root: RootOperationData) -> Result<Self, BackendInitError> {
        Ok(Self(SimpleOpStore::init(path, root)?))
    }
    pub fn load(path: &Path, root: RootOperationData) -> Self {
        Self(SimpleOpStore::load(path, root))
    }
}

#[async_trait]
impl OpStore for MotteOpStore {
    fn name(&self) -> &str {
        OP_STORE_NAME
    }
    fn root_operation_id(&self) -> &OperationId {
        self.0.root_operation_id()
    }
    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        self.0.read_view(id).await
    }
    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        self.0.write_view(contents).await
    }
    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        self.0.read_operation(id).await
    }
    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        self.0.write_operation(contents).await
    }
    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        self.0.resolve_operation_id_prefix(prefix).await
    }
    async fn gc(&self, head_ids: &[OperationId], keep_newer: SystemTime) -> OpStoreResult<()> {
        self.0.gc(head_ids, keep_newer).await
    }
}

#[derive(Debug)]
pub struct MotteOpHeadsStore {
    local: SimpleOpHeadsStore,
    dir: PathBuf,
}

impl MotteOpHeadsStore {
    pub fn init(dir: &Path, root_op_id: &OperationId) -> Result<Self, BackendInitError> {
        Ok(Self {
            local: SimpleOpHeadsStore::init(dir, root_op_id)?,
            dir: dir.to_owned(),
        })
    }
    pub fn load(dir: &Path) -> Self {
        Self {
            local: SimpleOpHeadsStore::load(dir),
            dir: dir.to_owned(),
        }
    }

    /// The daemon socket. Without one the update goes on with the local heads only and marks
    /// nothing as unsynced.
    fn ctl(&self) -> Option<CtlClient> {
        match socket_path(&self.dir.parent()?.join("store")) {
            Ok(sock) => Some(CtlClient::new(sock)),
            Err(e) => {
                eprintln!("warning: no daemon socket for the remote op heads: {e}");
                None
            }
        }
    }

    fn repo_dir(&self) -> Option<&Path> {
        self.dir.parent()
    }
}

/// Edits the set of unsynced heads in the repo dir: drops `remove`, adds `add`, and deletes the
/// file when the set is empty. The marker lock serializes the edit against jj op abandon and jj op
/// integrate, which skip the op-heads lock. A failure is a warning, never a command failure.
pub fn update_unsynced(repo: &Path, remove: &[String], add: Option<&str>) {
    let _lock = marker_lock(repo);
    edit_marker(repo, |h| remove.iter().any(|r| r == h), add);
}

/// Drops from the unsynced heads the ones that a drain swapped in and the ones that are not
/// local heads, and keeps the rest. It reads the local heads and the marks under one hold of the
/// marker lock: a writer creates its head before it marks it under that lock, so every mark it
/// reads names a head that existed before the read of the local heads.
pub fn clear_drained(repo: &Path, swapped: &[String]) {
    let _lock = marker_lock(repo);
    let Ok(local) = read_heads(repo) else { return };
    let covered = |h: &str| swapped.iter().any(|s| s == h) || !local.iter().any(|l| l == h);
    edit_marker(repo, covered, None);
}

/// Rewrites the marker without the heads that `drop` names and with `add`. The caller holds the
/// marker lock.
fn edit_marker(repo: &Path, drop: impl Fn(&str) -> bool, add: Option<&str>) {
    let path = repo.join(UNSYNCED_FILE);
    let mut set: Vec<String> = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .lines()
        .filter(|h| !h.is_empty() && !drop(h))
        .map(str::to_owned)
        .collect();
    if let Some(h) = add
        && !set.iter().any(|s| s == h)
    {
        set.push(h.to_owned());
    }
    if set.is_empty() {
        remove_marker(repo);
    } else if let Err(e) = std::fs::write(&path, set.join("\n") + "\n") {
        eprintln!("warning: cannot mark the op log unsynced: {e}");
    }
}

/// Holds the advisory lock that serializes every edit of the marker, never across a daemon call.
/// It comes after the op-heads lock, and the lock file stays. Without it the edit goes on, because
/// a marker failure never fails a command.
fn marker_lock(repo: &Path) -> Option<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(repo.join(MARKER_LOCK_FILE))
        .and_then(|f| f.lock().map(|()| f));
    file.map_err(|e| eprintln!("warning: cannot lock the unsynced mark: {e}"))
        .ok()
}

/// Deletes the set of unsynced heads of the repo dir.
pub fn clear_unsynced(repo: &Path) {
    let _lock = marker_lock(repo);
    remove_marker(repo);
}

fn remove_marker(repo: &Path) {
    match std::fs::remove_file(repo.join(UNSYNCED_FILE)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            eprintln!("warning: cannot clear the unsynced mark: {e}");
        }
        _ => {}
    }
}

#[async_trait]
impl OpHeadsStore for MotteOpHeadsStore {
    fn name(&self) -> &str {
        OP_HEADS_NAME
    }
    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let _op = crate::trace::span("jj.op_heads.update");
        let write_err = |e: StoreError| OpHeadsStoreError::Write {
            new_op_id: new_id.clone(),
            source: e.into(),
        };
        let deadline = Instant::now() + MIRROR_BUDGET;
        let ctl = self.ctl().map(Arc::new);
        let mut behind = false;
        let remote = match &ctl {
            Some(c) => {
                let c = c.clone();
                match within(deadline, move || {
                    c.take_prefetched_opheads()
                        .map_or_else(|| c.opheads(None), Ok)
                })
                .map_err(write_err)?
                {
                    Mirrored::Done(r) => Some(r),
                    Mirrored::Behind => {
                        behind = true;
                        None
                    }
                    Mirrored::Unsupported => None,
                }
            }
            None => None,
        };
        self.local.update_op_heads(old_ids, new_id).await?;
        let new = new_id.hex();
        let mut superseded: Vec<String> = old_ids.iter().map(|id| id.hex()).collect();
        if let (Some(ctl), Some(remote)) = (ctl, remote) {
            let local: Vec<String> = self
                .get_op_heads()
                .await?
                .iter()
                .map(|id| id.hex())
                .collect();
            let ops = self
                .repo_dir()
                .map(|p| p.join("op_store").join("operations"));
            let mut stale = superseded.clone();
            stale.extend(stale_heads(&remote.1, &local, ops.as_deref()));
            superseded.clone_from(&stale);
            let heads = [new.clone()];
            let mirrored = within(deadline, move || {
                mirror(&ctl, remote, &stale, &heads, deadline)
            })
            .map_err(write_err)?;
            match mirrored {
                Mirrored::Done(()) => {
                    superseded.push(new.clone());
                    if let Some(repo) = self.repo_dir() {
                        update_unsynced(repo, &superseded, None);
                    }
                }
                Mirrored::Behind => behind = true,
                Mirrored::Unsupported => {}
            }
        }
        if behind && let Some(repo) = self.repo_dir() {
            update_unsynced(repo, &superseded, Some(&new));
        }
        Ok(())
    }
    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        self.local.get_op_heads().await
    }
    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        self.local.lock().await
    }
}

/// The local heads that the remote op heads may lack, one hex ID per line, in the repo dir beside
/// `op_store`. A failed update adds its head, a completed one removes the heads it superseded, and
/// a flush or a drain deletes the file.
pub const UNSYNCED_FILE: &str = "motte_unsynced";

const MARKER_LOCK_FILE: &str = "motte_unsynced.lock";

/// The time an update may spend on the remote op heads; past it the remote stays behind.
pub const MIRROR_BUDGET: Duration = Duration::from_secs(2);

pub enum Mirrored<T> {
    Done(T),
    /// The call failed or ran out of time, so the remote op heads may lag.
    Behind,
    /// The daemon answers `unknown_op`, so it has no op heads to drain.
    Unsupported,
}

/// Runs `call` on a thread and waits until `deadline`, because a daemon call has no timeout of
/// its own. A call that answers nothing in time, or fails other than with a lost claim, is
/// `Behind`; the abandoned thread ends with the command.
pub fn within<T: Send + 'static>(
    deadline: Instant,
    call: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<Mirrored<T>, StoreError> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(call());
    });
    match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(r) => settle(r),
        Err(_) => unsynced(StoreError::Local("the op heads budget ran out".into())),
    }
}

/// Fails once `deadline` has passed. A worker that `within` stopped waiting for calls it before
/// every daemon call.
fn in_budget(deadline: Instant) -> Result<(), StoreError> {
    if Instant::now() >= deadline {
        return Err(StoreError::Local("the op heads budget ran out".into()));
    }
    Ok(())
}

/// Applies the update to the remote op heads, starting from the read `remote`: it drops `old`,
/// which holds the replaced operations and the remote heads that this op log superseded, and adds
/// the heads `new`. A writer that loses the race reads the winner's heads and applies its update again.
/// Every daemon call checks the deadline first, and the swap checks it after the read: a worker
/// that `within` abandoned has not read the heads of a writer that took the lock after it, so it
/// never swaps the remote back. A call already sent can still finish.
pub fn mirror(
    ctl: &CtlClient,
    mut remote: (i64, Vec<String>),
    old: &[String],
    new: &[String],
    deadline: Instant,
) -> Result<(), StoreError> {
    loop {
        let (generation, current) = remote;
        let heads = merged(current, old, new);
        in_budget(deadline)?;
        match ctl.opheads(Some((generation, &heads))) {
            Ok(_) => return Ok(()),
            Err(e) if e.is(&Code::GenerationConflict) => {}
            Err(e) => return Err(e),
        }
        in_budget(deadline)?;
        remote = ctl.opheads(None)?;
    }
}

/// The remote heads that this op log superseded: operations that its store holds and that are not
/// local heads. Every other remote head, such as another op log's, stays.
fn stale_heads(remote: &[String], local: &[String], ops: Option<&Path>) -> Vec<String> {
    remote
        .iter()
        .filter(|h| !local.contains(h) && ops.is_some_and(|d| d.join(h.as_str()).exists()))
        .cloned()
        .collect()
}

/// The remote heads without `old` and `new`, then `new`.
fn merged(current: Vec<String>, old: &[String], new: &[String]) -> Vec<String> {
    let mut heads: Vec<String> = current
        .into_iter()
        .filter(|h| !old.contains(h) && !new.contains(h))
        .collect();
    heads.extend_from_slice(new);
    heads
}

/// The local op heads of the repo dir, sorted, read as `get_op_heads` does.
pub fn read_heads(repo: &Path) -> Result<Vec<String>, StoreError> {
    let unreadable =
        |e: std::io::Error| StoreError::Local(format!("cannot read the local op heads: {e}"));
    let mut heads = Vec::new();
    for entry in std::fs::read_dir(repo.join("op_heads").join("heads")).map_err(unreadable)? {
        let name = entry.map_err(unreadable)?.file_name();
        if let Some(n) = name
            .to_str()
            .filter(|n| n.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            heads.push(n.to_owned());
        }
    }
    if heads.is_empty() {
        return Err(StoreError::Local("the op log has no local head".into()));
    }
    heads.sort();
    Ok(heads)
}

/// Makes the remote op heads show every local head, by the rule of an update, and keeps the remote
/// heads of other op logs, and returns the local heads that it swapped in. `local` lists the local
/// heads. It is read again after the remote read and after a lost swap, because a writer that
/// skips the op-heads lock can move the local heads meanwhile: a swap then only follows a remote
/// read that a final local list matched. A local update after that read changes the generation
/// before its mirror, so it either fails this swap or runs after it, and neither loses a head.
/// `upload` stores the operations of each list before the loop reads the remote for it, so the
/// list of a swap is always uploaded.
pub fn merge_remote(
    ctl: &CtlClient,
    repo: &Path,
    mut local: impl FnMut() -> Result<Vec<String>, StoreError>,
    mut upload: impl FnMut(&[String]) -> Result<(), StoreError>,
    deadline: Instant,
) -> Result<Vec<String>, StoreError> {
    let ops = repo.join("op_store").join("operations");
    let mut heads = local()?;
    loop {
        in_budget(deadline)?;
        upload(&heads)?;
        in_budget(deadline)?;
        let (generation, current) = ctl.opheads(None)?;
        let fresh = local()?;
        if fresh != heads {
            heads = fresh;
            continue;
        }
        let stale = stale_heads(&current, &heads, Some(&ops));
        let target = merged(current, &stale, &heads);
        in_budget(deadline)?;
        match ctl.opheads(Some((generation, &target))) {
            Ok(_) => return Ok(heads),
            Err(e) if e.is(&Code::GenerationConflict) => heads = local()?,
            Err(e) => return Err(e),
        }
    }
}

/// The ops of this op log that an earlier flush uploaded, one ID per line under the repo dir.
pub const FLUSHED_FILE: &str = "motte_flushed";

const PUT_PARALLEL: usize = 8;

/// Uploads the operations and views of `pending`, at most `PUT_PARALLEL` at once, then lists the operations in the flushed file.
/// With a `deadline`, it makes no daemon call after it.
pub fn put_ops(
    ctl: &CtlClient,
    repo_path: &Path,
    pending: &[(String, String)],
    deadline: Option<Instant>,
) -> Result<(), StoreError> {
    let op_store = repo_path.join("op_store");
    let mut sent = String::new();
    let mut jobs = Vec::new();
    for (op, view) in pending {
        for (kind, obj) in [("views", view), ("operations", op)] {
            let data = std::fs::read(op_store.join(kind).join(obj)).map_err(|e| {
                StoreError::Local(format!("cannot read {kind}/{obj} of the op log: {e}"))
            })?;
            jobs.push((kind, obj.as_str(), data));
        }
        sent.push_str(op);
        sent.push('\n');
    }
    for round in jobs.chunks(PUT_PARALLEL) {
        std::thread::scope(|scope| {
            let calls: Vec<_> = round
                .iter()
                .map(|(kind, obj, data)| {
                    scope.spawn(move || {
                        deadline.map_or(Ok(()), in_budget)?;
                        ctl.putop(kind, obj, data)
                    })
                })
                .collect();
            calls.into_iter().try_for_each(|c| {
                c.join()
                    .unwrap_or_else(|_| Err(StoreError::Local("an op upload panicked".into())))
            })
        })?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(repo_path.join(FLUSHED_FILE))
        .and_then(|mut f| std::io::Write::write_all(&mut f, sent.as_bytes()))
        .map_err(|e| StoreError::Local(format!("cannot list the uploaded operations: {e}")))
}

/// Stores the shallow roots in use at op log head `head` as a "views" object named after the head
/// (a view is named by a hash of its content, so the two never meet). A restore of that head reads
/// them back verbatim. Each id ends in a newline, so the body of an empty set is one newline and
/// the object is never empty.
pub fn put_shallow(ctl: &CtlClient, head: &str, roots: &[CommitId]) -> Result<(), StoreError> {
    let mut hex: Vec<String> = roots.iter().map(|r| r.hex()).collect();
    hex.sort();
    let body: String = if hex.is_empty() {
        "\n".to_owned()
    } else {
        hex.iter().map(|h| format!("{h}\n")).collect()
    };
    ctl.putop("views", head, body.as_bytes())
}

/// The roots that `put_shallow` stored for `head`; `None` when the head was flushed without them.
pub fn get_shallow(ctl: &CtlClient, head: &str) -> Result<Option<Vec<CommitId>>, StoreError> {
    match ctl.getop("views", head) {
        Ok(data) => Ok(Some(
            String::from_utf8_lossy(&data)
                .lines()
                .filter_map(|l| CommitId::try_from_hex(l.trim()))
                .collect(),
        )),
        Err(e) if e.is(&Code::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Sorts the outcome of a daemon call by `within`'s rule.
pub fn settle<T>(r: Result<T, StoreError>) -> Result<Mirrored<T>, StoreError> {
    match r {
        Ok(v) => Ok(Mirrored::Done(v)),
        Err(e) => unsynced(e),
    }
}

/// Fails on a lost claim. Any other failure leaves the remote op heads behind the local ones.
fn unsynced<T>(e: StoreError) -> Result<Mirrored<T>, StoreError> {
    match e {
        e if e.is(&Code::ClaimLost) => Err(e),
        e if e.is(&Code::UnknownOp) => Ok(Mirrored::Unsupported),
        e => {
            eprintln!("warning: the remote op heads were not updated: {e}");
            Ok(Mirrored::Behind)
        }
    }
}

/// Registers the motte store types so that `.jj/repo/*/type` files naming them can be loaded.
pub fn store_factories() -> StoreFactories {
    let mut f = StoreFactories::empty();
    f.add_backend(
        BACKEND_NAME,
        Box::new(|settings, store_path| Ok(Box::new(NativeBackend::load(settings, store_path)?))),
    );
    f.add_op_store(
        OP_STORE_NAME,
        Box::new(|_settings, store_path, root| Ok(Box::new(MotteOpStore::load(store_path, root)))),
    );
    f.add_op_heads_store(
        OP_HEADS_NAME,
        Box::new(|_settings, store_path| Ok(Box::new(MotteOpHeadsStore::load(store_path)))),
    );
    f
}
