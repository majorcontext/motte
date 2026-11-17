//! `VfsWorkingCopy`: a jj working copy on a kernel overlay over the `motte-vfsd` lazy FUSE lower layer.
//!
//! `snapshot` never walks the tree. It asks the daemon for the paths where the upper directory differs
//! from the base tree (after `syncfs` on the mount) and builds the new tree over exactly those paths.
//! `check_out` writes only the paths that differ between the old and new tree, through the mount.
//! Parts of the per-file logic follow jj-lib's `local_working_copy.rs` (Apache-2.0).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use async_trait::async_trait;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use jj_lib::backend::{
    CopyId, FileId, MergedTreeValue, MergedTreeValueExt as _, TreeId, TreeValue,
};
use jj_lib::commit::Commit;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::conflicts::{
    self, ConflictMarkerStyle, ConflictMaterializeOptions, MIN_CONFLICT_MARKER_LEN,
    MaterializedTreeValue, choose_materialized_conflict_marker_len,
    materialize_merge_result_to_bytes, materialize_tree_value,
};
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::local_working_copy::TreeStateSettings;
use jj_lib::lock::FileLock;
use jj_lib::matchers::{EverythingMatcher, PrefixMatcher};
use jj_lib::merge::Merge;
use jj_lib::merged_tree::{MergedTree, TreeDiffEntry};
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::{WorkspaceName, WorkspaceNameBuf};
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::{
    CheckoutError, CheckoutStats, LockedWorkingCopy, ResetError, SnapshotError, SnapshotOptions,
    SnapshotStats, UntrackedReason, WorkingCopy, WorkingCopyFactory, WorkingCopyStateError,
};
use serde::{Deserialize, Serialize};

use crate::ctl::CtlClient;

mod cache;
mod overlay;
use cache::{FileCache, Stamp};

pub const WC_NAME: &str = "motte-vfs";
const STATE_FILE: &str = "motte_wc.json";
const LOCK_FILE: &str = "working_copy.lock";

fn trace_on() -> bool {
    std::env::var_os("MOTTE_JJ_TRACE").is_some_and(|v| !v.is_empty())
}

fn trace_line(line: String) {
    let line = format!(
        "{} [{}]",
        line,
        std::env::args()
            .skip(1)
            .filter(|a| !a.starts_with("--"))
            .take(3)
            .collect::<Vec<_>>()
            .join(" ")
    );
    if let Some(path) = std::env::var_os("MOTTE_JJ_TRACE_FILE") {
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(f, "{line}");
        }
    } else {
        eprintln!("{line}");
    }
}

/// Set by `jj motte snapshot --force`: the snapshot reads every dirty file again.
pub static FORCE_FULL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn now_ns() -> i128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i128)
}

/// Dirty entries and bytes read by the latest snapshot of this process.
pub static LAST_SNAPSHOT: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

fn trace_line_if(line: String) {
    if trace_on() {
        trace_line(format!("[motte-vfs] {line}"));
    }
}

macro_rules! trace {
    ($($t:tt)*) => { if trace_on() { trace_line(format!("[motte-vfs] {}", format!($($t)*))); } };
}

/// Persistent state of the working copy (`.jj/working_copy/motte_wc.json`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StateFile {
    operation_id: String,
    workspace_name: String,
    tree_ids: Vec<String>,
    labels: Vec<String>,
    mount: PathBuf,
    sock: PathBuf,
    /// Marker length used when a conflicted path was written to the mount.
    conflict_marker_len: BTreeMap<String, u32>,
    /// Paths that a checkout could not write; the next snapshot settles them.
    #[serde(default)]
    unwritten: BTreeSet<String>,
}

/// How a new workspace finds its mount and daemon.
#[derive(Clone, Debug, Default)]
pub struct VfsConfig {
    pub mount: PathBuf,
    pub sock: PathBuf,
}

fn wc_err(
    message: &str,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> WorkingCopyStateError {
    WorkingCopyStateError {
        message: message.to_owned(),
        err: err.into(),
    }
}

fn load_tree(store: &Arc<Store>, st: &StateFile) -> Result<MergedTree, WorkingCopyStateError> {
    let ids: Vec<TreeId> = st
        .tree_ids
        .iter()
        .map(|h| TreeId::try_from_hex(h).ok_or_else(|| wc_err("bad tree id in state", h.clone())))
        .collect::<Result<_, _>>()?;
    if ids.is_empty() {
        return Ok(store.empty_merged_tree());
    }
    Ok(MergedTree::new(
        store.clone(),
        Merge::from_vec(ids),
        ConflictLabels::from_vec(st.labels.clone()),
    ))
}

fn store_tree(st: &mut StateFile, tree: &MergedTree) {
    st.tree_ids = tree.tree_ids().iter().map(|i| i.hex()).collect();
    st.labels = tree.labels().as_slice().to_vec();
}

fn read_state(state_path: &Path) -> Result<StateFile, WorkingCopyStateError> {
    let bytes = fs::read(state_path.join(STATE_FILE))
        .map_err(|e| wc_err("Failed to read motte working copy state", e))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| wc_err("Failed to parse motte working copy state", e))
}

fn write_state(state_path: &Path, st: &StateFile) -> Result<(), WorkingCopyStateError> {
    let mut tmp = tempfile::NamedTempFile::new_in(state_path)
        .map_err(|e| wc_err("Failed to write state", e))?;
    tmp.write_all(&serde_json::to_vec(st).unwrap())
        .map_err(|e| wc_err("Failed to write state", e))?;
    tmp.persist(state_path.join(STATE_FILE))
        .map_err(|e| wc_err("Failed to persist state", e.error))?;
    Ok(())
}

/// Without `syncfs` (not Linux) there is no daemon to flush for; tests only.
#[cfg(not(target_os = "linux"))]
pub fn syncfs(_mount: &Path) -> io::Result<()> {
    Ok(())
}

/// Flushes the mount so that every completed write is visible to the daemon.
#[cfg(target_os = "linux")]
pub fn syncfs(mount: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let f = fs::File::open(mount)?;
    // SAFETY: plain syscall on an open fd.
    let r = unsafe { libc::syncfs(f.as_raw_fd()) };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn conn_for(st: &StateFile) -> Arc<CtlClient> {
    let sock = std::env::var_os("MOTTE_VFS_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| st.sock.clone());
    Arc::new(CtlClient::new(sock))
}

pub struct VfsWorkingCopy {
    store: Arc<Store>,
    state_path: PathBuf,
    tree: MergedTree,
    op_id: OperationId,
    ws_name: WorkspaceNameBuf,
    marker_style: ConflictMarkerStyle,
}

impl VfsWorkingCopy {
    fn from_state(
        store: Arc<Store>,
        state_path: PathBuf,
        st: StateFile,
        settings: &UserSettings,
    ) -> Result<Self, WorkingCopyStateError> {
        let tree = load_tree(&store, &st)?;
        let marker_style = TreeStateSettings::try_from_user_settings(settings)
            .map_err(|e| wc_err("Failed to read the tree state settings", e))?
            .conflict_marker_style;
        Ok(Self {
            op_id: OperationId::try_from_hex(&st.operation_id)
                .ok_or_else(|| wc_err("bad operation id in state", st.operation_id.clone()))?,
            ws_name: st.workspace_name.clone().into(),
            store,
            state_path,
            tree,
            marker_style,
        })
    }
}

#[async_trait(?Send)]
impl WorkingCopy for VfsWorkingCopy {
    fn name(&self) -> &str {
        WC_NAME
    }

    fn workspace_name(&self) -> &WorkspaceName {
        &self.ws_name
    }

    fn operation_id(&self) -> &OperationId {
        &self.op_id
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        static ROOT: std::sync::OnceLock<Vec<RepoPathBuf>> = std::sync::OnceLock::new();
        Ok(ROOT.get_or_init(|| vec![RepoPathBuf::root()]))
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let lock = FileLock::lock(self.state_path.join(LOCK_FILE))
            .map_err(|e| wc_err("Failed to lock working copy", e))?;
        let st = read_state(&self.state_path)?;
        let tree = load_tree(&self.store, &st)?;
        let ctl = conn_for(&st);
        Ok(Box::new(LockedVfsWorkingCopy {
            store: self.store.clone(),
            state_path: self.state_path.clone(),
            old_operation_id: OperationId::try_from_hex(&st.operation_id)
                .ok_or_else(|| wc_err("bad operation id", st.operation_id.clone()))?,
            old_tree: tree.clone(),
            tree,
            st,
            ctl,
            marker_style: self.marker_style,
            dirty: false,
            new_workspace_name: None,
            _lock: lock,
        }))
    }
}

pub struct VfsWorkingCopyFactory {
    /// Set by `jj motte init`; loading reads the mount and socket from the state file.
    pub init: Option<VfsConfig>,
}

impl WorkingCopyFactory for VfsWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        _working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let cfg = self.init.clone().ok_or_else(|| {
            wc_err(
                "motte-vfs working copy must be created with `jj motte init`",
                "missing --mount/--sock",
            )
        })?;
        let mut st = StateFile {
            operation_id: operation_id.hex(),
            workspace_name: workspace_name.as_str().to_owned(),
            mount: cfg.mount,
            sock: cfg.sock,
            ..Default::default()
        };
        store_tree(&mut st, &store.empty_merged_tree());
        write_state(&state_path, &st)?;
        Ok(Box::new(VfsWorkingCopy::from_state(
            store, state_path, st, settings,
        )?))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        _working_copy_path: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let st = read_state(&state_path)?;
        Ok(Box::new(VfsWorkingCopy::from_state(
            store, state_path, st, settings,
        )?))
    }
}

pub struct LockedVfsWorkingCopy {
    store: Arc<Store>,
    state_path: PathBuf,
    st: StateFile,
    tree: MergedTree,
    old_tree: MergedTree,
    old_operation_id: OperationId,
    ctl: Arc<CtlClient>,
    marker_style: ConflictMarkerStyle,
    dirty: bool,
    new_workspace_name: Option<WorkspaceNameBuf>,
    _lock: FileLock,
}

fn snap_err(
    message: impl Into<String>,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> SnapshotError {
    SnapshotError::Other {
        message: message.into(),
        err: err.into(),
    }
}

fn co_err(
    message: impl Into<String>,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CheckoutError {
    CheckoutError::Other {
        message: message.into(),
        err: err.into(),
    }
}

struct Pending {
    rp: RepoPathBuf,
    disk: PathBuf,
    cur: MergedTreeValue,
    exec: bool,
    md: fs::Metadata,
    /// A blob that the store holds: the file of the last tree, or the blob of the cache.
    known: Option<gix::ObjectId>,
    ask_has: bool,
    under_cleared: bool,
}

fn blob_id(hex: &str) -> Option<gix::ObjectId> {
    gix::ObjectId::from_hex(hex.as_bytes()).ok()
}

fn stored_blob(cur: &MergedTreeValue) -> Option<gix::ObjectId> {
    match cur.as_resolved() {
        Some(Some(TreeValue::File { id, .. })) => {
            Some(gix::ObjectId::from_bytes_or_panic(id.as_bytes()))
        }
        _ => None,
    }
}

/// The tree of one snapshot with the blobs the shim read for it.
pub(crate) struct Built {
    pub(crate) tree: MergedTree,
    pub(crate) stats: SnapshotStats,
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    pub(crate) stamps: BTreeMap<String, Stamp>,
    pub(crate) timing: String,
}

type ReadPut = io::Result<(gix::ObjectId, Option<Vec<u8>>)>;

/// A file to read: its path, whether its bytes are kept, a blob the store holds already, and whether the
/// store is asked (`has`) about its blob before it is sent.
type Job = (PathBuf, bool, Option<gix::ObjectId>, bool);

const PUT_BATCH_FILES: usize = 64;
const PUT_BATCH_BYTES: usize = 8 << 20;

/// Reads each file and stores it as a blob, spread over worker threads with one daemon connection each.
/// A worker sends the blobs of up to `PUT_BATCH_FILES` files (or `PUT_BATCH_BYTES`) in one request.
fn read_and_put(sock: &Path, jobs: &[Job], workers: usize) -> Vec<ReadPut> {
    use crate::ctl::ObjectStore as _;
    let n = jobs.len();
    let out: std::sync::Mutex<Vec<Option<ReadPut>>> =
        std::sync::Mutex::new((0..n).map(|_| None).collect());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let chunk = n.div_ceil(workers.max(1)).clamp(1, PUT_BATCH_FILES);
    let work = || {
        let ctl = CtlClient::new(sock);
        let flush = |read: &mut Vec<(usize, Vec<u8>)>| {
            let ids: Vec<gix::ObjectId> = read
                .iter()
                .map(|(_, d)| crate::ctl::hash_object(gix::objs::Kind::Blob, d))
                .collect();
            let unsure: Vec<usize> = (0..read.len())
                .filter(|&k| jobs[read[k].0].2 != Some(ids[k]))
                .collect();
            let (ask, send): (Vec<usize>, Vec<usize>) =
                unsure.into_iter().partition(|&k| jobs[read[k].0].3);
            let asked: Vec<gix::ObjectId> = ask.iter().map(|&k| ids[k]).collect();
            let mut missing: Vec<usize> = match ctl.has(&asked) {
                Ok(present) if !asked.is_empty() => ask
                    .iter()
                    .zip(present)
                    .filter(|(_, p)| !p)
                    .map(|(&k, _)| k)
                    .collect(),
                _ => ask,
            };
            missing.extend(send);
            let objs: Vec<_> = missing
                .iter()
                .map(|&k| (gix::objs::Kind::Blob, read[k].1.as_slice()))
                .collect();
            let res = if objs.is_empty() {
                Ok(vec![])
            } else {
                ctl.put_many(&objs)
            };
            let mut out = out.lock().unwrap();
            for (k, (i, data)) in read.drain(..).enumerate() {
                out[i] = Some(match &res {
                    Ok(_) => Ok((ids[k], jobs[i].1.then_some(data))),
                    Err(e) => Err(io::Error::other(e.to_string())),
                });
            }
        };
        loop {
            let start = next.fetch_add(chunk, std::sync::atomic::Ordering::Relaxed);
            if start >= n {
                break;
            }
            let mut read: Vec<(usize, Vec<u8>)> = Vec::new();
            let mut bytes = 0;
            for (i, job) in jobs
                .iter()
                .enumerate()
                .take((start + chunk).min(n))
                .skip(start)
            {
                match fs::read(&job.0) {
                    Ok(data) => {
                        if !read.is_empty() && bytes + data.len() > PUT_BATCH_BYTES {
                            flush(&mut read);
                            bytes = 0;
                        }
                        bytes += data.len();
                        read.push((i, data));
                    }
                    Err(e) => out.lock().unwrap()[i] = Some(Err(e)),
                }
            }
            flush(&mut read);
        }
    };
    if n < 4 {
        work();
    } else {
        let work = &work;
        std::thread::scope(|s| {
            for _ in 0..workers.min(n) {
                s.spawn(work);
            }
        });
    }
    out.into_inner()
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap())
        .collect()
}

/// One path to settle. Kind `base` is a file or symlink that equals the base tree, held in `known`.
#[derive(Clone, Debug)]
pub(crate) struct Item {
    pub(crate) kind: String,
    pub(crate) opaque: bool,
    pub(crate) known: Option<MergedTreeValue>,
}

impl Item {
    pub(crate) fn of(kind: &str) -> Self {
        Self {
            kind: kind.to_owned(),
            opaque: false,
            known: None,
        }
    }
}

/// Per-directory gitignore chains, computed lazily from `.gitignore` files on the mount.
pub(crate) struct IgnoreCtx<'a> {
    mount: &'a Path,
    base: Arc<GitIgnoreFile>,
    dirs: HashMap<String, (Arc<GitIgnoreFile>, bool)>,
}

impl<'a> IgnoreCtx<'a> {
    pub(crate) fn new(mount: &'a Path, base: Arc<GitIgnoreFile>) -> Self {
        Self {
            mount,
            base,
            dirs: HashMap::new(),
        }
    }

    fn dir_state(&mut self, dir: &RepoPath) -> Result<(Arc<GitIgnoreFile>, bool), SnapshotError> {
        let key = dir.as_internal_file_string().to_owned();
        if let Some(s) = self.dirs.get(&key) {
            return Ok(s.clone());
        }
        let (parent_chain, parent_ignored) = match dir.parent() {
            Some(p) => self.dir_state(p)?,
            None => (self.base.clone(), false),
        };
        let ignored = parent_ignored || (!dir.is_root() && parent_chain.matches_dir(dir));
        let chain = if ignored {
            parent_chain
        } else {
            let disk = dir.to_fs_path(self.mount)?.join(".gitignore");
            match fs::symlink_metadata(&disk) {
                Ok(md) if md.is_file() => {
                    let bytes = fs::read(&disk)
                        .map_err(|e| snap_err(format!("Failed to read {}", disk.display()), e))?;
                    parent_chain.chain(dir, &disk, &bytes)?
                }
                _ => parent_chain,
            }
        };
        self.dirs.insert(key, (chain.clone(), ignored));
        Ok((chain, ignored))
    }

    pub(crate) fn is_ignored(&mut self, path: &RepoPath) -> Result<bool, SnapshotError> {
        let dir = path.parent().unwrap_or(RepoPath::root());
        let (chain, ignored) = self.dir_state(dir)?;
        Ok(ignored || chain.matches_file(path))
    }
}

fn is_reserved(path: &str) -> bool {
    let first = path.split('/').next().unwrap_or("");
    first == ".jj" || first == ".git"
}

impl LockedVfsWorkingCopy {
    /// Sets the tree without touching files; used when the mount already shows it.
    pub fn adopt_tree(&mut self, tree: MergedTree) {
        self.tree = tree;
        self.dirty = true;
        self.st.unwritten.clear();
        cache::discard(&self.state_path);
    }

    /// Records the marker length of each conflicted path that the mount shows with markers.
    pub fn adopt_conflict_markers(&mut self, markers: BTreeMap<String, u32>) {
        self.st.conflict_marker_len = markers;
    }

    fn mount(&self) -> &Path {
        &self.st.mount
    }

    async fn prune_conflict_markers(&mut self) -> Result<(), SnapshotError> {
        let mut gone = vec![];
        for p in self.st.conflict_marker_len.keys() {
            let rp = RepoPathBuf::from_internal_string(p.clone())
                .map_err(|e| snap_err(format!("invalid path {p:?} in state"), e))?;
            if self.tree.path_value(&rp).await?.is_resolved_or_absent() {
                gone.push(p.clone());
            }
        }
        for p in gone {
            self.st.conflict_marker_len.remove(&p);
        }
        Ok(())
    }

    async fn remove_subtree(
        &self,
        builder: &mut MergedTreeBuilder,
        path: &RepoPath,
    ) -> Result<(), SnapshotError> {
        let matcher = PrefixMatcher::new([path]);
        let mut entries = self.tree.entries_matching(&matcher);
        while let Some((p, v)) = entries.next().await {
            v?;
            builder.set_or_remove(p, None);
        }
        Ok(())
    }

    /// Applies the rules that keep a new path out of the tree: ignored, not auto-tracked, over the
    /// size limit. Returns true when the path stays untracked.
    #[allow(clippy::too_many_arguments)]
    async fn skip_untracked(
        &self,
        options: &SnapshotOptions<'_>,
        ignore: &mut IgnoreCtx<'_>,
        builder: &mut MergedTreeBuilder,
        stats: &mut SnapshotStats,
        rp: &RepoPathBuf,
        cur: &MergedTreeValue,
        md: &fs::Metadata,
    ) -> Result<bool, SnapshotError> {
        let mut untracked = false;
        if !options.force_tracking_matcher.matches(rp) && ignore.is_ignored(rp)? {
            untracked = true;
        } else if !options.start_tracking_matcher.matches(rp) {
            stats
                .untracked_paths
                .insert(rp.clone(), UntrackedReason::FileNotAutoTracked);
            untracked = true;
        } else if md.is_file()
            && md.len() > options.max_new_file_size
            && !options.force_tracking_matcher.matches(rp)
        {
            stats.untracked_paths.insert(
                rp.clone(),
                UntrackedReason::FileTooLarge {
                    size: md.len(),
                    max_size: options.max_new_file_size,
                },
            );
            untracked = true;
        }
        if untracked && cur.is_tree() {
            // an untracked file replaced a tracked directory
            self.remove_subtree(builder, rp).await?;
        }
        Ok(untracked)
    }

    /// Builds the tree of `items` over the last snapshot tree with jj's snapshot rules. A regular
    /// file whose `lstat` matches `cache` is not read again.
    async fn build_tree(
        &mut self,
        options: &SnapshotOptions<'_>,
        items: &BTreeMap<String, Item>,
        cache: Option<&FileCache>,
        trust_stamps: bool,
        start_ns: i128,
    ) -> Result<Built, SnapshotError> {
        let mut stats = SnapshotStats::default();
        let mut builder = MergedTreeBuilder::new(self.tree.clone());
        let mount_path = self.st.mount.clone();
        let mut ignore = IgnoreCtx::new(&mount_path, options.base_ignores.clone());
        let t = Instant::now();
        let mut n_files = 0usize;
        let mut n_bytes = 0u64;
        let mut d_put = std::time::Duration::ZERO;
        let mut pending: Vec<Pending> = vec![];
        let mut stamps: BTreeMap<String, Stamp> = BTreeMap::new();
        // Paths that are files or symlinks now. Items below them (tombstones of the directory
        // they replaced) must not reach the tree builder, or the directory would come back.
        let mut file_roots: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Subtrees that the builder dropped. A path below one is written whatever the old tree held.
        let mut cleared: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (path, item) in items {
            if path
                .match_indices('/')
                .any(|(i, _)| file_roots.contains(&path[..i]))
            {
                continue;
            }
            let rp = RepoPathBuf::from_internal_string(path.clone())
                .map_err(|e| snap_err(format!("invalid path {path:?} from daemon"), e))?;
            let under_cleared = path
                .match_indices('/')
                .any(|(i, _)| cleared.contains(&path[..i]));
            let mut kind = item.kind.as_str();
            if kind == "base" && self.tree.path_value(&rp).await?.as_resolved().is_none() {
                kind = "file";
            }
            match kind {
                "deleted" => {
                    self.remove_subtree(&mut builder, &rp).await?;
                    cleared.insert(path.clone());
                }
                "dir" => {
                    if item.opaque {
                        self.remove_subtree(&mut builder, &rp).await?;
                        cleared.insert(path.clone());
                    } else {
                        let cur = self.tree.path_value(&rp).await?;
                        if cur.is_file_like() {
                            builder.set_or_remove(rp.clone(), None);
                        }
                    }
                }
                "base" => {
                    let known = item.known.clone().expect("a base item holds its value");
                    let cur = self.tree.path_value(&rp).await?;
                    file_roots.insert(path.clone());
                    if !cur.is_file_like() {
                        let Ok(md) = fs::symlink_metadata(rp.to_fs_path(self.mount())?) else {
                            continue;
                        };
                        if self
                            .skip_untracked(
                                options,
                                &mut ignore,
                                &mut builder,
                                &mut stats,
                                &rp,
                                &cur,
                                &md,
                            )
                            .await?
                        {
                            continue;
                        }
                    }
                    if under_cleared || known != cur {
                        builder
                            .set_or_remove_merge(rp, known)
                            .map_err(|e| snap_err("conflict arity mismatch".to_owned(), e))?;
                    }
                }
                _ => {
                    let disk = rp.to_fs_path(self.mount())?;
                    let md = match fs::symlink_metadata(&disk) {
                        Ok(md) => md,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            self.remove_subtree(&mut builder, &rp).await?;
                            cleared.insert(path.clone());
                            continue;
                        }
                        Err(e) => {
                            return Err(snap_err(format!("Failed to stat {}", disk.display()), e));
                        }
                    };
                    if md.is_dir() {
                        let cur = self.tree.path_value(&rp).await?;
                        if cur.is_file_like() {
                            builder.set_or_remove(rp.clone(), None);
                        }
                        continue;
                    }
                    if !md.is_file() && !md.file_type().is_symlink() {
                        continue;
                    }
                    let cur = self.tree.path_value(&rp).await?;
                    file_roots.insert(path.clone());
                    if !cur.is_file_like()
                        && self
                            .skip_untracked(
                                options,
                                &mut ignore,
                                &mut builder,
                                &mut stats,
                                &rp,
                                &cur,
                                &md,
                            )
                            .await?
                    {
                        continue;
                    }
                    if let Some(progress) = options.progress {
                        progress(&rp);
                    }
                    let exec = md.mode() & 0o100 != 0;
                    if md.file_type().is_symlink() {
                        let target = fs::read_link(&disk).map_err(|e| {
                            snap_err(format!("Failed to read symlink {}", disk.display()), e)
                        })?;
                        let target = target.to_str().ok_or_else(|| {
                            SnapshotError::InvalidUtf8SymlinkTarget { path: disk.clone() }
                        })?;
                        let id = self.store.write_symlink(&rp, target).await?;
                        let new_value = Merge::normal(TreeValue::Symlink(id));
                        if under_cleared || new_value != cur {
                            builder
                                .set_or_remove_merge(rp, new_value)
                                .map_err(|e| snap_err("conflict arity mismatch".to_owned(), e))?;
                        }
                    } else {
                        if trust_stamps
                            && let Some(cur_value) = cur.as_resolved()
                            && let Some(sha) = cache.and_then(|c| c.hit(path, &md))
                            && let Some(id) = FileId::try_from_hex(sha)
                        {
                            let copy_id = match cur_value {
                                Some(TreeValue::File { copy_id, .. }) => copy_id.clone(),
                                _ => CopyId::placeholder(),
                            };
                            let new_value = Merge::normal(TreeValue::File {
                                id,
                                executable: exec,
                                copy_id,
                            });
                            if under_cleared || new_value != cur {
                                builder.set_or_remove_merge(rp, new_value).map_err(|e| {
                                    snap_err("conflict arity mismatch".to_owned(), e)
                                })?;
                            }
                            stamps.insert(path.clone(), Stamp::of(&md, sha.to_owned()));
                            continue;
                        }
                        n_bytes += md.len();
                        let known = stored_blob(&cur)
                            .or_else(|| cache.and_then(|c| c.sha(path)).and_then(blob_id));
                        let ask_has = cache.is_some_and(|c| !trust_stamps || c.sha(path).is_some());
                        pending.push(Pending {
                            rp,
                            disk,
                            cur,
                            exec,
                            md,
                            known,
                            ask_has,
                            under_cleared,
                        });
                    }
                }
            }
        }
        // Regular files: read and store in parallel; only conflicted paths need their bytes back.
        let jobs: Vec<Job> = pending
            .iter()
            .map(|p| {
                (
                    p.disk.clone(),
                    p.cur.as_resolved().is_none(),
                    p.known,
                    p.ask_has,
                )
            })
            .collect();
        let t_par = Instant::now();
        let results = read_and_put(self.ctl.path(), &jobs, 8);
        let d_fsread = t_par.elapsed();
        let t_put = Instant::now();
        for (pf, res) in pending.into_iter().zip(results) {
            let (oid, data) = match res {
                Ok(r) => r,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.remove_subtree(&mut builder, &pf.rp).await?;
                    continue;
                }
                Err(e) => {
                    return Err(snap_err(
                        format!("Failed to snapshot {}", pf.disk.display()),
                        e,
                    ));
                }
            };
            n_files += 1;
            if Stamp::cacheable(&pf.md, start_ns) {
                stamps.insert(
                    pf.rp.as_internal_file_string().to_owned(),
                    Stamp::of(&pf.md, oid.to_string()),
                );
            }
            let new_value = match (pf.cur.as_resolved(), data) {
                (Some(cur_value), _) => {
                    let copy_id = match cur_value {
                        Some(TreeValue::File { copy_id, .. }) => copy_id.clone(),
                        _ => CopyId::placeholder(),
                    };
                    Merge::normal(TreeValue::File {
                        id: FileId::from_bytes(oid.as_bytes()),
                        executable: pf.exec,
                        copy_id,
                    })
                }
                (None, Some(data)) => self.file_value(&pf.rp, &pf.cur, &data, pf.exec).await?,
                (None, None) => unreachable!("conflicted paths keep their bytes"),
            };
            if pf.under_cleared || new_value != pf.cur {
                builder
                    .set_or_remove_merge(pf.rp, new_value)
                    .map_err(|e| snap_err("conflict arity mismatch".to_owned(), e))?;
            }
        }
        d_put += t_put.elapsed();
        let t_read = t.elapsed();
        let t = Instant::now();
        let new_tree = builder.write_tree().await?;
        let t_tree = t.elapsed();
        let timing = format!(
            "read+blobs={:?} (read+put_parallel={:?} assemble={:?}) write_tree={:?}",
            t_read, d_fsread, d_put, t_tree
        );
        Ok(Built {
            tree: new_tree,
            stats,
            files: n_files,
            bytes: n_bytes,
            stamps,
            timing,
        })
    }
}

#[async_trait]
impl LockedWorkingCopy for LockedVfsWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        &self.old_operation_id
    }

    fn old_tree(&self) -> &MergedTree {
        &self.old_tree
    }

    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        let mut op = crate::trace::span("jj.snapshot");
        let t_all = Instant::now();
        syncfs(self.mount()).map_err(|e| snap_err("syncfs on the mount failed", e))?;
        let reply = self
            .ctl
            .dirty()
            .map_err(|e| snap_err("daemon dirty query failed", e))?;
        self.snapshot_overlay(options, reply, &mut op, t_all).await
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        let _op = crate::trace::span("jj.checkout");
        let new_tree = commit.tree();
        if self.tree.tree_ids_and_labels() == new_tree.tree_ids_and_labels() {
            return Ok(CheckoutStats::default());
        }
        let old_tree = self.tree.clone();
        let stats = self.update(&old_tree, &new_tree).await?;
        self.tree = new_tree;
        self.dirty = true;
        Ok(stats)
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        self.new_workspace_name = Some(new_workspace_name);
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.tree = commit.tree();
        self.dirty = true;
        Ok(())
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.tree = self.store.empty_merged_tree();
        self.st.conflict_marker_len.clear();
        self.reset(commit).await
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        static ROOT: std::sync::OnceLock<Vec<RepoPathBuf>> = std::sync::OnceLock::new();
        Ok(ROOT.get_or_init(|| vec![RepoPathBuf::root()]))
    }

    async fn set_sparse_patterns(
        &mut self,
        _new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        Err(co_err(
            "sparse patterns are not supported by the motte-vfs working copy",
            "the workspace is a lazy file system; every path is always present",
        ))
    }

    async fn finish(
        mut self: Box<Self>,
        operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        store_tree(&mut self.st, &self.tree);
        let op_changed = self.old_operation_id != operation_id || self.new_workspace_name.is_some();
        if self.dirty || op_changed {
            self.st.operation_id = operation_id.hex();
            if let Some(n) = &self.new_workspace_name {
                self.st.workspace_name = n.as_str().to_owned();
            }
            write_state(&self.state_path, &self.st)?;
        }
        let settings_style = self.marker_style;
        let wc = VfsWorkingCopy {
            op_id: operation_id,
            ws_name: self.st.workspace_name.clone().into(),
            tree: self.tree.clone(),
            store: self.store.clone(),
            state_path: self.state_path.clone(),
            marker_style: settings_style,
        };
        Ok(Box::new(wc))
    }
}

impl LockedVfsWorkingCopy {
    /// The tree value for a regular file with `data`, keeping conflicts parseable.
    async fn file_value(
        &mut self,
        rp: &RepoPath,
        cur: &MergedTreeValue,
        data: &[u8],
        exec: bool,
    ) -> Result<MergedTreeValue, SnapshotError> {
        if let Some(cur_value) = cur.as_resolved() {
            let id = self.store.write_file(rp, &mut &data[..]).await?;
            let copy_id = match cur_value {
                Some(TreeValue::File { copy_id, .. }) => copy_id.clone(),
                _ => CopyId::placeholder(),
            };
            return Ok(Merge::normal(TreeValue::File {
                id,
                executable: exec,
                copy_id,
            }));
        }
        if let Some(old_file_ids) = cur.to_file_merge() {
            let copy_id = cur
                .to_copy_id_merge()
                .unwrap()
                .resolve_trivial(jj_lib::merge::SameChange::Accept)
                .cloned()
                .flatten()
                .unwrap_or_else(CopyId::placeholder);
            let marker_len = self
                .st
                .conflict_marker_len
                .get(rp.as_internal_file_string())
                .map_or(MIN_CONFLICT_MARKER_LEN, |l| *l as usize);
            let new_ids =
                conflicts::update_from_content(&old_file_ids, &self.store, rp, data, marker_len)
                    .await?;
            return Ok(match new_ids.into_resolved() {
                Ok(file_id) => Merge::normal(TreeValue::File {
                    id: file_id.unwrap(),
                    executable: exec,
                    copy_id,
                }),
                Err(new_ids) => {
                    if new_ids != old_file_ids {
                        cur.with_new_file_ids(&new_ids)
                    } else {
                        cur.clone()
                    }
                }
            });
        }
        // The path holds directories, symlinks or other non-file terms (possibly conflicted) in
        // the tree but is a regular file on disk: the file replaces all of them.
        let id = self.store.write_file(rp, &mut &data[..]).await?;
        Ok(Merge::normal(TreeValue::File {
            id,
            executable: exec,
            copy_id: CopyId::placeholder(),
        }))
    }

    /// Writes only the paths that differ between `old` and `new` through the mount.
    /// Entries are handled in windows: blobs of a window are fetched in parallel, directories and
    /// removals are applied in order, and the file writes of the window run in parallel.
    async fn update(
        &mut self,
        old: &MergedTree,
        new: &MergedTree,
    ) -> Result<CheckoutStats, CheckoutError> {
        const WINDOW: usize = 512;
        let mut stats = CheckoutStats::default();
        let mount = self.st.mount.clone();
        let mut made_dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        let t_loop = Instant::now();
        let mut entries: Vec<(RepoPathBuf, MergedTreeValue, MergedTreeValue)> = vec![];
        {
            let mut diff = old.diff_for_file_system(new, &EverythingMatcher);
            while let Some(TreeDiffEntry { path, values }) = diff.next().await {
                let d = values?;
                if path.components().any(|c| {
                    let n = c.as_internal_str();
                    n.eq_ignore_ascii_case(".git") || n.eq_ignore_ascii_case(".jj")
                }) {
                    return Err(co_err(
                        format!(
                            "Refusing to check out reserved path {}",
                            path.as_internal_file_string()
                        ),
                        "reserved path component",
                    ));
                }
                entries.push((path, d.before, d.after));
            }
        }
        let t_diff = t_loop.elapsed();
        let mut markers: Vec<(String, Option<u32>)> = vec![];
        let mut skipped: Vec<String> = vec![];
        let (mut d_prefetch, mut d_writes) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
        let backend = self.store.backend();
        let native = backend.downcast_ref::<crate::backend::NativeBackend>();
        let mut rest = entries;
        while !rest.is_empty() {
            let tail = rest.split_off(WINDOW.min(rest.len()));
            let window = std::mem::replace(&mut rest, tail);
            if let Some(native) = native {
                let t = Instant::now();
                let ids: Vec<gix::ObjectId> = window
                    .iter()
                    .flat_map(|(_, _, after)| after.iter().flatten())
                    .filter_map(|v| match v {
                        TreeValue::File { id, .. } => {
                            Some(gix::ObjectId::from_bytes_or_panic(id.as_bytes()))
                        }
                        _ => None,
                    })
                    .collect();
                native.prefetch_blobs(&ids, 8);
                d_prefetch += t.elapsed();
            }
            let mut jobs: Vec<WriteJob> = vec![];
            for (path, before, after_value) in window {
                let after =
                    materialize_tree_value(&self.store, &path, after_value, new.labels()).await?;
                if matches!(
                    after,
                    MaterializedTreeValue::Absent | MaterializedTreeValue::AccessDenied(_)
                ) {
                    stats.removed_files += 1;
                } else if before.is_absent() {
                    stats.added_files += 1;
                } else {
                    stats.updated_files += 1;
                }
                if matches!(before.as_normal(), Some(TreeValue::GitSubmodule(_)))
                    || matches!(after, MaterializedTreeValue::GitSubmodule(_))
                {
                    continue;
                }
                let disk = path.to_fs_path(&mount)?;
                let after_absent = matches!(
                    after,
                    MaterializedTreeValue::Absent | MaterializedTreeValue::AccessDenied(_)
                );
                // For replacements the old file is unlinked inside the (parallel) write job.
                let remove_first = before.is_present() && !after_absent;
                if before.is_present() && after_absent {
                    match fs::symlink_metadata(&disk) {
                        Ok(md) if md.is_dir() => {
                            // an untracked directory sits where a tracked file was
                            stats.skipped_files += 1;
                            skipped.push(path.as_internal_file_string().to_owned());
                            continue;
                        }
                        Ok(_) => {
                            fs::remove_file(&disk).map_err(|e| {
                                co_err(format!("Failed to remove {}", disk.display()), e)
                            })?;
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(co_err(format!("Failed to stat {}", disk.display()), e));
                        }
                    }
                }
                if after_absent {
                    let mut dir = disk.parent();
                    while let Some(d) = dir {
                        if d == mount || fs::remove_dir(d).is_err() {
                            break;
                        }
                        made_dirs.remove(d);
                        dir = d.parent();
                    }
                    markers.push((path.as_internal_file_string().to_owned(), None));
                    continue;
                }
                // parent directories
                let parent = disk.parent().unwrap().to_owned();
                if !made_dirs.contains(&parent) {
                    let mut cur = mount.clone();
                    let rel = parent.strip_prefix(&mount).unwrap();
                    let mut blocked = false;
                    for comp in rel.components() {
                        cur.push(comp);
                        match fs::symlink_metadata(&cur) {
                            Ok(md) if md.is_dir() => {}
                            Ok(_) => {
                                blocked = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                                fs::create_dir(&cur).map_err(|e| {
                                    co_err(format!("Failed to create {}", cur.display()), e)
                                })?;
                            }
                            Err(e) => {
                                return Err(co_err(format!("Failed to stat {}", cur.display()), e));
                            }
                        }
                    }
                    if blocked {
                        stats.skipped_files += 1;
                        skipped.push(path.as_internal_file_string().to_owned());
                        continue;
                    }
                    made_dirs.insert(parent);
                }
                let rel = path.as_internal_file_string().to_owned();
                match after {
                    MaterializedTreeValue::File(mut file) => {
                        let mut data = Vec::new();
                        file.reader
                            .read_to_end(&mut data)
                            .await
                            .map_err(|e| co_err("Failed to read file contents", e))?;
                        jobs.push(WriteJob {
                            path: rel,
                            disk,
                            kind: JobKind::File {
                                data,
                                exec: file.executable,
                            },
                            marker: None,
                            remove_first,
                        });
                    }
                    MaterializedTreeValue::Symlink { id: _, target } => {
                        jobs.push(WriteJob {
                            path: rel,
                            disk,
                            kind: JobKind::Symlink(target),
                            marker: None,
                            remove_first,
                        });
                    }
                    MaterializedTreeValue::FileConflict(file) => {
                        let len = choose_materialized_conflict_marker_len(&file.contents);
                        let options = ConflictMaterializeOptions {
                            marker_style: self.marker_style,
                            marker_len: Some(len),
                            merge: self.store.merge_options().clone(),
                        };
                        let bytes = materialize_merge_result_to_bytes(
                            &file.contents,
                            &file.labels,
                            &options,
                        );
                        jobs.push(WriteJob {
                            path: rel,
                            disk,
                            kind: JobKind::File {
                                data: bytes.to_vec(),
                                exec: file.executable.unwrap_or(false),
                            },
                            marker: Some(len.try_into().unwrap_or(u32::MAX)),
                            remove_first,
                        });
                    }
                    MaterializedTreeValue::OtherConflict { id, labels } => {
                        jobs.push(WriteJob {
                            path: rel,
                            disk,
                            kind: JobKind::File {
                                data: id.describe(&labels).into_bytes(),
                                exec: false,
                            },
                            marker: None,
                            remove_first,
                        });
                    }
                    MaterializedTreeValue::Tree(_) => {
                        panic!("unexpected tree entry in diff at {path:?}")
                    }
                    _ => {}
                }
            }
            let t = Instant::now();
            let results = run_write_jobs(&jobs, 8);
            d_writes += t.elapsed();
            for (job, res) in jobs.into_iter().zip(results) {
                match res {
                    Ok(true) => markers.push((job.path, job.marker)),
                    Ok(false) => {
                        stats.skipped_files += 1;
                        skipped.push(job.path);
                    }
                    Err(e) => {
                        return Err(co_err(format!("Failed to write {}", job.disk.display()), e));
                    }
                }
            }
        }
        trace!(
            "update: diff={:?} prefetch_blobs={:?} parallel_writes={:?} total={:?}",
            t_diff,
            d_prefetch,
            d_writes,
            t_loop.elapsed()
        );
        self.st.unwritten.extend(skipped);
        for (p, m) in markers {
            match m {
                Some(l) => {
                    self.st.conflict_marker_len.insert(p, l);
                }
                None => {
                    self.st.conflict_marker_len.remove(&p);
                }
            }
        }
        Ok(stats)
    }
}

enum JobKind {
    File { data: Vec<u8>, exec: bool },
    Symlink(String),
}

struct WriteJob {
    path: String,
    disk: PathBuf,
    kind: JobKind,
    marker: Option<u32>,
    remove_first: bool,
}

/// Creates the file or symlink; `Ok(false)` if something already exists there (the path is skipped).
fn write_one(job: &WriteJob) -> io::Result<bool> {
    if job.remove_first {
        match fs::remove_file(&job.disk) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                // an untracked directory sits where a tracked file was
                if fs::symlink_metadata(&job.disk).is_ok_and(|m| m.is_dir()) {
                    return Ok(false);
                }
                return Err(e);
            }
        }
    }
    let attempt = || -> io::Result<bool> {
        match &job.kind {
            JobKind::File { data, exec } => {
                let mut f = match fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(if *exec { 0o777 } else { 0o666 })
                    .open(&job.disk)
                {
                    Ok(f) => f,
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
                    Err(e) => return Err(e),
                };
                f.write_all(data)?;
                if *exec {
                    f.set_permissions(fs::Permissions::from_mode(0o755))?;
                }
                Ok(true)
            }
            JobKind::Symlink(target) => match std::os::unix::fs::symlink(target, &job.disk) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(e),
            },
        }
    };
    match attempt() {
        // an empty parent was cleaned up after the job was queued
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = job.disk.parent() {
                fs::create_dir_all(parent)?;
            }
            attempt()
        }
        other => other,
    }
}

fn run_write_jobs(jobs: &[WriteJob], workers: usize) -> Vec<io::Result<bool>> {
    let n = jobs.len();
    if n < 8 {
        return jobs.iter().map(write_one).collect();
    }
    let out: std::sync::Mutex<Vec<Option<io::Result<bool>>>> =
        std::sync::Mutex::new((0..n).map(|_| None).collect());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let work = || {
        loop {
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if i >= n {
                break;
            }
            let r = write_one(&jobs[i]);
            out.lock().unwrap()[i] = Some(r);
        }
    };
    std::thread::scope(|s| {
        for _ in 0..workers.min(n) {
            s.spawn(work);
        }
    });
    out.into_inner()
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap())
        .collect()
}

trait ResolvedOrAbsent {
    fn is_resolved_or_absent(&self) -> bool;
}

impl ResolvedOrAbsent for MergedTreeValue {
    fn is_resolved_or_absent(&self) -> bool {
        self.is_resolved()
    }
}

/// The plain tree of the files that a checkout of `tree` writes, with each conflict materialized
/// with markers as `update` writes it.
pub async fn materialized_tree(
    store: &Arc<Store>,
    tree: &MergedTree,
    settings: &UserSettings,
) -> Result<TreeId, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(id) = tree.tree_ids().as_resolved() {
        return Ok(id.clone());
    }
    let marker_style = TreeStateSettings::try_from_user_settings(settings)?.conflict_marker_style;
    let mut builder = MergedTreeBuilder::new(tree.clone());
    let mut conflicts = tree.conflicts();
    while let Some((path, value)) = conflicts.next().await {
        let (data, executable) =
            match materialize_tree_value(store, &path, value?, tree.labels()).await? {
                MaterializedTreeValue::FileConflict(file) => {
                    let len = choose_materialized_conflict_marker_len(&file.contents);
                    let options = ConflictMaterializeOptions {
                        marker_style,
                        marker_len: Some(len),
                        merge: store.merge_options().clone(),
                    };
                    let bytes =
                        materialize_merge_result_to_bytes(&file.contents, &file.labels, &options);
                    (bytes.to_vec(), file.executable.unwrap_or(false))
                }
                MaterializedTreeValue::OtherConflict { id, labels } => {
                    (id.describe(&labels).into_bytes(), false)
                }
                _ => continue,
            };
        let id = store.write_file(&path, &mut data.as_slice()).await?;
        builder.set_or_remove(
            path,
            Some(TreeValue::File {
                id,
                executable,
                copy_id: CopyId::placeholder(),
            }),
        );
    }
    let written = builder.write_tree().await?;
    let id = written
        .tree_ids()
        .as_resolved()
        .ok_or("the materialized tree still has a conflict")?
        .clone();
    Ok(id)
}

/// The marker length of each conflicted file that a checkout of `tree` writes. It depends on the
/// contents only, not on the marker style.
pub async fn conflict_markers(
    store: &Arc<Store>,
    tree: &MergedTree,
) -> Result<BTreeMap<String, u32>, Box<dyn std::error::Error + Send + Sync>> {
    let mut markers = BTreeMap::new();
    let mut conflicts = tree.conflicts();
    while let Some((path, value)) = conflicts.next().await {
        if let MaterializedTreeValue::FileConflict(file) =
            materialize_tree_value(store, &path, value?, tree.labels()).await?
        {
            let len = choose_materialized_conflict_marker_len(&file.contents);
            markers.insert(
                path.as_internal_file_string().to_owned(),
                len.try_into().unwrap_or(u32::MAX),
            );
        }
    }
    Ok(markers)
}
