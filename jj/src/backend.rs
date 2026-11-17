//! `NativeBackend`: a jj `Backend` that stores git objects through an [`ObjectStore`] (the daemon
//! control socket), with no on-disk git repository.
//!
//! The conversion code is adapted from jj-lib's `git_backend.rs` (Apache-2.0, Copyright 2020 The
//! Jujutsu Authors) so that every commit, tree and blob has the same bytes, and therefore the same
//! id, as `GitBackend::write_commit` produces for identical inputs.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::Cursor;
use futures::stream::BoxStream;
use gix::bstr::BString;
use gix::objs::Kind;
use gix::objs::WriteTo as _;
use gix::objs::commit::signature_field_name;
use itertools::Itertools as _;
use jj_lib::backend::{
    Backend, BackendError, BackendInitError, BackendLoadError, BackendResult, ChangeId, Commit,
    CommitId, CopyHistory, CopyId, CopyRecord, FileId, MillisSinceEpoch, RelatedCopy, SecureSig,
    Signature, SigningFn, SymlinkId, Timestamp, Tree, TreeId, TreeValue, make_root_commit,
};
use jj_lib::git::GitSettings;
use jj_lib::git_backend::{
    CHANGE_ID_COMMIT_HEADER, JJ_CONFLICT_LABELS_COMMIT_HEADER, JJ_CONFLICT_README_FILE_NAME,
    JJ_TREES_COMMIT_HEADER, extract_change_id_from_commit, synthetic_change_id_from_git_commit_id,
};
use jj_lib::lock::FileLock;
use jj_lib::merge::{Merge, MergeBuilder};
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use smallvec::SmallVec;

use crate::ctl::{CtlClient, HASH_KIND, ObjectStore, StoreError};

pub const BACKEND_NAME: &str = "motte-objects";
const CHANGE_ID_LENGTH: usize = 16;
const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";
pub(crate) const SOCK_FILE: &str = "ctl_sock";
const SHALLOW_FILE: &str = "shallow";
const EXTRAS_FILE: &str = "extras";
const EXTRAS_LOCK: &str = "extras.lock";
const SHALLOW_LOCK: &str = "shallow.lock";

fn to_read_err(
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    object_type: &str,
    hash: String,
) -> BackendError {
    BackendError::ReadObject {
        object_type: object_type.to_owned(),
        hash,
        source: err.into(),
    }
}

fn map_store_err(err: StoreError, object_type: &str, hash: String) -> BackendError {
    match err {
        StoreError::NotFound(_) => BackendError::ObjectNotFound {
            object_type: object_type.to_owned(),
            hash,
            source: None,
        },
        other => to_read_err(other, object_type, hash),
    }
}

fn write_err(
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    object_type: &'static str,
) -> BackendError {
    BackendError::WriteObject {
        object_type,
        source: err.into(),
    }
}

/// Per-commit data that is not part of the git commit object.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Extra {
    change_id: Vec<u8>,
    predecessors: Vec<Vec<u8>>,
}

impl Extra {
    fn of(commit: &Commit) -> Self {
        Self {
            change_id: commit.change_id.to_bytes(),
            predecessors: commit.predecessors.iter().map(|p| p.to_bytes()).collect(),
        }
    }

    fn to_line(&self, id: &CommitId) -> String {
        format!(
            "{} {} {}\n",
            id.hex(),
            hex::encode(&self.change_id),
            self.predecessors.iter().map(hex::encode).join(",")
        )
    }

    fn from_line(line: &str) -> Option<(CommitId, Self)> {
        let mut it = line.split(' ');
        let id = CommitId::try_from_hex(it.next()?)?;
        let change_id = hex::decode(it.next()?).ok()?;
        let preds = it.next().unwrap_or("");
        let predecessors = preds
            .split(',')
            .filter(|s| !s.is_empty())
            .map(hex::decode)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        Some((
            id,
            Self {
                change_id,
                predecessors,
            },
        ))
    }
}

/// Append-only side table for the data git commits cannot hold (predecessors) and for change ids
/// of commits that were written without a `change-id` header. Local to the workspace.
struct ExtrasTable {
    path: PathBuf,
    map: HashMap<CommitId, Extra>,
    offset: u64,
}

impl ExtrasTable {
    fn refresh(&mut self) -> std::io::Result<()> {
        let Ok(meta) = fs::metadata(&self.path) else {
            return Ok(());
        };
        if meta.len() <= self.offset {
            return Ok(());
        }
        let data = fs::read(&self.path)?;
        let tail = &data[self.offset as usize..];
        let end = tail.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        for line in String::from_utf8_lossy(&tail[..end]).lines() {
            if let Some((id, extra)) = Extra::from_line(line) {
                self.map.insert(id, extra);
            }
        }
        self.offset += end as u64;
        Ok(())
    }

    fn append(&mut self, id: &CommitId, extra: &Extra) -> std::io::Result<()> {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(extra.to_line(id).as_bytes())?;
        self.map.insert(id.clone(), extra.clone());
        self.refresh()
    }
}

pub struct NativeBackend {
    store: Arc<dyn ObjectStore>,
    store_path: PathBuf,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    shallow: Mutex<HashSet<CommitId>>,
    extras: Mutex<ExtrasTable>,
    write_change_id_header: bool,
    prefetched: Mutex<HashMap<gix::ObjectId, Vec<u8>>>,
    tree_cache: Mutex<clru::CLruCache<TreeId, Tree>>,
    commit_cache: Mutex<clru::CLruCache<CommitId, Commit>>,
}

impl Debug for NativeBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBackend")
            .field("path", &self.store_path)
            .finish()
    }
}

fn cache_cap(n: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(n).unwrap()
}

impl NativeBackend {
    pub const NAME: &'static str = BACKEND_NAME;

    /// Creates a backend over `store`. `store_path` holds the local side tables.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        store_path: &Path,
        write_change_id_header: bool,
    ) -> std::io::Result<Self> {
        let shallow = Self::read_shallow(store_path).unwrap_or_default();
        let mut extras = ExtrasTable {
            path: store_path.join(EXTRAS_FILE),
            map: HashMap::new(),
            offset: 0,
        };
        extras.refresh()?;
        Ok(Self {
            store,
            store_path: store_path.to_owned(),
            root_commit_id: CommitId::from_bytes(&[0; 20]),
            root_change_id: ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH]),
            empty_tree_id: TreeId::from_bytes(gix::ObjectId::empty_tree(HASH_KIND).as_bytes()),
            shallow: Mutex::new(shallow),
            extras: Mutex::new(extras),
            write_change_id_header,
            prefetched: Mutex::new(HashMap::new()),
            tree_cache: Mutex::new(clru::CLruCache::new(cache_cap(16384))),
            commit_cache: Mutex::new(clru::CLruCache::new(cache_cap(8192))),
        })
    }

    /// Initializes a new backend directory. The socket path is recorded so that `load` finds it.
    pub fn init(
        settings: &UserSettings,
        store_path: &Path,
        sock: &Path,
        shallow: &[CommitId],
    ) -> Result<Self, BackendInitError> {
        fs::write(
            store_path.join(SOCK_FILE),
            sock.as_os_str().as_encoded_bytes(),
        )
        .map_err(|e| BackendInitError(e.into()))?;
        Self::set_shallow(store_path, shallow).map_err(|e| BackendInitError(e.into()))?;
        let git_settings =
            GitSettings::from_settings(settings).map_err(|e| BackendInitError(e.into()))?;
        let store = Arc::new(CtlClient::new(sock));
        Self::new(store, store_path, git_settings.write_change_id_header)
            .map_err(|e| BackendInitError(e.into()))
    }

    pub fn load(settings: &UserSettings, store_path: &Path) -> Result<Self, BackendLoadError> {
        let sock = match std::env::var_os("MOTTE_VFS_SOCK") {
            Some(s) => PathBuf::from(s),
            None => {
                let b =
                    fs::read(store_path.join(SOCK_FILE)).map_err(|e| BackendLoadError(e.into()))?;
                PathBuf::from(String::from_utf8_lossy(&b).into_owned())
            }
        };
        let git_settings =
            GitSettings::from_settings(settings).map_err(|e| BackendLoadError(e.into()))?;
        let store = Arc::new(CtlClient::new(sock));
        Self::new(store, store_path, git_settings.write_change_id_header)
            .map_err(|e| BackendLoadError(e.into()))
    }

    /// The roots in the `shallow` file; a missing file holds none.
    pub fn read_shallow(store_path: &Path) -> std::io::Result<HashSet<CommitId>> {
        match fs::read_to_string(store_path.join(SHALLOW_FILE)) {
            Ok(s) => Ok(s
                .lines()
                .filter_map(|l| CommitId::try_from_hex(l.trim()))
                .collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashSet::new()),
            Err(e) => Err(e),
        }
    }

    /// The roots now in the `shallow` file.
    pub fn shallow_roots(&self) -> std::io::Result<Vec<CommitId>> {
        Ok(Self::read_shallow(&self.store_path)?.into_iter().collect())
    }

    /// Marks commits whose parents are not served (like a shallow git clone). The file is replaced
    /// whole, so a reader or a crash sees the old or the new set.
    pub fn set_shallow(store_path: &Path, ids: &[CommitId]) -> std::io::Result<()> {
        let body = ids.iter().map(|i| i.hex()).sorted().join("\n");
        let mut tmp = tempfile::NamedTempFile::new_in(store_path)?;
        tmp.write_all(body.as_bytes())?;
        tmp.as_file().sync_all()?;
        tmp.persist(store_path.join(SHALLOW_FILE))
            .map_err(|e| e.error)?;
        fs::File::open(store_path)?.sync_all()
    }

    /// Adds roots to the `shallow` file under an interprocess lock, then to the memory set.
    pub fn add_shallow(&self, ids: &[CommitId]) -> std::io::Result<()> {
        let lock = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.store_path.join(SHALLOW_LOCK))?;
        lock.lock()?;
        let mut all = Self::read_shallow(&self.store_path)?;
        all.extend(ids.iter().cloned());
        let list: Vec<CommitId> = all.iter().cloned().collect();
        Self::set_shallow(&self.store_path, &list)?;
        self.shallow.lock().unwrap().extend(all);
        Ok(())
    }

    /// Fetches blobs in parallel so that the following reads are served from memory (each id once).
    pub fn prefetch_blobs(&self, ids: &[gix::ObjectId], workers: usize) {
        let todo: Vec<gix::ObjectId> = {
            let have = self.prefetched.lock().unwrap();
            ids.iter()
                .filter(|i| !have.contains_key(*i))
                .copied()
                .collect()
        };
        if todo.len() < 4 {
            return;
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..workers.min(todo.len()) {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(id) = todo.get(i) else { break };
                        if let Ok((Kind::Blob, data)) = self.store.get(id) {
                            self.prefetched.lock().unwrap().insert(*id, data);
                        }
                    }
                });
            }
        });
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn oid(&self, id: &impl jj_lib::object_id::ObjectId) -> BackendResult<gix::ObjectId> {
        gix::ObjectId::try_from(id.as_bytes()).map_err(|_| BackendError::InvalidHashLength {
            expected: 20,
            actual: id.as_bytes().len(),
            object_type: id.object_type(),
            hash: id.hex(),
        })
    }

    fn get_object(
        &self,
        id: &impl jj_lib::object_id::ObjectId,
        want: Kind,
    ) -> BackendResult<Vec<u8>> {
        let oid = self.oid(id)?;
        if want == Kind::Blob
            && let Some(data) = self.prefetched.lock().unwrap().remove(&oid)
        {
            return Ok(data);
        }
        let (kind, data) = self
            .store
            .get(&oid)
            .map_err(|e| map_store_err(e, &id.object_type(), id.hex()))?;
        if kind != want {
            return Err(to_read_err(
                format!("expected {want} but found {kind}"),
                &id.object_type(),
                id.hex(),
            ));
        }
        Ok(data)
    }

    fn put(
        &self,
        kind: Kind,
        data: &[u8],
        object_type: &'static str,
    ) -> BackendResult<gix::ObjectId> {
        self.store
            .put(kind, data)
            .map_err(|e| write_err(e, object_type))
    }

    fn write_tree_object(
        &self,
        entries: Vec<gix::objs::tree::Entry>,
    ) -> BackendResult<gix::ObjectId> {
        let tree = gix::objs::Tree { entries };
        let mut buf = Vec::with_capacity(entries_hint(&tree));
        tree.write_to(&mut buf).map_err(|e| write_err(e, "tree"))?;
        self.put(Kind::Tree, &buf, "tree")
    }

    fn read_extra(&self, id: &CommitId) -> Option<Extra> {
        let mut t = self.extras.lock().unwrap();
        if let Some(e) = t.map.get(id) {
            return Some(e.clone());
        }
        t.refresh().ok()?;
        t.map.get(id).cloned()
    }

    fn commit_from_git(&self, id: &CommitId, data: &[u8]) -> BackendResult<Commit> {
        let hex = id.hex();
        let commit = gix::objs::CommitRef::from_bytes(data, HASH_KIND)
            .map_err(|e| to_read_err(e, "commit", hex.clone()))?;
        let change_id = extract_change_id_from_commit(&commit)
            .unwrap_or_else(|| synthetic_change_id_from_git_commit_id(id));
        let parents = if self.shallow.lock().unwrap().contains(id) {
            vec![]
        } else {
            commit
                .parents()
                .map(|oid| CommitId::from_bytes(oid.as_bytes()))
                .collect_vec()
        };
        let conflict_labels = extract_conflict_labels(&commit);
        let root_tree = extract_root_tree(&commit)
            .map_err(|()| to_read_err("Invalid jj:trees header", "commit", hex.clone()))?;
        let description = String::from_utf8_lossy(commit.message).into_owned();
        let author = signature_from_git(
            commit
                .author()
                .map_err(|e| to_read_err(e, "commit", hex.clone()))?,
        );
        let committer = signature_from_git(
            commit
                .committer()
                .map_err(|e| to_read_err(e, "commit", hex.clone()))?,
        );
        let secure_sig = commit
            .extra_headers
            .iter()
            .any(|(k, _)| *k == signature_field_name(HASH_KIND))
            .then(|| gix::objs::CommitRefIter::signature(data, HASH_KIND))
            .transpose()
            .map_err(|e| to_read_err(e, "commit", hex.clone()))?
            .flatten()
            .map(|(sig, signed)| SecureSig {
                data: signed.to_bstring().into(),
                sig: sig.into_owned().into(),
            });
        Ok(Commit {
            parents,
            predecessors: vec![],
            root_tree,
            conflict_labels,
            change_id,
            description,
            author,
            committer,
            secure_sig,
        })
    }

    fn write_tree_conflict(&self, conflict: &Merge<TreeId>) -> BackendResult<gix::ObjectId> {
        let mut entries = itertools::chain(
            conflict
                .removes()
                .enumerate()
                .map(|(i, tree_id)| (format!(".jjconflict-base-{i}"), tree_id)),
            conflict
                .adds()
                .enumerate()
                .map(|(i, tree_id)| (format!(".jjconflict-side-{i}"), tree_id)),
        )
        .map(|(name, tree_id)| gix::objs::tree::Entry {
            mode: gix::object::tree::EntryKind::Tree.into(),
            filename: name.into(),
            oid: gix::ObjectId::from_bytes_or_panic(tree_id.as_bytes()),
        })
        .collect_vec();
        let readme_id = self.put(Kind::Blob, CONFLICT_README.as_bytes(), "blob")?;
        entries.push(gix::objs::tree::Entry {
            mode: gix::object::tree::EntryKind::Blob.into(),
            filename: JJ_CONFLICT_README_FILE_NAME.into(),
            oid: readme_id,
        });
        let first_tree_id = conflict.first();
        let first = self.get_object(first_tree_id, Kind::Tree)?;
        let first_tree = gix::objs::TreeRef::from_bytes(&first, HASH_KIND)
            .map_err(|e| to_read_err(e, "tree", first_tree_id.hex()))?;
        for entry in first_tree.entries.iter() {
            if !entry.filename.starts_with(b".jjconflict")
                && entry.filename != JJ_CONFLICT_README_FILE_NAME
            {
                entries.push((*entry).into());
            }
        }
        entries.sort_unstable();
        self.write_tree_object(entries)
    }
}

fn entries_hint(tree: &gix::objs::Tree) -> usize {
    tree.entries.len() * 40 + 16
}

const CONFLICT_README: &str = r#"This commit was made by jj, https://jj-vcs.dev/.
The commit contains file conflicts, and therefore looks wrong when used with
plain Git or other tools that are unfamiliar with jj.

The .jjconflict-* directories represent the different inputs to the conflict.
For details, see
https://docs.jj-vcs.dev/latest/git-compatibility/#format-mapping-details

If you see this file in your working copy, it probably means that you used a
regular `git` command to check out a conflicted commit. Use `jj abandon` to
recover.
"#;

fn extract_conflict_labels(commit: &gix::objs::CommitRef) -> Merge<String> {
    let Some(value) = commit
        .extra_headers()
        .find(JJ_CONFLICT_LABELS_COMMIT_HEADER)
    else {
        return Merge::resolved(String::new());
    };
    str::from_utf8(value)
        .expect("labels should be valid utf8")
        .split_terminator('\n')
        .map(str::to_owned)
        .collect::<MergeBuilder<_>>()
        .build()
}

fn extract_root_tree(commit: &gix::objs::CommitRef) -> Result<Merge<TreeId>, ()> {
    let Some(value) = commit.extra_headers().find(JJ_TREES_COMMIT_HEADER) else {
        let tree_id = TreeId::from_bytes(commit.tree().as_bytes());
        return Ok(Merge::resolved(tree_id));
    };
    let hash_len = commit.tree().kind().len_in_bytes();
    let mut tree_ids = SmallVec::new();
    for hex in value.split(|b| *b == b' ') {
        let tree_id = TreeId::try_from_hex(hex).ok_or(())?;
        if tree_id.as_bytes().len() != hash_len {
            return Err(());
        }
        tree_ids.push(tree_id);
    }
    if tree_ids.len() == 1 || tree_ids.len() % 2 == 0 {
        return Err(());
    }
    Ok(Merge::from_vec(tree_ids))
}

fn signature_from_git(signature: gix::actor::SignatureRef) -> Signature {
    let name = signature.name;
    let name = if name != EMPTY_STRING_PLACEHOLDER {
        String::from_utf8_lossy(name).into_owned()
    } else {
        "".to_string()
    };
    let email = signature.email;
    let email = if email != EMPTY_STRING_PLACEHOLDER {
        String::from_utf8_lossy(email).into_owned()
    } else {
        "".to_string()
    };
    let time = signature.time().unwrap_or_default();
    let timestamp = MillisSinceEpoch(time.seconds * 1000);
    let tz_offset = time.offset.div_euclid(60);
    Signature {
        name,
        email,
        timestamp: Timestamp {
            timestamp,
            tz_offset,
        },
    }
}

fn signature_to_git(signature: &Signature) -> gix::actor::Signature {
    let name = if !signature.name.is_empty() {
        &signature.name
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    let email = if !signature.email.is_empty() {
        &signature.email
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    let time = gix::date::Time::new(
        signature.timestamp.timestamp.0.div_euclid(1000),
        signature.timestamp.tz_offset * 60,
    );
    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time,
    }
}

#[async_trait]
impl Backend for NativeBackend {
    fn name(&self) -> &str {
        BACKEND_NAME
    }

    fn commit_id_length(&self) -> usize {
        20
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        8
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let data = self.get_object(id, Kind::Blob)?;
        Ok(Box::pin(Cursor::new(data)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut bytes = Vec::new();
        contents.read_to_end(&mut bytes).await.unwrap();
        let oid = self.put(Kind::Blob, &bytes, "file")?;
        Ok(FileId::new(oid.as_bytes().to_vec()))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let data = self.get_object(id, Kind::Blob)?;
        String::from_utf8(data).map_err(|err| BackendError::InvalidUtf8 {
            object_type: id.object_type(),
            hash: id.hex(),
            source: err.utf8_error(),
        })
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let oid = self.put(Kind::Blob, target.as_bytes(), "symlink")?;
        Ok(SymlinkId::new(oid.as_bytes().to_vec()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The motte backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The motte backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "The motte backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if id == &self.empty_tree_id {
            return Ok(Tree::default());
        }
        if let Some(t) = self.tree_cache.lock().unwrap().get(id) {
            return Ok(t.clone());
        }
        let _op = crate::trace::span("jj.read_tree");
        let data = self.get_object(id, Kind::Tree)?;
        let git_tree = gix::objs::TreeRef::from_bytes(&data, HASH_KIND)
            .map_err(|e| to_read_err(e, "tree", id.hex()))?;
        let mut entries: Vec<_> = git_tree
            .entries
            .iter()
            .map(|entry| -> BackendResult<_> {
                let name =
                    RepoPathComponentBuf::new(str::from_utf8(entry.filename).map_err(|err| {
                        BackendError::InvalidUtf8 {
                            object_type: id.object_type(),
                            hash: id.hex(),
                            source: err,
                        }
                    })?)
                    .unwrap();
                let value = match entry.mode.kind() {
                    gix::object::tree::EntryKind::Tree => {
                        TreeValue::Tree(TreeId::from_bytes(entry.oid.as_bytes()))
                    }
                    gix::object::tree::EntryKind::Blob => TreeValue::File {
                        id: FileId::from_bytes(entry.oid.as_bytes()),
                        executable: false,
                        copy_id: CopyId::placeholder(),
                    },
                    gix::object::tree::EntryKind::BlobExecutable => TreeValue::File {
                        id: FileId::from_bytes(entry.oid.as_bytes()),
                        executable: true,
                        copy_id: CopyId::placeholder(),
                    },
                    gix::object::tree::EntryKind::Link => {
                        TreeValue::Symlink(SymlinkId::from_bytes(entry.oid.as_bytes()))
                    }
                    gix::object::tree::EntryKind::Commit => {
                        TreeValue::GitSubmodule(CommitId::from_bytes(entry.oid.as_bytes()))
                    }
                };
                Ok((name, value))
            })
            .try_collect()?;
        if !entries.is_sorted_by_key(|(name, _)| name) {
            entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        }
        let tree = Tree::from_sorted_entries(entries);
        self.tree_cache
            .lock()
            .unwrap()
            .put(id.clone(), tree.clone());
        Ok(tree)
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let entries = contents
            .entries()
            .map(|entry| {
                let filename = BString::from(entry.name().as_internal_str());
                let (mode, oid): (gix::object::tree::EntryKind, &[u8]) = match entry.value() {
                    TreeValue::File {
                        id,
                        executable: false,
                        copy_id: _,
                    } => (gix::object::tree::EntryKind::Blob, id.as_bytes()),
                    TreeValue::File {
                        id,
                        executable: true,
                        copy_id: _,
                    } => (gix::object::tree::EntryKind::BlobExecutable, id.as_bytes()),
                    TreeValue::Symlink(id) => (gix::object::tree::EntryKind::Link, id.as_bytes()),
                    TreeValue::Tree(id) => (gix::object::tree::EntryKind::Tree, id.as_bytes()),
                    TreeValue::GitSubmodule(id) => {
                        (gix::object::tree::EntryKind::Commit, id.as_bytes())
                    }
                };
                gix::objs::tree::Entry {
                    mode: mode.into(),
                    filename,
                    oid: gix::ObjectId::from_bytes_or_panic(oid),
                }
            })
            .sorted_unstable()
            .collect();
        let oid = self.write_tree_object(entries)?;
        Ok(TreeId::from_bytes(oid.as_bytes()))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }
        if let Some(c) = self.commit_cache.lock().unwrap().get(id) {
            return Ok(c.clone());
        }
        let data = self.get_object(id, Kind::Commit)?;
        let mut commit = self.commit_from_git(id, &data)?;
        if commit.parents.is_empty() {
            commit.parents.push(self.root_commit_id.clone());
        }
        if let Some(extra) = self.read_extra(id) {
            if !extra.change_id.is_empty() {
                commit.change_id = ChangeId::new(extra.change_id);
            }
            for p in extra.predecessors {
                commit.predecessors.push(CommitId::new(p));
            }
        }
        self.commit_cache
            .lock()
            .unwrap()
            .put(id.clone(), commit.clone());
        Ok(commit)
    }

    async fn write_commit(
        &self,
        mut contents: Commit,
        mut sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let _op = crate::trace::span("jj.write_commit");
        assert!(contents.secure_sig.is_none(), "commit.secure_sig was set");

        let tree_ids = &contents.root_tree;
        let git_tree_id = match tree_ids.as_resolved() {
            Some(tree_id) => self.oid(tree_id)?,
            None => self.write_tree_conflict(tree_ids)?,
        };
        let author = signature_to_git(&contents.author);
        let mut committer = signature_to_git(&contents.committer);
        let message = &contents.description;
        if contents.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }
        contents.parents.retain(|id| *id != self.root_commit_id);
        let mut parents = SmallVec::new();
        for parent_id in &contents.parents {
            parents.push(self.oid(parent_id)?);
        }
        if contents.parents.is_empty() {
            contents.parents.push(self.root_commit_id.clone());
        }
        let mut extra_headers: Vec<(BString, BString)> = vec![];
        if !contents.conflict_labels.is_resolved() {
            assert!(
                contents
                    .conflict_labels
                    .iter()
                    .all(|label| !label.contains('\n'))
            );
            let mut joined_with_newlines = contents.conflict_labels.iter().join("\n");
            joined_with_newlines.push('\n');
            extra_headers.push((
                JJ_CONFLICT_LABELS_COMMIT_HEADER.into(),
                joined_with_newlines.into(),
            ));
        }
        if !tree_ids.is_resolved() {
            let value = tree_ids.iter().map(|id| id.hex()).join(" ");
            extra_headers.push((JJ_TREES_COMMIT_HEADER.into(), value.into()));
        }
        if self.write_change_id_header {
            extra_headers.push((
                CHANGE_ID_COMMIT_HEADER.into(),
                contents.change_id.reverse_hex().into(),
            ));
        }

        if tree_ids.iter().any(|id| id == &self.empty_tree_id) {
            let tree_id = self.write_tree_object(vec![])?;
            assert!(tree_id.is_empty_tree());
        }

        let extras = Extra::of(&contents);

        let _lock = FileLock::lock(self.store_path.join(EXTRAS_LOCK))
            .map_err(|e| BackendError::Other(e.into()))?;
        self.extras
            .lock()
            .unwrap()
            .refresh()
            .map_err(|e| BackendError::Other(e.into()))?;
        let id = loop {
            let mut commit = gix::objs::Commit {
                message: message.to_owned().into(),
                tree: git_tree_id,
                author: author.clone(),
                committer: committer.clone(),
                encoding: None,
                parents: parents.clone(),
                extra_headers: extra_headers.clone(),
            };

            if let Some(sign) = &mut sign_with {
                let mut data = Vec::with_capacity(512);
                commit.write_to(&mut data).unwrap();
                let sig = sign(&data).map_err(|err| write_err(err, "commit"))?;
                let field = signature_field_name(HASH_KIND);
                commit
                    .extra_headers
                    .push((field.into(), sig.clone().into()));
                contents.secure_sig = Some(SecureSig { data, sig });
            }

            let mut buf = Vec::with_capacity(512);
            commit
                .write_to(&mut buf)
                .map_err(|e| write_err(e, "commit"))?;
            let git_id = self.put(Kind::Commit, &buf, "commit")?;
            let cid = CommitId::from_bytes(git_id.as_bytes());

            let existing = self.extras.lock().unwrap().map.get(&cid).cloned();
            match existing {
                Some(existing) if existing != extras => {
                    committer.time.seconds -= 1;
                }
                _ => break cid,
            }
        };

        contents.committer.timestamp.timestamp = MillisSinceEpoch(committer.time.seconds * 1000);
        self.extras
            .lock()
            .unwrap()
            .append(&id, &extras)
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok((id, contents))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(futures::stream::empty().boxed())
    }

    async fn gc(
        &self,
        _heads: &[CommitId],
        _keep_newer: std::time::SystemTime,
    ) -> BackendResult<()> {
        Ok(())
    }
}
