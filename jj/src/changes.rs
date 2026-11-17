//! The work-tree diff of `jj motte changes`: the daemon's view against a jj tree, without a
//! snapshot. The view is the daemon's base tree plus its overlay, so the diff of a target tree is
//! the jj diff of the target against the base, overridden by the files the daemon reports.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::sync::Arc;

use bstr::BStr;
use futures::StreamExt as _;
use jj_lib::backend::{MergedTreeValueExt as _, TreeId, TreeValue};
use jj_lib::conflicts::{ConflictMaterializeOptions, materialize_tree_value};
use jj_lib::diff_presentation::LineCompareMode;
use jj_lib::diff_presentation::unified::{DiffLineType, git_diff_part, unified_diff_hunks};
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merge::Diff;
use jj_lib::merged_tree::{MergedTree, TreeDiffEntry};
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::store::Store;
use jj_lib::working_copy::SnapshotOptions;
use serde::Serialize;

use crate::ctl::{ChangeEntry, ChangesReply, CtlClient, hash_object};
use crate::wc::IgnoreCtx;

/// The patch cap when the caller names none.
pub const MAX_PATCH: usize = 1 << 20;
/// A file this large gets no counts and no hunk.
pub const LARGE: u64 = 2 * 1024 * 1024;
const CONTEXT: usize = 3;
const BINARY_PROBE: usize = 8000;
const GITLINK: u32 = 0o160000;
const CHUNK: usize = 32;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ChangedFile {
    pub path: String,
    pub status: &'static str,
    pub additions: usize,
    pub deletions: usize,
    pub binary: bool,
    pub large: bool,
}

#[derive(Clone, Debug)]
enum Side {
    Absent,
    /// A blob of a tree, read through the daemon.
    Blob {
        id: gix::ObjectId,
        mode: u32,
        size: Option<u64>,
    },
    /// A file of the view, read from the work tree.
    Disk {
        id: gix::ObjectId,
        mode: u32,
        size: u64,
    },
    Conflict(jj_lib::backend::MergedTreeValue),
}

impl Side {
    fn key(&self) -> Option<(gix::ObjectId, u32)> {
        match self {
            Side::Blob { id, mode, .. } | Side::Disk { id, mode, .. } => Some((*id, *mode)),
            _ => None,
        }
    }

    fn mode(&self) -> Option<u32> {
        match self {
            Side::Absent => None,
            Side::Blob { mode, .. } | Side::Disk { mode, .. } => Some(*mode),
            Side::Conflict(_) => Some(0o100644),
        }
    }

    fn hash(&self) -> String {
        self.key()
            .map_or_else(|| "0".repeat(7), |(id, _)| id.to_string()[..7].to_owned())
    }

    fn from_tree(value: jj_lib::backend::MergedTreeValue) -> Self {
        let Some(resolved) = value.as_resolved() else {
            return Side::Conflict(value);
        };
        let (id, mode) = match resolved {
            Some(TreeValue::File { id, executable, .. }) => {
                (id.as_bytes(), if *executable { 0o100755 } else { 0o100644 })
            }
            Some(TreeValue::Symlink(id)) => (id.as_bytes(), 0o120000),
            Some(TreeValue::GitSubmodule(id)) => (id.as_bytes(), 0o160000),
            _ => return Side::Absent,
        };
        match gix::ObjectId::try_from(id) {
            Ok(id) => Side::Blob {
                id,
                mode,
                size: None,
            },
            Err(_) => Side::Absent,
        }
    }

    /// The base side of a daemon entry.
    fn base(c: &ChangeEntry) -> Self {
        match c
            .old_sha
            .as_deref()
            .map(|s| gix::ObjectId::from_hex(s.as_bytes()))
        {
            Some(Ok(id)) => Side::Blob {
                id,
                mode: c.old_mode,
                size: Some(c.old_size),
            },
            _ => Side::Absent,
        }
    }

    /// The view side of a daemon entry. The daemon names only a modified file; `read_disk`
    /// names the rest.
    fn view(c: &ChangeEntry) -> Self {
        if c.new_mode == 0 {
            return Side::Absent;
        }
        let id = c
            .new_sha
            .as_deref()
            .and_then(|s| gix::ObjectId::from_hex(s.as_bytes()).ok())
            .unwrap_or_else(|| gix::ObjectId::null(crate::ctl::HASH_KIND));
        Side::Disk {
            id,
            mode: c.new_mode,
            size: c.size,
        }
    }
}

/// What the diff reads besides the view: the store, the daemon, the work tree root, the conflicts
/// of the working-copy commit and how to materialize a conflicted side.
pub struct DiffInput<'a> {
    pub store: &'a Arc<Store>,
    /// The rules of a jj snapshot for a new file: ignores, auto-track and the size limit.
    pub snapshot: &'a SnapshotOptions<'a>,
    pub ctl: &'a CtlClient,
    pub root: &'a Path,
    pub conflicts: &'a HashSet<String>,
    /// The tree of the working-copy commit, which says what the working copy tracks.
    pub wc_tree: &'a MergedTree,
    pub materialize: &'a ConflictMaterializeOptions,
}

/// Diffs `target` against the view. The file list is complete; the patch ends at the last whole
/// file within `cap`, and the bool reports a cut. Files go in chunks of `CHUNK`, so at most one
/// chunk of contents is held at a time.
pub async fn diff_view(
    input: &DiffInput<'_>,
    target: &MergedTree,
    view: &ChangesReply,
    cap: usize,
) -> Result<(Vec<ChangedFile>, String, bool), BoxError> {
    let mut pairs: Vec<(String, (Side, Side))> =
        pairs(input, target, view).await?.into_iter().collect();
    let mut out = (vec![], String::new(), false);
    for chunk in pairs.chunks_mut(CHUNK) {
        diff_chunk(input, target, chunk, cap, &mut out).await?;
    }
    Ok(out)
}

async fn diff_chunk(
    input: &DiffInput<'_>,
    target: &MergedTree,
    chunk: &mut [(String, (Side, Side))],
    cap: usize,
    (files, patch, truncated): &mut (Vec<ChangedFile>, String, bool),
) -> Result<(), BoxError> {
    let mut disk: HashMap<String, Vec<u8>> = HashMap::new();
    for (path, (old, new)) in chunk.iter_mut() {
        if let Some(data) = read_disk(input.root, path, old, new)? {
            disk.insert(path.clone(), data);
        }
    }
    let changed = |old: &Side, new: &Side| match (old.key(), new.key()) {
        (Some(a), Some(b)) => a != b,
        _ => !matches!((old, new), (Side::Absent, Side::Absent)),
    };
    let mut want: Vec<gix::ObjectId> = chunk
        .iter()
        .filter(|(_, (o, n))| changed(o, n))
        .flat_map(|(_, (o, n))| [o, n])
        .filter_map(|s| match s {
            Side::Blob { id, mode, size }
                if *mode != GITLINK && !size.is_some_and(|n| n > LARGE) =>
            {
                Some(*id)
            }
            _ => None,
        })
        .collect();
    want.sort_unstable();
    want.dedup();
    let blobs: HashMap<gix::ObjectId, Option<Vec<u8>>> = want
        .iter()
        .copied()
        .zip(input.ctl.get_blobs(&want, LARGE)?)
        .collect();
    for (path, (old, new)) in chunk.iter() {
        if !changed(old, new) {
            continue;
        }
        let status = if input.conflicts.contains(path) {
            "conflicted"
        } else if matches!(old, Side::Absent) {
            "added"
        } else if matches!(new, Side::Absent) {
            "deleted"
        } else {
            "modified"
        };
        let mut file = ChangedFile {
            path: path.clone(),
            status,
            additions: 0,
            deletions: 0,
            binary: false,
            large: false,
        };
        let before = content(input, target, path, old, &blobs, &disk).await?;
        let after = content(input, target, path, new, &blobs, &disk).await?;
        let (Some(before), Some(after)) = (before, after) else {
            file.large = true;
            files.push(file);
            continue;
        };
        if before == after && old.mode() == new.mode() {
            continue;
        }
        let text = render(&mut file, old, new, &before, &after);
        files.push(file);
        if *truncated || patch.len() + text.len() > cap {
            *truncated = true;
        } else {
            patch.push_str(&text);
        }
    }
    Ok(())
}

/// The old and new side of every path that the target and the view may disagree on.
async fn pairs(
    input: &DiffInput<'_>,
    target: &MergedTree,
    view: &ChangesReply,
) -> Result<BTreeMap<String, (Side, Side)>, BoxError> {
    let base_id = TreeId::try_from_hex(&view.base_tree).ok_or("bad base tree id")?;
    let base = MergedTree::resolved(input.store.clone(), base_id);
    let mut pairs = BTreeMap::new();
    let mut diff = target.diff(&base, &EverythingMatcher);
    while let Some(TreeDiffEntry { path, values }) = diff.next().await {
        let values = values?;
        pairs.insert(
            path.as_internal_file_string().to_owned(),
            (
                Side::from_tree(values.before),
                Side::from_tree(values.after),
            ),
        );
    }
    drop(diff);
    let mut ignore = IgnoreCtx::new(input.root, input.snapshot.base_ignores.clone());
    for c in &view.changes {
        let old = match pairs.remove(&c.path) {
            Some((old, _)) => old,
            None => Side::base(c),
        };
        let mut new = Side::view(c);
        if matches!(new, Side::Disk { .. }) && !tracks_new(input, &mut ignore, c).await? {
            new = Side::Absent;
        }
        pairs.insert(c.path.clone(), (old, new));
    }
    Ok(pairs)
}

/// Whether the working copy tracks the view file of `c`: the working-copy commit holds it, or a
/// jj snapshot would start tracking it.
async fn tracks_new(
    input: &DiffInput<'_>,
    ignore: &mut IgnoreCtx<'_>,
    c: &ChangeEntry,
) -> Result<bool, BoxError> {
    let rp = RepoPathBuf::from_internal_string(&c.path)?;
    if input.wc_tree.path_value(&rp).await?.is_file_like() {
        return Ok(true);
    }
    let rules = input.snapshot;
    let regular = c.new_mode & 0o170000 == 0o100000;
    Ok(!ignore.is_ignored(&rp)?
        && rules.start_tracking_matcher.matches(&rp)
        && !(regular && c.size > rules.max_new_file_size))
}

/// Reads a work-tree side that is not large, and names it by what it read, because the file can
/// change after the daemon hashed it. A large side the daemon left unnamed is hashed as a stream
/// when the old side is a blob it may equal. A file gone since is absent.
fn read_disk(
    root: &Path,
    path: &str,
    old: &Side,
    side: &mut Side,
) -> Result<Option<Vec<u8>>, BoxError> {
    let Side::Disk { mode, size, .. } = *side else {
        return Ok(None);
    };
    let p = root.join(path);
    let gone = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;
    let read = if size > LARGE {
        Ok(None)
    } else if mode == 0o120000 {
        std::fs::read_link(&p).map(|t| Some(t.as_os_str().as_bytes().to_vec()))
    } else {
        read_capped(&p).map(|d| (d.len() as u64 <= LARGE).then_some(d))
    };
    match read {
        Ok(Some(data)) => {
            let id = hash_object(gix::objs::Kind::Blob, &data);
            *side = Side::Disk {
                id,
                mode,
                size: data.len() as u64,
            };
            Ok(Some(data))
        }
        Ok(None) => {
            hash_large(&p, path, old, side)?;
            Ok(None)
        }
        Err(e) if gone(&e) => {
            *side = Side::Absent;
            Ok(None)
        }
        Err(e) => Err(format!("read {path}: {e}").into()),
    }
}

/// Names a large view file by its whole content when it may equal the old blob, and otherwise
/// leaves it unnamed. A file whose length differs from a known old size cannot equal it.
fn hash_large(p: &Path, path: &str, old: &Side, side: &mut Side) -> Result<(), BoxError> {
    let Side::Disk { mode, .. } = *side else {
        return Ok(());
    };
    let unnamed = (gix::ObjectId::null(crate::ctl::HASH_KIND), LARGE + 1);
    let named = match old {
        Side::Blob { size, .. } if mode != 0o120000 => match std::fs::metadata(p) {
            Ok(md) if size.is_some_and(|n| n != md.len()) => Ok(unnamed),
            Ok(_) => hash_stream(p),
            Err(e) => Err(e),
        },
        _ => Ok(unnamed),
    };
    let (id, size) = match named {
        Ok(named) => named,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            *side = Side::Absent;
            return Ok(());
        }
        Err(e) => return Err(format!("read {path}: {e}").into()),
    };
    *side = Side::Disk { id, mode, size };
    Ok(())
}

/// The file at `p`, cut after `LARGE + 1` bytes, so a file that grew past `LARGE` reads as large.
fn read_capped(p: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut data = vec![];
    std::fs::File::open(p)?
        .take(LARGE + 1)
        .read_to_end(&mut data)?;
    Ok(data)
}

/// The git blob name and size of the file at `p`, read in pieces; unnamed when its size moved
/// or it ended early during the read.
fn hash_stream(p: &Path) -> std::io::Result<(gix::ObjectId, u64)> {
    use std::io::Read as _;
    let mut f = std::fs::File::open(p)?;
    let len = f.metadata()?.len();
    let mut h = gix::hash::hasher(crate::ctl::HASH_KIND);
    h.update(format!("blob {len}\0").as_bytes());
    let mut buf = vec![0; 1 << 16];
    let mut n = 0u64;
    loop {
        let k = f.read(&mut buf)?;
        if k == 0 || n == len {
            break;
        }
        let k = k.min((len - n) as usize);
        h.update(&buf[..k]);
        n += k as u64;
    }
    if n != len || f.metadata()?.len() != len {
        return Ok((gix::ObjectId::null(crate::ctl::HASH_KIND), len));
    }
    let id = h
        .try_finalize()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok((id, len))
}

/// The content of one side, or `None` when it is too large to diff.
async fn content(
    input: &DiffInput<'_>,
    target: &MergedTree,
    path: &str,
    side: &Side,
    blobs: &HashMap<gix::ObjectId, Option<Vec<u8>>>,
    disk: &HashMap<String, Vec<u8>>,
) -> Result<Option<Vec<u8>>, BoxError> {
    let data = match side {
        Side::Absent => vec![],
        Side::Blob { id, mode, .. } if *mode == GITLINK => {
            format!("Subproject commit {id}\n").into()
        }
        Side::Blob { id, .. } => match blobs.get(id) {
            Some(Some(d)) => d.clone(),
            _ => return Ok(None),
        },
        Side::Disk { .. } => match disk.get(path) {
            Some(d) => d.clone(),
            None => return Ok(None),
        },
        Side::Conflict(value) => {
            let repo_path = RepoPathBuf::from_internal_string(path)?;
            let m = materialize_tree_value(input.store, &repo_path, value.clone(), target.labels())
                .await?;
            git_diff_part(&repo_path, m, input.materialize)
                .await?
                .content
                .contents
                .to_vec()
        }
    };
    Ok((data.len() as u64 <= LARGE).then_some(data))
}

/// Quotes a path the way git does with `core.quotePath`: a path with a quote, a backslash, a
/// control byte or a byte above 0x7f is written in C quotes with octal escapes.
fn quote(prefix: &str, path: &str) -> String {
    let full = format!("{prefix}{path}");
    if !full
        .bytes()
        .any(|b| !(0x20..0x7f).contains(&b) || b == b'"' || b == b'\\')
    {
        return full;
    }
    let mut out = String::from("\"");
    for b in full.bytes() {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\t' => out.push_str("\\t"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            0x0c => out.push_str("\\f"),
            b'\r' => out.push_str("\\r"),
            0x0b => out.push_str("\\v"),
            0x20..0x7f => out.push(b as char),
            _ => out.push_str(&format!("\\{b:03o}")),
        }
    }
    out.push('"');
    out
}

/// Renders the git patch of one file and sets its counts.
fn render(file: &mut ChangedFile, old: &Side, new: &Side, before: &[u8], after: &[u8]) -> String {
    if let (Some(a), Some(b)) = (old.mode(), new.mode())
        && a & 0o170000 != b & 0o170000
    {
        let gone = render(file, old, &Side::Absent, before, &[]);
        return gone + &render(file, &Side::Absent, new, &[], after);
    }
    let p = &file.path;
    let mut out = format!("diff --git {} {}\n", quote("a/", p), quote("b/", p));
    match (old.mode(), new.mode()) {
        (None, Some(m)) => out.push_str(&format!("new file mode {m:06o}\n")),
        (Some(m), None) => out.push_str(&format!("deleted file mode {m:06o}\n")),
        (Some(a), Some(b)) if a != b => {
            out.push_str(&format!("old mode {a:06o}\nnew mode {b:06o}\n"))
        }
        _ => {}
    }
    let index = format!("index {}..{}", old.hash(), new.hash());
    if before == after {
        if old.mode().is_none() || new.mode().is_none() {
            out.push_str(&index);
            out.push('\n');
        }
        return out;
    }
    out.push_str(&index);
    match (old.mode(), new.mode()) {
        (Some(a), Some(b)) if a == b => out.push_str(&format!(" {a:06o}\n")),
        _ => out.push('\n'),
    }
    let a = if old.mode().is_some() {
        quote("a/", p)
    } else {
        "/dev/null".into()
    };
    let b = if new.mode().is_some() {
        quote("b/", p)
    } else {
        "/dev/null".into()
    };
    let probe = |d: &[u8]| d[..d.len().min(BINARY_PROBE)].contains(&0);
    if probe(before) || probe(after) {
        file.binary = true;
        out.push_str(&format!("Binary files {a} and {b} differ\n"));
        return out;
    }
    out.push_str(&format!("--- {a}\n+++ {b}\n"));
    let contents = Diff::new(BStr::new(before), BStr::new(after));
    for hunk in unified_diff_hunks(contents, CONTEXT, LineCompareMode::Exact) {
        let range = |r: &std::ops::Range<usize>| match r.len() {
            0 => format!("{},0", r.start),
            1 => format!("{}", r.start + 1),
            n => format!("{},{n}", r.start + 1),
        };
        out.push_str(&format!(
            "@@ -{} +{} @@\n",
            range(&hunk.left_line_range),
            range(&hunk.right_line_range)
        ));
        for (kind, tokens) in &hunk.lines {
            let sigil = match kind {
                DiffLineType::Context => ' ',
                DiffLineType::Removed => {
                    file.deletions += 1;
                    '-'
                }
                DiffLineType::Added => {
                    file.additions += 1;
                    '+'
                }
            };
            out.push(sigil);
            let line: Vec<u8> = tokens.iter().flat_map(|(_, t)| t.iter().copied()).collect();
            out.push_str(&String::from_utf8_lossy(&line));
            if !line.ends_with(b"\n") {
                out.push_str("\n\\ No newline at end of file\n");
            }
        }
    }
    out
}
