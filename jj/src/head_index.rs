//! The commit index of an op log head in the store, so that a restore loads the index of the head
//! instead of walking the whole op log to build it. A flush stores the segment files of the head's
//! index that the store lacks, each a `views` object under a hash of its name, and a record that
//! lists the chain of the head. A restore reads the record and the segments at once.
//!
//! jj names a segment file by the BLAKE2b-512 hash of its bytes, so a segment read back is checked
//! against its name. Only the commit segments travel: the changed-path index is an optional
//! accelerator whose format can differ between versions, and a restore links the head to the
//! commit segments alone.

use std::collections::HashSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use blake2::{Blake2b512, Digest as _};
use prost::Message as _;

use crate::ctl::{Code, CtlClient, StoreError};

/// The segments that a flush stored, one file name for each line, in the repo dir.
pub const SENT_FILE: &str = "motte_index_sent";

const RECORD_HEADER: &str = "motte head index 1";

/// The mapping of an operation to its index, as jj's default index store writes it in
/// `op_links/<operation>`. Only the commit segment is read.
#[derive(Clone, PartialEq, prost::Message)]
struct SegmentControl {
    #[prost(bytes = "vec", tag = "1")]
    commit_segment_id: Vec<u8>,
    #[prost(uint32, optional, tag = "2")]
    changed_path_start_commit_pos: Option<u32>,
    #[prost(bytes = "vec", repeated, tag = "3")]
    changed_path_segment_ids: Vec<Vec<u8>>,
}

/// The commit index of one head: the format version of its segment files, the chain of their
/// names, the head's segment first, and the view of the head, so that a restore reads the head
/// operation, its view and the segments at once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadIndex {
    pub version: u32,
    pub view: String,
    pub chain: Vec<String>,
}

impl HeadIndex {
    fn encode(&self) -> String {
        let mut body = format!(
            "{RECORD_HEADER}\nversion {}\nview {}\n",
            self.version, self.view
        );
        for s in &self.chain {
            body.push_str(&format!("segment {s}\n"));
        }
        body
    }

    fn decode(data: &[u8]) -> Option<Self> {
        let text = String::from_utf8_lossy(data);
        let mut lines = text.lines();
        if lines.next()? != RECORD_HEADER {
            return None;
        }
        let version = lines.next()?.strip_prefix("version ")?.parse().ok()?;
        let view = lines.next()?.strip_prefix("view ")?;
        if view.is_empty() || !view.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let view = view.to_owned();
        let chain: Vec<String> = lines
            .filter_map(|l| l.strip_prefix("segment "))
            .filter(|s| is_segment_name(s))
            .map(str::to_owned)
            .collect();
        (!chain.is_empty()).then_some(Self {
            version,
            view,
            chain,
        })
    }
}

fn is_segment_name(s: &str) -> bool {
    s.len() == 128 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn derived(what: &str) -> String {
    jj_lib::content_hash::blake2b_hash(&format!("motte head index {what}"))
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub fn record_name(head: &str) -> String {
    derived(&format!("record {head}"))
}

pub fn segment_name(segment: &str) -> String {
    derived(&format!("segment {segment}"))
}

fn segments_dir(index_dir: &Path) -> PathBuf {
    index_dir.join("segments")
}

/// The format version and the parent of a commit segment file, from its header.
fn segment_header(data: &[u8]) -> Option<(u32, Option<String>)> {
    let version = u32::from_le_bytes(data.get(..4)?.try_into().ok()?);
    let len = u32::from_le_bytes(data.get(4..8)?.try_into().ok()?) as usize;
    if len == 0 {
        return Some((version, None));
    }
    let parent = std::str::from_utf8(data.get(8..8 + len)?).ok()?;
    is_segment_name(parent).then(|| (version, Some(parent.to_owned())))
}

/// The format version of the commit segments that this jj writes, read from a segment of the
/// index store at `index_dir`; `None` when it holds none.
pub fn local_version(index_dir: &Path) -> Option<u32> {
    let dir = std::fs::read_dir(segments_dir(index_dir)).ok()?;
    dir.flatten()
        .filter(|e| e.file_name().to_str().is_some_and(is_segment_name))
        .find_map(|e| segment_header(&std::fs::read(e.path()).ok()?).map(|(v, _)| v))
}

/// The index of `head`, whose view is `view`, in the local index store at `index_dir`, or `None`
/// when jj wrote none for it.
pub fn local(index_dir: &Path, head: &str, view: &str) -> std::io::Result<Option<HeadIndex>> {
    let link = match std::fs::read(index_dir.join("op_links").join(head)) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let control = SegmentControl::decode(&*link)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut next = Some(hex::encode(&control.commit_segment_id));
    let (mut chain, mut version) = (vec![], None);
    while let Some(name) = next.take() {
        let data = std::fs::read(segments_dir(index_dir).join(&name))?;
        let (v, parent) = segment_header(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "bad index segment header")
        })?;
        version.get_or_insert(v);
        chain.push(name);
        next = parent;
    }
    Ok(version.map(|version| HeadIndex {
        version,
        view: view.to_owned(),
        chain,
    }))
}

fn sent(repo_path: &Path) -> HashSet<String> {
    std::fs::read_to_string(repo_path.join(SENT_FILE))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn mark_sent(repo_path: &Path, names: &[&String]) -> Result<(), StoreError> {
    if names.is_empty() {
        return Ok(());
    }
    let mut body = String::new();
    for n in names {
        body.push_str(n);
        body.push('\n');
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(repo_path.join(SENT_FILE))
        .and_then(|mut f| f.write_all(body.as_bytes()))
        .map_err(|e| StoreError::Local(format!("cannot list the stored index segments: {e}")))
}

/// Stores the segments of `index` that no earlier flush of this repo stored, then the record of
/// `head`. The record goes last, so a record in the store names only stored segments.
pub fn put(
    ctl: &CtlClient,
    index_dir: &Path,
    repo_path: &Path,
    head: &str,
    index: &HeadIndex,
) -> Result<(), StoreError> {
    let done = sent(repo_path);
    let todo: Vec<&String> = index.chain.iter().filter(|s| !done.contains(*s)).collect();
    std::thread::scope(|scope| {
        let calls: Vec<_> = todo
            .iter()
            .map(|s| {
                scope.spawn(move || {
                    let data = std::fs::read(segments_dir(index_dir).join(s)).map_err(|e| {
                        StoreError::Local(format!("cannot read index segment {s}: {e}"))
                    })?;
                    ctl.putop("views", &segment_name(s), &data)
                })
            })
            .collect();
        calls.into_iter().try_for_each(|c| {
            c.join()
                .unwrap_or_else(|_| Err(StoreError::Local("a segment upload panicked".into())))
        })
    })?;
    mark_sent(repo_path, &todo)?;
    ctl.putop("views", &record_name(head), index.encode().as_bytes())
}

/// Fails unless each segment of `index` in `dir` has the format version of `index` and names the
/// next one as its parent, and the last names none: a segment missing from the chain would make jj
/// rebuild the index from the whole op log.
fn check_chain(dir: &Path, index: &HeadIndex) -> Result<(), StoreError> {
    for (i, s) in index.chain.iter().enumerate() {
        let data = std::fs::read(dir.join(s)).map_err(|e| StoreError::Local(e.to_string()))?;
        let expected = index.chain.get(i + 1).cloned();
        if segment_header(&data) != Some((index.version, expected)) {
            return Err(StoreError::Protocol(format!(
                "index segment {s} does not continue the chain of the record"
            )));
        }
    }
    Ok(())
}

/// Removes what `download` wrote for `head` that makes jj load the index of the head, so that jj
/// builds it from the op log instead.
pub fn forget(index_dir: &Path, head: &str) {
    let _ = std::fs::remove_file(index_dir.join("op_links").join(head));
}

/// The record of `head` in the store; `None` when a flush stored none.
pub fn get(ctl: &CtlClient, head: &str) -> Result<Option<HeadIndex>, StoreError> {
    match ctl.getop("views", &record_name(head)) {
        Ok(data) => Ok(HeadIndex::decode(&data)),
        Err(e) if e.is(&Code::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Reads the segments of `index` from the store on `width` connections at once, checks each
/// against its name, writes them into the index store at `index_dir`, and links `head` to the
/// commit segments alone. On success the index of `head` loads without a walk of the op log.
pub fn download(
    ctl: &CtlClient,
    index_dir: &Path,
    repo_path: &Path,
    head: &str,
    index: &HeadIndex,
    width: usize,
) -> Result<(), StoreError> {
    let dir = segments_dir(index_dir);
    std::fs::create_dir_all(&dir).map_err(|e| StoreError::Local(e.to_string()))?;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failed = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..width.max(1).min(index.chain.len()) {
            scope.spawn(|| {
                while let Some(s) = index
                    .chain
                    .get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
                {
                    let got = ctl.getop("views", &segment_name(s)).and_then(|data| {
                        if hex::encode(Blake2b512::digest(&data)) != *s {
                            return Err(StoreError::Protocol(format!(
                                "the daemon sent bytes that are not index segment {s}"
                            )));
                        }
                        std::fs::write(dir.join(s), data)
                            .map_err(|e| StoreError::Local(e.to_string()))
                    });
                    if let Err(e) = got {
                        failed.lock().unwrap().get_or_insert(e);
                    }
                }
            });
        }
    });
    if let Some(e) = failed.into_inner().unwrap() {
        return Err(e);
    }
    check_chain(&dir, index)?;
    let control = SegmentControl {
        commit_segment_id: hex::decode(&index.chain[0])
            .map_err(|e| StoreError::Protocol(e.to_string()))?,
        changed_path_start_commit_pos: None,
        changed_path_segment_ids: vec![],
    };
    let links = index_dir.join("op_links");
    std::fs::create_dir_all(&links).map_err(|e| StoreError::Local(e.to_string()))?;
    std::fs::write(links.join(head), control.encode_to_vec())
        .map_err(|e| StoreError::Local(e.to_string()))?;
    mark_sent(repo_path, &index.chain.iter().collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Catches a record that does not survive its encoding, and one whose chain names a file that
    /// is no segment name, which a restore would then write outside the segment directory.
    #[test]
    fn a_record_reads_back_and_drops_bad_names() {
        let index = HeadIndex {
            version: 6,
            view: "ef".repeat(64),
            chain: vec!["ab".repeat(64), "cd".repeat(64)],
        };
        let mut body = index.encode();
        assert_eq!(HeadIndex::decode(body.as_bytes()), Some(index));
        body.push_str("segment ../x\n");
        assert_eq!(HeadIndex::decode(body.as_bytes()).unwrap().chain.len(), 2);
    }
}
