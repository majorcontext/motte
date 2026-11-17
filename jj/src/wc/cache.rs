//! Sidecar of the overlay snapshot: the blob of each regular file that an earlier snapshot read,
//! keyed by what `lstat` shows. A file whose stamp still matches is not read again.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const FILE: &str = "motte_wc_cache.json";
const VERSION: u32 = 1;
/// A file that changed this recently can change again inside the timestamp granularity.
const RACY_NS: i128 = 50_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Stamp {
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    ino: u64,
    mode: u32,
    sha: String,
}

fn ns(sec: i64, nsec: i64) -> i64 {
    sec.saturating_mul(1_000_000_000).saturating_add(nsec)
}

impl Stamp {
    pub(crate) fn of(md: &fs::Metadata, sha: String) -> Self {
        Self {
            size: md.len(),
            mtime_ns: ns(md.mtime(), md.mtime_nsec()),
            ctime_ns: ns(md.ctime(), md.ctime_nsec()),
            ino: md.ino(),
            mode: md.mode(),
            sha,
        }
    }

    fn matches(&self, md: &fs::Metadata) -> bool {
        Self::of(md, String::new()).eq_ignoring_sha(self)
    }

    fn eq_ignoring_sha(&self, o: &Self) -> bool {
        (self.size, self.mtime_ns, self.ctime_ns, self.ino, self.mode)
            == (o.size, o.mtime_ns, o.ctime_ns, o.ino, o.mode)
    }

    /// Whether a file read at `start_ns` may be remembered.
    pub(crate) fn cacheable(md: &fs::Metadata, start_ns: i128) -> bool {
        let newest = i128::from(ns(md.mtime(), md.mtime_nsec()))
            .max(i128::from(ns(md.ctime(), md.ctime_nsec())));
        newest + RACY_NS <= start_ns
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileCache {
    version: u32,
    base_tree: String,
    mount: PathBuf,
    files: BTreeMap<String, Stamp>,
}

impl FileCache {
    /// The cache of `state`, or an empty one for this base and mount when the file is absent,
    /// unreadable, of another version, or of another base or mount.
    pub(crate) fn load(state: &Path, base_tree: &str, mount: &Path) -> Self {
        let empty = Self {
            version: VERSION,
            base_tree: base_tree.to_owned(),
            mount: mount.to_owned(),
            files: BTreeMap::new(),
        };
        fs::read(state.join(FILE))
            .ok()
            .and_then(|b| serde_json::from_slice::<Self>(&b).ok())
            .filter(|c| c.version == VERSION && c.base_tree == base_tree && c.mount == mount)
            .unwrap_or(empty)
    }

    pub(crate) fn with_files(&self, files: BTreeMap<String, Stamp>) -> Self {
        Self {
            files,
            ..self.clone()
        }
    }

    /// The blob that the stamp of `path` recorded, whatever the file shows now.
    pub(crate) fn sha(&self, path: &str) -> Option<&str> {
        self.files.get(path).map(|s| s.sha.as_str())
    }

    /// The blob of `path` when `md` is what the stamp recorded.
    pub(crate) fn hit(&self, path: &str, md: &fs::Metadata) -> Option<&str> {
        self.files
            .get(path)
            .filter(|s| s.matches(md))
            .map(|s| s.sha.as_str())
    }

    pub(crate) fn save(&self, state: &Path) -> io::Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(state)?;
        tmp.write_all(&serde_json::to_vec(self).map_err(io::Error::other)?)?;
        tmp.persist(state.join(FILE)).map_err(|e| e.error)?;
        Ok(())
    }
}

pub(crate) fn discard(state: &Path) {
    let _ = fs::remove_file(state.join(FILE));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rewrite_of_the_same_size_and_mtime_is_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("f");
        fs::write(&f, "one").unwrap();
        let md = fs::symlink_metadata(&f).unwrap();
        let cache =
            FileCache::default().with_files([("f".into(), Stamp::of(&md, "aa".into()))].into());
        assert_eq!(cache.hit("f", &md), Some("aa"));
        std::thread::sleep(std::time::Duration::from_millis(20));
        fs::write(&f, "two").unwrap();
        let file = fs::OpenOptions::new().write(true).open(&f).unwrap();
        file.set_modified(md.modified().unwrap()).unwrap();
        let again = fs::symlink_metadata(&f).unwrap();
        assert_eq!(
            (md.len(), md.mtime_nsec()),
            (again.len(), again.mtime_nsec())
        );
        assert_eq!(cache.hit("f", &again), None);
    }

    #[test]
    fn another_base_or_mount_discards_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let md = fs::symlink_metadata(dir.path()).unwrap();
        let c = FileCache::load(dir.path(), "b1", Path::new("/m"))
            .with_files([("f".into(), Stamp::of(&md, "aa".into()))].into());
        c.save(dir.path()).unwrap();
        assert_eq!(FileCache::load(dir.path(), "b1", Path::new("/m")), c);
        assert!(
            FileCache::load(dir.path(), "b2", Path::new("/m"))
                .files
                .is_empty()
        );
        assert!(
            FileCache::load(dir.path(), "b1", Path::new("/n"))
                .files
                .is_empty()
        );
        fs::write(dir.path().join(FILE), b"{").unwrap();
        assert!(
            FileCache::load(dir.path(), "b1", Path::new("/m"))
                .files
                .is_empty()
        );
    }
}
