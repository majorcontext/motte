//! The snapshot of overlay mode. The daemon lists what differs between the mount and its base. The
//! shim settles those paths, and the paths where the last snapshot tree differs from the base, with
//! jj's own rules, and reads a file again only when its `lstat` changed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

use futures::StreamExt as _;
use jj_lib::backend::{MergedTreeValueExt as _, TreeId};
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merged_tree::{MergedTree, TreeDiffEntry};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::working_copy::{SnapshotError, SnapshotOptions, SnapshotStats};

use super::cache::FileCache;
use super::{
    Built, FORCE_FULL, Item, LAST_SNAPSHOT, LockedVfsWorkingCopy, is_reserved, now_ns, snap_err,
};
use crate::ctl::{DirtyEntry, DirtyReply};

fn removed_by_ancestor(items: &BTreeMap<String, Item>, path: &str) -> bool {
    path.match_indices('/').any(|(i, _)| {
        items.get(&path[..i]).is_some_and(|it| {
            matches!(it.kind.as_str(), "deleted" | "file" | "symlink")
                || (it.kind == "dir" && it.opaque)
        })
    })
}

impl LockedVfsWorkingCopy {
    pub(super) async fn snapshot_overlay(
        &mut self,
        options: &SnapshotOptions<'_>,
        reply: DirtyReply,
        op: &mut crate::trace::Op,
        t_all: Instant,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        let start_ns = now_ns();
        let base_id = TreeId::try_from_hex(&reply.base_tree)
            .filter(|_| reply.base_tree.len() == 40)
            .ok_or_else(|| snap_err("the daemon names no base tree", reply.base_tree.clone()))?;
        let base = MergedTree::resolved(self.store.clone(), base_id);
        let n_entries = reply.entries.len();
        let items = self.overlay_items(&base, reply.entries).await?;
        op.set("dirty_files", n_entries as i64);
        LAST_SNAPSHOT[0].store(n_entries as u64, Ordering::Relaxed);
        LAST_SNAPSHOT[1].store(0, Ordering::Relaxed);
        let loaded = FileCache::load(&self.state_path, &reply.base_tree, &self.st.mount);
        let full = FORCE_FULL.load(Ordering::Relaxed);
        let built = self
            .build_tree(options, &items, Some(&loaded), !full, start_ns)
            .await?;
        let Built {
            tree,
            stats,
            files,
            bytes,
            stamps,
            timing,
        } = built;
        let next = loaded.with_files(stamps);
        if next != loaded
            && let Err(e) = next.save(&self.state_path)
        {
            eprintln!("warning: cannot write the snapshot cache: {e}");
        }
        if tree.tree_ids_and_labels() != self.tree.tree_ids_and_labels()
            || !self.st.unwritten.is_empty()
        {
            self.dirty = true;
        }
        self.tree = tree.clone();
        self.st.unwritten.clear();
        self.prune_conflict_markers().await?;
        op.set("bytes_read", bytes as i64);
        LAST_SNAPSHOT[1].store(bytes, Ordering::Relaxed);
        super::trace_line_if(format!(
            "snapshot(overlay): dirty_entries={n_entries} candidates={} files_read={files} bytes={bytes} {timing} total={:?}",
            items.len(),
            t_all.elapsed()
        ));
        Ok((tree, stats))
    }

    /// Every path to settle: the dirty set, and each path where the last snapshot tree differs from
    /// the base or that a checkout left unwritten. A path outside the dirty set holds its base
    /// entry, unless an ancestor in the dirty set removed the base subtree.
    async fn overlay_items(
        &self,
        base: &MergedTree,
        entries: Vec<DirtyEntry>,
    ) -> Result<BTreeMap<String, Item>, SnapshotError> {
        let mut items: BTreeMap<String, Item> = BTreeMap::new();
        for e in entries.into_iter().filter(|e| !is_reserved(&e.path)) {
            items.insert(
                e.path,
                Item {
                    kind: e.kind,
                    opaque: e.opaque,
                    known: None,
                },
            );
        }
        let mut candidates: BTreeSet<String> = self.st.unwritten.clone();
        let mut diff = self.tree.diff(base, &EverythingMatcher);
        while let Some(TreeDiffEntry { path, values }) = diff.next().await {
            values?;
            candidates.insert(path.as_internal_file_string().to_owned());
        }
        drop(diff);
        let mut settled: BTreeMap<String, Item> = BTreeMap::new();
        for path in candidates {
            if is_reserved(&path) || items.contains_key(&path) {
                continue;
            }
            let item = if removed_by_ancestor(&items, &path) {
                Item::of("deleted")
            } else {
                let rp = RepoPathBuf::from_internal_string(path.clone())
                    .map_err(|e| snap_err(format!("invalid path {path:?}"), e))?;
                if self.tree.path_value(&rp).await?.as_resolved().is_none() {
                    settled.insert(path, Item::of("file"));
                    continue;
                }
                let value = base.path_value(&rp).await?;
                if value.is_file_like() {
                    Item {
                        known: Some(value),
                        ..Item::of("base")
                    }
                } else if value.is_absent() {
                    Item::of("deleted")
                } else if value.is_tree() {
                    Item::of("dir")
                } else {
                    continue;
                }
            };
            settled.insert(path, item);
        }
        items.extend(settled);
        Ok(items)
    }
}
