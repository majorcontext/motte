use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use gix::objs::{Kind, Write as _};
use gix::odb::pack::Find as _;
use jj_lib::backend::CommitId;
use jj_lib::index::Index;
use motte_jj::ctl::{CtlClient, HASH_KIND, ObjectStore as _, StoreError};

use crate::push::{Fail, Outcome};

/// Largest blob that one `getbatch` may carry; a larger blob is read alone.
const BATCH_MAX: u64 = 1 << 30;

/// The commits to write, parents first, and the commits that origin has and the new ones sit on.
pub struct Plan {
    pub commits: Vec<gix::ObjectId>,
    pub base: BTreeSet<gix::ObjectId>,
}

pub struct Copied {
    pub objects: u64,
    pub bytes: u64,
}

fn store_fail(e: StoreError) -> Fail {
    Fail::new(
        Outcome::Network,
        format!("the daemon cannot serve the commits: {e}"),
    )
}

fn commit_parents(
    ctl: &CtlClient,
    id: &gix::ObjectId,
) -> Result<(gix::ObjectId, Vec<gix::ObjectId>), Fail> {
    let (_, raw) = ctl.get(id).map_err(store_fail)?;
    let commit = gix::objs::CommitRef::from_bytes(&raw, HASH_KIND)
        .map_err(|e| Fail::new(Outcome::Network, format!("bad commit {id}: {e}")))?;
    Ok((commit.tree(), commit.parents().collect()))
}

/// Walks from `targets` to the commits the transport repo holds or that lie in the history of
/// `start`, which origin has.
pub async fn plan(
    ctl: &CtlClient,
    odb: &gix::odb::Handle,
    index: &dyn Index,
    start: &CommitId,
    targets: &[gix::ObjectId],
) -> Result<Plan, Fail> {
    let (mut seen, mut plan) = (
        HashSet::new(),
        Plan {
            commits: vec![],
            base: BTreeSet::new(),
        },
    );
    let mut stack: Vec<(gix::ObjectId, bool)> = targets.iter().map(|t| (*t, false)).collect();
    while let Some((id, expanded)) = stack.pop() {
        if expanded {
            plan.commits.push(id);
            continue;
        }
        if odb.contains(&id) || !seen.insert(id) {
            continue;
        }
        stack.push((id, true));
        for parent in commit_parents(ctl, &id)?.1 {
            let jj_id = CommitId::from_bytes(parent.as_bytes());
            if odb.contains(&parent) {
                continue;
            }
            if index.is_ancestor(&jj_id, start).await.unwrap_or(false) {
                plan.base.insert(parent);
            } else {
                stack.push((parent, false));
            }
        }
    }
    Ok(plan)
}

/// Writes the commits of `plan` and the trees and blobs that differ from the first parent into
/// the transport repo, children before parents, so a tree on disk has all of its children.
pub fn copy(ctl: &CtlClient, objects_dir: &Path, plan: &Plan) -> Result<Copied, Fail> {
    let loose = gix::odb::loose::Store::at(objects_dir, HASH_KIND);
    let odb = gix::odb::at(objects_dir, HASH_KIND)
        .map_err(|e| Fail::new(Outcome::Network, e.to_string()))?;
    let mut copier = Copier {
        ctl,
        loose,
        odb,
        done: Copied {
            objects: 0,
            bytes: 0,
        },
    };
    let mut trees: HashMap<gix::ObjectId, gix::ObjectId> = HashMap::new();
    for id in &plan.commits {
        let (tree, parents) = commit_parents(ctl, id)?;
        let old = match parents.first() {
            Some(p) => match trees.get(p) {
                Some(t) => Some(*t),
                None => Some(commit_parents(ctl, p)?.0),
            },
            None => None,
        };
        copier.tree(tree, old)?;
        let (_, raw) = ctl.get(id).map_err(store_fail)?;
        copier.write(Kind::Commit, &raw)?;
        trees.insert(*id, tree);
    }
    Ok(copier.done)
}

struct Copier<'a> {
    ctl: &'a CtlClient,
    loose: gix::odb::loose::Store,
    odb: gix::odb::Handle,
    done: Copied,
}

impl Copier<'_> {
    fn write(&mut self, kind: Kind, data: &[u8]) -> Result<(), Fail> {
        self.loose
            .write_buf(kind, data)
            .map_err(|e| Fail::new(Outcome::Network, format!("cannot write an object: {e}")))?;
        self.done.objects += 1;
        self.done.bytes += data.len() as u64;
        Ok(())
    }

    fn tree(&mut self, id: gix::ObjectId, old: Option<gix::ObjectId>) -> Result<(), Fail> {
        if self.odb.contains(&id) {
            return Ok(());
        }
        let (_, raw) = self.ctl.get(&id).map_err(store_fail)?;
        let old_raw = match old {
            Some(o) => Some(self.ctl.get(&o).map_err(store_fail)?.1),
            None => None,
        };
        let old_entries: HashMap<&gix::bstr::BStr, (gix::objs::tree::EntryMode, gix::ObjectId)> =
            old_raw
                .as_deref()
                .and_then(|r| gix::objs::TreeRef::from_bytes(r, HASH_KIND).ok())
                .map(|t| {
                    t.entries
                        .iter()
                        .map(|e| (e.filename, (e.mode, e.oid.to_owned())))
                        .collect()
                })
                .unwrap_or_default();
        let tree = gix::objs::TreeRef::from_bytes(&raw, HASH_KIND)
            .map_err(|e| Fail::new(Outcome::Network, format!("bad tree {id}: {e}")))?;
        let mut blobs = vec![];
        for entry in &tree.entries {
            let prior = old_entries.get(entry.filename);
            let oid = entry.oid.to_owned();
            if prior.is_some_and(|(_, o)| *o == oid) {
                continue;
            }
            if entry.mode.is_tree() {
                let old_tree = prior.filter(|(m, _)| m.is_tree()).map(|(_, o)| *o);
                self.tree(oid, old_tree)?;
            } else if !entry.mode.is_commit() && !self.odb.contains(&oid) {
                blobs.push(oid);
            }
        }
        self.blobs(&blobs)?;
        self.write(Kind::Tree, &raw)
    }

    fn blobs(&mut self, ids: &[gix::ObjectId]) -> Result<(), Fail> {
        let datas = self.ctl.get_blobs(ids, BATCH_MAX).map_err(store_fail)?;
        for (id, data) in ids.iter().zip(datas) {
            let data = match data {
                Some(d) => d,
                None => self.ctl.get(id).map_err(store_fail)?.1,
            };
            self.write(Kind::Blob, &data)?;
        }
        Ok(())
    }
}
