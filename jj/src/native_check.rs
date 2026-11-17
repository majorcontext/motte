use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use gix::objs::{Kind, TreeRef};
use jj_lib::backend::CommitId;
use motte_jj::ctl::{CtlClient, HASH_KIND, ObjectStore as _, StoreError};

use crate::native_push::store_fail;
use crate::push::{self, Fail, Outcome};

/// Blobs that one `get_blobs` call checks, on a daemon without the `has` op.
const BLOB_BATCH: usize = 64;

struct Entry {
    name: Vec<u8>,
    tree: bool,
    submodule: bool,
    oid: gix::ObjectId,
}

/// Refuses a push when the store lacks a tree or a blob that a new commit adds to its first
/// parent. A subtree with the id of the parent's entry is in the store already. A blob is
/// checked with the `has` op, so the daemon asks the store whether it holds the blob and reads no
/// content.
pub(crate) fn objects(ctl: &CtlClient, commits: &[CommitId]) -> Result<(), Fail> {
    let mut check = Check {
        ctl,
        commits: HashMap::new(),
        trees: HashMap::new(),
        walked: HashSet::new(),
        blobs: HashSet::new(),
    };
    for id in commits {
        let oid = push::to_oid(id);
        let short = oid.to_string()[..12].to_owned();
        let (tree, parent) = check.commit(&oid, &short)?;
        let old = parent.and_then(|p| check.commit(&p, &short).ok().map(|(t, _)| t));
        let mut blobs = vec![];
        check.tree_blobs(tree, old, &short, &mut blobs)?;
        check_blobs(ctl, &blobs, &short)?;
    }
    Ok(())
}

fn check_blobs(ctl: &CtlClient, blobs: &[gix::ObjectId], short: &str) -> Result<(), Fail> {
    match ctl.has(blobs) {
        Ok(present) => match blobs.iter().zip(present).find(|(_, p)| !p) {
            Some((blob, _)) => Err(absent(
                StoreError::NotFound(blob.to_string()),
                "blob",
                blob,
                short,
            )),
            None => Ok(()),
        },
        Err(StoreError::Daemon(m)) if m.contains("unknown op") => {
            for chunk in blobs.chunks(BLOB_BATCH) {
                if ctl.get_blobs(chunk, 1).is_err() {
                    for blob in chunk {
                        ctl.get_blobs(&[*blob], 1)
                            .map_err(|e| absent(e, "blob", blob, short))?;
                    }
                }
            }
            Ok(())
        }
        Err(e) => Err(store_fail(e, "pushed")),
    }
}

/// Refuses a push when the store lacks the commit of a bookmark whose ref is not at it, which no
/// new commit names when the base or a `NAME@origin` holds it.
pub(crate) fn targets(ctl: &CtlClient, ids: &[CommitId]) -> Result<(), Fail> {
    for id in ids {
        let oid = push::to_oid(id);
        let short = oid.to_string()[..12].to_owned();
        ctl.get(&oid)
            .map_err(|e| absent(e, "commit", &oid, &short))?;
    }
    Ok(())
}

type TreeAndParent = (gix::ObjectId, Option<gix::ObjectId>);

/// The objects that one `objects` call read, so a parent is not fetched again for each child.
/// `walked` holds the (tree, old tree) pairs already compared, since the blobs to request depend
/// on both; `blobs` holds only blobs that a call has put in a batch.
struct Check<'a> {
    ctl: &'a CtlClient,
    commits: HashMap<gix::ObjectId, TreeAndParent>,
    trees: HashMap<gix::ObjectId, Rc<Vec<Entry>>>,
    walked: HashSet<(gix::ObjectId, Option<gix::ObjectId>)>,
    blobs: HashSet<gix::ObjectId>,
}

fn absent(e: StoreError, what: &str, id: &gix::ObjectId, commit: &str) -> Fail {
    match e {
        StoreError::NotFound(_) | StoreError::Daemon(_) => Fail::new(
            Outcome::Network,
            format!("the store does not hold the {what} {id} of commit {commit}: {e}"),
        ),
        e => store_fail(e, "pushed"),
    }
}

impl Check<'_> {
    fn commit(&mut self, id: &gix::ObjectId, short: &str) -> Result<TreeAndParent, Fail> {
        if let Some(known) = self.commits.get(id) {
            return Ok(*known);
        }
        let (_, raw) = self
            .ctl
            .get(id)
            .map_err(|e| absent(e, "commit", id, short))?;
        let commit = gix::objs::CommitRef::from_bytes(&raw, HASH_KIND)
            .map_err(|e| Fail::new(Outcome::Refused, format!("bad commit {id}: {e}")))?;
        let parsed = (commit.tree(), commit.parents().next());
        self.commits.insert(*id, parsed);
        Ok(parsed)
    }

    fn read_tree(&mut self, id: &gix::ObjectId, short: &str) -> Result<Rc<Vec<Entry>>, Fail> {
        if let Some(known) = self.trees.get(id) {
            return Ok(known.clone());
        }
        let (kind, raw) = self.ctl.get(id).map_err(|e| absent(e, "tree", id, short))?;
        let tree = TreeRef::from_bytes(&raw, HASH_KIND)
            .ok()
            .filter(|_| kind == Kind::Tree)
            .ok_or_else(|| Fail::new(Outcome::Refused, format!("bad tree {id}")))?;
        let entries: Rc<Vec<Entry>> = Rc::new(
            tree.entries
                .iter()
                .map(|e| Entry {
                    name: e.filename.to_vec(),
                    tree: e.mode.is_tree(),
                    submodule: e.mode.is_commit(),
                    oid: e.oid.to_owned(),
                })
                .collect(),
        );
        self.trees.insert(*id, entries.clone());
        Ok(entries)
    }

    /// Reads the tree `id` and the subtrees that differ from `old`, and collects their new blobs.
    fn tree_blobs(
        &mut self,
        id: gix::ObjectId,
        old: Option<gix::ObjectId>,
        short: &str,
        blobs: &mut Vec<gix::ObjectId>,
    ) -> Result<(), Fail> {
        if !self.walked.insert((id, old)) {
            return Ok(());
        }
        let prior: HashMap<Vec<u8>, (gix::ObjectId, bool)> = match old {
            Some(o) => self
                .read_tree(&o, short)
                .map(|t| {
                    t.iter()
                        .map(|e| (e.name.clone(), (e.oid, e.tree)))
                        .collect()
                })
                .unwrap_or_default(),
            None => HashMap::new(),
        };
        for e in self.read_tree(&id, short)?.iter() {
            let before = prior.get(&e.name);
            if e.submodule || before.is_some_and(|b| b.0 == e.oid) {
                continue;
            }
            if e.tree {
                let old = before.filter(|b| b.1).map(|b| b.0);
                self.tree_blobs(e.oid, old, short, blobs)?;
            } else if self.blobs.insert(e.oid) {
                blobs.push(e.oid);
            }
        }
        Ok(())
    }
}
