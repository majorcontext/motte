use std::collections::{HashMap, HashSet};

use gix::objs::Find as _;
use gix::objs::Kind;
use jj_lib::backend::CommitId;
use jj_lib::index::Index;
use motte_jj::ctl::{CtlClient, HASH_KIND, ObjectStore as _, StoreError};

use crate::init_cmd::WALK_LIMIT;
use crate::push::{Fail, Outcome, store_fail};

/// Most objects, and bytes, that one `putbatch` carries.
const BATCH_OBJECTS: usize = 64;
const BATCH_BYTES: usize = 8 << 20;

pub enum Walk {
    /// The new commits, parents first, and those among them whose parents origin did not send
    /// (the shallow roots).
    Done {
        commits: Vec<gix::ObjectId>,
        shallow: Vec<gix::ObjectId>,
    },
    /// A parent is in neither the transport repo nor the daemon: fetch more history.
    Deeper,
}

pub struct Stored {
    pub objects: u64,
    pub bytes: u64,
}

/// An object of the transport repo, if there is one, or else of the daemon, which holds what
/// origin sent before.
fn read(
    ctl: &CtlClient,
    odb: Option<&gix::odb::Handle>,
    id: &gix::ObjectId,
) -> Result<Option<(Vec<u8>, bool)>, Fail> {
    let mut buf = vec![];
    let local = match odb {
        Some(odb) => odb
            .try_find(id, &mut buf)
            .map_err(|e| Fail::new(Outcome::Network, format!("cannot read {id}: {e}")))?
            .is_some(),
        None => false,
    };
    if local {
        return Ok(Some((buf, true)));
    }
    match ctl.get(id) {
        Ok((_, data)) => Ok(Some((data, false))),
        Err(StoreError::NotFound(_)) => Ok(None),
        Err(e) => Err(store_fail(e, "fetched")),
    }
}

fn parse<'a>(id: &gix::ObjectId, raw: &'a [u8]) -> Result<gix::objs::CommitRef<'a>, Fail> {
    gix::objs::CommitRef::from_bytes(raw, HASH_KIND)
        .map_err(|e| Fail::new(Outcome::Refused, format!("bad commit {id}: {e}")))
}

/// Walks from `tips` to the commits that the jj index holds, and refuses a commit whose message
/// jj cannot represent. A commit that only the daemon holds joins the walk, because jj has not
/// indexed it, and its parents may be missing. With `last` set, a missing parent ends the walk
/// at its child, a shallow root, and without a transport repo a parent more than `WALK_LIMIT`
/// commits from its tip counts as missing; otherwise the walk asks for more history. A commit in `known`,
/// a root that an earlier fetch recorded, stays a root and the walk stops there.
pub async fn walk(
    ctl: &CtlClient,
    odb: Option<&gix::odb::Handle>,
    index: &dyn Index,
    known: &HashSet<CommitId>,
    tips: &[gix::ObjectId],
    last: bool,
) -> Result<Walk, Fail> {
    let (mut seen, mut missing) = (HashSet::new(), HashSet::new());
    let (mut commits, mut shallow) = (vec![], HashSet::new());
    let limit = WALK_LIMIT * tips.len();
    let mut stack: Vec<(gix::ObjectId, bool, Option<gix::ObjectId>, usize)> =
        tips.iter().map(|t| (*t, false, None, 0)).collect();
    while let Some((id, expanded, child, depth)) = stack.pop() {
        if expanded {
            commits.push(id);
            continue;
        }
        let jj_id = CommitId::from_bytes(id.as_bytes());
        if index.has_id(&jj_id).await.unwrap_or(false) {
            continue;
        }
        if known.contains(&jj_id) {
            if seen.insert(id) {
                commits.push(id);
                shallow.insert(id);
            }
            continue;
        }
        if let (true, Some(c), true) = (last, child, missing.contains(&id)) {
            shallow.insert(c);
            continue;
        }
        if !seen.insert(id) {
            continue;
        }
        let beyond = odb.is_none() && depth >= WALK_LIMIT;
        let found = if beyond { None } else { read(ctl, odb, &id)? };
        let Some((raw, _)) = found else {
            match child {
                Some(c) if last => {
                    missing.insert(id);
                    shallow.insert(c);
                    continue;
                }
                _ => return Ok(Walk::Deeper),
            }
        };
        if odb.is_some() && seen.len() - missing.len() > limit {
            return Err(Fail::new(
                Outcome::Refused,
                format!("more than {limit} new commits; fetch fewer bookmarks"),
            ));
        }
        let commit = parse(&id, &raw)?;
        if let Some(encoding) = commit.encoding
            && !encoding.eq_ignore_ascii_case(b"utf-8")
        {
            return Err(Fail::new(
                Outcome::Refused,
                format!("commit {id} has the encoding {encoding}, which jj cannot represent"),
            ));
        }
        stack.push((id, true, None, 0));
        stack.extend(commit.parents().map(|p| (p, false, Some(id), depth + 1)));
    }
    Ok(Walk::Done {
        commits,
        shallow: shallow.into_iter().collect(),
    })
}

/// Writes the commits and the trees and blobs that differ from the first parent to the daemon,
/// parents first, and each commit after its tree.
pub fn store(
    ctl: &CtlClient,
    odb: &gix::odb::Handle,
    commits: &[gix::ObjectId],
) -> Result<Stored, Fail> {
    let mut w = Writer {
        ctl,
        odb,
        queue: vec![],
        queued: 0,
        seen: HashSet::new(),
        done: Stored {
            objects: 0,
            bytes: 0,
        },
    };
    let mut trees: HashMap<gix::ObjectId, gix::ObjectId> = HashMap::new();
    for id in commits {
        let (raw, local) = read(ctl, Some(odb), id)?.expect("the walk read it");
        let (tree, first) = {
            let commit = parse(id, &raw)?;
            (commit.tree(), commit.parents().next())
        };
        let old = match first {
            Some(p) => match trees.get(&p) {
                Some(t) => Some(*t),
                None => read(ctl, Some(odb), &p)?
                    .and_then(|(r, _)| parse(&p, &r).ok().map(|c| c.tree())),
            },
            None => None,
        };
        w.tree(tree, old)?;
        if local {
            w.queue(Kind::Commit, raw)?;
        }
        trees.insert(*id, tree);
    }
    w.flush()?;
    Ok(w.done)
}

struct Writer<'a> {
    ctl: &'a CtlClient,
    odb: &'a gix::odb::Handle,
    queue: Vec<(Kind, Vec<u8>)>,
    queued: usize,
    seen: HashSet<gix::ObjectId>,
    done: Stored,
}

impl Writer<'_> {
    fn queue(&mut self, kind: Kind, data: Vec<u8>) -> Result<(), Fail> {
        self.queued += data.len();
        self.done.objects += 1;
        self.done.bytes += data.len() as u64;
        self.queue.push((kind, data));
        if self.queue.len() >= BATCH_OBJECTS || self.queued >= BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Fail> {
        let batch: Vec<(Kind, &[u8])> =
            self.queue.iter().map(|(k, d)| (*k, d.as_slice())).collect();
        self.ctl
            .put_many(&batch)
            .map_err(|e| store_fail(e, "fetched"))?;
        self.queue.clear();
        self.queued = 0;
        Ok(())
    }

    /// An object that the transport repo lacks was left out of the pack because origin assumed
    /// the base had it; the daemon must hold it.
    fn missing(&self, id: &gix::ObjectId) -> Result<(), Fail> {
        match self.ctl.get(id) {
            Ok(_) => Ok(()),
            Err(StoreError::NotFound(_)) => Err(Fail::new(
                Outcome::Network,
                format!("origin did not send object {id}"),
            )),
            Err(e) => Err(store_fail(e, "fetched")),
        }
    }

    fn tree(&mut self, id: gix::ObjectId, old: Option<gix::ObjectId>) -> Result<(), Fail> {
        if !self.seen.insert(id) {
            return Ok(());
        }
        let Some((raw, local)) = read(self.ctl, Some(self.odb), &id)? else {
            return Err(Fail::new(
                Outcome::Network,
                format!("origin did not send tree {id}"),
            ));
        };
        if !local {
            return Ok(());
        }
        let old_raw = match old {
            Some(o) => read(self.ctl, Some(self.odb), &o)?.map(|(r, _)| r),
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
            .map_err(|e| Fail::new(Outcome::Refused, format!("bad tree {id}: {e}")))?;
        for entry in &tree.entries {
            let prior = old_entries.get(entry.filename);
            let oid = entry.oid.to_owned();
            if prior.is_some_and(|(_, o)| *o == oid) {
                continue;
            }
            if entry.mode.is_tree() {
                let old_tree = prior.filter(|(m, _)| m.is_tree()).map(|(_, o)| *o);
                self.tree(oid, old_tree)?;
            } else if !entry.mode.is_commit() && self.seen.insert(oid) {
                let mut buf = vec![];
                if self
                    .odb
                    .try_find(&oid, &mut buf)
                    .map_err(|e| Fail::new(Outcome::Network, format!("cannot read {oid}: {e}")))?
                    .is_some()
                {
                    self.queue(Kind::Blob, buf)?;
                } else {
                    self.missing(&oid)?;
                }
            }
        }
        self.queue(Kind::Tree, raw)
    }
}
