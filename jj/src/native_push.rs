use std::collections::{BTreeSet, HashSet};

use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::RemoteName;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use motte_jj::ctl::{CtlClient, RefInfo, RefSwap, StoreError};

use crate::WALK_LIMIT;
use crate::native_check;
use crate::push::{self, Fail, Outcome, REMOTE, Target};
use crate::push_objects::Plan;
use crate::repo_config::Native;

pub async fn push(
    ui: &mut Ui,
    ws: &mut WorkspaceCommandHelper,
    names: &[String],
    native: &Native,
) -> Result<(), Fail> {
    let ctl = CtlClient::new(
        std::env::var("MOTTE_VFS_SOCK")
            .map_err(|_| Fail::new(Outcome::Refused, "set MOTTE_VFS_SOCK"))?,
    );
    let repo = ws.repo().clone();
    let targets = push::targets(&repo, names).await?;
    let base = CommitId::try_from_hex(&native.base).ok_or_else(|| {
        Fail::new(
            Outcome::Refused,
            "the origin base commit is not a commit ID",
        )
    })?;
    let commits = new_commits(&repo, &targets, &base).await?;
    let plan = Plan {
        commits: commits.iter().map(push::to_oid).collect(),
        base: BTreeSet::new(),
    };
    push::check_new_commits(&repo, &plan).await?;

    push::claim(&ctl, "pushed")?;
    let mut stored = vec![];
    for t in &targets {
        stored.push(read_ref(&ctl, &t.name, "pushed")?);
    }
    refuse_default_rewrite(&repo, &targets, &stored, native).await?;
    let moving: Vec<CommitId> = targets
        .iter()
        .zip(&stored)
        .filter(|(t, s)| s.as_ref().map(|r| r.commit.as_str()) != Some(t.id.hex().as_str()))
        .map(|(t, _)| t.id.clone())
        .collect();
    native_check::targets(&ctl, &moving)?;
    native_check::objects(&ctl, &commits)?;

    push::claim(&ctl, "pushed")?;
    let (done, failure) = swap_all(&ctl, &targets, &stored);
    let swapped = done.iter().any(|(_, swapped)| *swapped);
    if !done.is_empty() {
        let moved: Vec<String> = done
            .iter()
            .filter(|(_, swapped)| *swapped)
            .map(|(t, _)| t.name.clone())
            .collect();
        let recorded: Vec<Target> = done.into_iter().map(|(t, _)| t).collect();
        if let Err(e) = push::record(ui, ws, &recorded, !swapped).await {
            let moved: Vec<&str> = moved.iter().map(String::as_str).collect();
            return Err(failure.unwrap_or(if moved.is_empty() {
                e
            } else {
                e.after("pushed", &moved)
            }));
        }
    }
    failure.map_or(Ok(()), Err)
}

/// The commits that the bookmarks add to the history that the store held at the start of the
/// workspace and to the commits of `NAME@origin`, which the store has.
async fn new_commits(
    repo: &ReadonlyRepo,
    targets: &[Target],
    base: &CommitId,
) -> Result<Vec<CommitId>, Fail> {
    let mut held = vec![base.clone()];
    for (_, remote) in repo.view().remote_bookmarks(RemoteName::new(REMOTE)) {
        held.extend(remote.target.as_normal().cloned());
    }
    let limit = WALK_LIMIT * targets.len();
    let (mut seen, mut commits) = (HashSet::new(), vec![]);
    let mut stack: Vec<CommitId> = targets.iter().map(|t| t.id.clone()).collect();
    'next: while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        for h in &held {
            if repo.index().is_ancestor(&id, h).await.unwrap_or(false) {
                continue 'next;
            }
        }
        if commits.len() >= limit {
            return Err(Fail::new(
                Outcome::Refused,
                format!("more than {limit} new commits; push fewer bookmarks"),
            ));
        }
        let commit = repo
            .store()
            .get_commit_async(&id)
            .await
            .map_err(|e| Fail::new(Outcome::Refused, e.to_string()))?;
        stack.extend(commit.parent_ids().iter().cloned());
        commits.push(id);
    }
    Ok(commits)
}

pub(crate) fn store_fail(e: StoreError, sent: &str) -> Fail {
    match e {
        StoreError::Daemon(m) if m.contains("vcs_claim_lost") => Fail::new(
            Outcome::ClaimLost,
            format!("this workspace lost its claim on the op log; nothing was {sent}"),
        ),
        StoreError::Daemon(m) if m.contains("unknown op") => Fail::new(
            Outcome::Refused,
            "the daemon of this workspace has no ref store; the repo is not native",
        ),
        e => Fail::new(Outcome::Network, format!("the store failed: {e}")),
    }
}

pub(crate) fn read_ref(ctl: &CtlClient, name: &str, sent: &str) -> Result<Option<RefInfo>, Fail> {
    match ctl.get_ref(name) {
        Ok(r) => Ok(Some(r)),
        Err(StoreError::Daemon(m)) if m.contains("vcs_not_found") => Ok(None),
        Err(e) => Err(store_fail(e, sent)),
    }
}

/// The default branch and the start branch may move forward only, as the default branch of a git
/// origin does.
async fn refuse_default_rewrite(
    repo: &ReadonlyRepo,
    targets: &[Target],
    stored: &[Option<RefInfo>],
    native: &Native,
) -> Result<(), Fail> {
    for (t, s) in targets.iter().zip(stored) {
        let kind = if t.name == native.default_branch {
            "default"
        } else if native.start_branch.as_deref() == Some(t.name.as_str()) {
            "start"
        } else {
            continue;
        };
        let Some(tip) = s.as_ref().and_then(|r| CommitId::try_from_hex(&r.commit)) else {
            continue;
        };
        let known = repo.index().has_id(&tip).await.unwrap_or(false);
        if !known || !repo.index().is_ancestor(&tip, &t.id).await.unwrap_or(false) {
            return Err(Fail::new(
                Outcome::Refused,
                format!(
                    "refusing to rewrite {}, the {} branch of {REMOTE}: its tip is not an ancestor of the commit",
                    t.name, kind
                ),
            ));
        }
    }
    Ok(())
}

/// Moves the refs one after another. Returns the targets that the store now holds at the pushed
/// commit, each with whether this call moved it, and the failure that stopped the loop.
fn swap_all(
    ctl: &CtlClient,
    targets: &[Target],
    stored: &[Option<RefInfo>],
) -> (Vec<(Target, bool)>, Option<Fail>) {
    let mut done: Vec<(Target, bool)> = vec![];
    for (t, s) in targets.iter().zip(stored) {
        match swap(ctl, t, s.as_ref()) {
            Ok(swapped) => done.push((t.clone(), swapped)),
            Err(fail) => {
                let pushed: Vec<&str> = done
                    .iter()
                    .filter(|(_, swapped)| *swapped)
                    .map(|(t, _)| t.name.as_str())
                    .collect();
                let fail = if pushed.is_empty() {
                    fail
                } else {
                    fail.after("pushed", &pushed)
                };
                return (done, Some(fail));
            }
        }
    }
    (done, None)
}

/// Moves one ref when the store holds the commit that `NAME@origin` records. Returns false for a
/// ref that is at the commit already.
fn swap(ctl: &CtlClient, t: &Target, stored: Option<&RefInfo>) -> Result<bool, Fail> {
    let held = stored.map(|r| r.commit.as_str());
    if held == Some(t.id.hex().as_str()) {
        return Ok(false);
    }
    let stale = || {
        Fail::new(
            Outcome::Stale,
            format!("{REMOTE} moved {}; run jj motte fetch first", t.name),
        )
    };
    if held != t.lease.as_deref() {
        return Err(stale());
    }
    match ctl.swap_ref(
        &t.name,
        stored.map_or(0, |r| r.generation),
        &t.id.hex(),
        false,
    ) {
        Ok(RefSwap::Swapped(_)) => Ok(true),
        Ok(RefSwap::Conflict(_)) => Err(stale()),
        Err(e) => Err(store_fail(e, "pushed")),
    }
}
