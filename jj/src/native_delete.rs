use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::ui::Ui;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::{RefTarget, RemoteRef, RemoteRefState};
use jj_lib::ref_name::{RefName, RemoteName};
use jj_lib::repo::ReadonlyRepo;
use motte_jj::ctl::{CtlClient, RefInfo, RefSwap};

use crate::native_push::{read_ref, store_fail};
use crate::push::{self, Fail, Outcome, REMOTE};
use crate::repo_config::Native;
use std::io::Write as _;

/// Deletes the refs from the store when each is at the commit that `NAME@origin` records, then
/// forgets `NAME@origin`. A ref that the store lacks while `NAME@origin` remains is forgotten
/// without a swap. The local bookmark stays.
pub async fn delete(
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
    for name in names {
        let kind = if *name == native.default_branch {
            "default"
        } else if native.start_branch.as_deref() == Some(name.as_str()) {
            "start"
        } else {
            continue;
        };
        return Err(Fail::new(
            Outcome::Refused,
            format!("refusing to delete {name}, the {kind} branch of {REMOTE}"),
        ));
    }
    push::claim(&ctl, "deleted")?;
    let mut stored = vec![];
    for name in names {
        let info = read_ref(&ctl, name, "deleted")?;
        match &info {
            Some(info) => check_lease(&repo, info)?,
            None if !recorded(&repo, name) => {
                return Err(Fail::new(
                    Outcome::Refused,
                    format!("{REMOTE} has no bookmark {name}"),
                ));
            }
            None => {}
        }
        stored.push((name, info));
    }

    push::claim(&ctl, "deleted")?;
    let mut deleted: Vec<&str> = vec![];
    let mut forgotten: Vec<&str> = vec![];
    let mut failure = None;
    for (name, info) in &stored {
        if let Some(info) = info
            && let Err(fail) = remove(&ctl, info)
        {
            failure = Some(if deleted.is_empty() {
                fail
            } else {
                fail.after("deleted", &deleted)
            });
            break;
        }
        if info.is_some() {
            deleted.push(name);
        }
        forgotten.push(name);
    }
    if !forgotten.is_empty()
        && let Err(e) = forget(ui, ws, &forgotten).await
    {
        return Err(failure.unwrap_or_else(|| {
            if deleted.is_empty() {
                e
            } else {
                e.after("deleted", &deleted)
            }
        }));
    }
    failure.map_or(Ok(()), Err)
}

/// Whether `NAME@origin` is in the view: a ref that the store lost is deleted already.
fn recorded(repo: &ReadonlyRepo, name: &str) -> bool {
    let symbol = RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE));
    repo.view().get_remote_bookmark(symbol).target.is_present()
}

fn stale(name: &str) -> Fail {
    Fail::new(
        Outcome::Stale,
        format!("{REMOTE} moved {name}; run jj motte fetch first"),
    )
}

fn check_lease(repo: &ReadonlyRepo, info: &RefInfo) -> Result<(), Fail> {
    let symbol = RefName::new(&info.name).to_remote_symbol(RemoteName::new(REMOTE));
    let lease = repo.view().get_remote_bookmark(symbol).target.as_normal();
    if lease.map(ObjectId::hex).as_deref() != Some(info.commit.as_str()) {
        return Err(stale(&info.name));
    }
    Ok(())
}

fn remove(ctl: &CtlClient, info: &RefInfo) -> Result<(), Fail> {
    match ctl.swap_ref(&info.name, info.generation, "", true) {
        Ok(RefSwap::Swapped(_)) => Ok(()),
        Ok(RefSwap::Conflict(_)) => Err(stale(&info.name)),
        Err(e) => Err(store_fail(e, "deleted")),
    }
}

async fn forget(ui: &mut Ui, ws: &mut WorkspaceCommandHelper, names: &[&str]) -> Result<(), Fail> {
    let mut tx = ws.start_transaction();
    for name in names {
        let symbol = RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE));
        tx.repo_mut().set_remote_bookmark(
            symbol,
            RemoteRef {
                target: RefTarget::absent(),
                state: RemoteRefState::New,
            },
        );
    }
    tx.finish(ui, format!("delete {} from {REMOTE}", names.join(", ")))
        .await?;
    writeln!(ui.status(), "Deleted {} from {REMOTE}", names.join(", "))
        .map_err(jj_cli::command_error::CommandError::from)?;
    Ok(())
}
