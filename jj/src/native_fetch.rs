use std::collections::HashMap;

use gix::objs::Kind;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::RemoteName;
use jj_lib::repo::Repo as _;
use motte_jj::ctl::{CtlClient, ObjectStore as _, StoreError};

use crate::fetch::{self, Tip};
use crate::fetch_objects::{self, Walk};
use crate::native_push::store_fail;
use crate::push::{self, Fail, Outcome, REMOTE};

pub async fn fetch(
    ui: &mut Ui,
    ws: &mut WorkspaceCommandHelper,
    names: &[String],
    default_branch: &str,
) -> Result<(), Fail> {
    let ctl = CtlClient::new(
        std::env::var("MOTTE_VFS_SOCK")
            .map_err(|_| Fail::new(Outcome::Refused, "set MOTTE_VFS_SOCK"))?,
    );
    let repo = ws.repo().clone();

    push::claim(&ctl, "fetched")?;
    let refs = ctl.refs().map_err(|e| store_fail(e, "fetched"))?;
    let stored: HashMap<String, String> = refs.into_iter().map(|r| (r.name, r.commit)).collect();
    let recorded: Vec<&str> = repo
        .view()
        .remote_bookmarks(RemoteName::new(REMOTE))
        .map(|(name, _)| name.as_str())
        .collect();
    let gone: Vec<String> = recorded
        .iter()
        .filter(|n| {
            !stored.contains_key(**n) && (names.is_empty() || names.iter().any(|m| m == **n))
        })
        .map(|n| (*n).to_owned())
        .collect();
    let named: Vec<String> = names
        .iter()
        .filter(|n| !gone.contains(n))
        .cloned()
        .collect();
    let wanted = if !names.is_empty() && named.is_empty() {
        vec![]
    } else {
        fetch::wanted(ui, &named, &stored, Some(default_branch))?
    };
    let (old, pending) = fetch::compare(&repo, &wanted, &stored)?;
    if pending.is_empty() {
        if gone.is_empty() {
            return fetch::up_to_date(ui, &wanted);
        }
        push::claim(&ctl, "fetched")?;
        return fetch::record(ui, ws, &wanted, &old, &[], &[], &gone).await;
    }

    let mut tips = vec![];
    for name in &pending {
        let id = CommitId::try_from_hex(&stored[name]).ok_or_else(|| {
            Fail::new(
                Outcome::Refused,
                format!("the ref {name} does not name a commit"),
            )
        })?;
        match ctl.get(&push::to_oid(&id)) {
            Ok((Kind::Commit, _)) => tips.push(Tip {
                name: name.clone(),
                id,
            }),
            Ok(_) => {
                return Err(Fail::new(
                    Outcome::Refused,
                    format!("the ref {name} names {}, which is not a commit", id.hex()),
                ));
            }
            Err(StoreError::NotFound(_)) => {
                return Err(Fail::new(
                    Outcome::Refused,
                    format!("the store has no commit {} for bookmark {name}", id.hex()),
                ));
            }
            Err(e) => return Err(store_fail(e, "fetched")),
        }
    }
    let ids: Vec<gix::ObjectId> = tips.iter().map(|t| push::to_oid(&t.id)).collect();
    let known = fetch::known_roots(&repo)?;
    let Walk::Done { shallow, .. } =
        fetch_objects::walk(&ctl, None, repo.index(), &known, &ids, true).await?
    else {
        return Err(Fail::new(
            Outcome::Refused,
            "the store does not hold the commits of the fetched bookmarks",
        ));
    };
    push::claim(&ctl, "fetched")?;
    fetch::record(ui, ws, &wanted, &old, &tips, &shallow, &gone).await
}
