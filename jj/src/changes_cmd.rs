//! `jj motte changes`: the `protocol.WorkspaceChanges` document that harness reads for a jj
//! workspace through `harness.changes-command`. It loads one op head by ID and never snapshots,
//! so it writes no operation.

use futures::StreamExt as _;
use std::collections::HashSet;
use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::conflicts::ConflictMaterializeOptions;
use jj_lib::local_working_copy::TreeStateSettings;
use jj_lib::object_id::ObjectId as _;
use jj_lib::operation::Operation;
use jj_lib::repo::{Repo as _, RepoLoader};
use motte_jj::changes::{ChangedFile, DiffInput, MAX_PATCH, diff_view};
use motte_jj::ctl::CtlClient;
use serde::Serialize;

use crate::cmd_err;

#[derive(clap::Args, Clone, Debug)]
pub struct ChangesArgs {
    /// Print JSON, the only format
    #[arg(long)]
    json: bool,
}

/// The request that harness sets in the environment of the command.
struct Request {
    scope: String,
    base: Option<CommitId>,
    cap: usize,
}

impl Request {
    fn from_env() -> Result<Self, CommandError> {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let scope = env("HARNESS_CHANGES_SCOPE").unwrap_or_else(|| "uncommitted".into());
        if scope != "branch" && scope != "uncommitted" {
            return Err(cmd_err(format!(
                "HARNESS_CHANGES_SCOPE {scope:?} must be \"branch\" or \"uncommitted\""
            )));
        }
        let base = match env("HARNESS_CHANGES_BASE") {
            Some(b) => Some(
                CommitId::try_from_hex(&b)
                    .ok_or_else(|| cmd_err(format!("HARNESS_CHANGES_BASE {b:?} is no commit")))?,
            ),
            None => None,
        };
        let cap = match env("HARNESS_CHANGES_PATCH_BYTES") {
            Some(v) => v.parse().map_err(cmd_err)?,
            None => MAX_PATCH,
        };
        Ok(Self { scope, base, cap })
    }
}

#[derive(Serialize)]
struct WorkspaceChanges {
    dir: String,
    scope: String,
    engine: &'static str,
    branch: String,
    files: Vec<ChangedFile>,
    patch: String,
    truncated: bool,
}

/// The operation that harness reads: the one op head, or the head that ended last in whole
/// seconds, then the larger ID, as harness orders them.
async fn newest_op(loader: &RepoLoader) -> Result<Operation, CommandError> {
    let heads = loader
        .op_heads_store()
        .get_op_heads()
        .await
        .map_err(cmd_err)?;
    let key = |o: &Operation| {
        (
            o.metadata().time.end.timestamp.0.div_euclid(1000),
            o.id().hex(),
        )
    };
    let mut best: Option<Operation> = None;
    for id in heads {
        let op = loader.load_operation(&id).await.map_err(cmd_err)?;
        if best.as_ref().is_none_or(|b| key(&op) > key(b)) {
            best = Some(op);
        }
    }
    best.ok_or_else(|| cmd_err("the workspace has no operation head"))
}

pub async fn run(
    ui: &mut Ui,
    helper: &CommandHelper,
    _args: ChangesArgs,
) -> Result<(), CommandError> {
    let req = Request::from_env()?;
    let workspace = helper.load_workspace()?;
    let op = newest_op(workspace.repo_loader()).await?;
    let repo = workspace
        .repo_loader()
        .load_at(&op)
        .await
        .map_err(cmd_err)?;
    let root = workspace.workspace_root().to_owned();
    let sock_file = workspace.repo_path().join("store").join("ctl_sock");
    let ws = helper.for_workable_repo(ui, workspace, repo)?;
    let repo = ws.repo().clone();
    let store = repo.store();

    let wc_id = ws
        .get_wc_commit_id()
        .ok_or_else(|| cmd_err("no working-copy commit"))?;
    let wc = store.get_commit_async(wc_id).await.map_err(cmd_err)?;
    let target = match &req.base {
        Some(base) => store.get_commit_async(base).await.map_err(cmd_err)?.tree(),
        None => wc.parent_tree(repo.index()).await.map_err(cmd_err)?,
    };
    let wc_tree = wc.tree();
    let conflicts: HashSet<String> = wc_tree
        .conflicts()
        .map(|(p, _)| p.as_internal_file_string().to_owned())
        .collect()
        .await;
    let sock: std::path::PathBuf = match std::env::var_os("MOTTE_VFS_SOCK") {
        Some(s) => s.into(),
        None => std::fs::read_to_string(&sock_file)
            .map_err(|e| cmd_err(format!("no daemon socket: {e}")))?
            .into(),
    };
    let ctl = CtlClient::new(sock);
    let view = ctl.changes().map_err(cmd_err)?;
    let materialize = ConflictMaterializeOptions {
        marker_style: TreeStateSettings::try_from_user_settings(ws.settings())
            .map_err(cmd_err)?
            .conflict_marker_style,
        marker_len: None,
        merge: store.merge_options().clone(),
    };
    let auto_track = ws.env().auto_tracking_matcher(ui)?;
    let snapshot = ws.snapshot_options_with_start_tracking_matcher(auto_track.as_ref())?;
    let input = DiffInput {
        store,
        snapshot: &snapshot,
        ctl: &ctl,
        root: &root,
        conflicts: &conflicts,
        wc_tree: &wc_tree,
        materialize: &materialize,
    };
    let (files, patch, truncated) = diff_view(&input, &target, &view, req.cap)
        .await
        .map_err(cmd_err)?;
    let out = WorkspaceChanges {
        dir: root.display().to_string(),
        scope: req.scope,
        engine: "jj",
        branch: String::new(),
        files,
        patch,
        truncated,
    };
    writeln!(
        ui.stdout(),
        "{}",
        serde_json::to_string(&out).map_err(cmd_err)?
    )?;
    Ok(())
}
