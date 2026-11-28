use std::io::Write as _;

use futures::{StreamExt as _, TryStreamExt as _};
use jj_cli::cli_util::{CommandHelper, RevisionArg, WorkspaceCommandHelper};
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::{RefName, RemoteName};
use jj_lib::repo::Repo as _;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::rewrite::{MoveCommitsLocation, MoveCommitsTarget, RebaseOptions, move_commits};

use crate::push::{Fail, REMOTE};
use crate::{cmd_err, command_lock, fetch, repo_config};

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct FollowArgs {
    /// Print the outcome as one JSON object
    #[arg(long)]
    json: bool,
    /// Fetch the default branch, but rebase nothing and leave the mount as it is
    #[arg(long)]
    fetch_only: bool,
}

#[derive(serde::Serialize)]
struct Conflict {
    change: String,
    paths: Vec<String>,
}

#[derive(serde::Serialize, Default)]
struct Report {
    moved: bool,
    old: String,
    new: String,
    commits: u64,
    rebased: Vec<String>,
    conflicts: Vec<Conflict>,
    skipped: &'static str,
}

/// Fetches the default branch of origin and rebases the branch of the working copy onto it, in one
/// command that holds the command lock exclusive, so that no snapshot of a timer or of another
/// command runs in between. A held lock is the outcome `busy`, and nothing changes. The snapshot
/// waits until the branch moved and a rebase follows, so `unchanged` and `fetch_only` record no
/// edit of the mount.
pub(crate) async fn follow(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: FollowArgs,
) -> Result<(), CommandError> {
    let Some(_lock) = command_lock::try_exclusive(helper)? else {
        return print(
            ui,
            &args,
            &Report {
                skipped: "busy",
                ..Report::default()
            },
        );
    };
    let branch = default_branch(helper)?;
    let start = helper.workspace_helper_no_snapshot(ui).await?;
    if stale(&start).await? {
        return print(
            ui,
            &args,
            &Report {
                skipped: "stale",
                ..Report::default()
            },
        );
    }
    let old = tip(&start, &branch);
    drop(start);
    fetch::quietly(ui, helper, &branch)
        .await
        .map_err(Fail::into_error)?;
    let fetched = helper.workspace_helper_no_snapshot(ui).await?;
    let new = tip(&fetched, &branch);
    let mut report = Report {
        moved: new != old,
        old: old.as_ref().map(|c| c.hex()).unwrap_or_default(),
        new: new.as_ref().map(|c| c.hex()).unwrap_or_default(),
        ..Report::default()
    };
    // A branch that a `--fetch-only` run moved has not moved since, but the working copy may
    // still not be on it: that is a rebase too.
    let behind = match &new {
        Some(tip) => ids(ui, &fetched, &format!("{} & ::@", tip.hex()))
            .await?
            .is_empty(),
        None => false,
    };
    let Some(new_tip) = new.filter(|_| report.moved || behind) else {
        report.skipped = "unchanged";
        report.conflicts = conflicts(ui, &fetched).await?;
        return print(ui, &args, &report);
    };
    if report.moved {
        let from = old
            .as_ref()
            .map_or_else(|| "root()".to_owned(), |c| c.hex());
        let range = format!("{from}..{}", new_tip.hex());
        report.commits = ids(ui, &fetched, &range).await?.len() as u64;
    }
    if args.fetch_only {
        report.skipped = "fetch_only";
        report.conflicts = conflicts(ui, &fetched).await?;
        return print(ui, &args, &report);
    }
    drop(fetched);
    let mut ws = helper.workspace_helper(ui).await?;
    let wc = ws
        .get_wc_commit_id()
        .ok_or_else(|| cmd_err("the workspace has no working-copy commit"))?
        .clone();
    let moving = ResolvedRevsetExpression::commits(vec![new_tip.clone()])
        .range(&ResolvedRevsetExpression::commits(vec![wc]))
        .roots()
        .descendants();
    ws.check_rewritable_expr(&moving).await?;
    report.rebased = rebase(ui, &mut ws, &branch, new_tip).await?;
    report.conflicts = conflicts(ui, &helper.workspace_helper(ui).await?).await?;
    print(ui, &args, &report)
}

/// In one operation, settles the local commits that the moved branch landed, then rebases the
/// roots of `new..@` with their descendants onto `new`, and checks out. Returns the change IDs
/// that it moved.
async fn rebase(
    ui: &mut Ui,
    ws: &mut WorkspaceCommandHelper,
    branch: &str,
    new: CommitId,
) -> Result<Vec<String>, CommandError> {
    let mut tx = ws.start_transaction();
    let tips = [fetch::Tip {
        name: branch.to_owned(),
        id: new.clone(),
    }];
    // From the tip, not from the old tip: a `--fetch-only` run fetched the history that landed the
    // local commits without settling it. The walk is bounded.
    fetch::settle_landed(&mut tx, &tips, &[None])
        .await
        .map_err(Fail::into_error)?;
    let name = tx.base_workspace_helper().workspace_name().to_owned();
    let wc = tx
        .repo()
        .view()
        .get_wc_commit_id(&name)
        .ok_or_else(|| cmd_err("the workspace has no working-copy commit"))?
        .clone();
    let roots_expr = ResolvedRevsetExpression::commits(vec![new.clone()])
        .range(&ResolvedRevsetExpression::commits(vec![wc]))
        .roots();
    let roots: Vec<CommitId> = roots_expr
        .clone()
        .evaluate(tx.repo())
        .map_err(cmd_err)?
        .stream()
        .try_collect()
        .await
        .map_err(cmd_err)?;
    let moved: Vec<CommitId> = roots_expr
        .descendants()
        .evaluate(tx.repo())
        .map_err(cmd_err)?
        .stream()
        .try_collect()
        .await
        .map_err(cmd_err)?;
    let mut changes = vec![];
    for id in &moved {
        let c = tx
            .repo()
            .store()
            .get_commit_async(id)
            .await
            .map_err(cmd_err)?;
        changes.push(c.change_id().reverse_hex());
    }
    if !roots.is_empty() {
        let loc = MoveCommitsLocation {
            new_parent_ids: vec![new],
            new_child_ids: vec![],
            target: MoveCommitsTarget::Roots(roots),
        };
        move_commits(tx.repo_mut(), &loc, &RebaseOptions::default())
            .await
            .map_err(cmd_err)?;
    }
    if tx.repo().has_changes() {
        tx.finish(ui, format!("follow {branch} of {REMOTE}"))
            .await?;
    }
    Ok(changes)
}

/// Whether a snapshot would stop on the working copy: a checkout that did not finish left it
/// naming no operation, or it names an operation that the repo moved past with another tree.
/// `jj workspace update-stale` settles both.
pub(crate) async fn stale(ws: &WorkspaceCommandHelper) -> Result<bool, CommandError> {
    let wc = ws.working_copy();
    if wc.operation_id().hex() == motte_jj::wc::UNFINISHED_CHECKOUT {
        return Ok(true);
    }
    if wc.operation_id() == ws.repo().op_id() {
        return Ok(false);
    }
    let Some(commit_id) = ws.get_wc_commit_id() else {
        return Ok(false);
    };
    let commit = ws
        .repo()
        .store()
        .get_commit_async(commit_id)
        .await
        .map_err(cmd_err)?;
    let tree = wc.tree().map_err(cmd_err)?;
    Ok(tree.tree_ids_and_labels() != commit.tree().tree_ids_and_labels())
}

/// The branch of origin that the workspace follows: the default branch of a native repo, or the
/// branch that `init` or `restore` started on.
fn default_branch(helper: &CommandHelper) -> Result<String, CommandError> {
    if let Some(native) = repo_config::native(helper.settings()).map_err(cmd_err)? {
        return Ok(native.default_branch);
    }
    repo_config::origin_branch(helper.settings())
        .ok_or_else(|| cmd_err("this workspace has no default branch of origin to follow"))
}

fn tip(ws: &WorkspaceCommandHelper, branch: &str) -> Option<CommitId> {
    let symbol = RefName::new(branch).to_remote_symbol(RemoteName::new(REMOTE));
    ws.repo()
        .view()
        .get_remote_bookmark(symbol)
        .target
        .as_normal()
        .cloned()
}

async fn ids(
    ui: &Ui,
    ws: &WorkspaceCommandHelper,
    revset: &str,
) -> Result<Vec<CommitId>, CommandError> {
    let expr = ws.parse_revset(ui, &RevisionArg::from(revset.to_owned()))?;
    let ids = expr.evaluate_to_commit_ids()?.try_collect().await?;
    Ok(ids)
}

/// Every conflicted mutable change with its conflicted paths.
async fn conflicts(ui: &Ui, ws: &WorkspaceCommandHelper) -> Result<Vec<Conflict>, CommandError> {
    let expr = ws.parse_revset(ui, &RevisionArg::from("conflicts() & mutable()".to_owned()))?;
    let commits: Vec<_> = expr.evaluate_to_commits()?.try_collect().await?;
    let mut out = vec![];
    for c in commits {
        let paths: Vec<String> = c
            .tree()
            .conflicts()
            .map(|(path, _)| path.as_internal_file_string().to_owned())
            .collect()
            .await;
        out.push(Conflict {
            change: c.change_id().reverse_hex(),
            paths,
        });
    }
    Ok(out)
}

fn print(ui: &mut Ui, args: &FollowArgs, report: &Report) -> Result<(), CommandError> {
    if args.json {
        writeln!(
            ui.stdout(),
            "{}",
            serde_json::to_string(report).map_err(cmd_err)?
        )?;
        return Ok(());
    }
    let what = match report.skipped {
        "" if report.rebased.is_empty() => "nothing to rebase".to_owned(),
        "" => format!("rebased {} changes", report.rebased.len()),
        other => other.to_owned(),
    };
    writeln!(
        ui.stdout(),
        "{what}; {} conflicted changes",
        report.conflicts.len()
    )?;
    Ok(())
}
