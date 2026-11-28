use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use futures::{StreamExt as _, TryStreamExt as _};
use jj_cli::cli_util::{CommandHelper, WorkspaceCommandHelper};
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::revset::ResolvedRevsetExpression;
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{Code, CtlClient, ObjectStore, StoreError};
use motte_jj::stores::{
    FLUSHED_FILE, MIRROR_BUDGET, Mirrored, RESTORE_LIST_MAX, RestoreList, UNSYNCED_FILE,
    clear_drained, clear_unsynced, merge_remote, put_ops, put_restore_list, put_shallow,
    read_heads, settle, within,
};
use motte_jj::wc::materialized_tree;

use crate::{cmd_err, daemon};

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct SyncArgs {
    /// Do nothing unless an update left the remote op heads behind
    #[arg(long)]
    if_unsynced: bool,
}

/// A flush publishes again when an agent command moved the op head meanwhile, up to this many tries.
const FLUSH_TRIES: usize = 3;

pub(crate) async fn flush(ui: &mut Ui, helper: &CommandHelper) -> Result<(), CommandError> {
    let repo_path = helper.workspace_loader()?.repo_path().to_owned();
    let ctl = daemon(&repo_path).map_err(cmd_err)?;
    motte_jj::wc::FORCE_FULL.store(true, std::sync::atomic::Ordering::Relaxed);
    for _ in 0..FLUSH_TRIES {
        let ws = helper.workspace_helper(ui).await?;
        if let Some(published) = publish(&ws, &ctl, helper.settings(), false).await? {
            verify_mount(&ctl, &published.commit)?;
            writeln!(
                ui.stdout(),
                "{}",
                serde_json::json!({"op": published.head, "commit": published.commit, "reported": published.reported, "generation": published.generation})
            )?;
            return Ok(());
        }
    }
    Err(cmd_err(format!(
        "the op head moved during each of {FLUSH_TRIES} tries of the flush"
    )))
}

/// Fails unless the store holds the mount commit and its tree. A caller trusts a flush to delete
/// the only other copy of the workspace, and a mount of a commit whose tree the store lacks fails.
/// A daemon without the `has` op skips the tree check.
fn verify_mount(ctl: &CtlClient, commit: &str) -> Result<(), CommandError> {
    let oid = gix::ObjectId::from_hex(commit.as_bytes()).map_err(cmd_err)?;
    let (_, raw) = ctl.get(&oid).map_err(cmd_err)?;
    let tree =
        gix::ObjectId::from_hex(crate::init_cmd::commit_tree(&raw)?.as_bytes()).map_err(cmd_err)?;
    match ctl.has(&[tree]) {
        Ok(present) if present == [true] => Ok(()),
        Ok(_) => Err(cmd_err(format!(
            "the store lacks tree {tree} of the mount commit {commit}"
        ))),
        Err(e) if e.is(&Code::UnknownOp) => Ok(()),
        Err(e) => Err(cmd_err(e)),
    }
}

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct RematerializeArgs {
    /// A mount commit that a flush of a conflicted working copy stored
    #[arg(long)]
    commit: String,
}

/// Stores again the tree with conflict markers that the mount commit `commit` names, from the
/// working-copy commit that its message names. Any workspace of the repository can run it. The
/// tree is rebuilt with the settings of this workspace, and the command fails when it comes out
/// different from the one that the commit names.
pub(crate) async fn rematerialize(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: RematerializeArgs,
) -> Result<(), CommandError> {
    let ws = helper.workspace_helper_no_snapshot(ui).await?;
    let ctl = daemon(ws.repo_path()).map_err(cmd_err)?;
    let oid = gix::ObjectId::from_hex(args.commit.as_bytes()).map_err(cmd_err)?;
    let (_, raw) = ctl.get(&oid).map_err(cmd_err)?;
    let named = crate::init_cmd::commit_tree(&raw)?.to_owned();
    let text = String::from_utf8_lossy(&raw);
    let wc_hex = text
        .lines()
        .find_map(|l| l.strip_prefix("materialized working copy "))
        .ok_or_else(|| cmd_err("the commit is no mount commit of a conflicted working copy"))?;
    let wc_id = CommitId::try_from_hex(wc_hex).ok_or_else(|| cmd_err("bad commit id"))?;
    let store = ws.repo().store();
    let wc = store.get_commit_async(&wc_id).await.map_err(cmd_err)?;
    let tree = materialized_tree(store, &wc.tree(), helper.settings())
        .await
        .map_err(cmd_err)?;
    store
        .backend_impl::<NativeBackend>()
        .ok_or_else(|| cmd_err("this repo has no native backend"))?
        .object_store()
        .flush_deferred()
        .map_err(cmd_err)?;
    if tree.hex() != named {
        return Err(cmd_err(format!(
            "rebuilt tree {} differs from tree {named} of the commit; set the conflict marker style of the workspace that flushed it",
            tree.hex()
        )));
    }
    verify_mount(&ctl, &args.commit)?;
    writeln!(
        ui.stdout(),
        "stored tree {named} of mount commit {}",
        args.commit
    )?;
    Ok(())
}

pub(crate) struct Published {
    pub head: String,
    pub commit: String,
    /// Whether the daemon took the report of the workspace.
    pub reported: bool,
    /// The op-heads generation under which `head` is the remote head.
    pub generation: i64,
}

/// Stores the op log and the working copy of the loaded head, sets the remote op head to it and
/// deletes the unsynced marker and reports the head and the mount commit to the control plane
/// through the daemon. Under the op-heads lock it checks that no writer moved the local head;
/// `None` means one did, and nothing was set. A failed report fails the command only when
/// `report_fails_command` is set: a flush has stored everything by then, and its caller records the
/// result itself when `reported` is false.
pub(crate) async fn publish(
    ws: &WorkspaceCommandHelper,
    ctl: &CtlClient,
    settings: &jj_lib::settings::UserSettings,
    report_fails_command: bool,
) -> Result<Option<Published>, CommandError> {
    let repo = ws.repo().clone();
    let wc_id = ws
        .get_wc_commit_id()
        .ok_or_else(|| cmd_err("no working-copy commit"))?
        .clone();
    let commit = mount_commit(&repo, &wc_id, ctl, settings).await?;
    let pending = unflushed(&repo, ws.repo_path(), &[repo.operation().id().clone()]).await?;
    let head = repo.operation().id().hex();
    let roots = repo
        .store()
        .backend()
        .downcast_ref::<NativeBackend>()
        .ok_or_else(|| cmd_err("this repo has no native backend"))?
        .shallow_roots()
        .map_err(cmd_err)?;
    let list = restore_list(&repo).await?;
    let (ops, shallow, listed) = std::thread::scope(|scope| {
        let shallow = scope.spawn(|| put_shallow(ctl, &head, &roots));
        let listed = scope.spawn(|| put_restore_list(ctl, &head, &list));
        let ops = put_ops(ctl, ws.repo_path(), &pending, None);
        let joined = |r: std::thread::Result<Result<(), StoreError>>, what: &str| {
            r.unwrap_or_else(|_| Err(StoreError::Local(format!("the {what} upload panicked"))))
        };
        (
            ops,
            joined(shallow.join(), "shallow"),
            joined(listed.join(), "restore list"),
        )
    });
    ops.map_err(cmd_err)?;
    shallow.map_err(cmd_err)?;
    listed.map_err(cmd_err)?;
    let _lock = repo.op_heads_store().lock().await.map_err(cmd_err)?;
    if read_heads(ws.repo_path()).map_err(cmd_err)? != [head.clone()] {
        return Ok(None);
    }
    let generation = set_remote_head(ctl, &head)?;
    clear_unsynced(ws.repo_path());
    let reported = match ctl.set_workspace(&head, &commit, generation) {
        Ok(taken) => taken,
        Err(e) if !report_fails_command => {
            eprintln!(
                "warning: the workspace report failed, the control plane records the flush: {e}"
            );
            false
        }
        Err(e) => return Err(cmd_err(e)),
    };
    Ok(Some(Published {
        head,
        commit,
        reported,
        generation,
    }))
}

/// The restore list of the loaded head: its operations back to the root, newest first, and the
/// ancestors of its visible heads, newest first, each cut at `RESTORE_LIST_MAX`. The walk of the
/// ancestors stops at the cut, so the cost of a flush does not grow with the index. The hidden heads
/// of earlier operations are in their views, which a restore reads from the list.
async fn restore_list(repo: &Arc<ReadonlyRepo>) -> Result<RestoreList, CommandError> {
    let root = repo.op_store().root_operation_id().clone();
    let mut ops = Vec::new();
    let mut seen = HashSet::new();
    let mut todo = VecDeque::from([repo.op_id().clone()]);
    while let Some(id) = todo.pop_front() {
        if ops.len() == RESTORE_LIST_MAX {
            break;
        }
        if id == root || !seen.insert(id.clone()) {
            continue;
        }
        let op = repo.op_store().read_operation(&id).await.map_err(cmd_err)?;
        ops.push((id.hex(), op.view_id.hex()));
        todo.extend(op.parents);
    }
    let heads: Vec<CommitId> = repo.view().heads().iter().cloned().collect();
    let commits = ResolvedRevsetExpression::commits(heads)
        .ancestors()
        .evaluate(repo.as_ref())
        .map_err(cmd_err)?
        .stream()
        .take(RESTORE_LIST_MAX)
        .try_collect()
        .await
        .map_err(cmd_err)?;
    Ok(RestoreList { ops, commits })
}

/// Drains the op log at every local head, not only the loaded one. Under the op-heads lock it
/// merges the local heads into the remote ones and deletes the unsynced marks; a writer that skips
/// the lock (jj op abandon, jj op integrate) can still race the marks, and the local heads are
/// reread after the remote read for that reason. Past the mirror budget it leaves the marks and
/// drains nothing.
pub(crate) async fn sync(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: SyncArgs,
) -> Result<(), CommandError> {
    if !helper
        .global_args()
        .at_operation
        .as_deref()
        .is_some_and(is_operation_id)
    {
        return Err(cmd_err("pass --at-operation=<operation id>"));
    }
    let ws = helper.workspace_helper_no_snapshot(ui).await?;
    let repo_path = ws.repo_path().to_owned();
    if args.if_unsynced && !repo_path.join(UNSYNCED_FILE).exists() {
        writeln!(ui.stdout(), "{}", serde_json::json!({"drained": false}))?;
        return Ok(());
    }
    let ctl = Arc::new(daemon(&repo_path).map_err(cmd_err)?);
    let drained = drain(&ws, ctl, &repo_path).await?;
    match drained {
        Mirrored::Done(op) => writeln!(
            ui.stdout(),
            "{}",
            serde_json::json!({"drained": true, "op": op})
        )?,
        Mirrored::Behind => writeln!(ui.stdout(), "{}", serde_json::json!({"drained": false}))?,
        Mirrored::Unsupported => return Err(cmd_err("the daemon has no op heads")),
    }
    Ok(())
}

/// A concrete operation id, which jj loads without resolving the op heads: `@` and the revset
/// operators on it merge divergent heads by writing an operation.
fn is_operation_id(arg: &str) -> bool {
    arg.len() >= 12 && arg.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Uploads the op log, then merges the local heads into the remote ones under the lock. A store
/// failure or a spent budget leaves the marks and is `Behind`; a lost claim is an error.
async fn drain(
    ws: &WorkspaceCommandHelper,
    ctl: Arc<CtlClient>,
    repo_path: &Path,
) -> Result<Mirrored<String>, CommandError> {
    let store = ws.repo().op_heads_store().clone();
    let uploaded = async {
        let pending = unflushed_of(ws.repo(), repo_path, &read_heads(repo_path)?).await?;
        put_ops(&ctl, repo_path, &pending, None)
    }
    .await;
    match settle(uploaded).map_err(cmd_err)? {
        Mirrored::Done(()) => {}
        Mirrored::Behind => return Ok(Mirrored::Behind),
        Mirrored::Unsupported => return Ok(Mirrored::Unsupported),
    }
    let _lock = store.lock().await.map_err(cmd_err)?;
    let deadline = Instant::now() + MIRROR_BUDGET;
    let (repo, path) = (ws.repo().clone(), repo_path.to_owned());
    let done = within(deadline, move || {
        let upload = |heads: &[String]| {
            let ids: Vec<OperationId> =
                heads.iter().filter_map(OperationId::try_from_hex).collect();
            let pending = pollster::block_on(unflushed(&repo, &path, &ids)).map_err(|e| {
                StoreError::Local(format!("cannot list the unflushed operations: {}", e.error))
            })?;
            put_ops(&ctl, &path, &pending, Some(deadline))
        };
        merge_remote(&ctl, &path, || read_heads(&path), upload, deadline)
    })
    .map_err(cmd_err)?;
    Ok(match done {
        Mirrored::Done(swapped) => {
            clear_drained(repo_path, &swapped);
            Mirrored::Done(swapped.first().cloned().unwrap_or_default())
        }
        Mirrored::Behind => Mirrored::Behind,
        Mirrored::Unsupported => Mirrored::Unsupported,
    })
}

/// `unflushed` for heads in hex, with a load failure as a store error that settles as behind.
async fn unflushed_of(
    repo: &jj_lib::repo::ReadonlyRepo,
    repo_path: &Path,
    heads: &[String],
) -> Result<Vec<(String, String)>, StoreError> {
    let ids: Vec<OperationId> = heads.iter().filter_map(OperationId::try_from_hex).collect();
    unflushed(repo, repo_path, &ids).await.map_err(|e| {
        StoreError::Local(format!("cannot list the unflushed operations: {}", e.error))
    })
}

/// The operations reachable from `heads` that the flushed file does not list, each with its view.
async fn unflushed(
    repo: &jj_lib::repo::ReadonlyRepo,
    repo_path: &Path,
    heads: &[OperationId],
) -> Result<Vec<(String, String)>, CommandError> {
    let flushed: HashSet<String> = std::fs::read_to_string(repo_path.join(FLUSHED_FILE))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect();
    let root = repo.op_store().root_operation_id().clone();
    let mut seen = HashSet::new();
    let mut pending = Vec::new();
    let mut todo = heads.to_vec();
    while let Some(id) = todo.pop() {
        let hex = id.hex();
        if id == root || flushed.contains(&hex) || !seen.insert(hex.clone()) {
            continue;
        }
        let op = repo.loader().load_operation(&id).await.map_err(cmd_err)?;
        pending.push((hex, op.view_id().hex()));
        todo.extend(op.parent_ids().iter().cloned());
    }
    Ok(pending)
}

/// Sets the remote op heads to `head`, rereading them when another writer wins the swap, and returns
/// the generation under which `head` is the remote head: the one of the swap, which only grows.
fn set_remote_head(ctl: &CtlClient, head: &str) -> Result<i64, CommandError> {
    if let Some((generation, heads)) = ctl.last_opheads()
        && heads == [head]
    {
        return Ok(generation);
    }
    loop {
        let (generation, heads) = ctl.opheads(None).map_err(cmd_err)?;
        if heads == [head] {
            return Ok(generation);
        }
        match ctl.opheads(Some((generation, &[head.to_owned()]))) {
            Ok((swapped, _)) => return Ok(swapped),
            Err(e) if e.is(&Code::GenerationConflict) => {}
            Err(e) => return Err(cmd_err(e)),
        }
    }
}

/// The commit whose git tree is the files that the mount shows for the working-copy commit: the
/// commit itself, or for a conflicted one a commit of its materialized tree.
async fn mount_commit(
    repo: &jj_lib::repo::ReadonlyRepo,
    wc_id: &CommitId,
    ctl: &CtlClient,
    settings: &jj_lib::settings::UserSettings,
) -> Result<String, CommandError> {
    let wc = repo
        .store()
        .get_commit_async(wc_id)
        .await
        .map_err(cmd_err)?;
    if wc.tree().tree_ids().is_resolved() {
        return Ok(wc_id.hex());
    }
    let tree = materialized_tree(repo.store(), &wc.tree(), settings)
        .await
        .map_err(cmd_err)?;
    // The backend defers the blobs and trees that it writes until an operation or a view is
    // written, and a flush writes neither: without this the commit would name a tree that the
    // daemon never got.
    repo.store()
        .backend_impl::<NativeBackend>()
        .ok_or_else(|| cmd_err("this repo has no native backend"))?
        .object_store()
        .flush_deferred()
        .map_err(cmd_err)?;
    let body = format!(
        "tree {}\nauthor motte <motte@invalid> 0 +0000\ncommitter motte <motte@invalid> 0 +0000\n\nmaterialized working copy {}\n",
        tree.hex(),
        wc_id.hex()
    );
    let id = ctl
        .put(gix::objs::Kind::Commit, body.as_bytes())
        .map_err(cmd_err)?;
    Ok(id.to_string())
}
