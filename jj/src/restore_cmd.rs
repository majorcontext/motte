use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::{OperationId, View, ViewId};
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::workspace::Workspace;
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, ObjectStore as _};
use motte_jj::stores::{
    FLUSHED_FILE, LocalOnly, RESTORE_LIST_MAX, get_restore_list, get_shallow, prefetch_ops,
};
use motte_jj::trace::span;
use motte_jj::wc::{LockedVfsWorkingCopy, conflict_markers};

use crate::init_cmd::{
    OriginArgs, WALK_LIMIT, check_mount_tree, link_jj, new_workspace, shallow_boundary,
};
use crate::{cmd_err, repo_config, snapshot_cmd};

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct AttachArgs {
    /// Daemon control socket
    #[arg(long)]
    sock: PathBuf,
    /// Mount point of the lazy file system, which shows the working-copy commit of the operation
    #[arg(long)]
    mount: PathBuf,
    /// Directory outside the mount that holds .jj
    #[arg(long)]
    jj_root: PathBuf,
    /// Commit that the mount serves, which a flush named for the working-copy commit
    #[arg(long)]
    commit: String,
    /// Operation that the op log must be at
    #[arg(long)]
    op: String,
}

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct RestoreArgs {
    /// Daemon control socket
    #[arg(long)]
    sock: PathBuf,
    /// Mount point of the lazy file system, which shows the working-copy commit of the operation
    #[arg(long)]
    mount: PathBuf,
    /// Directory outside the mount that holds .jj (linked into the mount as a symlink)
    #[arg(long)]
    jj_root: PathBuf,
    /// Commit that the mount serves, which a flush named for the working-copy commit
    #[arg(long)]
    commit: String,
    /// Operation to restore
    #[arg(long)]
    op: String,
    /// Commit that the workspace was initialized on, whose parents the store does not serve
    #[arg(long)]
    base: String,
    /// Daemon connections that the restore reads on at once [default: $MOTTE_RESTORE_PREFETCH, or
    /// 64]
    #[arg(long)]
    prefetch_width: Option<usize>,
    #[command(flatten)]
    origin: OriginArgs,
}

/// The daemon connections that a restore reads on at once: the flag, else the environment variable
/// `MOTTE_RESTORE_PREFETCH`, else 64. A variable that is no number counts as unset, and 0 counts
/// as 1. The caller lowers it without a release when many workspaces
/// wake at once and the store needs the load spread out.
fn prefetch_width(flag: Option<usize>) -> usize {
    flag.or_else(|| std::env::var("MOTTE_RESTORE_PREFETCH").ok()?.parse().ok())
        .unwrap_or(DEFAULT_PREFETCH_WIDTH)
        .max(1)
}

pub(crate) async fn restore(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: RestoreArgs,
) -> Result<(), CommandError> {
    let mount = std::fs::canonicalize(&args.mount).map_err(cmd_err)?;
    if std::fs::symlink_metadata(mount.join(".jj")).is_ok() {
        return Err(cmd_err("the mount already has a .jj"));
    }
    std::fs::create_dir_all(&args.jj_root).map_err(cmd_err)?;
    let jj_root = std::fs::canonicalize(&args.jj_root).map_err(cmd_err)?;
    let sock = std::fs::canonicalize(&args.sock).map_err(cmd_err)?;
    let head = OperationId::try_from_hex(&args.op).ok_or_else(|| cmd_err("bad operation id"))?;
    let ctl = CtlClient::new(&sock);
    let phase = span("jj.restore.init");
    let base_oid = gix::ObjectId::from_hex(args.base.as_bytes()).map_err(cmd_err)?;
    let shallow = match get_shallow(&ctl, &head.hex()).map_err(cmd_err)? {
        Some(roots) => roots,
        None => shallow_boundary(&ctl, base_oid, WALK_LIMIT)?,
    };
    let local = LocalOnly::enter();
    let (mut workspace, repo) = new_workspace(ui, helper, &mount, &jj_root, &sock, shallow).await?;
    drop(local);
    args.origin
        .write(ui, helper, workspace.repo_path(), &args.base)?;

    drop(phase);

    let repo_path = workspace.repo_path().to_owned();
    let width = prefetch_width(args.prefetch_width);
    let mut early = prefetch(&ctl, &repo, &head, width)?;
    let mut view_heads = HashSet::new();
    let phase = span("jj.restore.op_walk");
    let fetched = fetch_op_log(&ctl, &repo, &repo_path, &head, &mut early, &mut view_heads).await?;
    drop(phase);
    drop(early);
    let view_heads: Vec<CommitId> = view_heads.into_iter().collect();
    let phase = span("jj.restore.view_heads");
    native(&repo)?.prefetch_commits(&view_heads, width);
    drop(phase);
    let heads = repo_path.join("op_heads").join("heads");
    for entry in std::fs::read_dir(&heads).map_err(cmd_err)? {
        std::fs::remove_file(entry.map_err(cmd_err)?.path()).map_err(cmd_err)?;
    }
    std::fs::write(heads.join(head.hex()), b"").map_err(cmd_err)?;
    std::fs::write(repo_path.join(FLUSHED_FILE), fetched).map_err(cmd_err)?;

    let phase = span("jj.restore.adopt");
    let wc_id = adopt_mount(&mut workspace, &ctl, &head, &args.commit, &mount, &jj_root).await?;
    drop(phase);
    if let Some(branch) = args.origin.branch()? {
        let _phase = span("jj.restore.branch");
        let base = CommitId::try_from_hex(&args.base).ok_or_else(|| cmd_err("bad commit id"))?;
        branch.restore(&mut workspace, &base).await?;
    }
    writeln!(
        ui.status(),
        "Restored motte workspace at operation {} (working copy {})",
        head.hex(),
        wc_id.hex()
    )?;
    Ok(())
}

/// Daemon connections that a restore reads on at once unless the caller sets it. A restore reads
/// one object for each operation, view and commit, at about one store round trip each.
const DEFAULT_PREFETCH_WIDTH: usize = 64;

/// The most bytes of op log objects that a restore holds before it writes them.
const PREFETCH_BYTES: usize = 64 << 20;

fn native(repo: &ReadonlyRepo) -> Result<&NativeBackend, CommandError> {
    repo.store()
        .backend_impl::<NativeBackend>()
        .ok_or_else(|| cmd_err("this repo has no native backend"))
}

type Early = HashMap<(&'static str, String), Vec<u8>>;

/// Reads the restore list of `head`, then its commits into the commit cache of the backend and
/// its op log objects into the returned map, all at once, on `width` connections between them. A head flushed without a list reads
/// nothing here.
fn prefetch(
    ctl: &CtlClient,
    repo: &ReadonlyRepo,
    head: &OperationId,
    width: usize,
) -> Result<Early, CommandError> {
    let phase = span("jj.restore.list");
    let Some(list) = get_restore_list(ctl, &head.hex()).map_err(cmd_err)? else {
        return Ok(Early::new());
    };
    drop(phase);
    let _phase = span("jj.restore.prefetch");
    let backend = native(repo)?;
    if width < 2 {
        backend.prefetch_commits(&list.commits, 1);
        return Ok(prefetch_ops(ctl, &list.ops, 1, PREFETCH_BYTES));
    }
    Ok(std::thread::scope(|scope| {
        scope.spawn(|| backend.prefetch_commits(&list.commits, width / 2));
        prefetch_ops(ctl, &list.ops, width - width / 2, PREFETCH_BYTES)
    }))
}

/// Reads the operations from `head` back to the ones that the repo holds, with their views, into
/// the op store of the repo, and returns the IDs of the operations, one for each line. It adds the
/// heads and working-copy commits of each view to `view_heads`, which the index build reads, up to
/// `RESTORE_LIST_MAX` distinct commits. An
/// object is taken from `early` when it is there. An object whose content does not hash to its ID
/// is refused.
async fn fetch_op_log(
    ctl: &CtlClient,
    repo: &ReadonlyRepo,
    repo_path: &Path,
    head: &OperationId,
    early: &mut Early,
    view_heads: &mut HashSet<CommitId>,
) -> Result<String, CommandError> {
    let op_store = repo_path.join("op_store");
    let root = repo.op_store().root_operation_id().clone();
    let mut fetched = String::new();
    let mut todo = vec![head.clone()];
    while let Some(id) = todo.pop() {
        let hex = id.hex();
        let path = op_store.join("operations").join(&hex);
        if id == root || path.exists() {
            continue;
        }
        fetched.push_str(&hex);
        fetched.push('\n');
        let data = match early.remove(&("operations", hex.clone())) {
            Some(data) => data,
            None => ctl.getop("operations", &hex).map_err(cmd_err)?,
        };
        std::fs::write(&path, data).map_err(cmd_err)?;
        let op = match repo.loader().load_operation(&id).await {
            Ok(op) if blake2b_hash(op.store_operation()).to_vec() == id.as_bytes() => op,
            other => {
                std::fs::remove_file(&path).map_err(cmd_err)?;
                other.map_err(cmd_err)?;
                return Err(cmd_err(format!(
                    "the daemon sent bytes that are not operation {hex}"
                )));
            }
        };
        let view = fetch_view(ctl, repo, &op_store, op.view_id(), early).await?;
        for id in view.head_ids.iter().chain(view.wc_commit_ids.values()) {
            if view_heads.len() < RESTORE_LIST_MAX {
                view_heads.insert(id.clone());
            }
        }
        todo.extend(op.parent_ids().iter().cloned());
    }
    Ok(fetched)
}

async fn fetch_view(
    ctl: &CtlClient,
    repo: &ReadonlyRepo,
    op_store: &Path,
    id: &ViewId,
    early: &mut Early,
) -> Result<View, CommandError> {
    let hex = id.hex();
    let path = op_store.join("views").join(&hex);
    let data = match early.remove(&("views", hex.clone())) {
        Some(data) => data,
        None => ctl.getop("views", &hex).map_err(cmd_err)?,
    };
    std::fs::write(&path, data).map_err(cmd_err)?;
    let view = repo.op_store().read_view(id).await;
    if let Ok(v) = view.as_ref()
        && blake2b_hash(v).to_vec() == id.as_bytes()
    {
        return view.map_err(cmd_err);
    }
    std::fs::remove_file(&path).map_err(cmd_err)?;
    view.map_err(cmd_err)?;
    Err(cmd_err(format!(
        "the daemon sent bytes that are not view {hex}"
    )))
}

pub(crate) async fn attach(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: AttachArgs,
) -> Result<(), CommandError> {
    let mount = std::fs::canonicalize(&args.mount).map_err(cmd_err)?;
    if std::fs::symlink_metadata(mount.join(".jj")).is_ok() {
        return Err(cmd_err("the mount already has a .jj"));
    }
    let jj_root = std::fs::canonicalize(&args.jj_root).map_err(cmd_err)?;
    let head = OperationId::try_from_hex(&args.op).ok_or_else(|| cmd_err("bad operation id"))?;
    let mut workspace = helper.load_workspace_at(&jj_root, helper.settings())?;
    let ctl = CtlClient::new(&args.sock);
    let wc_id = adopt_mount(&mut workspace, &ctl, &head, &args.commit, &mount, &jj_root).await?;
    repo_config::write(ui, helper, workspace.repo_path(), None, None, None, None)?;
    writeln!(
        ui.status(),
        "Attached motte workspace at operation {} (working copy {})",
        head.hex(),
        wc_id.hex()
    )?;
    Ok(())
}

/// Checks that the op log is at `head`, that `mounted` is the commit that a flush named for its
/// working-copy commit and that the daemon shows its tree; records the working-copy tree, with the
/// marker length of each conflicted file, and links `.jj` into the mount.
async fn adopt_mount(
    workspace: &mut Workspace,
    ctl: &CtlClient,
    head: &OperationId,
    mounted: &str,
    mount: &Path,
    jj_root: &Path,
) -> Result<CommitId, CommandError> {
    let phase = span("jj.restore.index");
    let repo = workspace
        .repo_loader()
        .load_at_head(Some(workspace.workspace_name()))
        .await
        .map_err(cmd_err)?;
    drop(phase);
    if repo.op_id() != head {
        return Err(cmd_err(format!(
            "the op log is at {}, not {}",
            repo.op_id().hex(),
            head.hex()
        )));
    }
    let wc_id = repo
        .view()
        .get_wc_commit_id(WorkspaceName::DEFAULT)
        .ok_or_else(|| cmd_err("the operation has no working-copy commit"))?
        .clone();
    let wc_commit = repo
        .store()
        .get_commit_async(&wc_id)
        .await
        .map_err(cmd_err)?;
    let oid = gix::ObjectId::from_hex(mounted.as_bytes()).map_err(cmd_err)?;
    let (_, raw) = ctl.get(&oid).map_err(cmd_err)?;
    let names_wc = mounted == wc_id.hex()
        || (!wc_commit.tree().tree_ids().is_resolved()
            && raw.ends_with(format!("materialized working copy {}\n", wc_id.hex()).as_bytes()));
    if !names_wc {
        return Err(cmd_err(format!(
            "the mount serves commit {mounted}, which no flush named for the working-copy commit {}",
            wc_id.hex()
        )));
    }
    check_mount_tree(ctl, &raw, mounted)?;
    let markers = conflict_markers(repo.store(), &wc_commit.tree())
        .await
        .map_err(cmd_err)?;
    let published = workspace.repo_path().join(snapshot_cmd::PUBLISHED_FILE);
    let mut locked = workspace
        .start_working_copy_mutation()
        .await
        .map_err(cmd_err)?;
    let wc = locked
        .locked_wc()
        .downcast_mut::<LockedVfsWorkingCopy>()
        .ok_or_else(|| cmd_err("not a motte-vfs working copy"))?;
    wc.adopt_tree(wc_commit.tree());
    wc.adopt_conflict_markers(markers);
    match std::fs::remove_file(published) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(cmd_err(e)),
        _ => {}
    }
    locked.finish(head.clone()).await.map_err(cmd_err)?;
    link_jj(mount, jj_root)?;
    Ok(wc_id)
}
