use futures::StreamExt as _;
use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, HASH_KIND, ObjectStore, StoreError};
use motte_jj::stores::{
    FLUSHED_FILE, MIRROR_BUDGET, Mirrored, MotteOpHeadsStore, MotteOpStore, UNSYNCED_FILE,
    clear_drained, clear_unsynced, get_shallow, merge_remote, put_ops, put_shallow, read_heads,
    settle, store_factories, within,
};
use motte_jj::wc::{
    LockedVfsWorkingCopy, VfsConfig, VfsWorkingCopyFactory, WC_NAME, conflict_markers,
    materialized_tree,
};
mod changes_cmd;
mod command_lock;
mod fetch;
mod fetch_objects;
mod native_check;
mod native_delete;
mod native_fetch;
mod native_push;
mod push;
mod push_git;
mod push_objects;
mod repo_config;
mod snapshot_cmd;
mod start_branch;

use repo_config::Origin;
use start_branch::StartBranch;

use jj_cli::cli_util::{CliRunner, CommandHelper, WorkspaceCommandHelper};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::Repo as _;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::{WorkingCopyFactories, Workspace, WorkspaceInitError};

#[derive(clap::Parser, Clone, Debug)]
enum CustomCommand {
    /// Commands for workspaces backed by a motte-vfsd mount
    Motte {
        #[command(subcommand)]
        cmd: MotteCommand,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
enum MotteCommand {
    /// Create a jj workspace on a motte-vfsd mount
    Init(InitArgs),
    /// Show what the daemon reports as changed since the last sync
    Dirty,
    /// Walk the whole mount, hash it like git, and compare with the working-copy commit's tree
    Verify,
    /// Upload the op log to the store, set the remote op heads to the local head, and print the
    /// head operation and the working-copy commit as JSON
    Flush,
    /// Upload the op log of the loaded operation and set the remote op heads to it, without a
    /// snapshot or an operation, and print whether it drained as JSON
    Sync(SyncArgs),
    /// Snapshot the working copy like any jj command, unless a command holds its lock, and print
    /// the outcome as JSON
    Snapshot(snapshot_cmd::SnapshotArgs),
    /// Rebuild the .jj of a workspace from the op log in the store
    Restore(RestoreArgs),
    /// Link the .jj under a root into a new mount of the working-copy commit of its op head
    Attach(AttachArgs),
    /// Print the work-tree diff for harness as JSON, read-only: no snapshot, no operation
    Changes(changes_cmd::ChangesArgs),
    /// Push bookmarks to origin: the GitHub repo with git, or the ref store of a native repo
    Push(push::PushArgs),
    /// Fetch the bookmarks of origin: from the GitHub repo with git, or from the ref store of a native repo
    Fetch(fetch::FetchArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct SyncArgs {
    /// Do nothing unless an update left the remote op heads behind
    #[arg(long)]
    if_unsynced: bool,
}

#[derive(clap::Args, Clone, Debug)]
struct AttachArgs {
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
struct RestoreArgs {
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
    /// Plain http(s) git URL of origin, which `jj motte push` pushes to
    #[arg(long)]
    origin_url: Option<String>,
    /// Branch of origin that the base commit is on: sets the bookmark and its tracked bookmark of
    /// origin where they are absent
    #[arg(long)]
    origin_branch: Option<String>,
    /// The repo is native: its origin is the store, which `jj motte push` and `fetch` use
    #[arg(long, conflicts_with = "origin_url")]
    origin_native: bool,
    /// Default branch of the native repo, which `jj motte push` and `fetch` use
    #[arg(long, requires = "origin_native")]
    origin_default_branch: Option<String>,
}

#[derive(clap::Args, Clone, Debug)]
struct InitArgs {
    /// Daemon control socket
    #[arg(long)]
    sock: PathBuf,
    /// Mount point of the lazy file system (default: current directory)
    #[arg(long)]
    mount: Option<PathBuf>,
    /// Git commit (in the daemon's store) that the mount shows
    #[arg(long)]
    commit: String,
    /// Directory outside the mount that holds .jj (linked into the mount as a symlink)
    #[arg(long)]
    jj_root: PathBuf,
    /// Show the history behind the commit even where the store lacks a parent; by default the
    /// history ends at the commits whose parents the store lacks
    #[arg(long)]
    full_history: bool,
    /// Plain http(s) git URL of origin, which `jj motte push` pushes to
    #[arg(long)]
    origin_url: Option<String>,
    /// Branch of origin that the commit is on: creates the bookmark and its tracked bookmark of
    /// origin at the commit
    #[arg(long)]
    origin_branch: Option<String>,
    /// The repo is native: its origin is the store, which `jj motte push` and `fetch` use
    #[arg(long, conflicts_with = "origin_url")]
    origin_native: bool,
    /// Default branch of the native repo, which `jj motte push` and `fetch` use
    #[arg(long, requires = "origin_native")]
    origin_default_branch: Option<String>,
}

pub(crate) fn cmd_err(e: impl std::fmt::Display) -> CommandError {
    user_error(e.to_string())
}

async fn run_custom_command(
    ui: &mut Ui,
    helper: &CommandHelper,
    command: CustomCommand,
) -> Result<(), CommandError> {
    let CustomCommand::Motte { cmd } = command;
    match cmd {
        MotteCommand::Init(args) => init(ui, helper, args).await,
        MotteCommand::Verify => verify(ui, helper).await,
        MotteCommand::Flush => flush(ui, helper).await,
        MotteCommand::Sync(args) => sync(ui, helper, args).await,
        MotteCommand::Snapshot(args) => snapshot_cmd::run(ui, helper, args).await,
        MotteCommand::Restore(args) => restore(ui, helper, args).await,
        MotteCommand::Attach(args) => attach(ui, helper, args).await,
        MotteCommand::Changes(args) => changes_cmd::run(ui, helper, args).await,
        MotteCommand::Push(args) => push::run(ui, helper, args).await,
        MotteCommand::Fetch(args) => fetch::run(ui, helper, args).await,
        MotteCommand::Dirty => {
            let sock =
                std::env::var("MOTTE_VFS_SOCK").map_err(|_| cmd_err("set MOTTE_VFS_SOCK"))?;
            let r = CtlClient::new(sock).dirty().map_err(cmd_err)?;
            writeln!(
                ui.stdout(),
                "token={} base_tree={} entries={}",
                r.token,
                r.base_tree,
                r.entries.len()
            )?;
            for e in r.entries.iter().take(50) {
                writeln!(
                    ui.stdout(),
                    "  {} {}{}",
                    e.kind,
                    e.path,
                    if e.opaque { " (opaque)" } else { "" }
                )?;
            }
            Ok(())
        }
    }
}

async fn init(ui: &mut Ui, helper: &CommandHelper, args: InitArgs) -> Result<(), CommandError> {
    let mount = match &args.mount {
        Some(m) => m.clone(),
        None => helper.cwd().to_owned(),
    };
    let mount = std::fs::canonicalize(&mount).map_err(cmd_err)?;
    let jj_root = args.jj_root.clone();
    std::fs::create_dir_all(&jj_root).map_err(cmd_err)?;
    let jj_root = std::fs::canonicalize(&jj_root).map_err(cmd_err)?;
    let sock = std::fs::canonicalize(&args.sock).map_err(cmd_err)?;

    // The daemon must show exactly the tree of the commit; otherwise the first snapshot would lie.
    let ctl = CtlClient::new(&sock);
    let commit_oid = gix::ObjectId::from_hex(args.commit.as_bytes()).map_err(cmd_err)?;
    let (_, raw) = ctl.get(&commit_oid).map_err(cmd_err)?;
    let tree_hex = std::str::from_utf8(&raw[5..45])
        .map_err(cmd_err)?
        .to_owned();
    let shown = ctl.dirty().map_err(cmd_err)?.base_tree;
    if shown != tree_hex {
        return Err(cmd_err(format!(
            "daemon shows tree {shown} but commit {} has tree {tree_hex}; restart the daemon on this commit",
            args.commit
        )));
    }
    let commit_id = CommitId::try_from_hex(&args.commit).ok_or_else(|| cmd_err("bad commit id"))?;

    let shallow = if args.full_history {
        vec![]
    } else {
        shallow_boundary(&ctl, commit_oid, WALK_LIMIT)?
    };
    let (mut workspace, repo) = new_workspace(ui, helper, &mount, &jj_root, &sock, shallow).await?;
    let origin = args
        .origin_url
        .as_deref()
        .map(|url| Origin::new(url, &args.commit))
        .transpose()?;
    let native = args.origin_native.then_some(args.commit.as_str());
    let default_branch = args
        .origin_default_branch
        .as_deref()
        .map(StartBranch::new)
        .transpose()?;
    let branch = args
        .origin_branch
        .as_deref()
        .map(StartBranch::new)
        .transpose()?;
    repo_config::write(
        ui,
        helper,
        workspace.repo_path(),
        origin.as_ref(),
        branch.as_ref(),
        native,
        default_branch.as_ref(),
    )?;

    let mut tx = repo.start_transaction();
    let base = tx
        .repo()
        .store()
        .get_commit_async(&commit_id)
        .await
        .map_err(cmd_err)?;
    tx.repo_mut().add_head(&base).await.map_err(cmd_err)?;
    if let Some(branch) = &branch {
        branch.set_absent(tx.repo_mut(), base.id());
    }
    let wc_commit = tx
        .repo_mut()
        .check_out(WorkspaceName::DEFAULT.to_owned(), &base)
        .await
        .map_err(cmd_err)?;
    tx.repo_mut().rebase_descendants().await.map_err(cmd_err)?;
    let repo = tx.commit("motte init").await.map_err(cmd_err)?;

    let mut locked = workspace
        .start_working_copy_mutation()
        .await
        .map_err(cmd_err)?;
    locked
        .locked_wc()
        .downcast_mut::<LockedVfsWorkingCopy>()
        .ok_or_else(|| cmd_err("not a motte-vfs working copy"))?
        .adopt_tree(wc_commit.tree());
    locked.finish(repo.op_id().clone()).await.map_err(cmd_err)?;

    let link = mount.join(".jj");
    std::os::unix::fs::symlink(jj_root.join(".jj"), &link)
        .map_err(|e| cmd_err(format!("cannot link {}: {e}", link.display())))?;
    writeln!(
        ui.status(),
        "Initialized motte workspace: working copy {} on {}, .jj at {} (linked from {})",
        wc_commit.id().hex(),
        base.id().hex(),
        jj_root.join(".jj").display(),
        link.display()
    )?;
    Ok(())
}

/// Most commits that `shallow_boundary` fetches. Init fetches each of them again when jj indexes the
/// history, so the walk costs about twice this many daemon round trips.
const WALK_LIMIT: usize = 1_000;

/// Walks the ancestry of `start` through the store and returns the held commits that have a parent
/// the store lacks, whose parents the backend must hide. The walk fetches at most `limit` commits;
/// past it, every held commit that points at an unfetched one joins the result.
fn shallow_boundary(
    ctl: &CtlClient,
    start: gix::ObjectId,
    limit: usize,
) -> Result<Vec<CommitId>, CommandError> {
    let mut held = HashSet::new();
    let mut missing = HashSet::new();
    let mut shallow = HashSet::new();
    let mut queue = VecDeque::from([(start, None)]);
    while let Some((id, via)) = queue.pop_front() {
        if held.contains(&id) {
            continue;
        }
        if missing.contains(&id) || held.len() == limit {
            shallow.extend(via);
            continue;
        }
        match ctl.get(&id) {
            Ok((_, raw)) => {
                let commit = gix::objs::CommitRef::from_bytes(&raw, HASH_KIND).map_err(cmd_err)?;
                queue.extend(commit.parents().map(|parent| (parent, Some(id))));
                held.insert(id);
            }
            Err(StoreError::NotFound(_)) if via.is_some() => {
                missing.insert(id);
                shallow.extend(via);
            }
            Err(e) => return Err(cmd_err(e)),
        }
    }
    Ok(shallow
        .into_iter()
        .map(|id: gix::ObjectId| CommitId::from_bytes(id.as_bytes()))
        .collect())
}

/// Creates the repo and workspace of `.jj` under `jj_root` for the mount, with the native backend
/// that treats the `shallow` commits as roots.
async fn new_workspace(
    ui: &mut Ui,
    helper: &CommandHelper,
    mount: &Path,
    jj_root: &Path,
    sock: &Path,
    shallow: Vec<CommitId>,
) -> Result<(Workspace, std::sync::Arc<jj_lib::repo::ReadonlyRepo>), CommandError> {
    let (settings, _) = helper.settings_for_new_workspace(ui, jj_root)?;
    let backend_init = {
        let sock = sock.to_owned();
        move |s: &jj_lib::settings::UserSettings, p: &Path| {
            let b: Box<dyn jj_lib::backend::Backend> =
                Box::new(NativeBackend::init(s, p, &sock, &shallow)?);
            Ok(b)
        }
    };
    let factory = VfsWorkingCopyFactory {
        init: Some(VfsConfig {
            mount: mount.to_owned(),
            sock: sock.to_owned(),
        }),
    };
    Ok(Workspace::init_with_factories(
        &settings,
        jj_root,
        WorkspaceName::DEFAULT,
        &backend_init,
        signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
        jj_lib::default_backend_factories::default_workspace_store_initializer(),
        &|_s, p, root| Ok(Box::new(MotteOpStore::init(p, root)?)),
        &|_s, p, root_op| Ok(Box::new(MotteOpHeadsStore::init(p, root_op)?)),
        jj_lib::default_backend_factories::default_index_store_initializer(),
        jj_lib::default_backend_factories::default_submodule_store_initializer(),
        &factory,
    )
    .await?)
}

/// A flush publishes again when an agent command moved the op head meanwhile, up to this many tries.
const FLUSH_TRIES: usize = 3;

async fn flush(ui: &mut Ui, helper: &CommandHelper) -> Result<(), CommandError> {
    let sock = std::env::var("MOTTE_VFS_SOCK").map_err(|_| cmd_err("set MOTTE_VFS_SOCK"))?;
    let ctl = CtlClient::new(sock);
    motte_jj::wc::FORCE_FULL.store(true, std::sync::atomic::Ordering::Relaxed);
    for _ in 0..FLUSH_TRIES {
        let ws = helper.workspace_helper(ui).await?;
        if let Some(published) = publish(&ws, &ctl, helper.settings(), false).await? {
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
    put_ops(ctl, ws.repo_path(), &pending, None).map_err(cmd_err)?;
    let head = repo.operation().id().hex();
    let roots = repo
        .store()
        .backend()
        .downcast_ref::<NativeBackend>()
        .ok_or_else(|| cmd_err("this repo has no native backend"))?
        .shallow_roots()
        .map_err(cmd_err)?;
    put_shallow(ctl, &head, &roots).map_err(cmd_err)?;
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

/// Drains the op log at every local head, not only the loaded one. Under the op-heads lock it
/// merges the local heads into the remote ones and deletes the unsynced marks; a writer that skips
/// the lock (jj op abandon, jj op integrate) can still race the marks, and the local heads are
/// reread after the remote read for that reason. Past the mirror budget it leaves the marks and
/// drains nothing.
async fn sync(ui: &mut Ui, helper: &CommandHelper, args: SyncArgs) -> Result<(), CommandError> {
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
    let sock = std::env::var("MOTTE_VFS_SOCK").map_err(|_| cmd_err("set MOTTE_VFS_SOCK"))?;
    let ctl = Arc::new(CtlClient::new(sock));
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
                StoreError::Daemon(format!("cannot list the unflushed operations: {}", e.error))
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
        StoreError::Daemon(format!("cannot list the unflushed operations: {}", e.error))
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
    loop {
        let (generation, heads) = ctl.opheads(None).map_err(cmd_err)?;
        if heads == [head] {
            return Ok(generation);
        }
        match ctl.opheads(Some((generation, &[head.to_owned()]))) {
            Ok((swapped, _)) => return Ok(swapped),
            Err(StoreError::Daemon(m)) if m.contains("vcs_generation_conflict") => {}
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

async fn restore(
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
    let base_oid = gix::ObjectId::from_hex(args.base.as_bytes()).map_err(cmd_err)?;
    let shallow = match get_shallow(&ctl, &head.hex()).map_err(cmd_err)? {
        Some(roots) => roots,
        None => shallow_boundary(&ctl, base_oid, WALK_LIMIT)?,
    };
    let (mut workspace, repo) = new_workspace(ui, helper, &mount, &jj_root, &sock, shallow).await?;
    let origin = args
        .origin_url
        .as_deref()
        .map(|url| Origin::new(url, &args.base))
        .transpose()?;
    let native = args.origin_native.then_some(args.base.as_str());
    let default_branch = args
        .origin_default_branch
        .as_deref()
        .map(StartBranch::new)
        .transpose()?;
    let branch = args
        .origin_branch
        .as_deref()
        .map(StartBranch::new)
        .transpose()?;
    repo_config::write(
        ui,
        helper,
        workspace.repo_path(),
        origin.as_ref(),
        branch.as_ref(),
        native,
        default_branch.as_ref(),
    )?;

    let repo_path = workspace.repo_path().to_owned();
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
        std::fs::write(&path, ctl.getop("operations", &hex).map_err(cmd_err)?).map_err(cmd_err)?;
        let op = repo.loader().load_operation(&id).await.map_err(cmd_err)?;
        let view = op.view_id().hex();
        let data = ctl.getop("views", &view).map_err(cmd_err)?;
        std::fs::write(op_store.join("views").join(&view), data).map_err(cmd_err)?;
        todo.extend(op.parent_ids().iter().cloned());
    }
    let heads = repo_path.join("op_heads").join("heads");
    for entry in std::fs::read_dir(&heads).map_err(cmd_err)? {
        std::fs::remove_file(entry.map_err(cmd_err)?.path()).map_err(cmd_err)?;
    }
    std::fs::write(heads.join(head.hex()), b"").map_err(cmd_err)?;

    std::fs::write(repo_path.join(FLUSHED_FILE), fetched).map_err(cmd_err)?;
    let wc_id = adopt_mount(&mut workspace, &ctl, &head, &args.commit, &mount, &jj_root).await?;
    if let Some(branch) = &branch {
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

async fn attach(ui: &mut Ui, helper: &CommandHelper, args: AttachArgs) -> Result<(), CommandError> {
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
    let repo = workspace
        .repo_loader()
        .load_at_head(Some(workspace.workspace_name()))
        .await
        .map_err(cmd_err)?;
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
    let tree_hex = std::str::from_utf8(raw.get(5..45).unwrap_or_default()).map_err(cmd_err)?;
    let shown = ctl.dirty().map_err(cmd_err)?.base_tree;
    if shown != tree_hex {
        return Err(cmd_err(format!(
            "daemon shows tree {shown} but commit {mounted} has tree {tree_hex}"
        )));
    }
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
    let link = mount.join(".jj");
    std::os::unix::fs::symlink(jj_root.join(".jj"), &link)
        .map_err(|e| cmd_err(format!("cannot link {}: {e}", link.display())))?;
    Ok(wc_id)
}

fn hash_mount_dir(dir: &Path, top: bool) -> std::io::Result<Option<gix::ObjectId>> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    let mut entries: Vec<gix::objs::tree::Entry> = vec![];
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        if top && (name == ".jj" || name == ".git") {
            continue;
        }
        let md = std::fs::symlink_metadata(e.path())?;
        let (mode, oid) = if md.file_type().is_symlink() {
            let t = std::fs::read_link(e.path())?;
            (
                gix::object::tree::EntryKind::Link,
                motte_jj::ctl::hash_object(gix::objs::Kind::Blob, t.as_os_str().as_bytes()),
            )
        } else if md.is_file() {
            let data = std::fs::read(e.path())?;
            let kind = if md.mode() & 0o100 != 0 {
                gix::object::tree::EntryKind::BlobExecutable
            } else {
                gix::object::tree::EntryKind::Blob
            };
            (
                kind,
                motte_jj::ctl::hash_object(gix::objs::Kind::Blob, &data),
            )
        } else if md.is_dir() {
            match hash_mount_dir(&e.path(), false)? {
                Some(id) => (gix::object::tree::EntryKind::Tree, id),
                None => continue,
            }
        } else {
            continue;
        };
        entries.push(gix::objs::tree::Entry {
            mode: mode.into(),
            filename: name.as_bytes().into(),
            oid,
        });
    }
    if entries.is_empty() {
        return Ok(None);
    }
    entries.sort_unstable();
    use gix::objs::WriteTo as _;
    let mut buf = vec![];
    gix::objs::Tree { entries }.write_to(&mut buf)?;
    Ok(Some(motte_jj::ctl::hash_object(
        gix::objs::Kind::Tree,
        &buf,
    )))
}

fn mount_entries(
    dir: &Path,
    rel: &str,
    top: bool,
    out: &mut std::collections::BTreeMap<String, String>,
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        if top && (name == ".jj" || name == ".git") {
            continue;
        }
        let p = if rel.is_empty() {
            name.to_string_lossy().into_owned()
        } else {
            format!("{rel}/{}", name.to_string_lossy())
        };
        let md = std::fs::symlink_metadata(e.path())?;
        if md.file_type().is_symlink() {
            let t = std::fs::read_link(e.path())?;
            out.insert(
                p,
                format!(
                    "L:{}",
                    motte_jj::ctl::hash_object(gix::objs::Kind::Blob, t.as_os_str().as_bytes())
                ),
            );
        } else if md.is_file() {
            let data = std::fs::read(e.path())?;
            let k = if md.mode() & 0o100 != 0 { "X" } else { "F" };
            out.insert(
                p,
                format!(
                    "{k}:{}",
                    motte_jj::ctl::hash_object(gix::objs::Kind::Blob, &data)
                ),
            );
        } else if md.is_dir() {
            mount_entries(&e.path(), &p, false, out)?;
        }
    }
    Ok(())
}

async fn describe_mismatch(tree: &jj_lib::merged_tree::MergedTree, root: &Path) -> Vec<String> {
    let mut mount = std::collections::BTreeMap::new();
    let _ = mount_entries(root, "", true, &mut mount);
    let mut want = std::collections::BTreeMap::new();
    let mut entries = tree.entries();
    while let Some((p, v)) = entries.next().await {
        let s = match v.map(|v| v.into_resolved()) {
            Ok(Ok(Some(jj_lib::backend::TreeValue::File { id, executable, .. }))) => {
                format!("{}:{}", if executable { "X" } else { "F" }, id.hex())
            }
            Ok(Ok(Some(jj_lib::backend::TreeValue::Symlink(id)))) => format!("L:{}", id.hex()),
            other => format!("other:{other:?}"),
        };
        want.insert(p.as_internal_file_string().to_owned(), s);
    }
    let mut diffs = vec![];
    for (p, m) in &mount {
        match want.get(p) {
            None => diffs.push(format!("only in mount: {p} {m}")),
            Some(w) if w != m => diffs.push(format!("differs: {p} mount={m} tree={w}")),
            _ => {}
        }
    }
    for p in want.keys() {
        if !mount.contains_key(p) {
            diffs.push(format!("only in tree: {p}"));
        }
    }
    diffs.truncate(25);
    diffs
}

async fn verify(ui: &mut Ui, helper: &CommandHelper) -> Result<(), CommandError> {
    let ws = helper.workspace_helper(ui).await?;
    let wc_id = ws
        .get_wc_commit_id()
        .ok_or_else(|| cmd_err("no working-copy commit"))?
        .clone();
    let commit = ws
        .repo()
        .store()
        .get_commit_async(&wc_id)
        .await
        .map_err(cmd_err)?;
    let tree = commit.tree();
    let root = ws.workspace_root().to_owned();
    if !tree.tree_ids().is_resolved() {
        writeln!(ui.stdout(), "VERIFY-SKIP conflicted working-copy tree")?;
        return Ok(());
    }
    let want = tree.tree_ids().first().hex();
    let t = std::time::Instant::now();
    let got = hash_mount_dir(&root, true)
        .map_err(cmd_err)?
        .unwrap_or_else(|| gix::ObjectId::empty_tree(gix::hash::Kind::Sha1))
        .to_string();
    if got == want {
        writeln!(ui.stdout(), "VERIFY-OK tree={want} walk={:?}", t.elapsed())?;
        Ok(())
    } else {
        writeln!(ui.stdout(), "VERIFY-FAIL mount={got} tree={want}")?;
        for d in describe_mismatch(&tree, &root).await {
            writeln!(ui.stdout(), "  {d}")?;
        }
        Err(cmd_err("mount does not match the working-copy tree"))
    }
}

fn main() -> std::process::ExitCode {
    motte_jj::trace::init();
    let mut wcf = WorkingCopyFactories::new();
    wcf.insert(
        WC_NAME.to_owned(),
        Box::new(VfsWorkingCopyFactory { init: None }),
    );
    let code = CliRunner::init()
        .add_store_factories(store_factories())
        .add_working_copy_factories(wcf)
        .add_subcommand(run_custom_command)
        .add_dispatch_hook(command_lock::hook)
        .run();
    motte_jj::trace::shutdown();
    code.into()
}
