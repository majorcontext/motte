use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::{Workspace, WorkspaceInitError};
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, HASH_KIND, ObjectStore, StoreError};
use motte_jj::stores::{MotteOpHeadsStore, MotteOpStore};
use motte_jj::wc::{LockedVfsWorkingCopy, VfsConfig, VfsWorkingCopyFactory};

use crate::cmd_err;
use crate::repo_config::{self, Origin};
use crate::start_branch::StartBranch;

/// The origin of a workspace, set at `init` and at `restore`.
#[derive(clap::Args, Clone, Debug)]
pub(crate) struct OriginArgs {
    /// Plain http(s) git URL of origin, which `jj motte push` pushes to
    #[arg(long)]
    origin_url: Option<String>,
    /// Branch of origin that the start commit is on: makes the bookmark and its tracked bookmark
    /// of origin where they are absent
    #[arg(long)]
    origin_branch: Option<String>,
    /// The repo is native: its origin is the store, which `jj motte push` and `fetch` use
    #[arg(long, conflicts_with = "origin_url")]
    origin_native: bool,
    /// Default branch of the native repo, which `jj motte push` and `fetch` use
    #[arg(long, requires = "origin_native")]
    origin_default_branch: Option<String>,
}

impl OriginArgs {
    pub(crate) fn branch(&self) -> Result<Option<StartBranch>, CommandError> {
        self.origin_branch
            .as_deref()
            .map(StartBranch::new)
            .transpose()
    }

    /// Writes the repo config that names the origin, with `start` as the commit that it holds.
    pub(crate) fn write(
        &self,
        ui: &mut Ui,
        helper: &CommandHelper,
        repo_path: &Path,
        start: &str,
    ) -> Result<(), CommandError> {
        let origin = self
            .origin_url
            .as_deref()
            .map(|url| Origin::new(url, start))
            .transpose()?;
        let default_branch = self
            .origin_default_branch
            .as_deref()
            .map(StartBranch::new)
            .transpose()?;
        let native = self.origin_native.then_some(start);
        repo_config::write(
            ui,
            helper,
            repo_path,
            origin.as_ref(),
            self.branch()?.as_ref(),
            native,
            default_branch.as_ref(),
        )
    }
}

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct InitArgs {
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
    #[command(flatten)]
    origin: OriginArgs,
}

/// The tree that a raw commit names, as 40 hex digits.
pub(crate) fn commit_tree(raw: &[u8]) -> Result<&str, CommandError> {
    raw.strip_prefix(b"tree ")
        .and_then(|rest| rest.get(..40))
        .and_then(|hex| std::str::from_utf8(hex).ok())
        .ok_or_else(|| cmd_err("the commit object does not start with a tree line"))
}

/// Fails unless the daemon shows exactly the tree of `commit`; otherwise the first snapshot lies.
pub(crate) fn check_mount_tree(
    ctl: &CtlClient,
    raw: &[u8],
    commit: &str,
) -> Result<(), CommandError> {
    let tree = commit_tree(raw)?;
    let shown = ctl.dirty().map_err(cmd_err)?.base_tree;
    if shown != tree {
        return Err(cmd_err(format!(
            "daemon shows tree {shown} but commit {commit} has tree {tree}; restart the daemon on this commit"
        )));
    }
    Ok(())
}

/// Links `.jj` of `jj_root` into the mount.
pub(crate) fn link_jj(mount: &Path, jj_root: &Path) -> Result<PathBuf, CommandError> {
    let link = mount.join(".jj");
    std::os::unix::fs::symlink(jj_root.join(".jj"), &link)
        .map_err(|e| cmd_err(format!("cannot link {}: {e}", link.display())))?;
    Ok(link)
}

pub(crate) async fn init(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: InitArgs,
) -> Result<(), CommandError> {
    let mount = match &args.mount {
        Some(m) => m.clone(),
        None => helper.cwd().to_owned(),
    };
    let mount = std::fs::canonicalize(&mount).map_err(cmd_err)?;
    std::fs::create_dir_all(&args.jj_root).map_err(cmd_err)?;
    let jj_root = std::fs::canonicalize(&args.jj_root).map_err(cmd_err)?;
    let sock = std::fs::canonicalize(&args.sock).map_err(cmd_err)?;

    let ctl = CtlClient::new(&sock);
    let commit_oid = gix::ObjectId::from_hex(args.commit.as_bytes()).map_err(cmd_err)?;
    let (_, raw) = ctl.get(&commit_oid).map_err(cmd_err)?;
    check_mount_tree(&ctl, &raw, &args.commit)?;
    let commit_id = CommitId::try_from_hex(&args.commit).ok_or_else(|| cmd_err("bad commit id"))?;
    let shallow = if args.full_history {
        vec![]
    } else {
        shallow_boundary(&ctl, commit_oid, WALK_LIMIT)?
    };
    let (mut workspace, repo) = new_workspace(ui, helper, &mount, &jj_root, &sock, shallow).await?;
    args.origin
        .write(ui, helper, workspace.repo_path(), &args.commit)?;

    let (repo, base, wc_commit) = start_commit(&repo, &commit_id, args.origin.branch()?).await?;
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

    let link = link_jj(&mount, &jj_root)?;
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

/// Adds the start commit as a head, makes the start bookmark, and checks out a new working-copy
/// commit on it in one operation.
async fn start_commit(
    repo: &Arc<ReadonlyRepo>,
    commit_id: &CommitId,
    branch: Option<StartBranch>,
) -> Result<
    (
        Arc<ReadonlyRepo>,
        jj_lib::commit::Commit,
        jj_lib::commit::Commit,
    ),
    CommandError,
> {
    let mut tx = repo.start_transaction();
    let base = tx
        .repo()
        .store()
        .get_commit_async(commit_id)
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
    Ok((repo, base, wc_commit))
}

/// Most commits that `shallow_boundary` fetches. Init fetches each of them again when jj indexes the
/// history, so the walk costs about twice this many daemon round trips.
pub(crate) const WALK_LIMIT: usize = 1_000;

/// Walks the ancestry of `start` through the store and returns the held commits that have a parent
/// the store lacks, whose parents the backend must hide. The walk fetches at most `limit` commits;
/// past it, every held commit that points at an unfetched one joins the result.
pub(crate) fn shallow_boundary(
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
pub(crate) async fn new_workspace(
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
