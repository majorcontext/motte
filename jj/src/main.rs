use std::io::Write as _;

use motte_jj::ctl::{CtlClient, socket_path};
use motte_jj::stores::store_factories;
use motte_jj::wc::{VfsWorkingCopyFactory, WC_NAME};

mod changes_cmd;
mod command_lock;
mod fetch;
mod fetch_objects;
mod init_cmd;
mod native_check;
mod native_delete;
mod native_fetch;
mod native_push;
mod publish_cmd;
mod push;
mod push_git;
mod push_objects;
mod repo_config;
mod restore_cmd;
mod snapshot_cmd;
mod start_branch;
mod verify_cmd;

use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::workspace::WorkingCopyFactories;

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
    Init(init_cmd::InitArgs),
    /// Show what the daemon reports as changed since the last sync
    Dirty,
    /// Walk the whole mount, hash it like git, and compare with the working-copy commit's tree
    Verify,
    /// Upload the op log to the store, set the remote op heads to the local head, and print the
    /// head operation and the working-copy commit as JSON
    Flush,
    /// Upload the op log of the loaded operation and set the remote op heads to it, without a
    /// snapshot or an operation, and print whether it drained as JSON
    Sync(publish_cmd::SyncArgs),
    /// Snapshot the working copy like any jj command, unless a command holds its lock, and print
    /// the outcome as JSON
    Snapshot(snapshot_cmd::SnapshotArgs),
    /// Rebuild the .jj of a workspace from the op log in the store
    Restore(restore_cmd::RestoreArgs),
    /// Store again the tree with conflict markers that a mount commit names
    Rematerialize(publish_cmd::RematerializeArgs),
    /// Link the .jj under a root into a new mount of the working-copy commit of its op head
    Attach(restore_cmd::AttachArgs),
    /// Print the work-tree diff for harness as JSON, read-only: no snapshot, no operation
    Changes(changes_cmd::ChangesArgs),
    /// Push bookmarks to origin: the GitHub repo with git, or the ref store of a native repo
    Push(push::PushArgs),
    /// Fetch the bookmarks of origin: from the GitHub repo with git, or from the ref store of a native repo
    Fetch(fetch::FetchArgs),
}

pub(crate) fn cmd_err(e: impl std::fmt::Display) -> CommandError {
    user_error(e.to_string())
}

/// The client of the daemon of the workspace whose repo directory is `repo_path`.
pub(crate) fn daemon(repo_path: &std::path::Path) -> Result<CtlClient, String> {
    socket_path(&repo_path.join("store"))
        .map(CtlClient::new)
        .map_err(|e| format!("no daemon socket: {e}; set MOTTE_VFS_SOCK"))
}

async fn run_custom_command(
    ui: &mut Ui,
    helper: &CommandHelper,
    command: CustomCommand,
) -> Result<(), CommandError> {
    let CustomCommand::Motte { cmd } = command;
    match cmd {
        MotteCommand::Init(args) => init_cmd::init(ui, helper, args).await,
        MotteCommand::Verify => verify_cmd::verify(ui, helper).await,
        MotteCommand::Flush => publish_cmd::flush(ui, helper).await,
        MotteCommand::Rematerialize(args) => publish_cmd::rematerialize(ui, helper, args).await,
        MotteCommand::Sync(args) => publish_cmd::sync(ui, helper, args).await,
        MotteCommand::Snapshot(args) => snapshot_cmd::run(ui, helper, args).await,
        MotteCommand::Restore(args) => restore_cmd::restore(ui, helper, args).await,
        MotteCommand::Attach(args) => restore_cmd::attach(ui, helper, args).await,
        MotteCommand::Changes(args) => changes_cmd::run(ui, helper, args).await,
        MotteCommand::Push(args) => push::run(ui, helper, args).await,
        MotteCommand::Fetch(args) => fetch::run(ui, helper, args).await,
        MotteCommand::Dirty => dirty(ui, helper),
    }
}

fn dirty(ui: &mut Ui, helper: &CommandHelper) -> Result<(), CommandError> {
    let repo_path = helper
        .workspace_loader()
        .map(|l| l.repo_path().to_owned())
        .unwrap_or_default();
    let r = daemon(&repo_path)
        .map_err(cmd_err)?
        .dirty()
        .map_err(cmd_err)?;
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

/// The release build sets MOTTE_VERSION to its tag; any other build reports the crate version.
const VERSION: &str = match option_env!("MOTTE_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

fn main() -> std::process::ExitCode {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|a| a == motte_jj::trace_export::ARG)
    {
        motte_jj::trace_export::run();
        return std::process::ExitCode::SUCCESS;
    }
    motte_jj::trace::init();
    let mut wcf = WorkingCopyFactories::new();
    wcf.insert(
        WC_NAME.to_owned(),
        Box::new(VfsWorkingCopyFactory { init: None }),
    );
    let code = CliRunner::init()
        .version(VERSION)
        .add_store_factories(store_factories())
        .add_working_copy_factories(wcf)
        .add_subcommand(run_custom_command)
        .add_dispatch_hook(command_lock::hook)
        .run();
    motte_jj::ctl::flush_all_deferred();
    motte_jj::trace::shutdown();
    code.into()
}
