//! `jj motte snapshot`: the snapshot that every jj command takes, run on a timer by the caller. The
//! operation it writes goes through the op-heads store, which mirrors it to the remote op heads
//! and leaves the unsynced mark when the mirror fails. The command then publishes the head like a
//! flush and reports it to the control plane. It moves no bookmark and no checkout. It holds the
//! command lock exclusive through the snapshot only, so the publish never keeps a command waiting.

use std::io::Write as _;
use std::sync::atomic::Ordering;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo as _;
use motte_jj::backend::NativeBackend;
use motte_jj::ctl::{CtlClient, TokenReply};
use motte_jj::trace;
use motte_jj::wc::{FORCE_FULL, LAST_SNAPSHOT, syncfs};
use serde::Serialize;

use crate::command_lock::try_exclusive;
use crate::publish_cmd::publish;
use crate::{cmd_err, daemon};

/// The head operation of the last publish that the control plane took, with what the daemon showed
/// before that snapshot began, in the repo dir beside `op_store`: `<op> <token> <base_tree>`.
pub const PUBLISHED_FILE: &str = "motte_published";

#[derive(clap::Args, Clone, Debug)]
pub struct SnapshotArgs {
    /// Skip nothing: read every dirty file again and publish the head
    #[arg(long)]
    force: bool,
}

#[derive(Serialize, Default)]
struct Report {
    snapshot: bool,
    op: String,
    commit: String,
    reported: bool,
    dirty_files: u64,
    bytes: u64,
    skipped: Option<&'static str>,
}

pub async fn run(
    ui: &mut Ui,
    helper: &CommandHelper,
    args: SnapshotArgs,
) -> Result<(), CommandError> {
    let mut span = trace::span("jj.motte.snapshot");
    let report = snapshot(ui, helper, args.force).await?;
    span.set("dirty_files", report.dirty_files as i64);
    span.set("snapshot_bytes", report.bytes as i64);
    span.set("reported", i64::from(report.reported));
    span.set_str("skipped", report.skipped.unwrap_or("none"));
    writeln!(
        ui.stdout(),
        "{}",
        serde_json::to_string(&report).map_err(cmd_err)?
    )?;
    Ok(())
}

fn skipped(op: String, why: &'static str) -> Report {
    Report {
        op,
        skipped: Some(why),
        ..Report::default()
    }
}

async fn snapshot(
    ui: &mut Ui,
    helper: &CommandHelper,
    force: bool,
) -> Result<Report, CommandError> {
    let Some(lock) = try_exclusive(helper)? else {
        return Ok(skipped(String::new(), "busy"));
    };
    let ws = helper.workspace_helper_no_snapshot(ui).await?;
    if ws
        .repo()
        .store()
        .backend()
        .downcast_ref::<NativeBackend>()
        .is_none()
    {
        return Err(cmd_err("this repo has no native backend"));
    }
    let before = ws.repo().operation().id().hex();
    let ctl = daemon(ws.repo_path()).map_err(cmd_err)?;
    let mount = ws.workspace().workspace_root().to_owned();
    syncfs(&mount).map_err(|e| cmd_err(format!("syncfs on the mount failed: {e}")))?;
    ctl.prefetch_opheads();
    let pre = ctl.token().map_err(cmd_err)?;
    drop(ws);
    FORCE_FULL.store(force, Ordering::Relaxed);
    let ws = helper.workspace_helper(ui).await?;
    drop(lock);
    let op = ws.repo().operation().id().hex();
    let took = op != before;
    let published_file = ws.repo_path().join(PUBLISHED_FILE);
    if !force
        && let Some(last) = std::fs::read_to_string(&published_file)
            .ok()
            .as_deref()
            .and_then(parse_published)
        && last.op == op
        && syncfs(&mount).is_ok()
        && still_clean(&ctl, &last.shown).map_err(cmd_err)?
    {
        return Ok(skipped(op, "clean"));
    }
    let Some(p) = publish(&ws, &ctl, helper.settings(), true).await? else {
        return Ok(Report {
            snapshot: took,
            op,
            ..Report::default()
        });
    };
    if p.reported
        && let Some(line) = record(&pre, &p.head)
        && let Err(e) = std::fs::write(&published_file, line)
    {
        eprintln!("warning: cannot record the published head: {e}");
    }
    let (dirty_files, bytes) = if took {
        (
            LAST_SNAPSHOT[0].load(Ordering::Relaxed),
            LAST_SNAPSHOT[1].load(Ordering::Relaxed),
        )
    } else {
        (0, 0)
    };
    Ok(Report {
        snapshot: took,
        op,
        commit: p.commit,
        reported: p.reported,
        dirty_files,
        bytes,
        skipped: None,
    })
}

/// The line for `motte_published`. An unsettled token equals no later token, so it is not recorded.
fn record(shown: &TokenReply, head: &str) -> Option<String> {
    shown
        .settled()
        .then(|| format!("{head} {} {}", shown.token, shown.base_tree))
}

struct Published {
    op: String,
    shown: TokenReply,
}

/// Whether the daemon still shows what the publish recorded: a settled token and the base tree,
/// both equal.
fn still_clean(ctl: &CtlClient, recorded: &TokenReply) -> Result<bool, motte_jj::ctl::StoreError> {
    let t = ctl.token()?;
    Ok(t.settled() && t.token == recorded.token && t.base_tree == recorded.base_tree)
}

/// Reads `<op> <token> <base_tree>`; a file without all three skips nothing.
fn parse_published(text: &str) -> Option<Published> {
    let mut fields = text.split_whitespace();
    let (op, token, base_tree) = (fields.next()?, fields.next()?, fields.next()?);
    Some(Published {
        op: op.to_owned(),
        shown: TokenReply {
            boot: String::new(),
            token: token.to_owned(),
            base_tree: base_tree.to_owned(),
        },
    })
}
