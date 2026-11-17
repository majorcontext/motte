use std::io::Write as _;

use jj_cli::cli_util::{CommandHelper, WorkspaceCommandHelper};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::{RefTarget, RemoteRef, RemoteRefState};
use jj_lib::ref_name::{RefName, RemoteName};
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use motte_jj::ctl::{Code, CtlClient, HASH_KIND, StoreError};
use motte_jj::trace;

use crate::daemon;
use crate::native_delete;
use crate::native_push;
use crate::push_git::{Finished, Git, Update, failure};
use crate::push_objects::{self, Plan};
use crate::repo_config::{self, Origin};

pub const REMOTE: &str = "origin";

#[derive(clap::Args, Clone, Debug)]
pub struct PushArgs {
    /// Bookmark to push; repeat for several
    #[arg(long, short = 'b', required_unless_present = "delete")]
    bookmark: Vec<String>,
    /// Bookmark to delete from origin, a native repo only; repeat for several
    #[arg(long)]
    delete: Vec<String>,
    /// Remote to push to
    #[arg(long, default_value = REMOTE)]
    remote: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Refused,
    Stale,
    Rejected,
    Network,
    ClaimLost,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Self::Refused => "refused",
            Self::Stale => "stale",
            Self::Rejected => "rejected",
            Self::Network => "network",
            Self::ClaimLost => "claim_lost",
        }
    }
}

pub struct Fail {
    pub outcome: Outcome,
    error: CommandError,
}

impl Fail {
    pub fn new(outcome: Outcome, message: impl Into<String>) -> Self {
        Self {
            outcome,
            error: user_error(message.into()),
        }
    }
}

impl Fail {
    /// Names the bookmarks that an earlier step of the same command already moved, and drops
    /// the claim that nothing was sent.
    pub fn after(self, verb: &str, done: &[&str]) -> Self {
        let message = self.error.error.to_string();
        let message = message
            .strip_suffix(&format!("; nothing was {verb}"))
            .unwrap_or(&message);
        Self {
            outcome: self.outcome,
            error: user_error(format!("{message}; {verb} {} before it", done.join(", "))),
        }
    }

    pub fn into_error(self) -> CommandError {
        self.error
    }
}

impl From<CommandError> for Fail {
    fn from(error: CommandError) -> Self {
        let lost = std::iter::successors(
            Some(&*error.error as &(dyn std::error::Error + 'static)),
            |e| e.source(),
        )
        .any(|e| {
            e.downcast_ref::<StoreError>()
                .is_some_and(|s| s.is(&Code::ClaimLost))
        });
        let outcome = if lost {
            Outcome::ClaimLost
        } else {
            Outcome::Refused
        };
        Self { outcome, error }
    }
}

#[derive(Clone)]
pub(crate) struct Target {
    pub name: String,
    pub id: CommitId,
    pub lease: Option<String>,
}

pub async fn run(ui: &mut Ui, helper: &CommandHelper, args: PushArgs) -> Result<(), CommandError> {
    let mut span = trace::span("jj.push");
    span.set_str("remote", &args.remote);
    span.set("refs", (args.bookmark.len() + args.delete.len()) as i64);
    let result = push(ui, helper, &args).await;
    span.set_str(
        "outcome",
        result.as_ref().map_or_else(|f| f.outcome.name(), |()| "ok"),
    );
    result.map_err(|f| f.error)
}

async fn push(ui: &mut Ui, helper: &CommandHelper, args: &PushArgs) -> Result<(), Fail> {
    if args.remote != REMOTE {
        return Err(Fail::new(
            Outcome::Refused,
            "only the remote origin is configured",
        ));
    }
    let mut names = args.bookmark.clone();
    names.sort();
    names.dedup();
    let mut deletes = args.delete.clone();
    deletes.sort();
    deletes.dedup();
    if !names.is_empty() && !deletes.is_empty() {
        return Err(Fail::new(
            Outcome::Refused,
            "push and delete in separate commands",
        ));
    }
    let mut ws = helper.workspace_helper(ui).await?;
    if let Some(native) =
        repo_config::native(helper.settings()).map_err(|e| Fail::new(Outcome::Refused, e))?
    {
        if deletes.is_empty() {
            return native_push::push(ui, &mut ws, &names, &native).await;
        }
        return native_delete::delete(ui, &mut ws, &deletes, &native).await;
    }
    if !deletes.is_empty() {
        return Err(Fail::new(
            Outcome::Refused,
            "deleting a bookmark of origin is supported only for a native repo",
        ));
    }
    git_push(ui, helper, &mut ws, &names).await
}

/// Pushes to a git origin. The claim is probed before and after the objects reach the transport
/// repo, so a writer that lost its claim sends nothing.
async fn git_push(
    ui: &mut Ui,
    helper: &CommandHelper,
    ws: &mut WorkspaceCommandHelper,
    names: &[String],
) -> Result<(), Fail> {
    let origin = Origin::from_settings(helper.settings()).ok_or_else(|| {
        Fail::new(
            Outcome::Refused,
            "this workspace has no origin; it has no GitHub repository",
        )
    })?;
    let ctl = daemon(ws.repo_path()).map_err(|e| Fail::new(Outcome::Refused, e))?;
    let repo = ws.repo().clone();
    let targets = targets(&repo, names).await?;
    let git = Git::open(ws.repo_path(), &origin.url)?;
    let odb = open_odb(&git)?;
    let ids: Vec<gix::ObjectId> = targets.iter().map(|t| to_oid(&t.id)).collect();
    let start = CommitId::try_from_hex(&origin.base).ok_or_else(|| {
        Fail::new(
            Outcome::Refused,
            "the origin base commit is not a commit ID",
        )
    })?;
    let plan = push_objects::plan(&ctl, &odb, repo.index(), &start, &ids).await?;
    check_new_commits(&repo, &plan).await?;

    claim(&ctl, "pushed")?;
    let origin_branch = repo_config::origin_branch(helper.settings());
    let mut asked = names.to_vec();
    asked.extend(origin_branch.clone());
    let remote = git.ls_remote(&asked)?;
    for t in &targets {
        if remote.default_branch.as_deref() == Some(&t.name) {
            let tip = remote.tips.get(&t.name).map(String::as_str);
            refuse_rewrite(&repo, t, "default", tip).await?;
        }
    }
    let pending: Vec<&Target> = targets
        .iter()
        .filter(|t| remote.tips.get(&t.name) != Some(&t.id.hex()))
        .collect();
    if pending.is_empty() {
        return record(ui, ws, &targets, true).await;
    }
    let mut branches: Vec<String> = names
        .iter()
        .filter(|n| remote.tips.contains_key(*n))
        .cloned()
        .collect();
    branches.extend(remote.default_branch.clone());
    branches.extend(origin_branch.filter(|b| remote.tips.contains_key(b)));
    fetch_bases(&git, &plan, branches)?;
    let copied = push_objects::copy(&ctl, &git.objects_dir(), &plan)?;
    writeln!(
        ui.status(),
        "Pushing {} commits ({} objects, {} bytes) to {REMOTE}",
        plan.commits.len(),
        copied.objects,
        copied.bytes
    )
    .map_err(CommandError::from)?;

    claim(&ctl, "pushed")?;
    let updates: Vec<Update> = pending
        .iter()
        .map(|t| Update {
            name: t.name.clone(),
            sha: t.id.hex(),
            lease: t.lease.clone(),
        })
        .collect();
    judge(&git.push(&updates)?)?;
    record(ui, ws, &targets, false).await
}

/// Fetches the base commits of the plan from origin into the transport repo, so that the new
/// commits have parents there.
fn fetch_bases(git: &Git, plan: &Plan, mut branches: Vec<String>) -> Result<(), Fail> {
    if plan.base.is_empty() {
        return Ok(());
    }
    let shas: Vec<String> = plan.base.iter().map(ToString::to_string).collect();
    branches.sort();
    branches.dedup();
    git.fetch(&shas, &branches)?;
    match plan.base.iter().find(|b| !git.has_all(&[b.to_string()])) {
        Some(missing) => Err(Fail::new(
            Outcome::Network,
            format!("origin does not have commit {missing}"),
        )),
        None => Ok(()),
    }
}

pub fn open_odb(git: &Git) -> Result<gix::odb::Handle, Fail> {
    gix::odb::at(git.objects_dir(), HASH_KIND)
        .map_err(|e| Fail::new(Outcome::Network, e.to_string()))
}

pub fn to_oid(id: &CommitId) -> gix::ObjectId {
    gix::ObjectId::from_bytes_or_panic(id.as_bytes())
}

/// Reads each bookmark and the commit of origin that the push expects to replace.
pub(crate) async fn targets(repo: &ReadonlyRepo, names: &[String]) -> Result<Vec<Target>, Fail> {
    let origin = RemoteName::new(REMOTE);
    let mut out = vec![];
    for name in names {
        let local = repo.view().get_local_bookmark(RefName::new(name));
        if local.is_absent() {
            return Err(Fail::new(
                Outcome::Refused,
                format!("bookmark {name} does not exist"),
            ));
        }
        let Some(id) = local.as_normal() else {
            return Err(Fail::new(
                Outcome::Refused,
                format!("bookmark {name} is conflicted"),
            ));
        };
        if id == repo.store().root_commit_id() {
            return Err(Fail::new(
                Outcome::Refused,
                format!("bookmark {name} is on the root commit"),
            ));
        }
        let remote = &repo
            .view()
            .get_remote_bookmark(RefName::new(name).to_remote_symbol(origin))
            .target;
        let lease = match (remote.is_absent(), remote.as_normal()) {
            (true, _) => None,
            (false, Some(lease)) => Some(lease.hex()),
            (false, None) => {
                return Err(Fail::new(
                    Outcome::Refused,
                    format!("{name}@{REMOTE} is conflicted"),
                ));
            }
        };
        out.push(Target {
            name: name.clone(),
            id: id.clone(),
            lease,
        });
    }
    Ok(out)
}

/// Refuses the commits that would reach origin as jj's internal state or with no message.
pub(crate) async fn check_new_commits(repo: &ReadonlyRepo, plan: &Plan) -> Result<(), Fail> {
    for id in &plan.commits {
        let commit = repo
            .store()
            .get_commit_async(&CommitId::from_bytes(id.as_bytes()))
            .await
            .map_err(|e| Fail::new(Outcome::Refused, e.to_string()))?;
        let short = &commit.id().hex()[..12];
        if commit.has_conflict() {
            return Err(Fail::new(
                Outcome::Refused,
                format!("commit {short} has conflicts; resolve them first"),
            ));
        }
        if commit.description().trim().is_empty() {
            return Err(Fail::new(
                Outcome::Refused,
                format!("commit {short} has no description; run jj describe"),
            ));
        }
    }
    Ok(())
}

/// A bookmark that names a protected branch of origin, of `kind`, may move forward only: `tip`,
/// the commit that origin holds for it, must be an ancestor of the target.
pub(crate) async fn refuse_rewrite(
    repo: &ReadonlyRepo,
    t: &Target,
    kind: &str,
    tip: Option<&str>,
) -> Result<(), Fail> {
    let Some(tip) = tip.and_then(CommitId::try_from_hex) else {
        return Ok(());
    };
    let known = repo.index().has_id(&tip).await.unwrap_or(false);
    if !known || !repo.index().is_ancestor(&tip, &t.id).await.unwrap_or(false) {
        return Err(Fail::new(
            Outcome::Refused,
            format!(
                "refusing to rewrite {}, the {kind} branch of {REMOTE}: its tip is not an ancestor of the commit",
                t.name
            ),
        ));
    }
    Ok(())
}

/// Reads the daemon's op heads, which fails with `vcs_claim_lost` once a newer claim owns the workspace.
pub fn claim(ctl: &CtlClient, sent: &str) -> Result<(), Fail> {
    match ctl.opheads(None) {
        Ok(_) => Ok(()),
        Err(e) if e.is(&Code::NotFound) => Ok(()),
        Err(e) if e.is(&Code::ClaimLost) => Err(store_fail(e, sent)),
        Err(e) => Err(Fail::new(
            Outcome::Network,
            format!("cannot confirm the claim: {e}; nothing was {sent}"),
        )),
    }
}

/// The outcome of a daemon failure: a lost claim, a daemon without a ref store, or the network.
/// `sent` names what the command had not done yet.
pub(crate) fn store_fail(e: StoreError, sent: &str) -> Fail {
    match e.code() {
        Some(Code::ClaimLost) => Fail::new(
            Outcome::ClaimLost,
            format!("this workspace lost its claim on the op log; nothing was {sent}"),
        ),
        Some(Code::UnknownOp) => Fail::new(
            Outcome::Refused,
            "the daemon of this workspace has no ref store; the repo is not native",
        ),
        _ => Fail::new(
            Outcome::Network,
            format!("the daemon failed; nothing was {sent}: {e}"),
        ),
    }
}

fn judge(finished: &Finished) -> Result<(), Fail> {
    if finished.ok {
        return Ok(());
    }
    let mut rejected = vec![];
    for line in finished.stdout.lines().filter(|l| l.starts_with('!')) {
        let fields: Vec<&str> = line.split('\t').collect();
        let (Some(refs), Some(summary)) = (fields.get(1), fields.get(2)) else {
            continue;
        };
        let name = refs.rsplit_once(":refs/heads/").map_or(*refs, |(_, n)| n);
        let reason = summary
            .rsplit_once('(')
            .map_or("", |(_, r)| r.trim_end_matches(')'));
        if !["atomic push failed", "atomic push failure"].contains(&reason) {
            rejected.push((name, reason));
        }
    }
    let stale = ["stale info", "fetch first", "non-fast-forward"];
    if let Some((name, _)) = rejected.iter().find(|(_, r)| stale.contains(r)) {
        return Err(Fail::new(
            Outcome::Stale,
            format!("{REMOTE} moved {name}; the remote has a newer commit"),
        ));
    }
    if let Some((name, reason)) = rejected.first() {
        return Err(Fail::new(
            Outcome::Rejected,
            format!("{REMOTE} rejected {name}: {reason}"),
        ));
    }
    Err(failure("push", finished))
}

/// Records each pushed bookmark as tracked at the pushed commit, in one operation.
pub(crate) async fn record(
    ui: &mut Ui,
    ws: &mut WorkspaceCommandHelper,
    targets: &[Target],
    already: bool,
) -> Result<(), Fail> {
    let mut tx = ws.start_transaction();
    for t in targets {
        let symbol = RefName::new(&t.name).to_remote_symbol(RemoteName::new(REMOTE));
        tx.repo_mut().set_remote_bookmark(
            symbol,
            RemoteRef {
                target: RefTarget::normal(t.id.clone()),
                state: RemoteRefState::Tracked,
            },
        );
    }
    let names: Vec<&str> = targets.iter().map(|t| t.name.as_str()).collect();
    tx.finish(ui, format!("push {} to {REMOTE}", names.join(", ")))
        .await?;
    let line = if already {
        format!("{} already at {REMOTE}", names.join(", "))
    } else {
        format!("Pushed {} to {REMOTE}", names.join(", "))
    };
    writeln!(ui.status(), "{line}").map_err(CommandError::from)?;
    Ok(())
}
