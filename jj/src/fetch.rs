use std::collections::HashSet;
use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::{RefTarget, RemoteRef, RemoteRefState};
use jj_lib::ref_name::{RefName, RemoteName};
use jj_lib::repo::Repo as _;
use motte_jj::ctl::CtlClient;
use motte_jj::trace;

use crate::daemon;
use crate::fetch_objects::{self, Walk};
use crate::init_cmd::WALK_LIMIT;
use crate::native_fetch;
use crate::push::{self, Fail, Outcome, REMOTE};
use crate::push_git::Git;
use crate::repo_config::{self, Origin};

/// Most bookmarks that a fetch with no `-b` takes from origin.
const MAX_BOOKMARKS: usize = 100;

/// Commits of history per head that each attempt asks for, up to the walk limit.
const DEPTHS: [usize; 4] = [1, 20, 200, WALK_LIMIT];

#[derive(clap::Args, Clone, Debug)]
pub struct FetchArgs {
    /// Bookmark to fetch; repeat for several. Without it, every bookmark of origin
    #[arg(long, short = 'b')]
    bookmark: Vec<String>,
    /// Remote to fetch from
    #[arg(long, default_value = REMOTE)]
    remote: String,
}

pub(crate) struct Tip {
    pub name: String,
    pub id: CommitId,
}

pub async fn run(ui: &mut Ui, helper: &CommandHelper, args: FetchArgs) -> Result<(), CommandError> {
    let mut span = trace::span("jj.fetch");
    span.set_str("remote", &args.remote);
    span.set("refs", args.bookmark.len() as i64);
    let result = fetch(ui, helper, &args).await;
    span.set_str(
        "outcome",
        result.as_ref().map_or_else(|f| f.outcome.name(), |()| "ok"),
    );
    result.map_err(Fail::into_error)
}

async fn fetch(ui: &mut Ui, helper: &CommandHelper, args: &FetchArgs) -> Result<(), Fail> {
    if args.remote != REMOTE {
        return Err(Fail::new(
            Outcome::Refused,
            "only the remote origin is configured",
        ));
    }
    let mut names = args.bookmark.clone();
    names.sort();
    names.dedup();
    let mut ws = helper.workspace_helper(ui).await?;
    if let Some(native) =
        repo_config::native(helper.settings()).map_err(|e| Fail::new(Outcome::Refused, e))?
    {
        return native_fetch::fetch(ui, &mut ws, &names, &native.default_branch).await;
    }
    let origin = Origin::from_settings(helper.settings()).ok_or_else(|| {
        Fail::new(
            Outcome::Refused,
            "this workspace has no origin; it has no GitHub repository",
        )
    })?;
    let ctl = daemon(ws.repo_path()).map_err(|e| Fail::new(Outcome::Refused, e))?;
    let repo = ws.repo().clone();

    push::claim(&ctl, "fetched")?;
    let git = Git::open(ws.repo_path(), &origin.url)?;
    let remote = git.ls_remote(&names)?;
    let wanted = wanted(ui, &names, &remote.tips, remote.default_branch.as_deref())?;
    let (old, pending) = compare(&repo, &wanted, &remote.tips)?;
    if pending.is_empty() {
        return up_to_date(ui, &wanted);
    }

    let Fetched {
        tips,
        commits,
        shallow,
    } = obtain(&git, &ctl, &repo, &pending).await?;
    push::claim(&ctl, "fetched")?;
    let stored = fetch_objects::store(&ctl, &push::open_odb(&git)?, &commits)?;
    writeln!(
        ui.status(),
        "Fetched {} commits ({} objects, {} bytes) from {REMOTE}",
        commits.len(),
        stored.objects,
        stored.bytes
    )
    .map_err(CommandError::from)?;
    record(ui, &mut ws, &wanted, &old, &tips, &shallow, &[]).await
}

/// The recorded `NAME@origin` of each wanted bookmark, and the wanted bookmarks whose commit at
/// origin differs from it.
pub(crate) fn compare(
    repo: &jj_lib::repo::ReadonlyRepo,
    wanted: &[String],
    tips: &std::collections::HashMap<String, String>,
) -> Result<(Vec<RemoteRef>, Vec<String>), Fail> {
    let mut old = vec![];
    for name in wanted {
        let symbol = RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE));
        let current = repo.view().get_remote_bookmark(symbol).clone();
        if !current.target.is_resolved() {
            return Err(Fail::new(
                Outcome::Refused,
                format!("{name}@{REMOTE} is conflicted"),
            ));
        }
        old.push(current);
    }
    let pending = wanted
        .iter()
        .zip(&old)
        .filter(|(n, o)| o.target.as_normal().map(|c| c.hex()) != tips.get(*n).cloned())
        .map(|(n, _)| n.clone())
        .collect();
    Ok((old, pending))
}

pub(crate) fn up_to_date(ui: &mut Ui, wanted: &[String]) -> Result<(), Fail> {
    for name in wanted {
        writeln!(ui.stdout(), "{name}: up to date").map_err(CommandError::from)?;
    }
    Ok(())
}

/// The shallow roots that earlier fetches recorded in the backend.
pub(crate) fn known_roots(repo: &jj_lib::repo::ReadonlyRepo) -> Result<HashSet<CommitId>, Fail> {
    Ok(repo
        .store()
        .backend()
        .downcast_ref::<motte_jj::backend::NativeBackend>()
        .ok_or_else(|| Fail::new(Outcome::Refused, "this repo has no native backend"))?
        .shallow_roots()
        .map_err(|e| Fail::new(Outcome::Refused, format!("cannot read the roots: {e}")))?
        .into_iter()
        .collect())
}

/// The bookmarks to fetch: the named ones, or the default branch and the first of the others.
pub(crate) fn wanted(
    ui: &mut Ui,
    names: &[String],
    tips: &std::collections::HashMap<String, String>,
    default: Option<&str>,
) -> Result<Vec<String>, Fail> {
    if !names.is_empty() {
        if let Some(missing) = names.iter().find(|n| !tips.contains_key(*n)) {
            return Err(Fail::new(
                Outcome::Refused,
                format!("{REMOTE} has no bookmark {missing}"),
            ));
        }
        return Ok(names.to_vec());
    }
    let mut all: Vec<String> = tips.keys().cloned().collect();
    all.sort();
    if let Some(d) = default.filter(|d| tips.contains_key(*d)) {
        all.retain(|n| n != d);
        all.insert(0, d.to_owned());
    }
    if all.len() > MAX_BOOKMARKS {
        writeln!(
            ui.warning_default(),
            "{REMOTE} has {} bookmarks; fetching {MAX_BOOKMARKS}. Name the others with -b",
            all.len()
        )
        .map_err(CommandError::from)?;
        all.truncate(MAX_BOOKMARKS);
    }
    Ok(all)
}

/// Fetches the heads with more history until the walk reaches commits that jj holds, or, at the
/// deepest attempt, the shallow boundary of origin. Returns the commit that each head has now,
/// which can differ from the one `ls-remote` showed, the commits to write, and those of them
/// that become roots.
struct Fetched {
    tips: Vec<Tip>,
    commits: Vec<gix::ObjectId>,
    shallow: Vec<gix::ObjectId>,
}

async fn obtain(
    git: &Git,
    ctl: &CtlClient,
    repo: &jj_lib::repo::ReadonlyRepo,
    pending: &[String],
) -> Result<Fetched, Fail> {
    let known = known_roots(repo)?;
    for (n, depth) in DEPTHS.into_iter().enumerate() {
        git.fetch_heads(pending, depth)?;
        let fetched = git.fetched()?;
        let mut tips = vec![];
        for name in pending {
            let id = fetched
                .get(name)
                .and_then(|s| CommitId::try_from_hex(s.as_str()))
                .ok_or_else(|| Fail::new(Outcome::Network, format!("git did not fetch {name}")))?;
            tips.push(Tip {
                name: name.clone(),
                id,
            });
        }
        let ids: Vec<gix::ObjectId> = tips.iter().map(|t| push::to_oid(&t.id)).collect();
        let odb = push::open_odb(git)?;
        let last = n + 1 == DEPTHS.len();
        if let Walk::Done { commits, shallow } =
            fetch_objects::walk(ctl, Some(&odb), repo.index(), &known, &ids, last).await?
        {
            return Ok(Fetched {
                tips,
                commits,
                shallow,
            });
        }
    }
    Err(Fail::new(
        Outcome::Refused,
        "origin did not send the commits of the fetched bookmarks",
    ))
}

/// Records each fetched head as `NAME@origin` in one operation, and forgets the `NAME@origin` of
/// each `gone` bookmark, keeping its local bookmark. A tracked bookmark whose local target is the
/// old `NAME@origin` follows it when that is a fast-forward.
pub(crate) async fn record(
    ui: &mut Ui,
    ws: &mut jj_cli::cli_util::WorkspaceCommandHelper,
    wanted: &[String],
    old: &[RemoteRef],
    tips: &[Tip],
    shallow: &[gix::ObjectId],
    gone: &[String],
) -> Result<(), Fail> {
    let mut tx = ws.start_transaction();
    add_tips(&mut tx, tips, shallow).await?;
    let mut lines = vec![];
    for (name, previous) in wanted.iter().zip(old) {
        let Some(tip) = tips.iter().find(|t| &t.name == name) else {
            lines.push(format!("{name}: up to date"));
            continue;
        };
        let before = previous.target.as_normal();
        lines.push(format!(
            "{name}: {} -> {}",
            before.map_or_else(|| "new".to_owned(), |c| short(&c.hex())),
            short(&tip.id.hex())
        ));
        let state = if previous.target.is_absent() {
            RemoteRefState::Tracked
        } else {
            previous.state
        };
        let symbol = RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE));
        tx.repo_mut().set_remote_bookmark(
            symbol,
            RemoteRef {
                target: RefTarget::normal(tip.id.clone()),
                state,
            },
        );
        if state == RemoteRefState::Tracked {
            follow_local(ui, &mut tx, name, tip, before).await?;
        }
    }
    for name in gone {
        let symbol = RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE));
        tx.repo_mut().set_remote_bookmark(
            symbol,
            RemoteRef {
                target: RefTarget::absent(),
                state: RemoteRefState::New,
            },
        );
        lines.push(format!("{name}: deleted on {REMOTE}"));
    }
    tx.finish(ui, format!("fetch {} from {REMOTE}", names(tips, gone)))
        .await?;
    for line in lines {
        writeln!(ui.stdout(), "{line}").map_err(CommandError::from)?;
    }
    Ok(())
}

/// Records the shallow roots and makes each fetched tip a head.
async fn add_tips(
    tx: &mut jj_cli::cli_util::WorkspaceCommandTransaction<'_>,
    tips: &[Tip],
    shallow: &[gix::ObjectId],
) -> Result<(), Fail> {
    if !shallow.is_empty() {
        let ids: Vec<CommitId> = shallow
            .iter()
            .map(|i| CommitId::from_bytes(i.as_bytes()))
            .collect();
        let backend = tx.repo().store().backend();
        let native = backend
            .downcast_ref::<motte_jj::backend::NativeBackend>()
            .ok_or_else(|| Fail::new(Outcome::Refused, "this repo has no native backend"))?;
        native
            .add_shallow(&ids)
            .map_err(|e| Fail::new(Outcome::Refused, format!("cannot record the roots: {e}")))?;
    }
    let mut commits = vec![];
    for t in tips {
        commits.push(
            tx.repo()
                .store()
                .get_commit_async(&t.id)
                .await
                .map_err(|e| Fail::new(Outcome::Refused, format!("{}: {e}", t.name)))?,
        );
    }
    tx.repo_mut()
        .add_heads(&commits)
        .await
        .map_err(|e| Fail::new(Outcome::Refused, e.to_string()))?;
    Ok(())
}

/// Moves the local bookmark of a tracked `NAME@origin` to the tip when its target is the old
/// `NAME@origin` and the move is a fast-forward, and warns when it stays elsewhere.
async fn follow_local(
    ui: &mut Ui,
    tx: &mut jj_cli::cli_util::WorkspaceCommandTransaction<'_>,
    name: &str,
    tip: &Tip,
    before: Option<&CommitId>,
) -> Result<(), Fail> {
    let local = tx
        .repo()
        .view()
        .get_local_bookmark(RefName::new(name))
        .clone();
    let follows = match (before, local.as_normal()) {
        (None, _) => local.is_absent(),
        (Some(b), Some(l)) if b == l => tx
            .repo()
            .index()
            .is_ancestor(b, &tip.id)
            .await
            .unwrap_or(false),
        _ => false,
    };
    if follows {
        tx.repo_mut()
            .set_local_bookmark_target(RefName::new(name), RefTarget::normal(tip.id.clone()));
    } else if local.as_normal() != Some(&tip.id) {
        writeln!(
            ui.warning_default(),
            "bookmark {name} is not at {name}@{REMOTE}; run jj rebase -d {name}@{REMOTE} or jj bookmark set"
        )
        .map_err(CommandError::from)?;
    }
    Ok(())
}

fn names(tips: &[Tip], gone: &[String]) -> String {
    tips.iter()
        .map(|t| t.name.as_str())
        .chain(gone.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(", ")
}

fn short(hex: &str) -> String {
    hex[..12.min(hex.len())].to_owned()
}
