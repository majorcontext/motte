use jj_cli::command_error::CommandError;
use jj_lib::backend::CommitId;
use jj_lib::op_store::{RefTarget, RemoteRef, RemoteRefState};
use jj_lib::ref_name::{RefName, RemoteName};
use jj_lib::repo::MutableRepo;
use jj_lib::view::View;
use jj_lib::workspace::Workspace;

use crate::cmd_err;

pub const REMOTE: &str = "origin";

fn valid_ref_name(name: &str) -> bool {
    name.len() <= 255
        && name != "@"
        && !name.is_empty()
        && !name.starts_with('/')
        && !name.ends_with(['/', '.'])
        && !name.contains("//")
        && !name.contains("..")
        && !name.contains("@{")
        && !name
            .chars()
            .any(|c| c.is_control() || matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\'))
        && name
            .split('/')
            .all(|part| !part.starts_with('.') && !part.ends_with(".lock"))
}

/// The branch of origin that a workspace starts on; it names the local bookmark, the tracked
/// bookmark of origin and the revset that `trunk()` resolves to.
pub struct StartBranch(String);

impl StartBranch {
    pub fn new(name: &str) -> Result<Self, CommandError> {
        if !valid_ref_name(name) {
            return Err(cmd_err(
                "the origin branch name is not a valid git ref name",
            ));
        }
        Ok(Self(name.to_owned()))
    }

    pub fn name(&self) -> &str {
        &self.0
    }

    pub fn trunk(&self) -> String {
        let quoted = self.0.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{quoted}\"@{REMOTE}")
    }

    pub fn is_missing(&self, view: &View) -> bool {
        let name = RefName::new(&self.0);
        view.get_local_bookmark(name).is_absent()
            || view
                .get_remote_bookmark(name.to_remote_symbol(RemoteName::new(REMOTE)))
                .is_absent()
    }

    /// Points each of the local bookmark and the tracked bookmark of origin at `commit` when it
    /// is absent; a bookmark that exists stays where it is.
    pub fn set_absent(&self, repo: &mut MutableRepo, commit: &CommitId) {
        let name = RefName::new(&self.0);
        if repo.get_local_bookmark(name).is_absent() {
            repo.set_local_bookmark_target(name, RefTarget::normal(commit.clone()));
        }
        let symbol = name.to_remote_symbol(RemoteName::new(REMOTE));
        if repo.get_remote_bookmark(symbol).is_absent() {
            repo.set_remote_bookmark(
                symbol,
                RemoteRef {
                    target: RefTarget::normal(commit.clone()),
                    state: RemoteRefState::Tracked,
                },
            );
        }
    }

    /// Sets the absent bookmarks of a restored workspace in one operation, and moves the
    /// working-copy state to it.
    pub async fn restore(
        &self,
        workspace: &mut Workspace,
        base: &CommitId,
    ) -> Result<(), CommandError> {
        let repo = workspace
            .repo_loader()
            .load_at_head(Some(workspace.workspace_name()))
            .await
            .map_err(cmd_err)?;
        if !self.is_missing(repo.view()) {
            return Ok(());
        }
        let mut tx = repo.start_transaction();
        self.set_absent(tx.repo_mut(), base);
        let repo = tx.commit("motte restore").await.map_err(cmd_err)?;
        let locked = workspace
            .start_working_copy_mutation()
            .await
            .map_err(cmd_err)?;
        locked.finish(repo.op_id().clone()).await.map_err(cmd_err)?;
        Ok(())
    }
}
