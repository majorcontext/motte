//! The command lock: every jj command except `jj motte snapshot` and the ones that never snapshot
//! (`--ignore-working-copy`) holds a shared flock on `motte_command.lock` of the repo dir until it
//! exits, and the snapshot command holds it exclusive while it snapshots. jj's own
//! `working_copy.lock` covers only the snapshot at the start of a command and the checkout at its
//! end, so it cannot keep a timer snapshot out of the gap in which a command builds its
//! transaction.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::time::{Duration, Instant};

use jj_cli::cli_util::{BoxedAsyncCliDispatch, CommandHelper};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;

use crate::cmd_err;

const COMMAND_LOCK_FILE: &str = "motte_command.lock";

/// The flush waits this long for a tick. The exec limit of the control plane is 2 minutes, and the flush
/// still has to snapshot and publish after the wait.
const FLUSH_WAIT: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq)]
enum Wait {
    Block,
    Within(Duration),
}

enum LockError {
    Busy,
    Io(std::io::Error),
}

/// What the hook does about the lock: run holding it (or none), fail as busy, or run with a warning.
enum Acquired {
    Held(Option<File>),
    Busy,
    Warn(String),
}

/// Only the flush gives up on a held lock: it must end before its caller does.
fn wait_for(subcommand: Option<&str>) -> Wait {
    if subcommand == Some("flush") {
        Wait::Within(FLUSH_WAIT)
    } else {
        Wait::Block
    }
}

fn acquire(repo_path: Option<&Path>, wait: Wait) -> Acquired {
    match repo_path.map(|p| lock_shared(p, wait)) {
        None => Acquired::Held(None),
        Some(Ok(file)) => Acquired::Held(Some(file)),
        Some(Err(LockError::Busy)) => Acquired::Busy,
        Some(Err(LockError::Io(e))) => Acquired::Warn(format!("cannot take the command lock: {e}")),
    }
}

fn open(repo_path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(repo_path.join(COMMAND_LOCK_FILE))
}

fn lock_shared(repo_path: &Path, wait: Wait) -> Result<File, LockError> {
    let file = open(repo_path).map_err(LockError::Io)?;
    let Wait::Within(limit) = wait else {
        file.lock_shared().map_err(LockError::Io)?;
        return Ok(file);
    };
    let deadline = Instant::now() + limit;
    loop {
        match file.try_lock_shared() {
            Ok(()) => return Ok(file),
            Err(TryLockError::Error(e)) => return Err(LockError::Io(e)),
            Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                return Err(LockError::Busy);
            }
            Err(TryLockError::WouldBlock) => std::thread::sleep(POLL.min(limit)),
        }
    }
}

/// The name of the `jj motte` subcommand that runs, if any.
fn motte_subcommand(helper: &CommandHelper) -> Option<&str> {
    helper
        .matches()
        .subcommand()
        .filter(|(name, _)| *name == "motte")
        .and_then(|(_, m)| m.subcommand_name())
}

/// Holds the shared lock for the command, or nothing when there is no repo yet (`jj motte init`,
/// `--help`), when the command never snapshots, or when it is the snapshot, which takes the lock
/// itself. The flush waits for a tick at most `FLUSH_WAIT` and then fails, so that it ends before
/// its caller gives up. A lock that cannot be taken is a warning, and the command runs.
pub async fn hook(
    ui: &mut Ui,
    helper: &CommandHelper,
    next: BoxedAsyncCliDispatch<'_>,
) -> Result<(), CommandError> {
    let subcommand = motte_subcommand(helper);
    let repo_path = helper
        .workspace_loader()
        .ok()
        .map(|l| l.repo_path().to_owned())
        .filter(|p| {
            p.is_dir()
                && subcommand != Some("snapshot")
                && !helper.global_args().ignore_working_copy
        });
    let _held = match acquire(repo_path.as_deref(), wait_for(subcommand)) {
        Acquired::Held(file) => file,
        Acquired::Busy => return Err(user_error("a snapshot tick holds the command lock")),
        Acquired::Warn(text) => {
            writeln!(ui.warning_default(), "{text}")?;
            None
        }
    };
    next.call(ui, helper).await
}

/// Takes the exclusive lock without waiting; `None` means a command holds it.
pub fn try_exclusive(helper: &CommandHelper) -> Result<Option<File>, CommandError> {
    let repo_path = helper.workspace_loader()?.repo_path().to_owned();
    let file = open(&repo_path).map_err(cmd_err)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(cmd_err(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flush_gives_up_on_a_held_exclusive_lock_within_its_wait() {
        let dir = tempfile::tempdir().unwrap();
        let held = open(dir.path()).unwrap();
        held.lock().unwrap();
        let start = Instant::now();
        let got = lock_shared(dir.path(), Wait::Within(Duration::from_millis(300)));
        assert!(matches!(got, Err(LockError::Busy)));
        assert!(start.elapsed() < Duration::from_secs(10));
        drop(held);
        assert!(lock_shared(dir.path(), Wait::Within(Duration::from_millis(300))).is_ok());
    }

    #[test]
    fn only_the_flush_gives_up_on_a_held_lock() {
        assert_eq!(wait_for(Some("flush")), Wait::Within(FLUSH_WAIT));
        for other in [Some("sync"), Some("changes"), None] {
            assert_eq!(wait_for(other), Wait::Block);
        }
    }

    #[test]
    fn a_lock_that_cannot_be_taken_warns_and_lets_the_command_run() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(COMMAND_LOCK_FILE)).unwrap();
        assert!(matches!(
            acquire(Some(dir.path()), Wait::Block),
            Acquired::Warn(_)
        ));
        assert!(matches!(acquire(None, Wait::Block), Acquired::Held(None)));
    }

    #[test]
    fn a_lock_file_that_cannot_be_opened_is_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(COMMAND_LOCK_FILE)).unwrap();
        assert!(matches!(
            lock_shared(dir.path(), Wait::Block),
            Err(LockError::Io(_))
        ));
    }
}
