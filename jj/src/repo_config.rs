use std::path::Path;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::config::{ConfigFile, ConfigNamePathBuf, ConfigSource, ConfigValue};
use jj_lib::settings::UserSettings;

use crate::cmd_err;
use crate::start_branch::StartBranch;

/// The argv that harness runs for the work-tree diff of a jj workspace, set in the repo config.
const CHANGES_COMMAND: [&str; 4] = ["jj", "motte", "changes", "--json"];

const ORIGIN_URL: &str = "motte.origin-url";
const ORIGIN_BASE: &str = "motte.origin-base";
const ORIGIN_BRANCH: &str = "motte.origin-branch";
const ORIGIN_NATIVE: &str = "motte.origin-native";
const ORIGIN_DEFAULT_BRANCH: &str = "motte.origin-default-branch";

/// Where `jj motte push` sends work: the plain git URL of origin, and the commit that the
/// workspace started from, which origin has.
pub struct Origin {
    pub url: String,
    pub base: String,
}

impl Origin {
    pub fn new(url: &str, base: &str) -> Result<Self, CommandError> {
        let authority = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .and_then(|rest| rest.split('/').next());
        match authority {
            Some(a) if !a.is_empty() && !a.contains('@') && !url.contains(char::is_whitespace) => {
                Ok(Self {
                    url: url.to_owned(),
                    base: base.to_owned(),
                })
            }
            _ => Err(cmd_err(
                "the origin URL must be http(s) without credentials",
            )),
        }
    }

    pub fn from_settings(settings: &UserSettings) -> Option<Self> {
        Some(Self {
            url: settings.get_string(ORIGIN_URL).ok()?,
            base: settings.get_string(ORIGIN_BASE).ok()?,
        })
    }
}

/// What a native workspace keeps of its origin, which is the store.
pub struct Native {
    /// The commit that the store held when the workspace started.
    pub base: String,
    pub default_branch: String,
    /// The branch that the workspace started on, when it started on one.
    pub start_branch: Option<String>,
}

/// The native origin of the workspace, or `None` for a workspace whose origin is not the store.
/// A native workspace that lacks a key is an error: `jj motte restore` writes both.
pub fn native(settings: &UserSettings) -> Result<Option<Native>, String> {
    if !settings.get_bool(ORIGIN_NATIVE).unwrap_or(false) {
        return Ok(None);
    }
    let key = |key: &'static str| {
        settings.get_string(key).map_err(|_| {
            format!(
                "this native workspace has no {key}; the caller writes the keys at workspace setup"
            )
        })
    };
    Ok(Some(Native {
        base: key(ORIGIN_BASE)?,
        default_branch: key(ORIGIN_DEFAULT_BRANCH)?,
        start_branch: settings.get_string(ORIGIN_BRANCH).ok(),
    }))
}

/// The branch that the workspace started on, when it started on one.
pub fn origin_branch(settings: &UserSettings) -> Option<String> {
    settings.get_string(ORIGIN_BRANCH).ok()
}

/// Names the changes command, the origin and the trunk when there are some, in the repo config file that
/// `jj config path --repo` names, which jj keeps outside `.jj`. A legacy `.jj/repo/config.toml`
/// makes harness fail, so it never writes one.
pub fn write(
    ui: &mut Ui,
    helper: &CommandHelper,
    repo_path: &Path,
    origin: Option<&Origin>,
    branch: Option<&StartBranch>,
    native_base: Option<&str>,
    default_branch: Option<&StartBranch>,
) -> Result<(), CommandError> {
    let mut env = helper.config_env().clone();
    env.reset_repo_path(repo_path);
    let Some(path) = env.repo_config_path(ui)? else {
        writeln!(
            ui.warning_default(),
            "no repo config path; harness.changes-command is not set"
        )?;
        return Ok(());
    };
    let mut file = ConfigFile::load_or_empty(ConfigSource::Repo, &path).map_err(cmd_err)?;
    file.set_value(
        "harness.changes-command",
        ConfigValue::from_iter(CHANGES_COMMAND),
    )
    .map_err(cmd_err)?;
    if let Some(origin) = origin {
        file.set_value(ORIGIN_URL, origin.url.as_str())
            .map_err(cmd_err)?;
        file.set_value(ORIGIN_BASE, origin.base.as_str())
            .map_err(cmd_err)?;
    }
    if let Some(branch) = branch {
        file.set_value(
            ConfigNamePathBuf::from_iter(["revset-aliases", "trunk()"]),
            branch.trunk(),
        )
        .map_err(cmd_err)?;
        file.set_value(ORIGIN_BRANCH, branch.name())
            .map_err(cmd_err)?;
    }
    if let Some(base) = native_base {
        file.set_value(ORIGIN_NATIVE, true).map_err(cmd_err)?;
        file.set_value(ORIGIN_BASE, base).map_err(cmd_err)?;
    }
    if let Some(branch) = default_branch {
        file.set_value(ORIGIN_DEFAULT_BRANCH, branch.name())
            .map_err(cmd_err)?;
    }
    file.save().map_err(cmd_err)
}
