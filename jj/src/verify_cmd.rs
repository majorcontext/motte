use std::io::Write as _;
use std::path::Path;

use futures::StreamExt as _;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo as _;

use crate::cmd_err;

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

pub(crate) async fn verify(ui: &mut Ui, helper: &CommandHelper) -> Result<(), CommandError> {
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
