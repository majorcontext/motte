use std::process::Command;

#[test]
fn version_prints_the_release_tag_or_the_crate_version() {
    let want = option_env!("MOTTE_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
    let out = Command::new(env!("CARGO_BIN_EXE_jj"))
        .arg("version")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("jj {want}")
    );
}
