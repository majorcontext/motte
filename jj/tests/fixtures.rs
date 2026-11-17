//! Replays `testdata/ctl/*.json` against the fake daemon and against the typed requests of the
//! shim. A fake that drifts from the contract, or a request that the shim encodes in another
//! shape, fails here.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use common::FakeDaemon;
use motte_jj::ctl::Request;
use serde_json::Value;

fn fixtures() -> Vec<(String, Vec<Value>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/ctl");
    let mut out = vec![];
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let doc: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let steps = doc["steps"].as_array().unwrap().clone();
        out.push((doc["group"].as_str().unwrap().to_owned(), steps));
    }
    assert!(out.len() >= 16, "the fixtures are missing");
    out
}

/// Whether `got` matches `want`: objects key for key, lists element for element, and `"*"` for any
/// value that is present.
fn matches(got: &Value, want: &Value) -> bool {
    match (got, want) {
        (_, Value::String(s)) if s == "*" => true,
        (Value::Object(g), Value::Object(w)) => {
            g.len() == w.len()
                && w.iter()
                    .all(|(k, w)| g.get(k).is_some_and(|g| matches(g, w)))
        }
        (Value::Array(g), Value::Array(w)) => {
            g.len() == w.len() && g.iter().zip(w).all(|(g, w)| matches(g, w))
        }
        _ => got == want,
    }
}

/// Sets the dirty set of the fake to what the fixtures name for `given`.
fn give(d: &FakeDaemon, given: &str, step: &Value) {
    let mut st = d.state.lock().unwrap();
    st.base_tree = step["answer"]["base_tree"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    (st.entries, st.changes) = match given {
        "edited" => (
            step["answer"]["entries"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            step["answer"]["changes"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        ),
        _ => (vec![], vec![]),
    };
}

#[test]
fn the_fake_daemon_answers_every_fixture_step_as_the_contract_says() {
    for (group, steps) in fixtures() {
        let d = FakeDaemon::full();
        let conn = UnixStream::connect(d.path()).unwrap();
        let (mut w, mut r) = (conn.try_clone().unwrap(), BufReader::new(conn));
        for step in steps {
            if let Some(given) = step["given"].as_str() {
                give(&d, given, &step);
            }
            let line = match step.get("raw") {
                Some(raw) => raw.as_str().unwrap().to_owned(),
                None => step["request"].to_string(),
            };
            writeln!(w, "{line}").unwrap();
            let mut buf = String::new();
            r.read_line(&mut buf).unwrap();
            let got: Value = serde_json::from_str(&buf).unwrap();
            assert!(
                matches(&got, &step["answer"]),
                "{group}/{}: request {line}\n got  {got}\n want {}",
                step["name"],
                step["answer"]
            );
        }
    }
}

#[test]
fn a_typed_request_encodes_every_fixture_request_as_the_fixture_writes_it() {
    let mut checked = 0;
    for (group, steps) in fixtures() {
        for step in steps {
            let Some(request) = step.get("request") else {
                continue;
            };
            let Ok(typed) = serde_json::from_value::<Request>(request.clone()) else {
                let code = step["answer"]["code"].as_str().unwrap_or_default();
                assert!(
                    ["invalid_request", "unknown_op"].contains(&code),
                    "{group}/{}: a well-formed request that Request cannot hold",
                    step["name"]
                );
                continue;
            };
            assert_eq!(
                serde_json::to_value(&typed).unwrap(),
                *request,
                "{group}/{}",
                step["name"]
            );
            checked += 1;
        }
    }
    assert!(checked > 40, "only {checked} requests were checked");
}
