//! A fake `motte-vfsd` control socket in overlay mode: an in-memory object store plus a settable dirty set.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use gix::objs::Kind;
use motte_jj::ctl::hash_object;
use serde_json::{Value, json};

#[derive(Default)]
pub struct State {
    pub objects: HashMap<String, (String, Vec<u8>)>,
    pub entries: Vec<Value>,
    pub changes: Vec<Value>,
    /// The tree the mount shows; `dirty`, `token` and `changes` report it.
    pub base_tree: String,
    /// The change token; empty plays a daemon that is not in overlay mode: `token` answers
    /// "unknown op" and `dirty` carries no token.
    pub token: String,
    pub ops: HashMap<String, u64>,
    pub traceparents: Vec<String>,
    /// The generation and heads of the remote op heads; unset answers "unknown op".
    pub opheads: Option<(i64, Vec<String>)>,
    /// The refs by name as (commit, generation); `ref_counter` is the last generation handed out.
    pub refs: std::collections::BTreeMap<String, (String, i64)>,
    pub ref_counter: i64,
}

pub struct FakeDaemon {
    pub state: Arc<Mutex<State>>,
    pub sock: PathBuf,
    _dir: tempfile::TempDir,
}

impl FakeDaemon {
    /// `batch: false` answers `putbatch` with "unknown op", like a daemon without it.
    pub fn start(batch: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("ctl.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let state = Arc::new(Mutex::new(State {
            token: "b0:1".into(),
            ..State::default()
        }));
        let st = state.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { return };
                let st = st.clone();
                std::thread::spawn(move || serve(conn, st, batch));
            }
        });
        Self {
            state,
            sock,
            _dir: dir,
        }
    }

    pub fn ops(&self, op: &str) -> u64 {
        self.state.lock().unwrap().ops.get(op).copied().unwrap_or(0)
    }

    pub fn path(&self) -> &Path {
        &self.sock
    }
}

fn kind_of(s: &str) -> Kind {
    match s {
        "blob" => Kind::Blob,
        "tree" => Kind::Tree,
        "commit" => Kind::Commit,
        _ => Kind::Tag,
    }
}

fn store(st: &mut State, kind: &str, data: &str) -> String {
    let data = B64.decode(data).unwrap();
    let sha = hash_object(kind_of(kind), &data).to_string();
    st.objects.insert(sha.clone(), (kind.to_owned(), data));
    sha
}

fn serve(conn: UnixStream, st: Arc<Mutex<State>>, batch: bool) {
    let mut w = conn.try_clone().unwrap();
    for line in BufReader::new(conn).lines() {
        let Ok(line) = line else { return };
        let rq: Value = serde_json::from_str(&line).unwrap();
        let op = rq["op"].as_str().unwrap_or("").to_owned();
        let mut st = st.lock().unwrap();
        *st.ops.entry(op.clone()).or_default() += 1;
        if let Some(tp) = rq["traceparent"].as_str() {
            st.traceparents.push(tp.to_owned());
        }
        let rs = match op.as_str() {
            "get" => match st.objects.get(rq["sha"].as_str().unwrap()) {
                Some((k, d)) => json!({"type": k, "data": B64.encode(d)}),
                None => json!({"err": "object not found"}),
            },
            "put" => {
                let sha = store(
                    &mut st,
                    rq["type"].as_str().unwrap(),
                    rq["data"].as_str().unwrap(),
                );
                json!({"sha": sha})
            }
            "putbatch" if batch => {
                let shas: Vec<String> = rq["objects"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|o| {
                        store(
                            &mut st,
                            o["type"].as_str().unwrap(),
                            o["data"].as_str().unwrap(),
                        )
                    })
                    .collect();
                json!({"shas": shas})
            }
            "dirty" => json!({
                "boot": "b0", "token": st.token, "base_tree": st.base_tree, "entries": st.entries,
            }),
            "token" if !st.token.is_empty() => json!({
                "boot": "b0", "token": st.token, "base_tree": st.base_tree,
            }),
            "changes" => {
                json!({"base_tree": st.base_tree, "token": st.token, "changes": st.changes})
            }
            "getbatch" => {
                let datas: Option<Vec<String>> = rq["shas"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| {
                        st.objects
                            .get(s.as_str().unwrap())
                            .map(|o| B64.encode(&o.1))
                    })
                    .collect();
                match datas {
                    Some(d) => json!({"datas": d}),
                    None => json!({"err": "object not found"}),
                }
            }
            "opheads" if st.opheads.is_some() => {
                let (generation, heads) = st.opheads.clone().unwrap();
                match rq["expect"].as_i64() {
                    Some(e) if e != generation => json!({"err": "store: vcs_generation_conflict"}),
                    Some(_) => {
                        let swapped = rq["heads"].as_array().unwrap().clone();
                        let next = swapped.iter().map(|h| h.as_str().unwrap().to_owned());
                        st.opheads = Some((generation + 1, next.collect()));
                        json!({"gen": generation + 1, "heads": swapped})
                    }
                    None => json!({"gen": generation, "heads": heads}),
                }
            }
            "refs" | "swapref" => serve_refs(&mut st, &rq),
            "has" => {
                let present: Vec<bool> = rq["shas"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| st.objects.contains_key(s.as_str().unwrap()))
                    .collect();
                json!({"present": present})
            }
            _ => json!({"err": "unknown op"}),
        };
        drop(st);
        let mut out = serde_json::to_vec(&rs).unwrap();
        out.push(b'\n');
        if w.write_all(&out).is_err() {
            return;
        }
    }
}

fn serve_refs(st: &mut State, rq: &Value) -> Value {
    let name = rq["name"].as_str().unwrap_or_default();
    let view = |n: &str, r: &(String, i64)| json!({"name": n, "commit": r.0, "generation": r.1});
    if rq["op"] == "refs" {
        return if name.is_empty() {
            json!({"refs": st.refs.iter().map(|(n, r)| view(n, r)).collect::<Vec<_>>()})
        } else {
            match st.refs.get(name) {
                Some(r) => json!({"ref": view(name, r)}),
                None => json!({"err": "store: vcs_not_found"}),
            }
        };
    }
    if name.contains("lost-claim") {
        return json!({"err": format!("store: vfs: PUT /v1/vcs/refs/{name}: 409 Conflict: vcs_claim_lost")});
    }
    let stored = st.refs.get(name).cloned();
    if stored.as_ref().map_or(0, |r| r.1) != rq["expect"].as_i64().unwrap() {
        let mut rs =
            json!({"err": "store: vcs_generation_conflict", "code": "vcs_generation_conflict"});
        if let Some(r) = &stored {
            rs["ref"] = view(name, r);
        }
        return rs;
    }
    if rq["delete"].as_bool().unwrap_or(false) {
        st.refs.remove(name);
        return json!({});
    }
    st.ref_counter += 1;
    let next = (rq["commit"].as_str().unwrap().to_owned(), st.ref_counter);
    st.refs.insert(name.to_owned(), next.clone());
    json!({"ref": view(name, &next)})
}
