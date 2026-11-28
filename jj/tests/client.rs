//! The control client against the fake daemon: error codes, the daemon that sends none, lost
//! answers and size limits. Each test names the failure it catches.

mod common;

use common::FakeDaemon;
use gix::objs::Kind;
use motte_jj::ctl::{Code, CtlClient, MAX_OBJECT_BYTES, ObjectStore, RefSwap, StoreError};

fn head(c: char) -> String {
    c.to_string().repeat(40)
}

#[test]
fn an_error_maps_to_the_same_code_whether_or_not_the_daemon_sends_one() {
    for codeless in [false, true] {
        let d = FakeDaemon::full();
        d.state.lock().unwrap().codeless = codeless;
        let c = CtlClient::new(d.path());
        let absent = gix::ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
        assert!(
            matches!(c.get(&absent), Err(StoreError::NotFound(_))),
            "codeless={codeless}"
        );
        assert!(c.get_ref("heads/none").unwrap_err().is(&Code::NotFound));
        c.swap_ref("heads/main", 0, &head('a'), false).unwrap();
        let moved = c.swap_ref("heads/main", 7, &head('b'), false).unwrap();
        assert!(
            matches!(moved, RefSwap::Conflict(Some(_))),
            "codeless={codeless}"
        );
        let swap = c.opheads(Some((9, &["a1".to_owned()]))).unwrap_err();
        assert!(
            swap.is(&Code::GenerationConflict),
            "codeless={codeless}: {swap}"
        );
        d.state
            .lock()
            .unwrap()
            .unsupported
            .insert("workspace".into());
        assert!(
            !c.set_workspace("a1", &head('a'), 1).unwrap(),
            "codeless={codeless}"
        );
        d.state
            .lock()
            .unwrap()
            .failing
            .insert("swapref".into(), "vcs_claim_lost".into());
        let lost = c.swap_ref("heads/x", 0, &head('a'), false).unwrap_err();
        assert!(lost.is(&Code::ClaimLost), "codeless={codeless}: {lost}");
    }
}

#[test]
fn a_line_that_is_too_long_is_answered_once_and_never_sent_again() {
    let d = FakeDaemon::full();
    d.state.lock().unwrap().max_line = 1000;
    let c = CtlClient::new(d.path());
    let err = c.put(Kind::Blob, &[b'x'; 2000]).unwrap_err();
    assert!(err.is(&Code::LineTooLong), "{err}");
    assert_eq!(d.state.lock().unwrap().lines, 1, "the request was resent");
}

#[test]
fn a_lost_answer_of_an_idempotent_request_is_resent_once() {
    let d = FakeDaemon::full();
    d.state.lock().unwrap().lose_answer_of = Some("put".into());
    let c = CtlClient::new(d.path());
    c.put(Kind::Blob, b"x").unwrap();
    assert_eq!(d.state.lock().unwrap().lines, 2);
}

#[test]
fn a_lost_op_heads_swap_is_read_back_when_it_landed_and_sent_again_when_it_did_not() {
    for landed in [true, false] {
        let d = FakeDaemon::full();
        {
            let mut st = d.state.lock().unwrap();
            if landed {
                st.lose_answer_of = Some("opheads".into());
            } else {
                st.drop_request_of = Some("opheads".into());
            }
        }
        let c = CtlClient::new(d.path());
        let got = c.opheads(Some((0, &["a1".to_owned()]))).unwrap();
        assert_eq!(got, (1, vec!["a1".to_owned()]), "landed={landed}");
        let st = d.state.lock().unwrap();
        assert_eq!(
            st.opheads,
            Some((1, vec!["a1".to_owned()])),
            "a swap ran twice or not at all"
        );
    }
}

#[test]
fn a_lost_ref_swap_is_judged_by_a_read_and_never_replayed() {
    let d = FakeDaemon::full();
    let c = CtlClient::new(d.path());
    c.swap_ref("heads/main", 0, &head('a'), false).unwrap();
    d.state.lock().unwrap().lose_answer_of = Some("swapref".into());
    let moved = c.swap_ref("heads/main", 1, &head('b'), false).unwrap();
    assert!(
        matches!(moved, RefSwap::Swapped(Some(ref r)) if r.commit == head('b') && r.generation == 2)
    );
    d.state.lock().unwrap().lose_answer_of = Some("swapref".into());
    assert_eq!(
        c.swap_ref("heads/main", 2, "", true).unwrap(),
        RefSwap::Swapped(None)
    );
    d.state.lock().unwrap().drop_request_of = Some("swapref".into());
    let created = c.swap_ref("heads/new", 0, &head('c'), false).unwrap();
    assert!(matches!(created, RefSwap::Swapped(Some(_))));
    assert_eq!(d.ops("swapref"), 4, "a landed swap was sent twice");
}

#[test]
fn an_object_over_16_mib_is_refused_before_it_is_sent() {
    let d = FakeDaemon::full();
    let c = CtlClient::new(d.path());
    let err = c
        .put(Kind::Blob, &vec![b'x'; MAX_OBJECT_BYTES as usize + 1])
        .unwrap_err();
    assert!(err.to_string().contains("16 MiB"), "{err}");
    assert_eq!(d.state.lock().unwrap().lines, 0, "the object was sent");
}

/// Catches a prefetch of the op log that holds every body it reads: views can be large, and a
/// restore of a long op log would hold them all before it writes any. The bodies past the budget
/// are left to the walk.
#[test]
fn an_op_prefetch_keeps_no_more_than_its_byte_budget() {
    let d = FakeDaemon::full();
    let ops: Vec<(String, String)> = (0..3).map(|i| (format!("a{i}"), format!("b{i}"))).collect();
    {
        let mut st = d.state.lock().unwrap();
        for (op, view) in &ops {
            st.op_objects
                .insert(("operations".into(), op.clone()), vec![0; 100]);
            st.op_objects
                .insert(("views".into(), view.clone()), vec![0; 1000]);
        }
    }
    let ctl = CtlClient::new(d.path());
    let got = motte_jj::stores::prefetch_ops(&ctl, &ops, 4, 2500);
    let bytes: usize = got.values().map(Vec::len).sum();
    assert!(bytes <= 2500, "kept {bytes} bytes");
    assert_eq!(
        got.keys().filter(|(kind, _)| *kind == "operations").count(),
        3,
        "{:?}",
        got.keys()
    );
    let all = motte_jj::stores::prefetch_ops(&ctl, &ops, 4, usize::MAX);
    assert_eq!(all.len(), 6);
}
