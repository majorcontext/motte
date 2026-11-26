//! The export of the trace of a command to an HTTPS endpoint. The command hands its spans to a
//! detached copy of itself, so each test waits on a channel for the request, never on the clock.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};

struct Tls {
    dir: tempfile::TempDir,
    config: Arc<rustls::ServerConfig>,
}

fn tls() -> Tls {
    let ck =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("ca.pem"), ck.cert.pem()).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![ck.cert.der().clone()], key)
        .unwrap();
    Tls {
        dir,
        config: Arc::new(config),
    }
}

/// Reads one head and returns it with the body that its `content-length` names.
fn read_request(r: &mut impl BufRead) -> (String, Vec<u8>) {
    let (mut head, mut len) = (String::new(), 0);
    loop {
        let mut line = String::new();
        r.read_line(&mut line).unwrap();
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            len = v.trim().parse().unwrap();
        }
        if line.trim().is_empty() {
            break;
        }
        head += &line;
    }
    let mut body = vec![0; len];
    r.read_exact(&mut body).unwrap();
    (head, body)
}

/// Serves one HTTPS request. It sends the request, then answers when `release` closes.
fn serve(tls: &Tls, release: Receiver<()>) -> (u16, Receiver<(String, Vec<u8>)>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let (tx, rx) = channel();
    let config = tls.config.clone();
    std::thread::spawn(move || {
        let conn = rustls::ServerConnection::new(config).unwrap();
        let mut s = rustls::StreamOwned::new(conn, l.accept().unwrap().0);
        let req = read_request(&mut BufReader::new(&mut s));
        tx.send(req).unwrap();
        let _ = release.recv();
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    });
    (port, rx)
}

/// Runs `jj version` with the endpoint. It returns after the command exited and closed its pipes.
fn jj(tls: &Tls, endpoint: &str, env: &[(&str, &str)]) -> std::process::Output {
    let ca = tls.dir.path().join("ca.pem");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_jj"));
    cmd.arg("version")
        .env("HOME", tls.dir.path())
        .env("SSL_CERT_FILE", ca)
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint);
    for k in [
        "HTTPS_PROXY",
        "https_proxy",
        "NO_PROXY",
        "no_proxy",
        "ALL_PROXY",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    ] {
        cmd.env_remove(k);
    }
    cmd.envs(env.iter().copied()).output().unwrap()
}

#[test]
fn an_https_endpoint_gets_the_command_span() {
    let t = tls();
    let (_hold, release) = channel::<()>();
    let (port, rx) = serve(&t, release);
    assert!(
        jj(
            &t,
            &format!("https://127.0.0.1:{port}"),
            &[("OTEL_EXPORTER_OTLP_HEADERS", "x-k=a%20b")]
        )
        .status
        .success()
    );
    let (head, body) = rx.recv().unwrap();
    assert!(head.starts_with("POST /v1/traces "), "{head}");
    assert!(head.contains("x-k: a b"), "{head}");
    assert!(body.windows(10).any(|w| w == b"jj.command"));
}

#[test]
fn a_command_ends_while_the_endpoint_holds_the_connection() {
    let t = tls();
    let (release_tx, release) = channel();
    let (port, rx) = serve(&t, release);
    let out = jj(&t, &format!("https://127.0.0.1:{port}"), &[]);
    assert!(out.status.success());
    rx.recv().unwrap();
    drop(release_tx);
}

#[test]
fn an_https_proxy_with_credentials_carries_the_request() {
    let t = tls();
    let (_hold, release) = channel::<()>();
    let (port, rx) = serve(&t, release);
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_port = proxy.local_addr().unwrap().port();
    let (ptx, prx) = channel();
    std::thread::spawn(move || {
        let mut down = proxy.accept().unwrap().0;
        let (head, _) = read_request(&mut BufReader::new(&mut down));
        ptx.send(head).unwrap();
        down.write_all(b"HTTP/1.1 200 OK\r\n\r\n").unwrap();
        let up = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let (mut a, mut b) = (down.try_clone().unwrap(), up.try_clone().unwrap());
        std::thread::spawn(move || std::io::copy(&mut a, &mut (&up)).unwrap());
        std::io::copy(&mut b, &mut down).unwrap();
    });
    let proxy_url = format!("http://u:p@127.0.0.1:{proxy_port}");
    jj(
        &t,
        &format!("https://localhost:{port}"),
        &[("HTTPS_PROXY", &proxy_url)],
    );
    let head = prx.recv().unwrap();
    assert!(
        head.starts_with(&format!("CONNECT localhost:{port} ")),
        "{head}"
    );
    assert!(
        head.to_ascii_lowercase()
            .contains("proxy-authorization: basic dtpw"),
        "{head}"
    );
    assert!(rx.recv().unwrap().1.windows(10).any(|w| w == b"jj.command"));
}
