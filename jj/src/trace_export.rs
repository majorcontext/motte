//! The export mode of the binary. A command hands its finished spans to a detached copy of itself
//! that runs this mode, so that the command never waits for the network.

use std::io::Read as _;
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};

/// The first argument that selects the export mode.
pub const ARG: &str = "--motte-trace-export";

const DEFAULT_TIMEOUT_MS: u64 = 10_000;

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn endpoint() -> Option<String> {
    env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").or_else(|| {
        let base = env("OTEL_EXPORTER_OTLP_ENDPOINT")?;
        Some(format!("{}/v1/traces", base.trim_end_matches('/')))
    })
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = (b[i] == b'%')
            .then(|| s.get(i + 1..i + 3))
            .flatten()
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match hex {
            Some(v) => {
                out.push(v);
                i += 3;
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn headers() -> HeaderMap {
    let mut map = HeaderMap::new();
    for var in [
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    ] {
        for pair in env(var).iter().flat_map(|v| v.split(',')) {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            if let (Ok(k), Ok(v)) = (
                HeaderName::from_bytes(k.trim().as_bytes()),
                HeaderValue::from_str(&percent_decode(v.trim())),
            ) {
                map.insert(k, v);
            }
        }
    }
    map
}

fn send(body: Vec<u8>) -> Option<()> {
    let timeout = env("OTEL_EXPORTER_OTLP_TIMEOUT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TIMEOUT_MS);
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(timeout))
        .build()
        .ok()?
        .post(endpoint()?)
        .header(CONTENT_TYPE, "application/x-protobuf")
        .headers(headers())
        .body(body)
        .send()
        .ok()?;
    Some(())
}

/// Sends the request that stdin holds. Every failure is silent.
pub fn run() {
    let mut body = Vec::new();
    if std::io::stdin().read_to_end(&mut body).is_ok() {
        let _ = send(body);
    }
}

#[cfg(unix)]
pub fn spawn(body: &[u8]) {
    use std::io::Write as _;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};

    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let mut cmd = Command::new(exe);
    cmd.arg(ARG)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: the closure calls only async-signal-safe functions between fork and exec.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            for fd in 3..1024 {
                libc::close(fd);
            }
            Ok(())
        });
    }
    if let Ok(mut child) = cmd.spawn()
        && let Some(mut stdin) = child.stdin.take()
    {
        let _ = stdin.write_all(body);
    }
}
