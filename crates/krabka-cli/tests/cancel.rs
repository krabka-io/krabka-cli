//! Ctrl-C during a networked command.

#![cfg(unix)]

use std::{
    io::Read as _,
    net::TcpListener,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use assert2::assert;
use serde_json::{Value, json};

#[test]
fn ctrl_c_during_a_command_exits_130_with_the_envelope() {
    // A listener that accepts and never answers holds the command at its
    // first request, so the signal is what ends it.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args([
            "--output",
            "json",
            "topics",
            "--list",
            "--bootstrap-server",
            &address,
            "--request-timeout-ms",
            "60000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    listener.set_nonblocking(true).unwrap();
    let _held = loop {
        if let Ok((stream, _)) = listener.accept() {
            break stream;
        }
        assert!(Instant::now() < deadline, "krabka never connected");
        std::thread::sleep(Duration::from_millis(10));
    };
    let pid = rustix::process::Pid::from_child(&child);
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let status = child.wait().unwrap();
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    let envelope: Value = serde_json::from_str(stderr.trim()).expect("one JSON line on stderr");
    assert!(
        (status.code(), stdout.as_str(), envelope)
            == (
                Some(130),
                "",
                json!({"error": {
                    "code": 130,
                    "message": "cancelled; a request in flight may already have reached the broker",
                }})
            )
    );
}
