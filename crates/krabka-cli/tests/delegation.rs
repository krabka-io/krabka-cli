//! Process-level tests for delegation to `krabka-<name>` plugins.
//!
//! Each case writes shell stubs into a temporary directory, puts that
//! directory first on `PATH`, and runs the built `krabka` binary.

#![cfg(unix)]

use std::{
    os::unix::{fs::PermissionsExt as _, process::CommandExt as _},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_cli::external::{EXTERNAL_PREFIX, KNOWN_EXTERNALS};

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

struct Stub {
    name: &'static str,
    script: &'static str,
    mode: u32,
}

fn install(dir: &Path, stub: &Stub) -> PathBuf {
    let path = dir.join(format!("{EXTERNAL_PREFIX}{}", stub.name));
    std::fs::write(&path, format!("#!/bin/sh\n{}\n", stub.script)).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(stub.mode)).unwrap();
    path
}

fn search_path(dir: &Path) -> std::ffi::OsString {
    std::env::join_paths([dir, Path::new("/usr/bin"), Path::new("/bin")]).unwrap()
}

fn krabka(dir: &Path, argv: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_krabka"));
    command.args(argv).env("PATH", search_path(dir));
    command
}

fn outcome(dir: &Path, argv: &[&str]) -> Outcome {
    let out = krabka(dir, argv).output().expect("run krabka");
    Outcome {
        code: out.status.code(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

fn outcome_of(code: i32, stdout: &str, stderr: &str) -> Outcome {
    Outcome {
        code: Some(code),
        stdout: stdout.into(),
        stderr: stderr.into(),
    }
}

#[test]
fn a_plugin_decides_the_outcome_of_its_subcommand() {
    struct Case {
        stub: Stub,
        argv: &'static [&'static str],
        expected: Outcome,
    }
    let cases = [
        Case {
            stub: Stub {
                name: "echo",
                script: r#"for arg in "$@"; do printf '[%s]\n' "$arg"; done"#,
                mode: 0o755,
            },
            argv: &["echo", "two words", "--flag", "", "-x=1"],
            expected: outcome_of(0, "[two words]\n[--flag]\n[]\n[-x=1]\n", ""),
        },
        Case {
            stub: Stub {
                name: "streams",
                script: r"printf 'to stdout\n'; printf 'to stderr\n' >&2",
                mode: 0o755,
            },
            argv: &["streams"],
            expected: outcome_of(0, "to stdout\n", "to stderr\n"),
        },
        Case {
            stub: Stub {
                name: "fails",
                script: "exit 1",
                mode: 0o755,
            },
            argv: &["fails"],
            expected: outcome_of(1, "", ""),
        },
        Case {
            stub: Stub {
                name: "answer",
                script: "exit 42",
                mode: 0o755,
            },
            argv: &["answer"],
            expected: outcome_of(42, "", ""),
        },
        Case {
            stub: Stub {
                name: "terminated",
                script: "kill -TERM $$",
                mode: 0o755,
            },
            argv: &["terminated"],
            expected: outcome_of(128 + 15, "", ""),
        },
        Case {
            stub: Stub {
                name: "killed",
                script: "kill -KILL $$",
                mode: 0o755,
            },
            argv: &["killed"],
            expected: outcome_of(128 + 9, "", ""),
        },
    ];
    for case in cases {
        let dir = tempfile::tempdir().unwrap();
        install(dir.path(), &case.stub);
        check!(outcome(dir.path(), case.argv) == case.expected);
    }
}

#[test]
fn a_missing_plugin_exits_127() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        outcome(dir.path(), &["no-such-plugin", "--flag"])
            == outcome_of(
                127,
                "",
                "krabka: `no-such-plugin` is not a krabka command, and no `krabka-no-such-plugin` was found on PATH\n\
                 krabka: see `krabka --help` for the built-in commands\n",
            )
    );
}

#[test]
fn a_plugin_that_is_not_executable_exits_126_and_names_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = install(
        dir.path(),
        &Stub {
            name: "broken",
            script: "exit 0",
            mode: 0o644,
        },
    );
    assert!(
        outcome(dir.path(), &["broken"])
            == outcome_of(
                126,
                "",
                &format!(
                    "krabka: {} was found on PATH but is not executable\n",
                    path.display()
                ),
            )
    );
}

#[test]
fn an_executable_later_on_path_wins_over_an_unexecutable_earlier_one() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    install(
        first.path(),
        &Stub {
            name: "twice",
            script: "exit 3",
            mode: 0o644,
        },
    );
    install(
        second.path(),
        &Stub {
            name: "twice",
            script: "exit 4",
            mode: 0o755,
        },
    );
    let path = std::env::join_paths([first.path(), second.path(), Path::new("/bin")]).unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .arg("twice")
        .env("PATH", path)
        .status()
        .unwrap();
    assert!(status.code() == Some(4));
}

#[test]
fn a_plugin_on_path_does_not_shadow_a_built_in() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    install(
        dir.path(),
        &Stub {
            name: "format",
            script: "echo shadowed; exit 99",
            mode: 0o755,
        },
    );
    let out = krabka(
        dir.path(),
        &["format", "--log-dir", log_dir.path().to_str().unwrap()],
    )
    .output()
    .unwrap();
    check!(out.status.code() == Some(0));
    check!(!String::from_utf8_lossy(&out.stdout).contains("shadowed"));
    check!(log_dir.path().join("meta.properties.json").is_file());
}

#[test]
fn every_known_external_is_delegated_and_named_in_help() {
    let dir = tempfile::tempdir().unwrap();
    for external in KNOWN_EXTERNALS {
        let path = install(
            dir.path(),
            &Stub {
                name: external.name,
                script: r#"printf '%s %s\n' "$(basename "$0")" "$*""#,
                mode: 0o755,
            },
        );
        check!(path.file_name().unwrap() == external.binary);
        check!(
            outcome(dir.path(), &[external.name, "--probe"])
                == outcome_of(0, &format!("{} --probe\n", external.binary), "")
        );
    }

    let help = outcome(dir.path(), &["--help"]);
    let short = outcome(dir.path(), &["-h"]);
    for external in KNOWN_EXTERNALS {
        check!(help.stdout.contains(&format!("{}  ", external.name)));
        check!(help.stdout.contains(&format!("`{}`", external.binary)));
        check!(short.stdout.contains(&format!("`{}`", external.name)));
    }
}

// Waits for the stub to report that its signal trap is installed.
fn wait_for(marker: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(Instant::now() < deadline, "the stub never became ready");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn ctrl_c_reaches_the_plugin_and_the_plugin_decides_the_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    install(
        dir.path(),
        &Stub {
            name: "patient",
            script: r#"trap 'exit 7' INT; touch "$1"; while :; do sleep 0.05; done"#,
            mode: 0o755,
        },
    );
    let marker = dir.path().join("ready");
    // Its own process group stands in for the terminal's foreground group, so
    // the signal reaches `krabka` and the plugin together, as Ctrl-C does.
    let mut child = krabka(dir.path(), &["patient", marker.to_str().unwrap()])
        .process_group(0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(&marker);
    let group = rustix::process::Pid::from_child(&child);
    rustix::process::kill_process_group(group, rustix::process::Signal::INT).unwrap();
    assert!(child.wait().unwrap().code() == Some(7));
}

#[test]
fn sigterm_to_krabka_is_forwarded_to_the_plugin() {
    let dir = tempfile::tempdir().unwrap();
    install(
        dir.path(),
        &Stub {
            name: "graceful",
            script: r#"trap 'exit 9' TERM; touch "$1"; while :; do sleep 0.05; done"#,
            mode: 0o755,
        },
    );
    let marker = dir.path().join("ready");
    let mut child = krabka(dir.path(), &["graceful", marker.to_str().unwrap()])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(&marker);
    let pid = rustix::process::Pid::from_child(&child);
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    assert!(child.wait().unwrap().code() == Some(9));
}

#[test]
fn a_reader_that_closes_early_is_a_clean_exit() {
    use std::io::Write as _;

    let dir = tempfile::tempdir().unwrap();
    let mut child = krabka(
        dir.path(),
        &[
            "--output",
            "json",
            "gres",
            "balance-dry-run",
            "--metrics-file",
            "-",
        ],
    )
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    // Close the read end before the command has anything to write.
    drop(child.stdout.take());
    child
        .stdin
        .take()
        .unwrap()
        .write_all(br#"{"tenants":[]}"#)
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.code() == Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
