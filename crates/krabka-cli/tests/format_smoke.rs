//! Dispatch tests for the built-in `krabka format`.
//!
//! The formatter itself is `krabka-format`, and krabka-broker tests what it
//! writes. This suite checks what `krabka` owns: `format` is compiled in rather
//! than delegated, and the formatter's exit code reaches the process unchanged.

use std::process::Command;

use assert2::{assert, check};

/// Runs `krabka format` on `dir`, as node 1 unless `args` names a node.
fn run_format(dir: &tempfile::TempDir, args: &[&str]) -> std::process::Output {
    let node = if args.contains(&"--node-id") {
        &[][..]
    } else {
        &["--node-id", "1"][..]
    };
    Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(["format", "--log-dir", dir.path().to_str().unwrap()])
        .args(node)
        .args(args)
        .output()
        .expect("run krabka format")
}

#[test]
fn format_writes_a_bootstrap_manifest_into_an_empty_directory() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(&dir, &[]);
    assert!(out.status.code() == Some(0));
    check!(dir.path().join("bootstrap.json").is_file());
    check!(dir.path().join("meta.properties").is_file());
}

#[test]
fn format_exit_codes_reach_the_process_unchanged() {
    // `krabka-format` owns these codes: 2 for SCRAM iterations below 4096 (it
    // shares clap's usage code, an upstream collision), 3 for a non-empty log
    // directory, 4 for a bootstrap failure, 5 for an invalid feature level.
    let cases: &[(&[&str], bool, i32)] = &[
        (
            &[
                "--add-scram",
                "SCRAM-SHA-512=[name=admin,password=p,iterations=1]",
            ],
            false,
            2,
        ),
        (&[], true, 3),
        (
            &[
                "--node-id",
                "3",
                "--initial-controllers",
                "2@two.example:9093:00000000-0000-0000-0000-000000000002",
            ],
            false,
            4,
        ),
        (&["--feature", "kraft.version=1"], false, 5),
    ];
    for (args, occupied, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        if *occupied {
            std::fs::write(dir.path().join("occupied"), b"x").unwrap();
        }
        let out = run_format(&dir, args);
        check!(out.status.code() == Some(*expected), "args: {args:?}");
    }
}
