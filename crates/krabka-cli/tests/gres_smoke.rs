//! Smoke test for the `krabka gres` binary surface.
//!
//! `balance-dry-run` plans from a metrics snapshot without a broker, so it
//! drives the `gres` dispatch end to end in a subprocess.

use std::{
    io::Write as _,
    process::{Command, Stdio},
};

use assert2::assert;
use serde_json::{Value, json};

#[test]
fn gres_balance_dry_run_plans_nothing_for_an_empty_snapshot() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args([
            "--output",
            "json",
            "gres",
            "balance-dry-run",
            "--metrics-file",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn krabka gres");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(br#"{"tenants":[]}"#)
        .expect("write snapshot");
    let out = child.wait_with_output().expect("run krabka gres");

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let payload: Value = serde_json::from_slice(&out.stdout).expect("JSON output");
    assert!(
        payload
            == json!({
                "data": {
                    "dryRun": true,
                    "goalsApplied": [
                        "co_location_integrity",
                        "range_limit",
                        "range_size",
                        "load_skew",
                        "auto_shard_conversion",
                    ],
                    "operations": [],
                }
            })
    );
}
