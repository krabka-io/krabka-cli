//! Candidate-broker qualification lane for Milestone 20.
//!
//! The runner supplies authenticated and deliberately unauthorized Kafka
//! properties files. The test prints the exact command matrix as JSON; CI can
//! archive stdout without inventing a second evidence format.

use std::{env, process::Command};

use assert2::assert;
use serde_json::{Value, json};

const COMMANDS: &[&str] = &[
    "topics-create",
    "configs-describe",
    "acls-deny",
    "offsets-inspect",
    "features-inspect",
    "reassignment-verify",
    "unauthorized-request",
    "topics-delete",
];

fn run(bootstrap: &str, config: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(args)
        .args([
            "--bootstrap-server",
            bootstrap,
            "--command-config",
            config,
            "--output",
            "json",
        ])
        .output()
        .expect("run krabka")
}

#[test]
#[ignore = "requires a candidate broker and qualification credentials"]
fn authenticated_admin_matrix_matches_real_broker_state() {
    let bootstrap = env::var("KRABKA_CANDIDATE_BOOTSTRAP").expect("candidate bootstrap");
    let config = env::var("KRABKA_COMMAND_CONFIG").expect("authenticated command config");
    let unauthorized =
        env::var("KRABKA_UNAUTHORIZED_COMMAND_CONFIG").expect("unauthorized command config");
    let topic = format!("m20-cli-{}", std::process::id());
    let topic_ref = topic.as_str();

    let cases: [(&str, Vec<&str>, bool); 8] = [
        (
            "topics-create",
            vec![
                "topics",
                "--create",
                "--topic",
                topic_ref,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--config",
                "cleanup.policy=compact",
            ],
            true,
        ),
        (
            "configs-describe",
            vec![
                "configs",
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                topic_ref,
            ],
            true,
        ),
        (
            "acls-deny",
            vec![
                "acls",
                "--add",
                "--topic",
                topic_ref,
                "--deny-principal",
                "User:m20-denied",
                "--operation",
                "read",
            ],
            true,
        ),
        (
            "offsets-inspect",
            vec![
                "consumer-groups",
                "--describe",
                "--group",
                "m20-qualification",
            ],
            true,
        ),
        ("features-inspect", vec!["features", "--describe"], false),
        (
            "reassignment-verify",
            vec![
                "reassign-partitions",
                "--verify",
                "--topic",
                topic_ref,
                "--replication-factor",
                "1",
            ],
            true,
        ),
        ("unauthorized-request", vec!["topics", "--list"], false),
        (
            "topics-delete",
            vec!["topics", "--delete", "--topic", topic_ref, "--yes"],
            true,
        ),
    ];

    assert!(cases.iter().map(|case| case.0).collect::<Vec<_>>() == COMMANDS);
    let mut evidence = Vec::<Value>::new();
    for (name, args, succeeds) in cases {
        let selected_config = if name == "unauthorized-request" {
            &unauthorized
        } else {
            &config
        };
        let output = run(&bootstrap, selected_config, &args);
        assert!(output.status.success() == succeeds);
        let stream = if succeeds {
            &output.stdout
        } else {
            &output.stderr
        };
        let payload: Value = serde_json::from_slice(stream).expect("structured output");
        if name == "configs-describe" {
            assert!(payload.to_string().contains("cleanup.policy"));
            assert!(payload.to_string().contains("compact"));
        }
        evidence.push(json!({
            "name": name,
            "argv": args,
            "exit": output.status.code(),
            "payload": payload,
        }));
    }
    println!(
        "{}",
        serde_json::to_string(&json!({
            "cli_revision": option_env!("GIT_COMMIT").unwrap_or("unknown"),
            "broker_revision": env::var("KRABKA_CANDIDATE_REVISION").unwrap_or_default(),
            "commands": evidence,
        }))
        .unwrap()
    );
}
