//! Where `krabka` writes: payloads to stdout, failures and logs to stderr.

mod support;

use std::{collections::BTreeMap, process::Command};

use assert2::{assert, check};
use krabka_protocol::owned::{
    metadata_request,
    metadata_response::{MetadataResponse, MetadataResponseTopic},
};
use serde_json::{Value, json};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;

struct Outcome {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn krabka(args: &[&str], rust_log: &str) -> Outcome {
    let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(args)
        .env("RUST_LOG", rust_log)
        .output()
        .expect("run krabka");
    Outcome {
        code: out.status.code(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

async fn broker(topics: &[(&str, i16)]) -> MockBroker {
    let reply = respond(
        &MetadataResponse {
            topics: topics
                .iter()
                .map(|(name, error_code)| MetadataResponseTopic {
                    name: Some((*name).into()),
                    error_code: *error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[(metadata_request::API_KEY, 0, METADATA_VERSION)],
        BTreeMap::from([((metadata_request::API_KEY, METADATA_VERSION), reply)]),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_json_success_is_the_only_thing_on_stdout_and_logs_go_to_stderr() {
    let broker = broker(&[("orders", 0)]).await;
    let address = broker.address();
    let out = tokio::task::spawn_blocking(move || {
        krabka(
            &[
                "--output",
                "json",
                "topics",
                "--list",
                "--bootstrap-server",
                &address,
            ],
            "debug",
        )
    })
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&out.stdout).expect("stdout is one JSON document");
    assert!(
        (out.code, payload)
            == (
                Some(0),
                json!({"data": [{
                    "topic": "orders",
                    "topic_id": null,
                    "partitions": 0,
                    "replication_factor": 0,
                    "error": null,
                }]})
            )
    );
    check!(out.stdout.matches('\n').count() == 1);
    check!(
        broker.received().last()
            == Some(&Received {
                api_key: metadata_request::API_KEY,
                version: METADATA_VERSION,
            })
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_row_is_printed_with_the_others_and_the_command_exits_1() {
    let broker = broker(&[("orders", 0), ("missing", 3)]).await;
    let address = broker.address();
    let out = tokio::task::spawn_blocking(move || {
        krabka(
            &[
                "topics",
                "--describe",
                "--topic",
                "orders",
                "--topic",
                "missing",
                "--bootstrap-server",
                &address,
            ],
            "off",
        )
    })
    .await
    .unwrap();
    check!(out.code == Some(1));
    check!(
        out.stdout
            == "Topic: orders\tPartitionCount: 0\tReplicationFactor: 0\nmissing\tERROR\tUNKNOWN_TOPIC_OR_PARTITION (3)\n"
    );
    check!(out.stderr.is_empty());
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_injected_timeout_logs_to_stderr_before_the_error_envelope() {
    let broker = MockBroker::start(
        &[(metadata_request::API_KEY, 0, METADATA_VERSION)],
        BTreeMap::from([((metadata_request::API_KEY, METADATA_VERSION), Reply::Silent)]),
    )
    .await;
    let address = broker.address();
    let out = tokio::task::spawn_blocking(move || {
        krabka(
            &[
                "--output",
                "json",
                "topics",
                "--list",
                "--bootstrap-server",
                &address,
                "--request-timeout-ms",
                "200",
            ],
            "debug",
        )
    })
    .await
    .unwrap();
    check!(out.code == Some(1));
    check!(out.stdout.is_empty());
    // The client's background telemetry task may log at debug after the
    // command ends, so the envelope is found by its shape, not its position.
    let lines = out.stderr.lines().collect::<Vec<_>>();
    let envelope_at = lines
        .iter()
        .rposition(|line| serde_json::from_str::<Value>(line).is_ok())
        .expect("an error envelope on stderr");
    let envelope: Value = serde_json::from_str(lines[envelope_at]).unwrap();
    check!(envelope["error"]["code"] == json!(1));
    // The client's instrumented request logs its failure before the envelope.
    check!(
        lines[..envelope_at]
            .iter()
            .any(|line| line.contains("request timed out")),
        "no log line on stderr before the envelope"
    );
    broker.stop();
}

#[test]
fn a_failure_is_one_line_on_stderr_in_either_format() {
    let args = ["topics", "--list", "--dry-run"];
    let human = krabka(&args, "off");
    check!(human.code == Some(1));
    check!(human.stdout.is_empty());
    check!(human.stderr == "krabka topics: --dry-run is only valid with --create or --delete\n");

    let json = krabka(&[&["--output", "json"][..], &args[..]].concat(), "off");
    check!(json.code == Some(1));
    check!(json.stdout.is_empty());
    check!(
        serde_json::from_str::<Value>(&json.stderr).unwrap()
            == json!({"error": {"code": 1, "message": "--dry-run is only valid with --create or --delete"}})
    );
    check!(json.stderr.matches('\n').count() == 1);
}

#[test]
fn json_log_format_writes_json_log_lines_to_stderr() {
    // A port that refuses connections: the client logs the refusal.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let address = format!("127.0.0.1:{port}");
    let out = krabka(
        &[
            "--log-format",
            "json",
            "--output",
            "json",
            "topics",
            "--list",
            "--bootstrap-server",
            &address,
        ],
        "debug",
    );
    check!(out.code == Some(1));
    check!(out.stdout.is_empty());
    let lines = out
        .stderr
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("every stderr line is JSON"))
        .collect::<Vec<_>>();
    let (envelope, logs) = lines.split_last().expect("stderr is not empty");
    check!(envelope["error"]["code"] == json!(1));
    check!(!logs.is_empty());
    check!(logs.iter().all(|log| log["level"].is_string()));
}
