//! `--dry-run`, `--yes` and the confirmation prompt, against a scripted
//! broker.

mod support;

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
};

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_topics_request,
    create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
    delete_topics_request,
    delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    metadata_request,
    metadata_response::{MetadataResponse, MetadataResponseTopic},
};
use serde_json::{Value, json};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;
const DELETE_VERSION: i16 = 6;
const CREATE_VERSION: i16 = 7;

// A broker that knows `orders`, not `missing`, and answers `DeleteTopics` and
// `CreateTopics` as a real broker would for those two names.
async fn broker() -> MockBroker {
    let metadata = respond(
        &MetadataResponse {
            topics: vec![
                MetadataResponseTopic {
                    name: Some("orders".into()),
                    ..Default::default()
                },
                MetadataResponseTopic {
                    name: Some("missing".into()),
                    error_code: 3,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    );
    let delete = respond(
        &DeleteTopicsResponse {
            responses: vec![
                DeletableTopicResult {
                    name: Some("orders".into()),
                    ..Default::default()
                },
                DeletableTopicResult {
                    name: Some("missing".into()),
                    error_code: 3,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        DELETE_VERSION,
        delete_topics_request::FLEXIBLE_MIN,
    );
    let create = respond(
        &CreateTopicsResponse {
            topics: vec![
                CreatableTopicResult {
                    name: "orders".into(),
                    error_code: 36,
                    error_message: Some("Topic 'orders' already exists.".into()),
                    ..Default::default()
                },
                CreatableTopicResult {
                    name: "missing".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        CREATE_VERSION,
        create_topics_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (delete_topics_request::API_KEY, 1, DELETE_VERSION),
            (create_topics_request::API_KEY, 2, CREATE_VERSION),
        ],
        BTreeMap::from([
            ((metadata_request::API_KEY, METADATA_VERSION), metadata),
            ((delete_topics_request::API_KEY, DELETE_VERSION), delete),
            ((create_topics_request::API_KEY, CREATE_VERSION), create),
        ]),
    )
    .await
}

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: Vec<String>, env: Vec<(&'static str, &'static str)>) -> Run {
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .envs(env)
            .env("RUST_LOG", "off")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        Run {
            code: out.status.code(),
            stdout: String::from_utf8(out.stdout).unwrap(),
            stderr: String::from_utf8(out.stderr).unwrap(),
        }
    })
    .await
    .unwrap()
}

fn topics(action: &str, address: &str, extra: &[&str]) -> Vec<String> {
    [
        "--output", "json", "topics", action, "--topic", "orders", "--topic", "missing",
    ]
    .iter()
    .chain(["--bootstrap-server", address].iter())
    .chain(extra)
    .map(|arg| (*arg).to_owned())
    .collect()
}

fn mutating(received: &[Received], api_key: i16) -> usize {
    received
        .iter()
        .filter(|request| request.api_key == api_key)
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_sends_no_mutation_and_reports_what_the_real_run_reports() {
    for (action, api_key) in [
        ("--delete", delete_topics_request::API_KEY),
        ("--create", create_topics_request::API_KEY),
    ] {
        let dry_broker = broker().await;
        let dry = krabka(
            topics(action, &dry_broker.address(), &["--dry-run"]),
            Vec::new(),
        )
        .await;
        check!(mutating(&dry_broker.received(), api_key) == 0, "{action}");
        dry_broker.stop();

        let real_broker = broker().await;
        let real = krabka(
            topics(action, &real_broker.address(), &["--yes"]),
            Vec::new(),
        )
        .await;
        check!(mutating(&real_broker.received(), api_key) == 1, "{action}");
        real_broker.stop();

        check!(
            (dry.code, real.code) == (Some(1), Some(1)),
            "{action}: one row fails in both"
        );
        let mut dry_report: Value = serde_json::from_str(&dry.stdout).unwrap();
        let real_report: Value = serde_json::from_str(&real.stdout).unwrap();
        check!(dry_report["dry_run"] == json!(true), "{action}");
        dry_report.as_object_mut().unwrap().remove("dry_run");
        check!(dry_report == real_report, "{action}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_without_yes_on_a_non_interactive_stdin_is_refused_before_any_mutation() {
    let broker = broker().await;
    let run = krabka(topics("--delete", &broker.address(), &[]), Vec::new()).await;
    check!(mutating(&broker.received(), delete_topics_request::API_KEY) == 0);
    check!(run.code == Some(2));
    check!(run.stdout.is_empty());
    let envelope: Value = serde_json::from_str(&run.stderr).unwrap();
    assert!(
        envelope
            == json!({"error": {
                "code": 2,
                "message": "refusing to prompt for confirmation on a non-interactive stdin; pass --yes to proceed",
            }})
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn krabka_assume_yes_answers_the_prompt() {
    let broker = broker().await;
    let run = krabka(
        topics("--delete", &broker.address(), &[]),
        vec![("KRABKA_ASSUME_YES", "true")],
    )
    .await;
    check!(mutating(&broker.received(), delete_topics_request::API_KEY) == 1);
    check!(run.code == Some(1));
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_that_cannot_read_metadata_fails_without_a_report() {
    let broker = MockBroker::start(
        &[(metadata_request::API_KEY, 0, METADATA_VERSION)],
        BTreeMap::from([((metadata_request::API_KEY, METADATA_VERSION), Reply::Silent)]),
    )
    .await;
    let run = krabka(
        topics(
            "--delete",
            &broker.address(),
            &["--dry-run", "--request-timeout-ms", "200"],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    check!(mutating(&broker.received(), delete_topics_request::API_KEY) == 0);
    broker.stop();
}
