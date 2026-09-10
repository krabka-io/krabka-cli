//! Candidate-broker qualification lane for Milestone 20.
//!
//! The runner supplies authenticated admin credentials and credentials for a
//! principal that starts authorized, then is explicitly denied by this test.
//! Every CLI response, exit status, and subsequent state observation is emitted
//! as one JSON evidence document.

use std::{
    env,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use assert2::assert;
use serde_json::{Value, json};

fn run(bootstrap: &str, config: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(args)
        .args([
            "--bootstrap-server",
            bootstrap,
            "--command-config",
            config,
            "--request-timeout-ms",
            "2000",
            "--timeout",
            "2s",
            "--output",
            "json",
        ])
        .output()
        .expect("run krabka")
}

fn record(
    evidence: &mut Vec<Value>,
    name: &str,
    bootstrap: &str,
    config: &str,
    config_name: &str,
    args: &[&str],
    expected_exit: i32,
) -> Value {
    let output = run(bootstrap, config, args);
    assert!(output.status.code() == Some(expected_exit), "{name}");
    let stream = if output.stdout.is_empty() {
        &output.stderr
    } else {
        &output.stdout
    };
    let payload: Value = serde_json::from_slice(stream).expect("structured CLI output");
    emit(
        evidence,
        json!({
            "name": name,
            "argv": args,
            "connection": {"bootstrap": bootstrap, "config": config_name},
            "exit": output.status.code(),
            "payload": payload,
        }),
    );
    payload
}

fn emit(evidence: &mut Vec<Value>, entry: Value) {
    eprintln!(
        "{}",
        serde_json::to_string(&entry).expect("serialize evidence")
    );
    evidence.push(entry);
}

fn data(payload: &Value) -> &Value {
    payload.get("data").expect("successful command has data")
}

fn exact_revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn exact_image_digest(value: &str) -> bool {
    value
        .rsplit_once("@sha256:")
        .is_some_and(|(image, digest)| {
            !image.is_empty()
                && digest.len() == 64
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn scoped_user_principal(value: &str) -> bool {
    value.starts_with("User:") && value != "User:*"
}

#[test]
fn qualification_identifiers_are_strict() {
    assert!(exact_image_digest(&format!(
        "registry/broker@sha256:{}",
        "a".repeat(64)
    )));
    assert!(!exact_image_digest("registry/broker@sha256:latest"));
    assert!(!exact_image_digest("registry/broker@sha256:"));
    assert!(scoped_user_principal("User:denied"));
    assert!(!scoped_user_principal("User:*"));
}

struct Matrix<'a> {
    bootstrap: &'a str,
    admin_config: &'a str,
    denied_config: &'a str,
    denied_principal: &'a str,
    topic: &'a str,
    group: &'a str,
    evidence: Vec<Value>,
}

impl Matrix<'_> {
    fn record(
        &mut self,
        name: &str,
        config: &str,
        config_name: &str,
        args: &[&str],
        exit: i32,
    ) -> Value {
        record(
            &mut self.evidence,
            name,
            self.bootstrap,
            config,
            config_name,
            args,
            exit,
        )
    }
}

fn qualify_topic_and_config(matrix: &mut Matrix<'_>) {
    let created = matrix.record(
        "topics-create",
        matrix.admin_config,
        "admin",
        &[
            "topics",
            "--create",
            "--topic",
            matrix.topic,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
        0,
    );
    assert!(data(&created)[0]["topic"] == matrix.topic);
    assert!(data(&created)[0]["error"].is_null());

    let started = Instant::now();
    let mut attempt = 0;
    let created_state = loop {
        attempt += 1;
        let output = run(
            matrix.bootstrap,
            matrix.admin_config,
            &["topics", "--describe", "--topic", matrix.topic],
        );
        let stream = if output.stdout.is_empty() {
            &output.stderr
        } else {
            &output.stdout
        };
        let payload: Value = serde_json::from_slice(stream).expect("structured topic state");
        let exit = output.status.code();
        emit(
            &mut matrix.evidence,
            json!({
                "name": "topics-create-state",
                "attempt": attempt,
                "argv": ["topics", "--describe", "--topic", matrix.topic],
                "connection": {"bootstrap": matrix.bootstrap, "config": "admin"},
                "exit": exit,
                "payload": payload,
            }),
        );
        if exit == Some(0) {
            break payload;
        }
        assert!(data(&payload)[0]["error"]["code"] == 3);
        assert!(started.elapsed() < Duration::from_secs(30));
        thread::sleep(Duration::from_millis(250));
    };
    assert!(data(&created_state)[0]["partitions"] == 3);
    assert!(data(&created_state)[0]["replication_factor"] == 1);

    matrix.record(
        "configs-alter",
        matrix.admin_config,
        "admin",
        &[
            "configs",
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            matrix.topic,
            "--add-config",
            "cleanup.policy=compact",
        ],
        0,
    );
    let started = Instant::now();
    let mut attempt = 0;
    let config_state = loop {
        attempt += 1;
        let payload = matrix.record(
            "configs-alter-state",
            matrix.admin_config,
            "admin",
            &[
                "configs",
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                matrix.topic,
            ],
            0,
        );
        if data(&payload)[0]["configs"]["cleanup.policy"] == "compact" {
            break payload;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "attempt {attempt}"
        );
        thread::sleep(Duration::from_millis(250));
    };
    assert!(data(&config_state)[0]["configs"]["cleanup.policy"] == "compact");
}

fn qualify_acl_denial(matrix: &mut Matrix<'_>) {
    matrix.record(
        "acl-precondition",
        matrix.denied_config,
        "denied-principal",
        &["topics", "--describe", "--topic", matrix.topic],
        0,
    );
    matrix.record(
        "acl-deny-create",
        matrix.admin_config,
        "admin",
        &[
            "acls",
            "--add",
            "--topic",
            matrix.topic,
            "--deny-principal",
            matrix.denied_principal,
            "--operation",
            "describe",
        ],
        0,
    );
    let acl_state = matrix.record(
        "acl-deny-state",
        matrix.admin_config,
        "admin",
        &[
            "acls",
            "--list",
            "--topic",
            matrix.topic,
            "--deny-principal",
            matrix.denied_principal,
            "--operation",
            "describe",
        ],
        0,
    );
    assert!(
        data(&acl_state)
            .as_array()
            .is_some_and(|entries| entries.iter().any(|entry| {
                entry["principal"] == matrix.denied_principal
                    && entry["permission"] == "Deny"
                    && entry["operation"] == "Describe"
            }))
    );
    let denied = matrix.record(
        "acl-denied-request",
        matrix.denied_config,
        "denied-principal",
        &["topics", "--describe", "--topic", matrix.topic],
        1,
    );
    assert!(data(&denied)[0]["error"]["code"] == 29);
}

fn qualify_offsets_and_features(matrix: &mut Matrix<'_>) {
    matrix.record(
        "offsets-reset",
        matrix.admin_config,
        "admin",
        &[
            "consumer-groups",
            "--reset-offsets",
            "--group",
            matrix.group,
            "--topic",
            matrix.topic,
            "--partition",
            "0",
            "--to-offset",
            "7",
            "--yes",
        ],
        0,
    );
    let offset_state = matrix.record(
        "offsets-reset-state",
        matrix.admin_config,
        "admin",
        &["consumer-groups", "--describe", "--group", matrix.group],
        0,
    );
    assert!(
        data(&offset_state)
            .as_array()
            .is_some_and(|offsets| offsets.iter().any(|offset| {
                offset["topic"] == matrix.topic && offset["partition"] == 0 && offset["offset"] == 7
            }))
    );
    let invalid = matrix.record(
        "offsets-invalid",
        matrix.admin_config,
        "admin",
        &[
            "consumer-groups",
            "--reset-offsets",
            "--group",
            matrix.group,
            "--topic",
            matrix.topic,
            "--partition",
            "-1",
            "--to-offset",
            "0",
            "--yes",
        ],
        1,
    );
    assert!(invalid["error"]["code"] == 1);

    let features = matrix.record(
        "features-inspect",
        matrix.admin_config,
        "admin",
        &["features", "--describe"],
        0,
    );
    assert!(
        data(&features)["supported"]
            .as_array()
            .is_some_and(|features| !features.is_empty())
    );
    assert!(data(&features).get("finalized_features_epoch").is_some());
}

fn qualify_reassignment(matrix: &mut Matrix<'_>) {
    let submitted = matrix.record(
        "reassignment-submit",
        matrix.admin_config,
        "admin",
        &[
            "reassign-partitions",
            "--execute",
            "--topic",
            matrix.topic,
            "--replication-factor",
            "2",
            "--yes",
        ],
        0,
    );
    assert!(data(&submitted)["status"] == "ReassignmentSubmitted");

    let started = Instant::now();
    let mut attempt = 0;
    let completed = loop {
        attempt += 1;
        let output = run(
            matrix.bootstrap,
            matrix.admin_config,
            &[
                "reassign-partitions",
                "--verify",
                "--topic",
                matrix.topic,
                "--replication-factor",
                "2",
            ],
        );
        let payload: Value = serde_json::from_slice(&output.stdout).expect("structured verify");
        let status = data(&payload)["status"].as_str().expect("status string");
        emit(
            &mut matrix.evidence,
            json!({
                "name": "reassignment-progress",
                "attempt": attempt,
                "argv": ["reassign-partitions", "--verify", "--topic", matrix.topic, "--replication-factor", "2"],
                "connection": {"bootstrap": matrix.bootstrap, "config": "admin"},
                "exit": output.status.code(),
                "payload": payload,
            }),
        );
        if status == "InSync" {
            assert!(output.status.code() == Some(0));
            break true;
        }
        assert!(status == "ReassignmentInProgress" || status == "ReplicationFactorMismatch");
        assert!(output.status.code() == Some(1));
        if started.elapsed() >= Duration::from_secs(30) {
            break false;
        }
        thread::sleep(Duration::from_millis(250));
    };
    assert!(completed, "reassignment did not complete within 30 seconds");
    let state = matrix.record(
        "reassignment-complete-state",
        matrix.admin_config,
        "admin",
        &["topics", "--describe", "--topic", matrix.topic],
        0,
    );
    assert!(data(&state)[0]["replication_factor"] == 2);
}

fn cleanup_and_verify_deletion(matrix: &mut Matrix<'_>) {
    let removed_acl = matrix.record(
        "acl-cleanup",
        matrix.admin_config,
        "admin",
        &[
            "acls",
            "--remove",
            "--topic",
            matrix.topic,
            "--deny-principal",
            matrix.denied_principal,
            "--operation",
            "describe",
            "--yes",
        ],
        0,
    );
    assert!(
        data(&removed_acl)
            .as_array()
            .is_some_and(|entries| entries.iter().any(|entry| {
                entry["resource_name"] == matrix.topic
                    && entry["principal"] == matrix.denied_principal
                    && entry["permission"] == "Deny"
                    && entry["operation"] == "Describe"
            }))
    );
    matrix.record(
        "topics-delete",
        matrix.admin_config,
        "admin",
        &["topics", "--delete", "--topic", matrix.topic, "--yes"],
        0,
    );

    let started = Instant::now();
    let mut attempt = 0;
    while started.elapsed() < Duration::from_secs(30) {
        attempt += 1;
        let output = run(
            matrix.bootstrap,
            matrix.admin_config,
            &["topics", "--describe", "--topic", matrix.topic],
        );
        let stream = if output.stdout.is_empty() {
            &output.stderr
        } else {
            &output.stdout
        };
        let payload: Value = serde_json::from_slice(stream).expect("structured delete state");
        let exit = output.status.code();
        emit(
            &mut matrix.evidence,
            json!({
                "name": "topics-delete-state",
                "attempt": attempt,
                "argv": ["topics", "--describe", "--topic", matrix.topic],
                "connection": {"bootstrap": matrix.bootstrap, "config": "admin"},
                "exit": exit,
                "payload": payload,
            }),
        );
        if exit == Some(1) {
            assert!(data(&payload)[0]["error"]["code"] == 3);
            return;
        }
        assert!(exit == Some(0));
        thread::sleep(Duration::from_millis(250));
    }
    panic!("topic deletion did not complete within 30 seconds");
}

#[test]
#[ignore = "requires a multi-broker candidate and qualification credentials"]
fn authenticated_admin_matrix_matches_real_broker_state() {
    let bootstrap = env::var("KRABKA_CANDIDATE_BOOTSTRAP").expect("candidate bootstrap");
    let cli_revision = env::var("KRABKA_CLI_REVISION").expect("immutable CLI revision");
    let broker_revision =
        env::var("KRABKA_CANDIDATE_REVISION").expect("immutable candidate broker revision");
    let broker_image =
        env::var("KRABKA_CANDIDATE_IMAGE").expect("candidate broker image with sha256 digest");
    let admin_config = env::var("KRABKA_COMMAND_CONFIG").expect("admin command config");
    let denied_config =
        env::var("KRABKA_DENIED_COMMAND_CONFIG").expect("denied-principal command config");
    let denied_principal =
        env::var("KRABKA_DENIED_PRINCIPAL").expect("Kafka principal named by denied config");
    assert!(exact_revision(&cli_revision));
    assert!(exact_revision(&broker_revision));
    assert!(exact_image_digest(&broker_image));
    assert!(scoped_user_principal(&denied_principal));

    let run_id = uuid::Uuid::new_v4();
    let topic = format!("m20-cli-{run_id}");
    let group = format!("m20-cli-group-{run_id}");
    let mut matrix = Matrix {
        bootstrap: &bootstrap,
        admin_config: &admin_config,
        denied_config: &denied_config,
        denied_principal: &denied_principal,
        topic: &topic,
        group: &group,
        evidence: Vec::new(),
    };

    qualify_topic_and_config(&mut matrix);
    qualify_acl_denial(&mut matrix);
    qualify_offsets_and_features(&mut matrix);
    qualify_reassignment(&mut matrix);
    cleanup_and_verify_deletion(&mut matrix);

    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": 1,
            "cli": {"revision": cli_revision, "version": env!("CARGO_PKG_VERSION")},
            "broker": {"revision": broker_revision, "image": broker_image},
            "commands": matrix.evidence,
        }))
        .unwrap()
    );
}
