//! `krabka transactions`, `krabka delegation-tokens` and
//! `krabka delete-records`, run as a binary against a scripted broker.

mod support;

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
};

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_delegation_token_request,
    create_delegation_token_response::CreateDelegationTokenResponse,
    delete_records_request,
    delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
    },
    describe_delegation_token_request,
    describe_delegation_token_response::{
        DescribeDelegationTokenResponse, DescribedDelegationToken, DescribedDelegationTokenRenewer,
    },
    describe_transactions_request,
    describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
    expire_delegation_token_request,
    expire_delegation_token_response::ExpireDelegationTokenResponse,
    find_coordinator_request,
    find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    init_producer_id_request,
    init_producer_id_response::InitProducerIdResponse,
    metadata_request,
    metadata_response::{
        MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    },
    renew_delegation_token_request,
    renew_delegation_token_response::RenewDelegationTokenResponse,
};
use serde_json::{Value, json};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;
const FIND_COORDINATOR_VERSION: i16 = 4;
const DESCRIBE_TRANSACTIONS_VERSION: i16 = 0;
const INIT_PRODUCER_ID_VERSION: i16 = 4;
const DELETE_RECORDS_VERSION: i16 = 2;
const TOKEN_VERSION: i16 = 2;
// v3 carries the requester of a new token.
const CREATE_TOKEN_VERSION: i16 = 3;
const DESCRIBE_TOKEN_VERSION: i16 = 3;

const HMAC_BASE64: &str = "c2VjcmV0LWhtYWMtdmFsdWU=";

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: Vec<String>, env: Vec<(&'static str, String)>) -> Run {
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .env_remove("KRABKA_ASSUME_YES")
            .env_remove("KRABKA_DELEGATION_TOKEN_HMAC")
            .envs(env)
            .env("RUST_LOG", "trace")
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

// The last stderr line that is not a tracing log line. The client's
// telemetry task can still log at debug while the client closes, after the
// command has printed its failure.
fn last_message(stderr: &str) -> Option<&str> {
    stderr.lines().rev().find(|line| {
        let plain = line.trim_start_matches("\u{1b}[2m");
        !(plain.len() > 11 && plain.as_bytes()[4] == b'-' && plain.as_bytes()[10] == b'T')
    })
}

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_owned()).collect()
}

fn count(received: &[Received], api_key: i16) -> usize {
    received
        .iter()
        .filter(|request| request.api_key == api_key)
        .count()
}

fn transaction(id: &str, error_code: i16) -> TransactionState {
    TransactionState {
        error_code,
        transactional_id: id.into(),
        transaction_state: "Ongoing".into(),
        transaction_timeout_ms: 60_000,
        transaction_start_time_ms: -1,
        producer_id: 4242,
        producer_epoch: 7,
        ..Default::default()
    }
}

// A transaction coordinator that answers `DescribeTransactions` with `states`
// and `InitProducerId` with `init_producer_id_error`, behind a bootstrap
// broker whose `FindCoordinator` answer names it for every key in `keys`.
struct Transactions {
    bootstrap: MockBroker,
    coordinator: MockBroker,
}

impl Transactions {
    async fn start(
        keys: &[&str],
        states: Vec<TransactionState>,
        init_producer_id_error: i16,
    ) -> Self {
        let coordinator = MockBroker::start(
            &[
                (
                    describe_transactions_request::API_KEY,
                    0,
                    DESCRIBE_TRANSACTIONS_VERSION,
                ),
                (
                    init_producer_id_request::API_KEY,
                    0,
                    INIT_PRODUCER_ID_VERSION,
                ),
            ],
            BTreeMap::from([
                (
                    (
                        describe_transactions_request::API_KEY,
                        DESCRIBE_TRANSACTIONS_VERSION,
                    ),
                    respond(
                        &DescribeTransactionsResponse {
                            transaction_states: states,
                            ..Default::default()
                        },
                        DESCRIBE_TRANSACTIONS_VERSION,
                        describe_transactions_request::FLEXIBLE_MIN,
                    ),
                ),
                (
                    (init_producer_id_request::API_KEY, INIT_PRODUCER_ID_VERSION),
                    respond(
                        &InitProducerIdResponse {
                            error_code: init_producer_id_error,
                            ..Default::default()
                        },
                        INIT_PRODUCER_ID_VERSION,
                        init_producer_id_request::FLEXIBLE_MIN,
                    ),
                ),
            ]),
        )
        .await;
        let port = coordinator
            .address()
            .rsplit_once(':')
            .unwrap()
            .1
            .parse()
            .unwrap();
        let bootstrap = MockBroker::start(
            &[(
                find_coordinator_request::API_KEY,
                0,
                FIND_COORDINATOR_VERSION,
            )],
            BTreeMap::from([(
                (find_coordinator_request::API_KEY, FIND_COORDINATOR_VERSION),
                respond(
                    &FindCoordinatorResponse {
                        coordinators: keys
                            .iter()
                            .map(|key| Coordinator {
                                key: (*key).into(),
                                node_id: 1,
                                host: "127.0.0.1".into(),
                                port,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    },
                    FIND_COORDINATOR_VERSION,
                    find_coordinator_request::FLEXIBLE_MIN,
                ),
            )]),
        )
        .await;
        Self {
            bootstrap,
            coordinator,
        }
    }

    fn stop(self) {
        self.bootstrap.stop();
        self.coordinator.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_describe_prints_kafkas_table() {
    let brokers = Transactions::start(&["payments"], vec![transaction("payments", 0)], 0).await;
    let run = krabka(
        args(&[
            "transactions",
            "--bootstrap-server",
            &brokers.bootstrap.address(),
            "describe",
            "--transactional-id",
            "payments",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(0), "{}", run.stderr);
    check!(
        run.stdout
            == "CoordinatorId\tTransactionalId\tProducerId\tProducerEpoch\tTransactionState\t\
                TransactionTimeoutMs\tCurrentTransactionStartTimeMs\tTransactionDurationMs\t\
                TopicPartitions\t\n\
                1            \tpayments       \t4242      \t7            \tOngoing         \t\
                60000               \tNone                         \tNone                 \t\
                \x20              \t\n"
    );
    brokers.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_describe_of_an_unknown_id_fails_with_kafkas_context() {
    let brokers = Transactions::start(&["nope"], vec![transaction("nope", 105)], 0).await;
    let run = krabka(
        args(&[
            "--output",
            "json",
            "transactions",
            "--bootstrap-server",
            &brokers.bootstrap.address(),
            "describe",
            "--transactional-id",
            "nope",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    let envelope: Value = serde_json::from_str(last_message(&run.stderr).unwrap()).unwrap();
    check!(
        envelope
            == json!({"error": {
                "code": 1,
                "message": "Failed to describe transaction state of transactional-id `nope`: \
                            DescribeTransactions failed: TRANSACTIONAL_ID_NOT_FOUND (105): The \
                            transactionalId could not be found. Enable debug logging for \
                            additional detail.",
            }})
    );
    brokers.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn force_terminate_asks_first_then_fences_the_producer() {
    let cases: [(&[&str], i16, Option<i32>, usize); 4] = [
        // No --yes on a non-interactive stdin: refused before any request.
        (&[], 0, Some(2), 0),
        (&["--dry-run"], 0, Some(0), 0),
        (&["--yes"], 0, Some(0), 1),
        (&["--yes"], 53, Some(1), 1),
    ];
    for (extra, error, code, fenced) in cases {
        let brokers = Transactions::start(&["fence-me"], Vec::new(), error).await;
        let mut command = args(&[
            "transactions",
            "--bootstrap-server",
            &brokers.bootstrap.address(),
            "forceTerminateTransaction",
            "--transactionalId",
            "fence-me",
        ]);
        command.extend(args(extra));
        let run = krabka(command, Vec::new()).await;
        check!(run.code == code, "{extra:?}: {}", run.stderr);
        check!(
            count(
                &brokers.coordinator.received(),
                init_producer_id_request::API_KEY
            ) == fenced,
            "{extra:?}"
        );
        brokers.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_force_terminate_failure_names_the_id_and_the_kafka_error() {
    let brokers = Transactions::start(&["fence-me"], Vec::new(), 53).await;
    let run = krabka(
        args(&[
            "transactions",
            "--bootstrap-server",
            &brokers.bootstrap.address(),
            "forceTerminateTransaction",
            "--transactionalId",
            "fence-me",
            "--yes",
        ]),
        Vec::new(),
    )
    .await;
    check!(
        last_message(&run.stderr)
            == Some(
                "krabka transactions: Failed to force terminate transactionalId `fence-me`: \
                 InitProducerId failed: TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53): Transactional \
                 Id authorization failed. Enable debug logging for additional detail."
            )
    );
    brokers.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_subcommand_exits_1_through_the_error_envelope() {
    let run = krabka(
        args(&[
            "--output",
            "json",
            "transactions",
            "--bootstrap-server",
            "unreachable.invalid:1",
            "list",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    let envelope: Value = serde_json::from_str(last_message(&run.stderr).unwrap()).unwrap();
    check!(
        envelope
            == json!({"error": {
                "code": 1,
                "message": "Failed to list transactions: not supported by this build; it needs \
                            AdminClient::list_transactions from a newer krabka-client-rs",
            }})
    );
}

// A broker that leads partitions 0 and 1 of `t` and answers DeleteRecords
// with a low watermark of 11 for partition 1 and OFFSET_OUT_OF_RANGE for
// partition 0.
async fn records_broker() -> MockBroker {
    let metadata = respond(
        &MetadataResponse {
            // Port 0 makes the client reuse the bootstrap address for the
            // leader.
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            }],
            topics: vec![
                MetadataResponseTopic {
                    name: Some("t".into()),
                    partitions: (0..2)
                        .map(|partition_index| MetadataResponsePartition {
                            partition_index,
                            leader_id: 1,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                MetadataResponseTopic {
                    name: Some("gone".into()),
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
        &DeleteRecordsResponse {
            topics: vec![DeleteRecordsTopicResult {
                name: "t".into(),
                partitions: vec![
                    DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: -1,
                        error_code: 1,
                        ..Default::default()
                    },
                    DeleteRecordsPartitionResult {
                        partition_index: 1,
                        low_watermark: 11,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        },
        DELETE_RECORDS_VERSION,
        delete_records_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (delete_records_request::API_KEY, 0, DELETE_RECORDS_VERSION),
        ],
        BTreeMap::from([
            ((metadata_request::API_KEY, METADATA_VERSION), metadata),
            (
                (delete_records_request::API_KEY, DELETE_RECORDS_VERSION),
                delete,
            ),
        ]),
    )
    .await
}

fn offset_file(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("offsets.json");
    std::fs::write(&path, body).unwrap();
    path.to_str().unwrap().to_owned()
}

const OFFSETS: &str = r#"{"partitions":[{"topic":"t","partition":1,"offset":-1},{"topic":"t","partition":0,"offset":1}],"version":1}"#;

#[tokio::test(flavor = "multi_thread")]
async fn delete_records_prints_kafkas_report_and_exits_1_when_a_partition_fails() {
    let dir = tempfile::tempdir().unwrap();
    let file = offset_file(&dir, OFFSETS);
    let broker = records_broker().await;
    let run = krabka(
        args(&[
            "delete-records",
            "--bootstrap-server",
            &broker.address(),
            "--offset-json-file",
            &file,
            "--yes",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1), "{}", run.stderr);
    check!(
        run.stdout
            == "Executing records delete operation\n\
                Records delete operation completed:\n\
                partition: t-1\tlow_watermark: 11\n\
                partition: t-0\terror: org.apache.kafka.common.errors.OffsetOutOfRangeException: \
                The requested offset is not within the range of offsets maintained by the \
                server.\n"
    );
    check!(count(&broker.received(), delete_records_request::API_KEY) == 1);
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_records_refuses_without_yes_and_a_dry_run_deletes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let file = offset_file(&dir, OFFSETS);
    let cases: [(&[&str], Option<i32>); 2] = [(&[], Some(2)), (&["--dry-run"], Some(0))];
    for (extra, code) in cases {
        let broker = records_broker().await;
        let mut command = args(&[
            "delete-records",
            "--bootstrap-server",
            &broker.address(),
            "--offset-json-file",
            &file,
        ]);
        command.extend(args(extra));
        let run = krabka(command, Vec::new()).await;
        check!(run.code == code, "{extra:?}: {}", run.stderr);
        check!(
            count(&broker.received(), delete_records_request::API_KEY) == 0,
            "{extra:?}"
        );
        broker.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_records_dry_run_reports_what_it_would_delete() {
    let dir = tempfile::tempdir().unwrap();
    let file = offset_file(
        &dir,
        r#"{"partitions":[{"topic":"t","partition":1,"offset":-1},{"topic":"gone","partition":0,"offset":5}]}"#,
    );
    let broker = records_broker().await;
    let run = krabka(
        args(&[
            "--output",
            "json",
            "delete-records",
            "--bootstrap-server",
            &broker.address(),
            "--offset-json-file",
            &file,
            "--dry-run",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    let report: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(
        report
            == json!({
                "data": [
                    {"topic": "t", "partition": 1, "offset": -1, "low_watermark": null, "error": null},
                    {
                        "topic": "gone",
                        "partition": 0,
                        "offset": 5,
                        "low_watermark": null,
                        "error": {
                            "code": 3,
                            "name": "UNKNOWN_TOPIC_OR_PARTITION",
                            "message": "This server does not host this topic-partition.",
                        },
                    },
                ],
                "dry_run": true,
            })
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_offset_file_fails_before_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let file = offset_file(&dir, r#"{"version":1}"#);
    let run = krabka(
        args(&[
            "delete-records",
            "--bootstrap-server",
            "unreachable.invalid:1",
            "--offset-json-file",
            &file,
            "--yes",
        ]),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(last_message(&run.stderr) == Some("krabka delete-records: Missing partitions field"));
}

// A broker that answers the four delegation-token RPCs.
async fn token_broker() -> MockBroker {
    let described = DescribedDelegationToken {
        principal_type: "User".into(),
        principal_name: "alice".into(),
        token_requester_principal_type: "User".into(),
        token_requester_principal_name: "admin".into(),
        issue_timestamp: 1_700_000_000_000,
        expiry_timestamp: 1_700_086_400_000,
        max_timestamp: 1_700_604_800_000,
        token_id: "tok-1".into(),
        hmac: b"secret-hmac-value".to_vec().into(),
        renewers: vec![DescribedDelegationTokenRenewer {
            principal_type: "User".into(),
            principal_name: "bob".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    MockBroker::start(
        &[
            (
                create_delegation_token_request::API_KEY,
                1,
                CREATE_TOKEN_VERSION,
            ),
            (renew_delegation_token_request::API_KEY, 1, TOKEN_VERSION),
            (expire_delegation_token_request::API_KEY, 1, TOKEN_VERSION),
            (
                describe_delegation_token_request::API_KEY,
                1,
                DESCRIBE_TOKEN_VERSION,
            ),
        ],
        BTreeMap::from([
            (
                (
                    create_delegation_token_request::API_KEY,
                    CREATE_TOKEN_VERSION,
                ),
                respond(
                    &CreateDelegationTokenResponse {
                        principal_type: "User".into(),
                        principal_name: "alice".into(),
                        token_requester_principal_type: "User".into(),
                        token_requester_principal_name: "admin".into(),
                        issue_timestamp_ms: 1_700_000_000_000,
                        expiry_timestamp_ms: 1_700_086_400_000,
                        max_timestamp_ms: 1_700_604_800_000,
                        token_id: "tok-1".into(),
                        hmac: b"secret-hmac-value".to_vec().into(),
                        ..Default::default()
                    },
                    CREATE_TOKEN_VERSION,
                    create_delegation_token_request::FLEXIBLE_MIN,
                ),
            ),
            (
                (renew_delegation_token_request::API_KEY, TOKEN_VERSION),
                respond(
                    &RenewDelegationTokenResponse {
                        expiry_timestamp_ms: 1_700_172_800_000,
                        ..Default::default()
                    },
                    TOKEN_VERSION,
                    renew_delegation_token_request::FLEXIBLE_MIN,
                ),
            ),
            (
                (expire_delegation_token_request::API_KEY, TOKEN_VERSION),
                respond(
                    &ExpireDelegationTokenResponse::default(),
                    TOKEN_VERSION,
                    expire_delegation_token_request::FLEXIBLE_MIN,
                ),
            ),
            (
                (
                    describe_delegation_token_request::API_KEY,
                    DESCRIBE_TOKEN_VERSION,
                ),
                respond(
                    &DescribeDelegationTokenResponse {
                        tokens: vec![described],
                        ..Default::default()
                    },
                    DESCRIBE_TOKEN_VERSION,
                    describe_delegation_token_request::FLEXIBLE_MIN,
                ),
            ),
        ]),
    )
    .await
}

fn token_args(address: &str, rest: &[&str]) -> Vec<String> {
    let mut command = args(&[
        "delegation-tokens",
        "--bootstrap-server",
        address,
        "--command-config",
        "/dev/null",
    ]);
    command.extend(args(rest));
    command
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_describe_prints_kafkas_table_and_no_log_carries_the_hmac() {
    let broker = token_broker().await;
    let run = krabka(
        token_args(
            &broker.address(),
            &["--describe", "--owner-principal", "User:alice"],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(0), "{}", run.stderr);
    check!(
        run.stdout
            == "Calling describe token operation for owners: [User:alice]\n\
                Total number of tokens : 1\n\
                TOKENID         HMAC                           OWNER           REQUESTER       \
                RENEWERS                  ISSUEDATE       EXPIRYDATE      MAXDATE        \n\
                \n\
                tok-1           c2VjcmV0LWhtYWMtdmFsdWU=       User:alice      User:admin      \
                [User:bob]                2023-11-14T22:13 2023-11-15T22:13 2023-11-21T22:13\n"
    );
    check!(!run.stderr.contains(HMAC_BASE64));
    check!(!run.stderr.contains("secret-hmac-value"));
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_create_prints_the_new_token() {
    let broker = token_broker().await;
    let run = krabka(
        token_args(
            &broker.address(),
            &[
                "--create",
                "--max-life-time-period",
                "-1",
                "--renewer-principal",
                "User:bob",
            ],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(0), "{}", run.stderr);
    let lines = run.stdout.lines().collect::<Vec<_>>();
    check!(
        lines
            == [
                "Calling create token operation with renewers :[User:bob] , max-life-time-period :-1",
                "Created delegation token with tokenId : tok-1",
                "",
                "TOKENID         HMAC                           OWNER           REQUESTER       \
                 RENEWERS                  ISSUEDATE       EXPIRYDATE      MAXDATE        ",
                "",
                "tok-1           c2VjcmV0LWhtYWMtdmFsdWU=       User:alice      User:admin      \
                 [User:bob]                2023-11-14T22:13 2023-11-15T22:13 2023-11-21T22:13",
            ]
    );
    check!(!run.stderr.contains(HMAC_BASE64));
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_renew_takes_the_hmac_from_the_environment() {
    let broker = token_broker().await;
    let run = krabka(
        token_args(&broker.address(), &["--renew", "--renew-time-period", "-1"]),
        vec![("KRABKA_DELEGATION_TOKEN_HMAC", HMAC_BASE64.to_owned())],
    )
    .await;
    check!(run.code == Some(0), "{}", run.stderr);
    check!(
        run.stdout
            == format!(
                "Calling renew token operation with hmac :{HMAC_BASE64} , renew-time-period :-1\n\
                 Completed renew operation. New expiry date : 2023-11-16T22:13\n"
            )
    );
    check!(count(&broker.received(), renew_delegation_token_request::API_KEY) == 1);
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_expire_asks_first_and_reads_the_hmac_from_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let hmac_file = dir.path().join("hmac");
    std::fs::write(&hmac_file, format!("{HMAC_BASE64}\n")).unwrap();
    let hmac_file = hmac_file.to_str().unwrap();
    let cases: [(&[&str], Option<i32>, usize); 3] = [
        (&[], Some(2), 0),
        (&["--dry-run"], Some(0), 0),
        (&["--yes"], Some(0), 1),
    ];
    for (extra, code, expired) in cases {
        let broker = token_broker().await;
        let mut rest = vec![
            "--expire",
            "--expiry-time-period",
            "-1",
            "--hmac-file",
            hmac_file,
        ];
        rest.extend_from_slice(extra);
        let run = krabka(token_args(&broker.address(), &rest), Vec::new()).await;
        check!(run.code == code, "{extra:?}: {}", run.stderr);
        check!(
            count(&broker.received(), expire_delegation_token_request::API_KEY) == expired,
            "{extra:?}"
        );
        if expired == 1 {
            let lines = run.stdout.lines().collect::<Vec<_>>();
            check!(
                lines[0]
                    == format!(
                        "Calling expire token operation with hmac :{HMAC_BASE64} , \
                         expire-time-period :-1"
                    )
            );
            check!(lines[1].starts_with("Completed expire operation. New expiry date : "));
        }
        broker.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_hmac_that_is_not_base64_is_refused_before_any_request() {
    let broker = token_broker().await;
    let run = krabka(
        token_args(
            &broker.address(),
            &["--expire", "--hmac", "!!", "--expiry-time-period", "-1"],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(
        last_message(&run.stderr) == Some("krabka delegation-tokens: Illegal base64 character 21")
    );
    assert!(
        broker
            .received()
            .iter()
            .all(|request| request.api_key == 18)
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broker_refusal_names_kafkas_error_and_exits_1() {
    let broker = MockBroker::start(
        &[(
            describe_delegation_token_request::API_KEY,
            1,
            DESCRIBE_TOKEN_VERSION,
        )],
        BTreeMap::from([(
            (
                describe_delegation_token_request::API_KEY,
                DESCRIBE_TOKEN_VERSION,
            ),
            respond(
                &DescribeDelegationTokenResponse {
                    error_code: 64,
                    ..Default::default()
                },
                DESCRIBE_TOKEN_VERSION,
                describe_delegation_token_request::FLEXIBLE_MIN,
            ),
        )]),
    )
    .await;
    let run = krabka(
        token_args(
            &broker.address(),
            &["--describe", "--owner-principal", "User:alice"],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(
        last_message(&run.stderr)
            == Some(
                "krabka delegation-tokens: DescribeDelegationToken failed: \
                 DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64): Delegation Token requests are not \
                 allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated \
                 channels."
            )
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_broker_times_out_with_exit_1() {
    let broker = MockBroker::start(
        &[(
            describe_delegation_token_request::API_KEY,
            1,
            DESCRIBE_TOKEN_VERSION,
        )],
        BTreeMap::from([(
            (
                describe_delegation_token_request::API_KEY,
                DESCRIBE_TOKEN_VERSION,
            ),
            Reply::Silent,
        )]),
    )
    .await;
    let run = krabka(
        token_args(
            &broker.address(),
            &[
                "--describe",
                "--owner-principal",
                "User:alice",
                "--request-timeout-ms",
                "200",
            ],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_renew_reports_the_kafka_error_without_the_hmac() {
    let broker = MockBroker::start(
        &[(renew_delegation_token_request::API_KEY, 1, TOKEN_VERSION)],
        BTreeMap::from([(
            (renew_delegation_token_request::API_KEY, TOKEN_VERSION),
            respond(
                &RenewDelegationTokenResponse {
                    error_code: 62,
                    ..Default::default()
                },
                TOKEN_VERSION,
                renew_delegation_token_request::FLEXIBLE_MIN,
            ),
        )]),
    )
    .await;
    let run = krabka(
        token_args(
            &broker.address(),
            &[
                "--renew",
                "--renew-time-period",
                "-1",
                "--hmac",
                HMAC_BASE64,
            ],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    check!(
        last_message(&run.stderr)
            == Some(
                "krabka delegation-tokens: RenewDelegationToken failed: DELEGATION_TOKEN_NOT_FOUND \
                 (62): Delegation Token is not found on server."
            )
    );
    check!(!run.stderr.contains(HMAC_BASE64));
    broker.stop();
}
