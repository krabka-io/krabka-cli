//! `krabka transactions`, `krabka delegation-tokens` and
//! `krabka delete-records`, run as a binary against a scripted broker.

mod support;

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_delegation_token_request::{self, CreateDelegationTokenRequest},
        create_delegation_token_response::CreateDelegationTokenResponse,
        delete_records_request,
        delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        },
        describe_cluster_request,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_delegation_token_request::{self, DescribeDelegationTokenRequest},
        describe_delegation_token_response::{
            DescribeDelegationTokenResponse, DescribedDelegationToken,
            DescribedDelegationTokenRenewer,
        },
        describe_producers_request::{
            self, DescribeProducersRequest, TopicRequest as DescribeProducersTopicRequest,
        },
        describe_producers_response::{
            DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
        },
        describe_transactions_request::{self, DescribeTransactionsRequest},
        describe_transactions_response::{
            DescribeTransactionsResponse, TopicData, TransactionState,
        },
        expire_delegation_token_request::{self, ExpireDelegationTokenRequest},
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        find_coordinator_request::{self, FindCoordinatorRequest},
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        init_producer_id_request,
        init_producer_id_response::InitProducerIdResponse,
        list_transactions_request::{self, ListTransactionsRequest},
        list_transactions_response::{
            ListTransactionsResponse, TransactionState as ListedTransaction,
        },
        metadata_request::{self, MetadataRequest},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        renew_delegation_token_request::{self, RenewDelegationTokenRequest},
        renew_delegation_token_response::RenewDelegationTokenResponse,
        write_txn_markers_request::{
            self, WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
        },
        write_txn_markers_response::{
            WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
            WritableTxnMarkerTopicResult, WriteTxnMarkersResponse,
        },
    },
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
                 Completed renew operation. New expiry date : 2023-11-16T22:13"
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

// ---------------------------------------------------------------------------
// A scripted cluster that also decodes the requests it receives.

// One request that a `Cluster` broker received: its api key, its version and
// its body after the request header.
#[derive(Debug, Clone)]
struct Request {
    api_key: i16,
    version: i16,
    body: Vec<u8>,
}

impl Request {
    // The request decoded as `T`, past the client id and the tagged-fields
    // byte of a flexible header.
    fn decode<T: for<'a> Decode<'a>>(&self, flexible_min: i16) -> T {
        let client_id_len =
            usize::try_from(i16::from_be_bytes([self.body[0], self.body[1]])).unwrap();
        let tagged = usize::from(self.version >= flexible_min);
        let mut body = &self.body[2 + client_id_len + tagged..];
        T::decode(&mut body, self.version).unwrap()
    }
}

type Handler = dyn Fn(u16, &Request) -> Reply + Send + Sync;

// A broker whose answers come from `handler`, given its own port, and which
// records every request other than `ApiVersions`.
struct Node {
    broker: krabka_client_core::MockBroker,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Node {
    async fn start(advertised: Vec<(i16, i16, i16)>, handler: Arc<Handler>) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let port = Arc::new(Mutex::new(0_u16));
        let own = Arc::clone(&port);
        let versions = api_versions_body(&advertised);
        let broker = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(versions.clone());
            }
            let request = Request {
                api_key,
                version,
                body: body.to_vec(),
            };
            log.lock().unwrap().push(request.clone());
            match handler(*own.lock().unwrap(), &request) {
                Reply::Respond(body) => Some(body),
                Reply::Silent => None,
            }
        })
        .await;
        *port.lock().unwrap() = broker.addr.port();
        Self { broker, requests }
    }

    fn address(&self) -> String {
        self.broker.addr.to_string()
    }

    fn requests(&self, api_key: i16) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.api_key == api_key)
            .cloned()
            .collect()
    }

    fn stop(self) {
        self.broker.stop();
    }
}

fn api_versions_body(advertised: &[(i16, i16, i16)]) -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: std::iter::once((api_versions_request::API_KEY, 0, 3))
            .chain(advertised.iter().copied())
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut body = Vec::new();
    response.encode(&mut body, 0).unwrap();
    body
}

const DESCRIBE_CLUSTER_VERSION: i16 = 1;
const LIST_TRANSACTIONS_VERSION: i16 = 1;
const DESCRIBE_PRODUCERS_VERSION: i16 = 0;
const WRITE_TXN_MARKERS_VERSION: i16 = 1;

fn transaction_apis() -> Vec<(i16, i16, i16)> {
    vec![
        (metadata_request::API_KEY, 0, METADATA_VERSION),
        (
            describe_cluster_request::API_KEY,
            0,
            DESCRIBE_CLUSTER_VERSION,
        ),
        (
            list_transactions_request::API_KEY,
            0,
            LIST_TRANSACTIONS_VERSION,
        ),
        (
            describe_producers_request::API_KEY,
            0,
            DESCRIBE_PRODUCERS_VERSION,
        ),
        (
            write_txn_markers_request::API_KEY,
            1,
            WRITE_TXN_MARKERS_VERSION,
        ),
        (
            find_coordinator_request::API_KEY,
            0,
            FIND_COORDINATOR_VERSION,
        ),
        (
            describe_transactions_request::API_KEY,
            0,
            DESCRIBE_TRANSACTIONS_VERSION,
        ),
    ]
}

// A topic partition of a script.
type Partition = (&'static str, i32);

// The DescribeProducers answer of one partition: its producers, or an error
// code.
type ProducersAnswer = Result<Vec<ProducerState>, i16>;

// What the one broker of a transactions cluster answers.
#[derive(Clone, Default)]
struct Script {
    // The topics that Metadata names, with their partition counts. Broker 1
    // leads and holds every partition.
    topics: Vec<(&'static str, i32)>,
    // The ListTransactions error code and listing.
    list_error: i16,
    listings: Vec<(&'static str, i64, &'static str)>,
    // The DescribeProducers answer, per partition: an error code, or the
    // producers.
    producers: Vec<(Partition, ProducersAnswer)>,
    // The WriteTxnMarkers error code.
    marker_error: i16,
    // The DescribeTransactions answer of each transactional id.
    descriptions: Vec<TransactionState>,
}

fn metadata_body(port: u16, topics: &[(&'static str, i32)]) -> Reply {
    respond(
        &MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            controller_id: 1,
            topics: topics
                .iter()
                .map(|(name, count)| MetadataResponseTopic {
                    name: Some((*name).into()),
                    partitions: (0..*count)
                        .map(|partition_index| MetadataResponsePartition {
                            partition_index,
                            leader_id: 1,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    )
}

fn handle(script: &Script, port: u16, request: &Request) -> Reply {
    match request.api_key {
        metadata_request::API_KEY => {
            let asked: MetadataRequest = request.decode(metadata_request::FLEXIBLE_MIN);
            let topics = script
                .topics
                .iter()
                .filter(|(name, _)| {
                    asked.topics.as_ref().is_none_or(|asked| {
                        asked
                            .iter()
                            .any(|topic| topic.name.as_deref() == Some(*name))
                    })
                })
                .copied()
                .collect::<Vec<_>>();
            metadata_body(port, &topics)
        }
        describe_cluster_request::API_KEY => respond(
            &DescribeClusterResponse {
                endpoint_type: 1,
                controller_id: 1,
                cluster_id: "c".into(),
                brokers: vec![DescribeClusterBroker {
                    broker_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(port),
                    ..Default::default()
                }],
                ..Default::default()
            },
            DESCRIBE_CLUSTER_VERSION,
            describe_cluster_request::FLEXIBLE_MIN,
        ),
        list_transactions_request::API_KEY => {
            let asked: ListTransactionsRequest =
                request.decode(list_transactions_request::FLEXIBLE_MIN);
            respond(
                &ListTransactionsResponse {
                    error_code: script.list_error,
                    transaction_states: script
                        .listings
                        .iter()
                        .filter(|(_, producer_id, _)| {
                            asked.producer_id_filters.is_empty()
                                || asked.producer_id_filters.contains(producer_id)
                        })
                        .map(|(id, producer_id, state)| ListedTransaction {
                            transactional_id: (*id).into(),
                            producer_id: *producer_id,
                            transaction_state: (*state).into(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                request.version,
                list_transactions_request::FLEXIBLE_MIN,
            )
        }
        describe_producers_request::API_KEY => {
            let asked: DescribeProducersRequest =
                request.decode(describe_producers_request::FLEXIBLE_MIN);
            respond(
                &DescribeProducersResponse {
                    topics: asked
                        .topics
                        .iter()
                        .map(|topic| TopicResponse {
                            name: topic.name.clone(),
                            partitions: topic
                                .partition_indexes
                                .iter()
                                .map(|partition| {
                                    let answer = script
                                        .producers
                                        .iter()
                                        .find(|((name, index), _)| {
                                            *name == topic.name && index == partition
                                        })
                                        .map_or(Ok(Vec::new()), |(_, answer)| answer.clone());
                                    PartitionResponse {
                                        partition_index: *partition,
                                        error_code: answer.clone().err().unwrap_or(0),
                                        active_producers: answer.unwrap_or_default(),
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                DESCRIBE_PRODUCERS_VERSION,
                describe_producers_request::FLEXIBLE_MIN,
            )
        }
        write_txn_markers_request::API_KEY => {
            let asked: WriteTxnMarkersRequest =
                request.decode(write_txn_markers_request::FLEXIBLE_MIN);
            respond(
                &WriteTxnMarkersResponse {
                    markers: asked
                        .markers
                        .iter()
                        .map(|marker| WritableTxnMarkerResult {
                            producer_id: marker.producer_id,
                            topics: marker
                                .topics
                                .iter()
                                .map(|topic| WritableTxnMarkerTopicResult {
                                    name: topic.name.clone(),
                                    partitions: topic
                                        .partition_indexes
                                        .iter()
                                        .map(|partition| WritableTxnMarkerPartitionResult {
                                            partition_index: *partition,
                                            error_code: script.marker_error,
                                            ..Default::default()
                                        })
                                        .collect(),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                WRITE_TXN_MARKERS_VERSION,
                write_txn_markers_request::FLEXIBLE_MIN,
            )
        }
        find_coordinator_request::API_KEY => {
            let asked: FindCoordinatorRequest =
                request.decode(find_coordinator_request::FLEXIBLE_MIN);
            respond(
                &FindCoordinatorResponse {
                    coordinators: asked
                        .coordinator_keys
                        .iter()
                        .map(|key| Coordinator {
                            key: key.clone(),
                            node_id: 1,
                            host: "127.0.0.1".into(),
                            port: i32::from(port),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                FIND_COORDINATOR_VERSION,
                find_coordinator_request::FLEXIBLE_MIN,
            )
        }
        describe_transactions_request::API_KEY => {
            let asked: DescribeTransactionsRequest =
                request.decode(describe_transactions_request::FLEXIBLE_MIN);
            respond(
                &DescribeTransactionsResponse {
                    transaction_states: asked
                        .transactional_ids
                        .iter()
                        .map(|id| {
                            script
                                .descriptions
                                .iter()
                                .find(|state| &state.transactional_id == id)
                                .cloned()
                                .unwrap_or_else(|| transaction(id, 105))
                        })
                        .collect(),
                    ..Default::default()
                },
                DESCRIBE_TRANSACTIONS_VERSION,
                describe_transactions_request::FLEXIBLE_MIN,
            )
        }
        _ => Reply::Silent,
    }
}

async fn cluster(script: Script) -> Node {
    Node::start(
        transaction_apis(),
        Arc::new(move |port, request| handle(&script, port, request)),
    )
    .await
}

fn short_deadline() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        "request.timeout.ms=1000\ndefault.api.timeout.ms=2000\n",
    )
    .unwrap();
    file
}

async fn transactions(node: &Node, rest: &[&str]) -> Run {
    let config = short_deadline();
    let mut command = args(&[
        "transactions",
        "--bootstrap-server",
        &node.address(),
        "--command-config",
        config.path().to_str().unwrap(),
    ]);
    command.extend(args(rest));
    krabka(command, Vec::new()).await
}

// A producer state that last wrote at `last_timestamp`, with an open
// transaction from `start` when `start` is not -1.
fn producer_state(producer_id: i64, start: i64, last_timestamp: i64) -> ProducerState {
    ProducerState {
        producer_id,
        producer_epoch: 4,
        last_sequence: 11,
        last_timestamp,
        coordinator_epoch: 3,
        current_txn_start_offset: start,
        ..Default::default()
    }
}

fn epoch_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_list_prints_kafkas_table_and_sends_the_filters() {
    let script = Script {
        listings: vec![("pay", 5, "Ongoing"), ("ship", 6, "CompleteCommit")],
        ..Script::default()
    };
    let cases: [(&[&str], ListTransactionsRequest); 2] = [
        (
            &[],
            ListTransactionsRequest {
                duration_filter: -1,
                ..Default::default()
            },
        ),
        (
            &["--duration-filter", "60000"],
            ListTransactionsRequest {
                duration_filter: 60_000,
                ..Default::default()
            },
        ),
    ];
    for (extra, expected) in cases {
        let node = cluster(script.clone()).await;
        let run = transactions(&node, &[&["list"][..], extra].concat()).await;
        check!(run.code == Some(0), "{extra:?}: {}", run.stderr);
        check!(
            run.stdout
                == "TransactionalId\tCoordinator\tProducerId\tTransactionState\t\n\
                    pay            \t1          \t5         \tOngoing         \t\n\
                    ship           \t1          \t6         \tCompleteCommit  \t\n",
            "{extra:?}"
        );
        let sent = node.requests(list_transactions_request::API_KEY);
        check!(
            sent.iter()
                .map(|request| request
                    .decode::<ListTransactionsRequest>(list_transactions_request::FLEXIBLE_MIN))
                .collect::<Vec<_>>()
                == [expected],
            "{extra:?}"
        );
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_list_failures_carry_kafkas_messages() {
    let cases: [(&[&str], i16, &str); 2] = [
        (
            &[],
            15,
            "krabka transactions: Failed to list transactions: ListTransactions failed: \
             COORDINATOR_NOT_AVAILABLE (15): ListTransactions request sent to broker 1 failed \
             because the coordinator is shutting down. Enable debug logging for additional \
             detail.",
        ),
        // A pattern needs ListTransactions v2, and the broker has v1.
        (
            &["--transactional-id-pattern", "pay.*"],
            0,
            "krabka transactions: Failed to list transactions: Transactional ID pattern filter \
             can be set only when using API version 2 or higher. If client is connected to an \
             older broker, do not specify the pattern filter. Enable debug logging for \
             additional detail.",
        ),
    ];
    for (extra, list_error, expected) in cases {
        let node = cluster(Script {
            list_error,
            ..Script::default()
        })
        .await;
        let run = transactions(&node, &[&["list"][..], extra].concat()).await;
        check!(run.code == Some(1), "{extra:?}");
        check!(run.stdout.is_empty(), "{extra:?}");
        check!(last_message(&run.stderr) == Some(expected), "{extra:?}");
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_describe_producers_prints_kafkas_table() {
    let script = Script {
        topics: vec![("t", 1)],
        producers: vec![(
            ("t", 0),
            Ok(vec![
                producer_state(12, 40, 1_700_000_000_000),
                ProducerState {
                    coordinator_epoch: -1,
                    ..producer_state(13, -1, 5)
                },
            ]),
        )],
        ..Script::default()
    };
    // With --broker-id the request goes to broker 1 itself, without a
    // leader lookup.
    let cases: [(&[&str], usize); 2] = [(&[], 1), (&["--broker-id", "1"], 0)];
    for (extra, lookups) in cases {
        let node = cluster(script.clone()).await;
        let run = transactions(
            &node,
            &[
                &["describe-producers", "--topic", "t", "--partition", "0"][..],
                extra,
            ]
            .concat(),
        )
        .await;
        check!(run.code == Some(0), "{extra:?}: {}", run.stderr);
        check!(
            run.stdout
                == "ProducerId\tProducerEpoch\tLatestCoordinatorEpoch\tLastSequence\t\
                    LastTimestamp\tCurrentTransactionStartOffset\t\n\
                    12        \t4            \t3                     \t11          \t\
                    1700000000000\t40                           \t\n\
                    13        \t4            \t-1                    \t11          \t\
                    5            \tNone                         \t\n",
            "{extra:?}"
        );
        check!(
            node.requests(metadata_request::API_KEY).len() == lookups,
            "{extra:?}"
        );
        check!(
            node.requests(describe_producers_request::API_KEY)
                .iter()
                .map(|request| request
                    .decode::<DescribeProducersRequest>(describe_producers_request::FLEXIBLE_MIN))
                .collect::<Vec<_>>()
                == [DescribeProducersRequest {
                    topics: vec![DescribeProducersTopicRequest {
                        name: "t".into(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            "{extra:?}"
        );
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_describe_producers_failures_name_the_partition_and_the_broker() {
    let cases: [(&[&str], i16, &str); 2] = [
        (
            &[],
            29,
            "krabka transactions: Failed to describe producers for partition t-0 on leader: \
             DescribeProducers failed: TOPIC_AUTHORIZATION_FAILED (29): Topic authorization \
             failed. Enable debug logging for additional detail.",
        ),
        (
            &["--broker-id", "1"],
            6,
            "krabka transactions: Failed to describe producers for partition t-0 on broker 1: \
             DescribeProducers failed: NOT_LEADER_OR_FOLLOWER (6): Failed to describe active \
             producers for partition t-0 on brokerId 1. Enable debug logging for additional \
             detail.",
        ),
    ];
    for (extra, code, expected) in cases {
        let node = cluster(Script {
            topics: vec![("t", 1)],
            producers: vec![(("t", 0), Err(code))],
            ..Script::default()
        })
        .await;
        let run = transactions(
            &node,
            &[
                &["describe-producers", "--topic", "t", "--partition", "0"][..],
                extra,
            ]
            .concat(),
        )
        .await;
        check!(run.code == Some(1), "{extra:?}");
        check!(last_message(&run.stderr) == Some(expected), "{extra:?}");
        node.stop();
    }
}

fn abort_marker(
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
) -> WriteTxnMarkersRequest {
    WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            producer_id,
            producer_epoch,
            transaction_result: false,
            coordinator_epoch,
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_abort_writes_the_abort_marker() {
    let producers = vec![(
        ("t", 0),
        Ok(vec![
            producer_state(8, 90, 0),
            ProducerState {
                coordinator_epoch: -1,
                ..producer_state(9, 100, 0)
            },
        ]),
    )];
    let cases: [(&[&str], WriteTxnMarkersRequest); 2] = [
        // The producer whose transaction starts at 100; its coordinator epoch
        // -1 becomes 0.
        (&["--start-offset", "100"], abort_marker(9, 4, 0)),
        (
            &[
                "--producer-id",
                "5",
                "--producer-epoch",
                "2",
                "--coordinator-epoch",
                "7",
            ],
            abort_marker(5, 2, 7),
        ),
    ];
    for (extra, expected) in cases {
        let node = cluster(Script {
            topics: vec![("t", 1)],
            producers: producers.clone(),
            ..Script::default()
        })
        .await;
        let run = transactions(
            &node,
            &[
                &["abort", "--topic", "t", "--partition", "0", "--yes"][..],
                extra,
            ]
            .concat(),
        )
        .await;
        check!(run.code == Some(0), "{extra:?}: {}", run.stderr);
        check!(run.stdout.is_empty(), "{extra:?}");
        check!(
            node.requests(write_txn_markers_request::API_KEY)
                .iter()
                .map(|request| request
                    .decode::<WriteTxnMarkersRequest>(write_txn_markers_request::FLEXIBLE_MIN))
                .collect::<Vec<_>>()
                == [expected],
            "{extra:?}"
        );
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_abort_asks_first_and_fails_as_kafka_does() {
    struct Case {
        extra: Vec<&'static str>,
        marker_error: i16,
        code: Option<i32>,
        markers: usize,
        message: Option<&'static str>,
    }
    let producer_flags = [
        "--producer-id",
        "5",
        "--producer-epoch",
        "2",
        "--coordinator-epoch",
        "7",
    ];
    let with = |extra: &[&'static str]| [&producer_flags[..], extra].concat();
    let cases = [
        // No --yes on a non-interactive stdin: refused before any marker.
        Case {
            extra: with(&[]),
            marker_error: 0,
            code: Some(2),
            markers: 0,
            message: None,
        },
        Case {
            extra: with(&["--dry-run"]),
            marker_error: 0,
            code: Some(0),
            markers: 0,
            message: None,
        },
        Case {
            extra: with(&["--yes"]),
            marker_error: 48,
            code: Some(1),
            markers: 1,
            message: Some(
                "krabka transactions: Failed to abort transaction \
                 AbortTransactionSpec(topicPartition=t-0, producerId=5, producerEpoch=2, \
                 coordinatorEpoch=7): WriteTxnMarkers failed: INVALID_TXN_STATE (48): The \
                 producer attempted a transactional operation in an invalid state. Enable debug \
                 logging for additional detail.",
            ),
        },
        Case {
            extra: vec!["--start-offset", "55", "--yes"],
            marker_error: 0,
            code: Some(1),
            markers: 0,
            message: Some(
                "krabka transactions: Could not find any open transactions starting at offset \
                 55 on partition t-0",
            ),
        },
        Case {
            extra: vec!["--producer-id", "5"],
            marker_error: 0,
            code: Some(1),
            markers: 0,
            message: Some("krabka transactions: Missing required argument --producer-epoch"),
        },
    ];
    for case in cases {
        let node = cluster(Script {
            topics: vec![("t", 1)],
            producers: vec![(("t", 0), Ok(vec![producer_state(8, 90, 0)]))],
            marker_error: case.marker_error,
            ..Script::default()
        })
        .await;
        let extra = &case.extra;
        let run = transactions(
            &node,
            &[&["abort", "--topic", "t", "--partition", "0"][..], extra].concat(),
        )
        .await;
        check!(run.code == case.code, "{extra:?}: {}", run.stderr);
        check!(
            node.requests(write_txn_markers_request::API_KEY).len() == case.markers,
            "{extra:?}"
        );
        if let Some(message) = case.message {
            check!(last_message(&run.stderr) == Some(message), "{extra:?}");
        }
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_find_hanging_prints_the_hanging_transactions() {
    let old = epoch_ms() - 30 * 60_000 - 30_000;
    let script = Script {
        topics: vec![("t", 2)],
        producers: vec![
            (
                ("t", 0),
                Ok(vec![
                    // No transactional id holds producer 1: hanging.
                    producer_state(1, 10, old),
                    // Producer 2's transaction includes t-0: not hanging.
                    producer_state(2, 20, old),
                    // A recent write: not a candidate.
                    producer_state(3, 30, epoch_ms()),
                ]),
            ),
            (
                ("t", 1),
                Ok(vec![
                    // Producer 2's transaction does not include t-1: hanging.
                    producer_state(2, 21, old),
                    // No open transaction.
                    producer_state(4, -1, old),
                ]),
            ),
        ],
        listings: vec![("pay", 2, "Ongoing")],
        descriptions: vec![TransactionState {
            topics: vec![TopicData {
                topic: "t".into(),
                partitions: vec![0],
                ..Default::default()
            }],
            ..transaction("pay", 0)
        }],
        ..Script::default()
    };
    for extra in [&["--topic", "t"][..], &["--broker-id", "1"][..]] {
        let node = cluster(script.clone()).await;
        let run = transactions(&node, &[&["find-hanging"][..], extra].concat()).await;
        check!(run.code == Some(0), "{extra:?}: {}", run.stderr);
        let lines = run.stdout.lines().collect::<Vec<_>>();
        check!(
            lines
                == [
                    "Topic\tPartition\tProducerId\tProducerEpoch\tCoordinatorEpoch\tStartOffset\t\
                     LastTimestamp\tDuration(min)\t"
                        .to_owned(),
                    // By producer id, in `HashMap<Long, ...>` order.
                    format!(
                        "t    \t0        \t1         \t4            \t3               \t10         \
                         \t{old}\t30           \t"
                    ),
                    format!(
                        "t    \t1        \t2         \t4            \t3               \t21         \
                         \t{old}\t30           \t"
                    ),
                ],
            "{extra:?}"
        );
        check!(
            node.requests(list_transactions_request::API_KEY)
                .iter()
                .map(|request| request
                    .decode::<ListTransactionsRequest>(list_transactions_request::FLEXIBLE_MIN)
                    .producer_id_filters)
                .collect::<Vec<_>>()
                == [vec![1, 2]],
            "{extra:?}"
        );
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_find_hanging_with_no_candidate_prints_the_header() {
    let node = cluster(Script {
        topics: vec![("t", 1)],
        producers: vec![(("t", 0), Ok(vec![producer_state(1, -1, 0)]))],
        ..Script::default()
    })
    .await;
    let run = transactions(&node, &["find-hanging", "--topic", "t", "--partition", "0"]).await;
    check!(run.code == Some(0), "{}", run.stderr);
    check!(
        run.stdout
            == "Topic\tPartition\tProducerId\tProducerEpoch\tCoordinatorEpoch\tStartOffset\t\
                LastTimestamp\tDuration(min)\t\n"
    );
    check!(node.requests(list_transactions_request::API_KEY).is_empty());
    node.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_find_hanging_checks_its_scope_before_connecting() {
    let cases: [(&[&str], &str); 2] = [
        (
            &[],
            "krabka transactions: The `find-hanging` command requires either --topic or \
             --broker-id to limit the scope of the search",
        ),
        (
            &["--partition", "0", "--broker-id", "1"],
            "krabka transactions: The --partition argument requires --topic to be provided",
        ),
    ];
    for (extra, expected) in cases {
        let run = krabka(
            [
                args(&[
                    "transactions",
                    "--bootstrap-server",
                    "unreachable.invalid:1",
                    "find-hanging",
                ]),
                args(extra),
            ]
            .concat(),
            Vec::new(),
        )
        .await;
        check!(run.code == Some(1), "{extra:?}");
        check!(last_message(&run.stderr) == Some(expected), "{extra:?}");
    }
}

// ---------------------------------------------------------------------------
// The delegation-token requests that the options send.

async fn token_node() -> Node {
    Node::start(
        vec![
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
        Arc::new(|_, request: &Request| match request.api_key {
            create_delegation_token_request::API_KEY => respond(
                &CreateDelegationTokenResponse {
                    principal_type: "Group".into(),
                    principal_name: "ops".into(),
                    token_requester_principal_type: "User".into(),
                    token_requester_principal_name: "admin".into(),
                    issue_timestamp_ms: 1_700_000_000_000,
                    expiry_timestamp_ms: 1_700_086_400_000,
                    max_timestamp_ms: 1_700_604_800_000,
                    token_id: "tok-2".into(),
                    hmac: b"secret-hmac-value".to_vec().into(),
                    ..Default::default()
                },
                CREATE_TOKEN_VERSION,
                create_delegation_token_request::FLEXIBLE_MIN,
            ),
            renew_delegation_token_request::API_KEY => respond(
                &RenewDelegationTokenResponse {
                    expiry_timestamp_ms: 1_700_172_800_000,
                    ..Default::default()
                },
                TOKEN_VERSION,
                renew_delegation_token_request::FLEXIBLE_MIN,
            ),
            expire_delegation_token_request::API_KEY => respond(
                &ExpireDelegationTokenResponse {
                    expiry_timestamp_ms: 1_700_003_600_000,
                    ..Default::default()
                },
                TOKEN_VERSION,
                expire_delegation_token_request::FLEXIBLE_MIN,
            ),
            describe_delegation_token_request::API_KEY => respond(
                &DescribeDelegationTokenResponse::default(),
                DESCRIBE_TOKEN_VERSION,
                describe_delegation_token_request::FLEXIBLE_MIN,
            ),
            _ => Reply::Silent,
        }),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_renew_and_expire_send_the_period_and_print_the_new_expiry() {
    let cases = [
        (
            vec!["--renew", "--renew-time-period", "3600000"],
            format!(
                "Calling renew token operation with hmac :{HMAC_BASE64} , renew-time-period \
                 :3600000\nCompleted renew operation. New expiry date : 2023-11-16T22:13"
            ),
            Some(RenewDelegationTokenRequest {
                hmac: b"secret-hmac-value".to_vec().into(),
                renew_period_ms: 3_600_000,
                ..Default::default()
            }),
            None,
        ),
        (
            vec!["--expire", "--expiry-time-period", "3600000", "--yes"],
            format!(
                "Calling expire token operation with hmac :{HMAC_BASE64} , expire-time-period \
                 :3600000\nCompleted expire operation. New expiry date : 2023-11-14T23:13"
            ),
            None,
            Some(ExpireDelegationTokenRequest {
                hmac: b"secret-hmac-value".to_vec().into(),
                expiry_time_period_ms: 3_600_000,
                ..Default::default()
            }),
        ),
    ];
    for (rest, stdout, renew, expire) in cases {
        let node = token_node().await;
        let rest = [&rest[..], &["--hmac", HMAC_BASE64]].concat();
        let run = krabka(token_args(&node.address(), &rest), Vec::new()).await;
        check!(run.code == Some(0), "{rest:?}: {}", run.stderr);
        check!(run.stdout == stdout, "{rest:?}");
        check!(
            node.requests(renew_delegation_token_request::API_KEY)
                .iter()
                .map(|request| request.decode::<RenewDelegationTokenRequest>(
                    renew_delegation_token_request::FLEXIBLE_MIN
                ))
                .collect::<Vec<_>>()
                == renew.into_iter().collect::<Vec<_>>(),
            "{rest:?}"
        );
        check!(
            node.requests(expire_delegation_token_request::API_KEY)
                .iter()
                .map(|request| request.decode::<ExpireDelegationTokenRequest>(
                    expire_delegation_token_request::FLEXIBLE_MIN
                ))
                .collect::<Vec<_>>()
                == expire.into_iter().collect::<Vec<_>>(),
            "{rest:?}"
        );
        check!(!run.stderr.contains(HMAC_BASE64));
        node.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_create_for_a_non_user_owner_sends_the_owner() {
    let node = token_node().await;
    let run = krabka(
        token_args(
            &node.address(),
            &[
                "--create",
                "--max-life-time-period",
                "86400000",
                "--owner-principal",
                "Group:ops",
            ],
        ),
        Vec::new(),
    )
    .await;
    check!(run.code == Some(0), "{}", run.stderr);
    check!(
        run.stdout
            == "Calling create token operation with renewers :[] , max-life-time-period \
                :86400000\n\
                Created delegation token with tokenId : tok-2\n\
                \n\
                TOKENID         HMAC                           OWNER           REQUESTER       \
                RENEWERS                  ISSUEDATE       EXPIRYDATE      MAXDATE        \n\
                \n\
                tok-2           c2VjcmV0LWhtYWMtdmFsdWU=       Group:ops       User:admin      \
                []                        2023-11-14T22:13 2023-11-15T22:13 2023-11-21T22:13\n"
    );
    check!(
        node.requests(create_delegation_token_request::API_KEY)
            .iter()
            .map(|request| request.decode::<CreateDelegationTokenRequest>(
                create_delegation_token_request::FLEXIBLE_MIN
            ))
            .collect::<Vec<_>>()
            == [CreateDelegationTokenRequest {
                owner_principal_type: Some("Group".into()),
                owner_principal_name: Some("ops".into()),
                renewers: Vec::new(),
                max_lifetime_ms: 86_400_000,
                ..Default::default()
            }]
    );
    node.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delegation_tokens_describe_without_an_owner_describes_the_current_user() {
    let node = token_node().await;
    let run = krabka(token_args(&node.address(), &["--describe"]), Vec::new()).await;
    check!(run.code == Some(0), "{}", run.stderr);
    // Kafka prints the token count with `printf` and no `%n`, so the header's
    // leading `%n` ends that line.
    check!(
        run.stdout
            == "Calling describe token operation for current user.\n\
                Total number of tokens : 0\n\
                TOKENID         HMAC                           OWNER           REQUESTER       \
                RENEWERS                  ISSUEDATE       EXPIRYDATE      MAXDATE        \n"
    );
    // Kafka's command passes its empty owner list, not null.
    check!(
        node.requests(describe_delegation_token_request::API_KEY)
            .iter()
            .map(|request| request.decode::<DescribeDelegationTokenRequest>(
                describe_delegation_token_request::FLEXIBLE_MIN
            ))
            .collect::<Vec<_>>()
            == [DescribeDelegationTokenRequest {
                owners: Some(Vec::new()),
                ..Default::default()
            }]
    );
    node.stop();
}
