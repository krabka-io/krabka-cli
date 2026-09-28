//! `krabka metadata-quorum`, `krabka cluster`, `krabka leader-election` and
//! `krabka reassign-partitions`, run as the binary against a scripted
//! broker.

mod support;

use std::{collections::BTreeMap, path::Path, process::Stdio};

use assert2::{assert, check};
use krabka_protocol::{
    owned::{
        alter_partition_reassignments_request,
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
        common::describe_quorum_response::replica_state::ReplicaState,
        describe_quorum_request,
        describe_quorum_response::{DescribeQuorumResponse, PartitionData, TopicData},
        list_partition_reassignments_request,
        list_partition_reassignments_response::{
            ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
            OngoingTopicReassignment,
        },
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
        remove_raft_voter_request, unregister_broker_request,
        unregister_broker_response::UnregisterBrokerResponse,
    },
    primitives::uuid::Uuid,
};
use serde_json::{Value, json};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;
const LIST_VERSION: i16 = 0;
const ALTER_VERSION: i16 = 0;
const QUORUM_VERSION: i16 = 2;
const UNREGISTER_VERSION: i16 = 0;

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: &[&str]) -> Run {
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .env("RUST_LOG", "off")
            .env_remove("KRABKA_ASSUME_YES")
            .env_remove("KRABKA_OUTPUT")
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

fn count(received: &[Received], api_key: i16) -> usize {
    received
        .iter()
        .filter(|request| request.api_key == api_key)
        .count()
}

fn replica(id: i32, last: u8, log_end_offset: i64) -> ReplicaState {
    let mut directory = [0; 16];
    directory[15] = last;
    ReplicaState {
        replica_id: id,
        replica_directory_id: Uuid(directory),
        log_end_offset,
        last_fetch_timestamp: 1_790_000_001_000,
        last_caught_up_timestamp: 1_790_000_001_000,
        ..Default::default()
    }
}

async fn quorum_broker() -> MockBroker {
    let quorum = respond(
        &DescribeQuorumResponse {
            topics: vec![TopicData {
                topic_name: "__cluster_metadata".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 3,
                    high_watermark: 42,
                    current_voters: vec![replica(1, 1, 45), replica(2, 2, 40)],
                    observers: vec![replica(3, 0, 44)],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        QUORUM_VERSION,
        describe_quorum_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[(describe_quorum_request::API_KEY, 0, QUORUM_VERSION)],
        BTreeMap::from([((describe_quorum_request::API_KEY, QUORUM_VERSION), quorum)]),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_replication_prints_kafkas_table() {
    let broker = quorum_broker().await;
    let address = broker.address();
    let run = krabka(&[
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "describe",
        "--replication",
    ])
    .await;
    check!(run.code == Some(0));
    check!(
        run.stdout
            == "NodeId\tDirectoryId           \tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus  \t\n\
                1     \tAAAAAAAAAAAAAAAAAAAAAQ\t45          \t0  \t1790000001000     \t1790000001000        \tLeader  \t\n\
                2     \tAAAAAAAAAAAAAAAAAAAAAg\t40          \t5  \t1790000001000     \t1790000001000        \tFollower\t\n\
                3     \tAAAAAAAAAAAAAAAAAAAAAA\t44          \t1  \t1790000001000     \t1790000001000        \tObserver\t\n"
    );
    let json = krabka(&[
        "--output",
        "json",
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "describe",
        "--replication",
    ])
    .await;
    let payload: Value = serde_json::from_str(&json.stdout).unwrap();
    check!(
        payload["data"][1]
            == json!({
                "node_id": "2",
                "directory_id": "AAAAAAAAAAAAAAAAAAAAAg",
                "log_end_offset": "40",
                "lag": "5",
                "last_fetch_timestamp": "1790000001000",
                "last_caught_up_timestamp": "1790000001000",
                "status": "Follower",
            })
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quorum_request_the_broker_drops_fails_with_exit_1_and_no_report() {
    let broker = MockBroker::start(
        &[(describe_quorum_request::API_KEY, 0, QUORUM_VERSION)],
        BTreeMap::from([(
            (describe_quorum_request::API_KEY, QUORUM_VERSION),
            Reply::Silent,
        )]),
    )
    .await;
    let address = broker.address();
    let run = krabka(&[
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "--request-timeout-ms",
        "200",
        "describe",
        "--replication",
    ])
    .await;
    check!((run.code, run.stdout.as_str()) == (Some(1), ""));
    check!(run.stderr.starts_with("krabka metadata-quorum: "));
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_flag_misuse_fails_with_kafkas_message_before_connecting() {
    let run = krabka(&[
        "metadata-quorum",
        "--bootstrap-server",
        "127.0.0.1:1",
        "describe",
        "--status",
        "--replication",
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(1),
                "",
                "krabka metadata-quorum: Only one of --status or --replication should be \
                 specified with describe sub-command\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_controller_refuses_without_confirmation_and_dry_runs_without_a_request() {
    let broker = quorum_broker().await;
    let address = broker.address();
    let base = [
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "remove-controller",
        "-i",
        "2",
        "-d",
        "AAAAAAAAAAAAAAAAAAAAAg",
    ];
    let refused = krabka(&base).await;
    check!(refused.code == Some(2));
    check!(refused.stderr.contains("pass --yes to proceed"));
    let dry = krabka(&[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\nDRY RUN of removing  KRaft controller 2 with \
                 directory id AAAAAAAAAAAAAAAAAAAAAg\n"
            )
    );
    let yes = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!(yes.code == Some(1));
    check!(yes.stderr.contains("is not supported by this build"));
    check!(count(&broker.received(), remove_raft_voter_request::API_KEY) == 0);
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn add_controller_dry_run_reads_the_identity_that_krabka_format_writes() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("meta");
    std::fs::create_dir(&log_dir).unwrap();
    let formatted = krabka(&[
        "format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--directory-id",
        "00000000-0000-0000-0000-000000000007",
    ])
    .await;
    assert!(formatted.code == Some(0), "{}", formatted.stderr);
    let config = dir.path().join("controller.properties");
    std::fs::write(
        &config,
        format!(
            "node.id=7\nprocess.roles=controller\nlog.dirs={}\ncontroller.listener.names=CONTROLLER\n\
             listeners=CONTROLLER://controller-7:9093\n",
            log_dir.display()
        ),
    )
    .unwrap();
    let run = krabka(&[
        "metadata-quorum",
        "--bootstrap-controller",
        "127.0.0.1:1",
        "--command-config",
        config.to_str().unwrap(),
        "add-controller",
        "--dry-run",
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\nDRY RUN of adding controller 7 with directory id \
                 AAAAAAAAAAAAAAAAAAAABw and endpoints: CONTROLLER://controller-7:9093\n"
            )
    );
    let missing = krabka(&[
        "metadata-quorum",
        "--bootstrap-controller",
        "127.0.0.1:1",
        "add-controller",
    ])
    .await;
    check!(
        (missing.code, missing.stderr.as_str())
            == (
                Some(1),
                "krabka metadata-quorum: You must supply the configuration file of the \
                 controller you are adding when using add-controller.\n"
            )
    );
}

async fn unregister_broker(error_code: i16) -> MockBroker {
    let reply = respond(
        &UnregisterBrokerResponse {
            error_code,
            error_message: (error_code != 0).then(|| "Cluster authorization failed.".into()),
            ..Default::default()
        },
        UNREGISTER_VERSION,
        unregister_broker_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[(unregister_broker_request::API_KEY, 0, UNREGISTER_VERSION)],
        BTreeMap::from([(
            (unregister_broker_request::API_KEY, UNREGISTER_VERSION),
            reply,
        )]),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_unregister_confirms_dry_runs_and_reports_as_kafka_cluster_does() {
    let broker = unregister_broker(0).await;
    let address = broker.address();
    let base = ["cluster", "unregister", "-b", &address, "--id", "5"];
    let refused = krabka(&base).await;
    check!(refused.code == Some(2));
    let dry = krabka(&[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\nBroker 5 is no longer registered.\n"
            )
    );
    check!(count(&broker.received(), unregister_broker_request::API_KEY) == 0);
    let done = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!((done.code, done.stdout.as_str()) == (Some(0), "Broker 5 is no longer registered.\n"));
    check!(count(&broker.received(), unregister_broker_request::API_KEY) == 1);
    broker.stop();

    let broker = unregister_broker(31).await;
    let address = broker.address();
    let failed = krabka(&["cluster", "unregister", "-b", &address, "-i", "5", "--yes"]).await;
    check!(
        (failed.code, failed.stderr.as_str())
            == (
                Some(1),
                "krabka cluster: UnregisterBroker failed: CLUSTER_AUTHORIZATION_FAILED (31): \
                 Cluster authorization failed.\n"
            )
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_id_is_deferred_and_fenced_listing_needs_a_broker_bootstrap() {
    let broker = unregister_broker(0).await;
    let address = broker.address();
    let run = krabka(&["cluster", "cluster-id", "--bootstrap-server", &address]).await;
    check!(
        (run.code, run.stderr.as_str())
            == (
                Some(1),
                "krabka cluster: krabka cluster cluster-id is not supported by this build: it \
                 needs AdminClient::describe_cluster, which the pinned krabka-client-admin does \
                 not provide\n"
            )
    );
    broker.stop();
    let fenced = krabka(&[
        "cluster",
        "list-endpoints",
        "-C",
        "127.0.0.1:1",
        "--include-fenced-brokers",
    ])
    .await;
    check!(
        (fenced.code, fenced.stderr.as_str())
            == (
                Some(1),
                "krabka cluster: The option --include-fenced-brokers is only supported with \
                 --bootstrap-server option\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_election_validates_the_file_before_the_deferred_call() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("election.json");
    std::fs::write(
        &file,
        r#"{"partitions": [{"topic": "foo", "partition": 1}, {"topic": "foo", "partition": 1}]}"#,
    )
    .unwrap();
    let duplicate = krabka(&[
        "leader-election",
        "--bootstrap-server",
        "127.0.0.1:1",
        "--election-type",
        "preferred",
        "--path-to-json-file",
        file.to_str().unwrap(),
    ])
    .await;
    check!(
        (duplicate.code, duplicate.stderr.as_str())
            == (
                Some(1),
                "krabka leader-election: Replica election data contains duplicate partitions: \
                 [foo-1]\n"
            )
    );
    let both = krabka(&[
        "leader-election",
        "--bootstrap-server",
        "127.0.0.1:1",
        "--election-type",
        "preferred",
        "--all-topic-partitions",
        "--topic",
        "foo",
    ])
    .await;
    check!(both.code == Some(1));
    check!(
        both.stderr
            .contains("One and only one of the following options is required")
    );

    let broker = quorum_broker().await;
    let address = broker.address();
    let deferred = krabka(&[
        "--output",
        "json",
        "leader-election",
        "--bootstrap-server",
        &address,
        "--election-type",
        "unclean",
        "--all-topic-partitions",
    ])
    .await;
    check!(deferred.code == Some(1));
    let envelope: Value = serde_json::from_str(&deferred.stderr).unwrap();
    check!(
        envelope
            == json!({"error": {
                "code": 1,
                "message": "leader-election is not supported by this build: it needs \
                            AdminClient::elect_leaders, which the pinned krabka-client-admin \
                            does not provide",
            }})
    );
    broker.stop();
}

/// A broker with topic `foo` (partitions 0 and 1 on brokers 1,2) and one
/// active reassignment of `foo-1`, which answers `AlterPartitionReassignments`
/// for both partitions.
async fn reassignment_broker(active: bool) -> MockBroker {
    let metadata = respond(
        &MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("foo".into()),
                partitions: (0..2)
                    .map(|partition_index| MetadataResponsePartition {
                        partition_index,
                        replica_nodes: vec![1, 2],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    );
    let list = respond(
        &ListPartitionReassignmentsResponse {
            topics: if active {
                vec![OngoingTopicReassignment {
                    name: "foo".into(),
                    partitions: vec![OngoingPartitionReassignment {
                        partition_index: 1,
                        replicas: vec![1, 2, 3],
                        adding_replicas: vec![3],
                        removing_replicas: vec![1],
                        ..Default::default()
                    }],
                    ..Default::default()
                }]
            } else {
                Vec::new()
            },
            ..Default::default()
        },
        LIST_VERSION,
        list_partition_reassignments_request::FLEXIBLE_MIN,
    );
    let alter = respond(
        &AlterPartitionReassignmentsResponse {
            responses: vec![ReassignableTopicResponse {
                name: "foo".into(),
                partitions: vec![
                    ReassignablePartitionResponse {
                        partition_index: 0,
                        ..Default::default()
                    },
                    ReassignablePartitionResponse {
                        partition_index: 1,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        },
        ALTER_VERSION,
        alter_partition_reassignments_request::FLEXIBLE_MIN,
    );
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (
                list_partition_reassignments_request::API_KEY,
                0,
                LIST_VERSION,
            ),
            (
                alter_partition_reassignments_request::API_KEY,
                0,
                ALTER_VERSION,
            ),
        ],
        BTreeMap::from([
            ((metadata_request::API_KEY, METADATA_VERSION), metadata),
            (
                (list_partition_reassignments_request::API_KEY, LIST_VERSION),
                list,
            ),
            (
                (
                    alter_partition_reassignments_request::API_KEY,
                    ALTER_VERSION,
                ),
                alter,
            ),
        ]),
    )
    .await
}

fn write(dir: &Path, name: &str, text: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path.to_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn list_prints_active_reassignments_with_adding_and_removing_replicas() {
    let broker = reassignment_broker(true).await;
    let address = broker.address();
    let run = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--list",
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "Current partition reassignments:\nfoo-1: replicas: 1,2,3. adding: 3. removing: \
                 1.\n"
            )
    );
    broker.stop();
    let idle = reassignment_broker(false).await;
    let address = idle.address();
    let run = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--list",
    ])
    .await;
    check!(run.stdout == "No partition reassignments found.\n");
    idle.stop();
}

const PLAN: &str = r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[2,1],"log_dirs":["any","any"]}]}"#;

#[tokio::test(flavor = "multi_thread")]
async fn execute_prints_the_rollback_file_confirms_and_dry_runs() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(dir.path(), "plan.json", PLAN);
    let broker = reassignment_broker(false).await;
    let address = broker.address();
    let base = [
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--execute",
        "--reassignment-json-file",
        &plan,
    ];
    let rollback = "Current partition replica assignment\n\n\
        {\"version\":1,\"partitions\":[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2],\"log_dirs\":[\"any\",\"any\"]}]}\n\n\
        Save this to use as the --reassignment-json-file option during rollback\n";

    let refused = krabka(&base).await;
    check!(refused.code == Some(2));
    let dry = krabka(&[&base[..], &["--dry-run", "--throttle", "1000"]].concat()).await;
    check!(
        (dry.code, dry.stdout)
            == (
                Some(0),
                format!(
                    "DRY RUN: no change was made.\n{rollback}Warning: You must run --verify \
                     periodically, until the reassignment completes, to ensure the throttle is \
                     removed.\nThe inter-broker throttle limit was set to 1000 B/s\nSuccessfully \
                     started partition reassignment for foo-0\n"
                )
            )
    );
    check!(
        count(
            &broker.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 0
    );
    let throttled = krabka(&[&base[..], &["--yes", "--throttle", "1000"]].concat()).await;
    check!(throttled.code == Some(1));
    check!(
        throttled
            .stderr
            .contains("--throttle is not supported by this build")
    );
    check!(
        count(
            &broker.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 0
    );

    let done = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!(
        (done.code, done.stdout)
            == (
                Some(0),
                format!("{rollback}Successfully started partition reassignment for foo-0\n")
            )
    );
    check!(
        count(
            &broker.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 1
    );
    broker.stop();

    let busy = reassignment_broker(true).await;
    let address = busy.address();
    let blocked = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--execute",
        "--reassignment-json-file",
        &plan,
        "--yes",
    ])
    .await;
    check!(blocked.code == Some(1));
    check!(
        blocked
            .stderr
            .contains("Cannot execute because there is an existing partition assignment.")
    );
    check!(
        count(
            &busy.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 0
    );
    busy.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_progress_and_exits_1_while_a_partition_moves() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]},{"topic":"foo","partition":1,"replicas":[2,3]}]}"#,
    );
    let broker = reassignment_broker(true).await;
    let address = broker.address();
    let run = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--verify",
        "--reassignment-json-file",
        &plan,
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(1),
                "Status of partition reassignment:\nReassignment of partition foo-0 is \
                 completed.\nReassignment of partition foo-1 is still in progress.\n\n"
            )
    );
    broker.stop();

    let done = write(
        dir.path(),
        "done.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"#,
    );
    let idle = reassignment_broker(false).await;
    let address = idle.address();
    let kept = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--verify",
        "--reassignment-json-file",
        &done,
        "--preserve-throttles",
    ])
    .await;
    check!(
        (kept.code, kept.stdout.as_str())
            == (
                Some(0),
                "Status of partition reassignment:\nReassignment of partition foo-0 is \
                 completed.\n\n"
            )
    );
    let cleared = krabka(&[
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--verify",
        "--reassignment-json-file",
        &done,
    ])
    .await;
    check!(cleared.code == Some(1));
    check!(
        cleared
            .stdout
            .contains("clearing the reassignment throttles is not supported by this build")
    );
    idle.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_confirms_dry_runs_and_cancels_only_active_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]},{"topic":"foo","partition":1,"replicas":[2,3]}]}"#,
    );
    let broker = reassignment_broker(true).await;
    let address = broker.address();
    let base = [
        "reassign-partitions",
        "--bootstrap-server",
        &address,
        "--cancel",
        "--reassignment-json-file",
        &plan,
        "--preserve-throttles",
    ];
    let refused = krabka(&base).await;
    check!(refused.code == Some(2));
    let dry = krabka(&[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\nSuccessfully cancelled partition reassignment for: \
                 foo-1\nNone of the specified partition moves are active.\n"
            )
    );
    check!(
        count(
            &broker.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 0
    );
    let done = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!(
        (done.code, done.stdout.as_str())
            == (
                Some(0),
                "Successfully cancelled partition reassignment for: foo-1\nNone of the specified \
                 partition moves are active.\n"
            )
    );
    check!(
        count(
            &broker.received(),
            alter_partition_reassignments_request::API_KEY
        ) == 1
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn option_rules_fail_with_kafkas_messages_before_connecting() {
    let cases: [(&[&str], &str); 3] = [
        (
            &["--bootstrap-server", "127.0.0.1:1", "--verify"],
            "Missing required argument \"[reassignment-json-file]\"",
        ),
        (
            &["--bootstrap-server", "127.0.0.1:1", "--list", "--generate"],
            "Command must include exactly one action: --generate, --execute, --verify, --cancel, \
             --list",
        ),
        (
            &[
                "--bootstrap-controller",
                "127.0.0.1:1",
                "--verify",
                "--reassignment-json-file",
                "r.json",
            ],
            "Option \"[bootstrap-controller]\" can't be used with action \"[verify]\"",
        ),
    ];
    for (argv, message) in cases {
        let run = krabka(&[&["reassign-partitions"][..], argv].concat()).await;
        check!(
            (run.code, run.stderr) == (Some(1), format!("krabka reassign-partitions: {message}\n")),
            "{argv:?}"
        );
    }
}
