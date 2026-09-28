//! `krabka topics` against a scripted broker: the requests that each action
//! sends, and the whole stdout, stderr and exit code that it produces.

mod support;

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
};

use assert2::check;
use krabka_protocol::{
    owned::{
        create_partitions_request,
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
        create_topics_request,
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        delete_topics_request,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        describe_configs_request,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
        list_partition_reassignments_request,
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
    },
    primitives::uuid::Uuid,
};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;
const CREATE_VERSION: i16 = 7;
const DELETE_VERSION: i16 = 6;
const PARTITIONS_VERSION: i16 = 3;
const CONFIGS_VERSION: i16 = 4;
const REASSIGNMENTS_VERSION: i16 = 0;

// `4IgIMEgZQYS5IcnSzjHAqw`, the ID that Kafka printed for `orders` in the
// run that the conformance suite captured.
const ORDERS_ID: [u8; 16] = [
    0xe0, 0x88, 0x08, 0x30, 0x48, 0x19, 0x41, 0x84, 0xb9, 0x21, 0xc9, 0xd2, 0xce, 0x31, 0xc0, 0xab,
];

#[derive(Debug, PartialEq, Eq)]
struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: Vec<String>) -> Run {
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
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

fn topics(address: &str, args: &[&str]) -> Vec<String> {
    ["topics", "--bootstrap-server", address]
        .iter()
        .chain(args)
        .map(|arg| (*arg).to_owned())
        .collect()
}

fn run(code: i32, stdout: &str, stderr: &str) -> Run {
    Run {
        code: Some(code),
        stdout: stdout.to_owned(),
        stderr: stderr.to_owned(),
    }
}

fn topic(name: &str, id: [u8; 16], partitions: &[&[i32]]) -> MetadataResponseTopic {
    MetadataResponseTopic {
        name: Some(name.into()),
        topic_id: Uuid(id),
        is_internal: name.starts_with("__"),
        partitions: partitions
            .iter()
            .enumerate()
            .map(|(index, replicas)| MetadataResponsePartition {
                partition_index: i32::try_from(index).unwrap(),
                leader_id: replicas[0],
                replica_nodes: replicas.to_vec(),
                isr_nodes: replicas.to_vec(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

// The cluster that every test starts from: `orders` with three partitions,
// two topics without overrides, and the consumer-offsets topic.
fn cluster() -> Vec<MetadataResponseTopic> {
    vec![
        topic("orders", ORDERS_ID, &[&[1], &[1], &[1]]),
        topic("alpha", [1; 16], &[&[1]]),
        topic("bravo", [2; 16], &[&[1]]),
        topic("__consumer_offsets", [3; 16], &[&[1]]),
    ]
}

fn metadata(topics: Vec<MetadataResponseTopic>) -> Reply {
    respond(
        &MetadataResponse {
            topics,
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    )
}

fn created(results: &[(&str, i16, Option<&str>)]) -> Reply {
    respond(
        &CreateTopicsResponse {
            topics: results
                .iter()
                .map(|(name, error_code, message)| CreatableTopicResult {
                    name: (*name).into(),
                    error_code: *error_code,
                    error_message: message.map(str::to_owned),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        CREATE_VERSION,
        create_topics_request::FLEXIBLE_MIN,
    )
}

fn deleted(results: &[(&str, i16)]) -> Reply {
    respond(
        &DeleteTopicsResponse {
            responses: results
                .iter()
                .map(|(name, error_code)| DeletableTopicResult {
                    name: Some((*name).into()),
                    error_code: *error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        DELETE_VERSION,
        delete_topics_request::FLEXIBLE_MIN,
    )
}

fn partitions_created(results: &[(&str, i16, Option<&str>)]) -> Reply {
    respond(
        &CreatePartitionsResponse {
            results: results
                .iter()
                .map(|(name, error_code, message)| CreatePartitionsTopicResult {
                    name: (*name).into(),
                    error_code: *error_code,
                    error_message: message.map(str::to_owned),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        PARTITIONS_VERSION,
        create_partitions_request::FLEXIBLE_MIN,
    )
}

// `DescribeConfigs` for `orders` with one dynamic override among the
// defaults, and for every other topic with defaults only.
fn configs() -> Reply {
    let entry = |name: &str, value: &str, config_source| DescribeConfigsResourceResult {
        name: name.into(),
        value: Some(value.into()),
        config_source,
        ..Default::default()
    };
    let result = |name: &str, overridden: bool| DescribeConfigsResult {
        resource_type: 2,
        resource_name: name.into(),
        configs: [
            Some(entry("cleanup.policy", "delete", 5)),
            overridden.then(|| entry("retention.ms", "1000", 1)),
        ]
        .into_iter()
        .flatten()
        .collect(),
        ..Default::default()
    };
    respond(
        &DescribeConfigsResponse {
            results: vec![
                result("bravo", false),
                result("orders", true),
                result("alpha", false),
            ],
            ..Default::default()
        },
        CONFIGS_VERSION,
        describe_configs_request::FLEXIBLE_MIN,
    )
}

fn no_reassignments() -> Reply {
    respond(
        &ListPartitionReassignmentsResponse::default(),
        REASSIGNMENTS_VERSION,
        list_partition_reassignments_request::FLEXIBLE_MIN,
    )
}

async fn broker(replies: Vec<((i16, i16), Reply)>) -> MockBroker {
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (create_topics_request::API_KEY, 2, CREATE_VERSION),
            (delete_topics_request::API_KEY, 1, DELETE_VERSION),
            (create_partitions_request::API_KEY, 0, PARTITIONS_VERSION),
            (describe_configs_request::API_KEY, 1, CONFIGS_VERSION),
            (list_partition_reassignments_request::API_KEY, 0, 0),
        ],
        replies.into_iter().collect::<BTreeMap<_, _>>(),
    )
    .await
}

fn sent(broker: &MockBroker) -> Vec<i16> {
    broker
        .received()
        .iter()
        .map(|Received { api_key, .. }| *api_key)
        .filter(|api_key| *api_key != 18)
        .collect()
}

const METADATA: (i16, i16) = (metadata_request::API_KEY, METADATA_VERSION);

struct Case {
    name: &'static str,
    args: &'static [&'static str],
    replies: Vec<((i16, i16), Reply)>,
    sent: Vec<i16>,
    run: Run,
}

const CREATE: (i16, i16) = (create_topics_request::API_KEY, CREATE_VERSION);
const DELETE: (i16, i16) = (delete_topics_request::API_KEY, DELETE_VERSION);
const ALTER: (i16, i16) = (create_partitions_request::API_KEY, PARTITIONS_VERSION);

fn list_and_create_cases() -> Vec<Case> {
    let create = CREATE;
    let collision = "WARNING: Due to limitations in metric names, topics with a period ('.') or \
                     underscore ('_') could collide. To avoid issues it is best to use either, \
                     but not both.\n";
    vec![
        Case {
            name: "list prints every topic, sorted",
            args: &["--list"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "__consumer_offsets\nalpha\nbravo\norders\n", ""),
        },
        Case {
            name: "list filters by pattern and internal topics",
            args: &["--list", "--exclude-internal", "--topic", ".*a.*"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "alpha\nbravo\n", ""),
        },
        Case {
            name: "a list that matches nothing prints one empty line",
            args: &["--list", "--topic", "missing"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "\n", ""),
        },
        Case {
            name: "create",
            args: &["--create", "--topic", "payments", "--partitions", "3"],
            replies: vec![(create, created(&[("payments", 0, None)]))],
            sent: vec![create_topics_request::API_KEY],
            run: run(0, "Created topic payments.\n", ""),
        },
        Case {
            name: "create warns about a colliding name",
            args: &["--create", "--topic", "my.topic_x"],
            replies: vec![(create, created(&[("my.topic_x", 0, None)]))],
            sent: vec![create_topics_request::API_KEY],
            run: run(0, &format!("{collision}Created topic my.topic_x.\n"), ""),
        },
        Case {
            name: "create of an existing topic fails on stdout",
            args: &["--create", "--topic", "orders"],
            replies: vec![(
                create,
                created(&[("orders", 36, Some("Topic 'orders' already exists."))]),
            )],
            sent: vec![create_topics_request::API_KEY],
            run: run(
                1,
                "Error while executing topic command : Topic 'orders' already exists.\n",
                "",
            ),
        },
        Case {
            name: "create --if-not-exists still sends the request and prints nothing",
            args: &["--create", "--topic", "orders", "--if-not-exists"],
            replies: vec![(
                create,
                created(&[("orders", 36, Some("Topic 'orders' already exists."))]),
            )],
            sent: vec![create_topics_request::API_KEY],
            run: run(0, "", ""),
        },
        Case {
            name: "a partial create prints every row and exits 1",
            args: &["--create", "--topic", "a", "--topic", "b", "--topic", "c"],
            replies: vec![(
                create,
                created(&[
                    ("c", 0, None),
                    ("b", 40, Some("Unknown topic config name: foo")),
                    ("a", 0, None),
                ]),
            )],
            sent: vec![create_topics_request::API_KEY],
            run: run(
                1,
                "Created topic a.\nError while executing topic command : Unknown topic config \
                 name: foo\nCreated topic c.\n",
                "",
            ),
        },
    ]
}

fn alter_delete_and_describe_cases() -> Vec<Case> {
    let delete = DELETE;
    let alter = ALTER;
    vec![
        Case {
            name: "alter prints nothing on success",
            args: &["--alter", "--topic", "orders", "--partitions", "4"],
            replies: vec![
                (METADATA, metadata(cluster())),
                (alter, partitions_created(&[("orders", 0, None)])),
            ],
            sent: vec![
                metadata_request::API_KEY,
                create_partitions_request::API_KEY,
            ],
            run: run(0, "", ""),
        },
        Case {
            name: "alter to fewer partitions fails with the broker's message",
            args: &["--alter", "--topic", "orders", "--partitions", "2"],
            replies: vec![
                (METADATA, metadata(cluster())),
                (
                    alter,
                    partitions_created(&[(
                        "orders",
                        37,
                        Some(
                            "The topic orders currently has 3 partition(s); 2 would not be an increase.",
                        ),
                    )]),
                ),
            ],
            sent: vec![
                metadata_request::API_KEY,
                create_partitions_request::API_KEY,
            ],
            run: run(
                1,
                "Error while executing topic command : The topic orders currently has 3 \
                 partition(s); 2 would not be an increase.\n",
                "",
            ),
        },
        Case {
            name: "alter of a missing topic fails before any mutation",
            args: &["--alter", "--topic", "missing", "--partitions", "2"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(
                1,
                "",
                "krabka topics: Topic 'missing' does not exist as expected\n",
            ),
        },
        Case {
            name: "alter --if-exists of a missing topic does nothing",
            args: &[
                "--alter",
                "--topic",
                "missing",
                "--partitions",
                "2",
                "--if-exists",
            ],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "", ""),
        },
        Case {
            name: "delete resolves the pattern and prints nothing",
            args: &["--delete", "--topic", "alpha|bravo", "--yes"],
            replies: vec![
                (METADATA, metadata(cluster())),
                (delete, deleted(&[("alpha", 0), ("bravo", 0)])),
            ],
            sent: vec![metadata_request::API_KEY, delete_topics_request::API_KEY],
            run: run(0, "", ""),
        },
        Case {
            name: "delete of a missing topic fails before any mutation",
            args: &["--delete", "--topic", "missing", "--yes"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(
                1,
                "",
                "krabka topics: Topic 'missing' does not exist as expected\n",
            ),
        },
        Case {
            name: "delete --if-exists of a missing topic does nothing",
            args: &["--delete", "--topic", "missing", "--if-exists"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "", ""),
        },
        Case {
            name: "a delete dry run names the topics and sends no mutation",
            args: &["--delete", "--topic", "alpha|bravo", "--dry-run"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(0, "DRY RUN: no change was made.\nalpha\nbravo\n", ""),
        },
        Case {
            name: "describe --topics-with-overrides prints the summary of overridden topics",
            args: &["--describe", "--topics-with-overrides"],
            replies: vec![
                (METADATA, metadata(cluster())),
                (
                    (describe_configs_request::API_KEY, CONFIGS_VERSION),
                    configs(),
                ),
                (
                    (
                        list_partition_reassignments_request::API_KEY,
                        REASSIGNMENTS_VERSION,
                    ),
                    no_reassignments(),
                ),
            ],
            sent: vec![
                metadata_request::API_KEY,
                metadata_request::API_KEY,
                describe_configs_request::API_KEY,
                list_partition_reassignments_request::API_KEY,
            ],
            run: run(
                0,
                "Topic: orders\tTopicId: 4IgIMEgZQYS5IcnSzjHAqw\tPartitionCount: 3\t\
                 ReplicationFactor: 1\tConfigs: retention.ms=1000\n",
                "",
            ),
        },
        Case {
            name: "describe of a missing topic fails with Kafka's message",
            args: &["--describe", "--topic", "missing"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(
                1,
                "",
                "krabka topics: Topic 'missing' does not exist as expected\n",
            ),
        },
        Case {
            name: "describe of an unknown topic ID fails with Kafka's message",
            args: &["--describe", "--topic-id", "AQEBAQEBAQEBAQEBAQEBAQ"],
            replies: vec![(
                METADATA,
                metadata(vec![topic("orders", ORDERS_ID, &[&[1]])]),
            )],
            sent: vec![metadata_request::API_KEY],
            run: run(
                1,
                "",
                "krabka topics: TopicId 'AQEBAQEBAQEBAQEBAQEBAQ' does not exist as expected\n",
            ),
        },
        Case {
            name: "the per-partition describe needs describe_topics",
            args: &["--describe", "--topic", "orders"],
            replies: vec![(METADATA, metadata(cluster()))],
            sent: vec![metadata_request::API_KEY],
            run: run(
                1,
                "",
                "krabka topics: --describe with per-partition Leader, Isr and Elr lines is not \
                 supported by this build: it needs AdminClient::describe_topics, and \
                 AdminClient::describe_cluster for --unavailable-partitions, which the pinned \
                 krabka-client-admin does not have\n",
            ),
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn each_action_sends_its_requests_and_prints_what_kafka_topics_prints() {
    let cases = list_and_create_cases()
        .into_iter()
        .chain(alter_delete_and_describe_cases());
    for case in cases {
        let broker = broker(case.replies).await;
        let outcome = krabka(topics(&broker.address(), case.args)).await;
        check!(
            (outcome, sent(&broker)) == (case.run, case.sent),
            "{}",
            case.name
        );
        broker.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_metadata_timeout_fails_through_the_output_layer_without_a_mutation() {
    let broker = broker(vec![(METADATA, Reply::Silent)]).await;
    let outcome = krabka(topics(
        &broker.address(),
        &[
            "--delete",
            "--topic",
            "orders",
            "--yes",
            "--request-timeout-ms",
            "200",
        ],
    ))
    .await;
    check!(
        outcome
            == run(
                1,
                "",
                "krabka topics: client-core: request timed out after 200ms\n",
            )
    );
    // The client may retry the read. It sends nothing else.
    check!(
        sent(&broker)
            .iter()
            .all(|api_key| *api_key == metadata_request::API_KEY)
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_command_line_fails_before_connecting() {
    let cases: &[(&[&str], &str)] = &[
        (
            &["--create"],
            "krabka topics: Missing required argument \"[topic]\"\n",
        ),
        (
            &["--list", "--describe"],
            "krabka topics: Command must include exactly one action: --list, --describe, \
             --create, --alter or --delete\n",
        ),
        (
            &[
                "--create",
                "--topic",
                "t",
                "--partitions",
                "1",
                "--replica-assignment",
                "1",
            ],
            "krabka topics: Option \"[replica-assignment]\" can't be used with option \
             \"[partitions]\"\n",
        ),
    ];
    let broker = broker(Vec::new()).await;
    for (args, stderr) in cases {
        let outcome = krabka(topics(&broker.address(), args)).await;
        check!(outcome == run(1, "", stderr), "{args:?}");
    }
    check!(broker.received().is_empty());
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_json_rendering_carries_every_row() {
    let broker = broker(vec![(
        (create_topics_request::API_KEY, CREATE_VERSION),
        created(&[("b", 36, Some("Topic 'b' already exists.")), ("a", 0, None)]),
    )])
    .await;
    let mut args = vec!["--output".to_owned(), "json".to_owned()];
    args.extend(topics(
        &broker.address(),
        &["--create", "--topic", "a", "--topic", "b"],
    ));
    let outcome = krabka(args).await;
    let expected = serde_json::json!({"data": [
        {"topic": "a", "topic_id": null, "error": null},
        {"topic": "b", "topic_id": null, "error": {
            "code": 36,
            "name": "TOPIC_ALREADY_EXISTS",
            "message": "Topic 'b' already exists.",
        }},
    ]});
    check!(
        (
            outcome.code,
            serde_json::from_str::<serde_json::Value>(&outcome.stdout).unwrap(),
            outcome.stderr,
        ) == (Some(1), expected, String::new())
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_config_prints_kafkas_notice_on_stderr() {
    let broker = broker(vec![(METADATA, metadata(cluster()))]).await;
    let outcome = krabka(topics(
        &broker.address(),
        &[
            "--list",
            "--exclude-internal",
            "--delete-config",
            "retention.ms",
        ],
    ))
    .await;
    check!(
        outcome
            == run(
                0,
                "alpha\nbravo\norders\n",
                "delete-config option is no longer supported and deprecated since version 4.0. \
                 The config will be fully removed in future releases.\n",
            )
    );
    broker.stop();
}
