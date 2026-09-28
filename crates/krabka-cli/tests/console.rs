//! `krabka console-consumer` and `krabka console-producer`, run as the built
//! binary against a scripted broker, with stdout and stderr checked apart.

mod support;

use std::{
    collections::BTreeMap,
    io::Write as _,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode as _, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition},
        fetch_request,
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        find_coordinator_request,
        find_coordinator_response::FindCoordinatorResponse,
        heartbeat_request,
        heartbeat_response::HeartbeatResponse,
        init_producer_id_request,
        init_producer_id_response::InitProducerIdResponse,
        join_group_request,
        join_group_response::JoinGroupResponse,
        leave_group_request,
        leave_group_response::LeaveGroupResponse,
        metadata_request,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_fetch_request,
        offset_fetch_response::OffsetFetchResponse,
        produce_request::{self, ProduceRequest},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
        sync_group_request,
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch, RecordHeader, RecordsPayload},
};
use serde_json::{Value, json};

use self::support::{MockBroker, Received, Reply, respond};

const TOPIC: &str = "orders";
const TOPIC_ID: Uuid = Uuid([7; 16]);
const METADATA_VERSION: i16 = 12;
const FETCH_VERSION: i16 = 12;
const FIND_COORDINATOR_VERSION: i16 = 3;
const JOIN_GROUP_VERSION: i16 = 7;
const SYNC_GROUP_VERSION: i16 = 5;
const OFFSET_FETCH_VERSION: i16 = 7;
const HEARTBEAT_VERSION: i16 = 4;
const LEAVE_GROUP_VERSION: i16 = 4;
const INIT_PRODUCER_ID_VERSION: i16 = 4;
const PRODUCE_VERSION: i16 = 9;

struct Outcome {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn krabka(args: &[&str], stdin: &[u8]) -> Outcome {
    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(args)
        .env("RUST_LOG", "off")
        .env_remove("KRABKA_BOOTSTRAP_SERVER")
        .env_remove("KRABKA_OUTPUT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run krabka");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(stdin)
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait for krabka");
    Outcome {
        code: out.status.code(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

async fn run(args: Vec<String>, stdin: &'static [u8]) -> Outcome {
    tokio::task::spawn_blocking(move || {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        krabka(&args, stdin)
    })
    .await
    .unwrap()
}

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// The metadata of one broker that leads partition 0 of `orders`. The
/// broker advertises port 0, as a single in-process broker does, so every
/// client routes to the bootstrap address.
fn metadata() -> Reply {
    respond(
        &MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 0,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            }],
            topics: vec![MetadataResponseTopic {
                name: Some(TOPIC.into()),
                topic_id: TOPIC_ID,
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    )
}

fn batch(base_offset: i64, values: &[&str]) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
        base_timestamp: 1_000,
        max_timestamp: 1_000,
        records: values
            .iter()
            .zip(0..)
            .map(|(value, delta)| Record {
                offset_delta: delta,
                timestamp_delta: i64::from(delta),
                key: Some(
                    format!("k{}", base_offset + i64::from(delta))
                        .into_bytes()
                        .into(),
                ),
                value: Some(value.as_bytes().to_vec().into()),
                headers: vec![RecordHeader {
                    key: "h".into(),
                    value: Some(b"x".to_vec().into()),
                }],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn fetch(batches: Vec<RecordBatch>) -> Reply {
    respond(
        &FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: TOPIC.into(),
                topic_id: TOPIC_ID,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    high_watermark: 3,
                    last_stable_offset: 3,
                    log_start_offset: 0,
                    records: Some(RecordsPayload::V2(batches)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        FETCH_VERSION,
        fetch_request::FLEXIBLE_MIN,
    )
}

/// A broker that serves one partition of `orders` holding three records.
async fn partition_broker() -> MockBroker {
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (fetch_request::API_KEY, 4, FETCH_VERSION),
        ],
        BTreeMap::from([
            ((metadata_request::API_KEY, METADATA_VERSION), metadata()),
            (
                (fetch_request::API_KEY, FETCH_VERSION),
                fetch(vec![batch(0, &["v0", "v1", "v2"])]),
            ),
        ]),
    )
    .await
}

fn api_keys(received: &[Received]) -> Vec<i16> {
    let mut keys = received
        .iter()
        .map(|request| request.api_key)
        .collect::<Vec<_>>();
    keys.dedup();
    keys
}

/// `--partition 0 --offset earliest` reads the partition without a group:
/// no `FindCoordinator`, no `JoinGroup`, only metadata and fetches.
#[tokio::test(flavor = "multi_thread")]
async fn the_partition_path_fetches_without_joining_a_group() {
    let broker = partition_broker().await;
    let out = run(
        args(&[
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            "2",
            "--formatter-property",
            "print.key=true",
            "--formatter-property",
            "print.offset=true",
            "--formatter-property",
            "print.headers=true",
            "--formatter-property",
            "print.partition=true",
        ]),
        b"",
    )
    .await;
    check!(out.code == Some(0));
    check!(
        out.stdout == "Partition:0\tOffset:0\th:x\tk0\tv0\nPartition:0\tOffset:1\th:x\tk1\tv1\n"
    );
    check!(out.stderr == "Processed a total of 2 messages\n");
    let keys = api_keys(&broker.received());
    check!(!keys.contains(&join_group_request::API_KEY), "{keys:?}");
    check!(
        !keys.contains(&find_coordinator_request::API_KEY),
        "{keys:?}"
    );
    check!(
        keys == [
            api_versions_request::API_KEY,
            metadata_request::API_KEY,
            api_versions_request::API_KEY,
            fetch_request::API_KEY,
        ]
    );
    broker.stop();
}

/// `--offset n` starts at `n`, and a record before it in the same batch is
/// not printed.
#[tokio::test(flavor = "multi_thread")]
async fn an_offset_skips_the_records_before_it() {
    let broker = partition_broker().await;
    let out = run(
        args(&[
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "2",
            "--max-messages",
            "1",
        ]),
        b"",
    )
    .await;
    check!((out.code, out.stdout.as_str()) == (Some(0), "v2\n"));
    broker.stop();
}

/// `--timeout-ms` ends a run that receives nothing, whether the broker
/// answers with no records or does not answer, and the run still exits 0 as
/// the JVM tool does.
#[tokio::test(flavor = "multi_thread")]
async fn timeout_ms_bounds_a_run_that_receives_nothing() {
    for fetch_reply in [fetch(Vec::new()), Reply::Silent] {
        let silent = fetch_reply == Reply::Silent;
        let broker = MockBroker::start(
            &[
                (metadata_request::API_KEY, 0, METADATA_VERSION),
                (fetch_request::API_KEY, 4, FETCH_VERSION),
            ],
            BTreeMap::from([
                ((metadata_request::API_KEY, METADATA_VERSION), metadata()),
                ((fetch_request::API_KEY, FETCH_VERSION), fetch_reply),
            ]),
        )
        .await;
        let started = std::time::Instant::now();
        let out = run(
            args(&[
                "console-consumer",
                "--bootstrap-server",
                &broker.address(),
                "--topic",
                TOPIC,
                "--partition",
                "0",
                "--offset",
                "earliest",
                "--timeout-ms",
                "300",
            ]),
            b"",
        )
        .await;
        check!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "silent: {silent}"
        );
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str())
                == (Some(0), "", "Processed a total of 0 messages\n"),
            "silent: {silent}"
        );
        broker.stop();
    }
}

/// Under `--output json` each record is one `{"data": ...}` line on stdout,
/// and stderr stays empty.
#[tokio::test(flavor = "multi_thread")]
async fn json_output_is_one_line_per_record() {
    let broker = partition_broker().await;
    let out = run(
        args(&[
            "--output",
            "json",
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "1",
            "--max-messages",
            "1",
        ]),
        b"",
    )
    .await;
    check!(out.code == Some(0));
    check!(out.stderr.is_empty());
    let line: Value = serde_json::from_str(&out.stdout).expect("one JSON line");
    check!(
        line == json!({"data": {
            "topic": TOPIC, "partition": 0, "offset": 1, "timestamp": 1_001,
            "key": "k1", "value": "v1", "headers": [{"key": "h", "value": "x"}],
        }})
    );
    broker.stop();
}

/// The default offset of `--partition` is `latest`, which needs a client call
/// that the pinned client lacks; the command says so and exits 1.
#[tokio::test(flavor = "multi_thread")]
async fn latest_on_the_partition_path_is_not_supported_by_this_build() {
    let broker = partition_broker().await;
    let out = run(
        args(&[
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
        ]),
        b"",
    )
    .await;
    check!(out.code == Some(1));
    check!(out.stdout.is_empty());
    check!(
        out.stderr.contains("not supported by this build"),
        "{}",
        out.stderr
    );
    check!(
        out.stderr.contains("AdminClient::list_offsets"),
        "{}",
        out.stderr
    );
    broker.stop();
}

/// A broker that coordinates a group of one member, assigns it partition 0
/// of `orders`, and serves three records.
async fn group_broker() -> MockBroker {
    let assignment = {
        let mut bytes = 3_i16.to_be_bytes().to_vec();
        ConsumerProtocolAssignment {
            assigned_partitions: vec![TopicPartition {
                topic: TOPIC.into(),
                partitions: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut bytes, 3)
        .unwrap();
        bytes
    };
    let flexible = |key| match key {
        k if k == find_coordinator_request::API_KEY => find_coordinator_request::FLEXIBLE_MIN,
        k if k == join_group_request::API_KEY => join_group_request::FLEXIBLE_MIN,
        k if k == sync_group_request::API_KEY => sync_group_request::FLEXIBLE_MIN,
        k if k == offset_fetch_request::API_KEY => offset_fetch_request::FLEXIBLE_MIN,
        k if k == heartbeat_request::API_KEY => heartbeat_request::FLEXIBLE_MIN,
        _ => leave_group_request::FLEXIBLE_MIN,
    };
    let reply = |key: i16, version: i16, message: &dyn Fn(&mut Vec<u8>)| {
        let mut body = Vec::new();
        if version >= flexible(key) {
            body.push(0);
        }
        message(&mut body);
        ((key, version), Reply::Respond(body))
    };
    MockBroker::start(
        &[
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (fetch_request::API_KEY, 4, FETCH_VERSION),
            (
                find_coordinator_request::API_KEY,
                0,
                FIND_COORDINATOR_VERSION,
            ),
            (join_group_request::API_KEY, 0, JOIN_GROUP_VERSION),
            (sync_group_request::API_KEY, 0, SYNC_GROUP_VERSION),
            (offset_fetch_request::API_KEY, 1, OFFSET_FETCH_VERSION),
            (heartbeat_request::API_KEY, 0, HEARTBEAT_VERSION),
            (leave_group_request::API_KEY, 0, LEAVE_GROUP_VERSION),
        ],
        BTreeMap::from([
            ((metadata_request::API_KEY, METADATA_VERSION), metadata()),
            (
                (fetch_request::API_KEY, FETCH_VERSION),
                fetch(vec![batch(0, &["v0", "v1", "v2"])]),
            ),
            reply(
                find_coordinator_request::API_KEY,
                FIND_COORDINATOR_VERSION,
                &|body| {
                    FindCoordinatorResponse {
                        node_id: 0,
                        host: "127.0.0.1".into(),
                        port: 0,
                        ..Default::default()
                    }
                    .encode(body, FIND_COORDINATOR_VERSION)
                    .unwrap();
                },
            ),
            reply(join_group_request::API_KEY, JOIN_GROUP_VERSION, &|body| {
                JoinGroupResponse {
                    generation_id: 1,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    leader: "other-member".into(),
                    member_id: "member-1".into(),
                    ..Default::default()
                }
                .encode(body, JOIN_GROUP_VERSION)
                .unwrap();
            }),
            reply(sync_group_request::API_KEY, SYNC_GROUP_VERSION, &|body| {
                SyncGroupResponse {
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    assignment: assignment.clone().into(),
                    ..Default::default()
                }
                .encode(body, SYNC_GROUP_VERSION)
                .unwrap();
            }),
            reply(
                offset_fetch_request::API_KEY,
                OFFSET_FETCH_VERSION,
                &|body| {
                    OffsetFetchResponse::default()
                        .encode(body, OFFSET_FETCH_VERSION)
                        .unwrap();
                },
            ),
            reply(heartbeat_request::API_KEY, HEARTBEAT_VERSION, &|body| {
                HeartbeatResponse::default()
                    .encode(body, HEARTBEAT_VERSION)
                    .unwrap();
            }),
            reply(leave_group_request::API_KEY, LEAVE_GROUP_VERSION, &|body| {
                LeaveGroupResponse::default()
                    .encode(body, LEAVE_GROUP_VERSION)
                    .unwrap();
            }),
        ]),
    )
    .await
}

/// The issue's first acceptance check: with `2>/dev/null`, stdout holds
/// exactly one record and nothing else. The subscribed path joins the group.
#[tokio::test(flavor = "multi_thread")]
async fn max_messages_one_writes_exactly_one_record_to_stdout() {
    let broker = group_broker().await;
    let out = run(
        args(&[
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--from-beginning",
            "--max-messages",
            "1",
        ]),
        b"",
    )
    .await;
    check!(out.code == Some(0));
    check!(out.stdout == "v0\n");
    check!(out.stderr == "Processed a total of 1 messages\n");
    let keys = api_keys(&broker.received());
    check!(keys.contains(&join_group_request::API_KEY), "{keys:?}");
    check!(keys.contains(&sync_group_request::API_KEY), "{keys:?}");
    check!(keys.contains(&fetch_request::API_KEY), "{keys:?}");
    broker.stop();
}

/// `--max-messages n` stops after exactly `n` records, although the broker
/// served more.
#[tokio::test(flavor = "multi_thread")]
async fn max_messages_stops_after_exactly_n_records() {
    let broker = group_broker().await;
    let out = run(
        args(&[
            "console-consumer",
            "--bootstrap-server",
            &broker.address(),
            "--topic",
            TOPIC,
            "--from-beginning",
            "--max-messages",
            "2",
            "--group",
            "workers",
            "--formatter-property",
            "print.key=true",
        ]),
        b"",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr.as_str())
            == (
                Some(0),
                "k0\tv0\nk1\tv1\n",
                "Processed a total of 2 messages\n"
            )
    );
    broker.stop();
}

#[test]
fn a_bad_command_line_exits_2_with_the_jvm_message() {
    let out = krabka(
        &[
            "console-consumer",
            "--bootstrap-server",
            "h:1",
            "--topic",
            "t",
            "--offset",
            "3",
        ],
        b"",
    );
    check!(out.code == Some(2));
    check!(out.stdout.is_empty());
    check!(
        out.stderr
            == "krabka console-consumer: The partition is required when offset is specified.\n"
    );
    let out = krabka(&["console-producer", "--bootstrap-server", "h:1"], b"");
    check!(
        (out.code, out.stderr.as_str())
            == (
                Some(2),
                "krabka console-producer: Missing required argument \"[topic]\"\n"
            )
    );
}

/// A produced record as text: key, value and headers.
type Produced = (
    Option<String>,
    Option<String>,
    Vec<(String, Option<String>)>,
);

/// A broker that accepts every produce, and the records it received.
struct ProduceBroker {
    broker: krabka_client_core::MockBroker,
    records: Arc<Mutex<Vec<Produced>>>,
}

async fn produce_broker() -> ProduceBroker {
    produce_broker_with(metadata()).await
}

async fn produce_broker_with(metadata: Reply) -> ProduceBroker {
    let records = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&records);
    let api_versions = {
        let mut body = Vec::new();
        ApiVersionsResponse {
            api_keys: [
                (api_versions_request::API_KEY, 0, 3),
                (metadata_request::API_KEY, 0, METADATA_VERSION),
                (
                    init_producer_id_request::API_KEY,
                    0,
                    INIT_PRODUCER_ID_VERSION,
                ),
                (produce_request::API_KEY, 3, PRODUCE_VERSION),
            ]
            .into_iter()
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        }
        .encode(&mut body, 0)
        .unwrap();
        body
    };
    let Reply::Respond(metadata) = metadata else {
        unreachable!("metadata always responds")
    };
    let broker = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
        let mut out = Vec::new();
        match api_key {
            k if k == api_versions_request::API_KEY => return Some(api_versions.clone()),
            k if k == metadata_request::API_KEY => return Some(metadata.clone()),
            k if k == init_producer_id_request::API_KEY => {
                out.push(0);
                InitProducerIdResponse {
                    producer_id: 42,
                    producer_epoch: 0,
                    ..Default::default()
                }
                .encode(&mut out, version)
                .unwrap();
            }
            k if k == produce_request::API_KEY => {
                let request = decode_produce(body, version);
                let mut log = log.lock().unwrap();
                let mut responses = Vec::new();
                for topic in &request.topic_data {
                    for partition in &topic.partition_data {
                        let Some(RecordsPayload::V2(batches)) = &partition.records else {
                            continue;
                        };
                        for record in batches.iter().flat_map(|batch| &batch.records) {
                            log.push((
                                text(record.key.as_ref()),
                                text(record.value.as_ref()),
                                record
                                    .headers
                                    .iter()
                                    .map(|header| (header.key.clone(), text(header.value.as_ref())))
                                    .collect(),
                            ));
                        }
                        responses.push((topic.name.clone(), topic.topic_id, partition.index));
                    }
                }
                out.push(0);
                ProduceResponse {
                    responses: responses
                        .into_iter()
                        .map(|(name, topic_id, index)| TopicProduceResponse {
                            name,
                            topic_id,
                            partition_responses: vec![PartitionProduceResponse {
                                index,
                                ..Default::default()
                            }],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }
                .encode(&mut out, version)
                .unwrap();
            }
            _ => return None,
        }
        Some(out)
    })
    .await;
    ProduceBroker { broker, records }
}

fn text<B: AsRef<[u8]>>(bytes: Option<&B>) -> Option<String> {
    bytes.map(|bytes| String::from_utf8_lossy(bytes.as_ref()).into_owned())
}

fn decode_produce(body: &[u8], version: i16) -> ProduceRequest {
    // The request header after the correlation id: client id, then the
    // tagged fields of a flexible header.
    let client_id = usize::try_from(i16::from_be_bytes([body[0], body[1]]).max(0)).unwrap();
    let mut rest = &body[2 + client_id..];
    if version >= produce_request::FLEXIBLE_MIN {
        rest = &rest[1..];
    }
    ProduceRequest::decode(&mut rest, version).expect("a valid produce request")
}

/// `parse.key=true` with `key.separator=:` splits `a:b` into key `a` and
/// value `b`. A line with no separator stops the run with a stderr line and
/// exit 1, as the JVM tool does; what was read before it is still sent.
#[tokio::test(flavor = "multi_thread")]
async fn parse_key_splits_lines_and_a_line_without_the_separator_fails_the_run() {
    let broker = produce_broker().await;
    let out = run(
        args(&[
            "console-producer",
            "--bootstrap-server",
            &broker.broker.addr.to_string(),
            "--topic",
            TOPIC,
            "--reader-property",
            "parse.key=true",
            "--reader-property",
            "key.separator=:",
        ]),
        b"a:b\nc:d\nnosep\ne:f\n",
    )
    .await;
    check!(out.code == Some(1));
    check!(out.stdout.is_empty());
    check!(
        out.stderr == "krabka console-producer: No key separator found on line number 3: 'nosep'\n"
    );
    check!(
        *broker.records.lock().unwrap()
            == [
                (Some("a".into()), Some("b".into()), Vec::new()),
                (Some("c".into()), Some("d".into()), Vec::new()),
            ]
    );
    broker.broker.stop();
}

/// `ignore.error=true` keeps going past a line without the separator, and
/// sends it as a value with no key.
#[tokio::test(flavor = "multi_thread")]
async fn ignore_error_sends_every_line() {
    let broker = produce_broker().await;
    let out = run(
        args(&[
            "console-producer",
            "--bootstrap-server",
            &broker.broker.addr.to_string(),
            "--topic",
            TOPIC,
            "--property",
            "parse.key=true",
            "--property",
            "ignore.error=true",
            "--property",
            "parse.headers=true",
            "--sync",
        ]),
        b"h1:x\tk\tv\nplain\n",
    )
    .await;
    check!(out.code == Some(0));
    check!(out.stdout.is_empty());
    check!(
        out.stderr
            == "Warning: --property is deprecated and will be removed in a future version. Use --reader-property instead.\n"
    );
    check!(
        *broker.records.lock().unwrap()
            == [
                (
                    Some("k".into()),
                    Some("v".into()),
                    vec![("h1".into(), Some("x".into()))]
                ),
                (None, Some("plain".into()), Vec::new()),
            ]
    );
    broker.broker.stop();
}

/// Under `--output json` the producer reports what it sent as one envelope.
#[tokio::test(flavor = "multi_thread")]
async fn json_output_reports_the_records_sent() {
    let broker = produce_broker().await;
    let out = run(
        args(&[
            "--output",
            "json",
            "console-producer",
            "--bootstrap-server",
            &broker.broker.addr.to_string(),
            "--topic",
            TOPIC,
        ]),
        b"one\ntwo\n",
    )
    .await;
    check!(out.code == Some(0));
    check!(out.stderr.is_empty());
    check!(
        serde_json::from_str::<Value>(&out.stdout).unwrap()
            == json!({"data": {"topic": TOPIC, "sent": 2, "failed": 0}})
    );
    assert!(broker.records.lock().unwrap().len() == 2);
    broker.broker.stop();
}

/// A topic that never appears in the metadata fails the run after
/// `--max-block-ms`, with the JVM producer's message, and nothing is sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_topic_fails_after_max_block_ms() {
    let no_topics = respond(
        &MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 0,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            }],
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    );
    let broker = produce_broker_with(no_topics).await;
    let out = run(
        args(&[
            "console-producer",
            "--bootstrap-server",
            &broker.broker.addr.to_string(),
            "--topic",
            TOPIC,
            "--max-block-ms",
            "300",
        ]),
        b"lost\n",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr.as_str())
            == (
                Some(1),
                "",
                "krabka console-producer: Topic orders not present in metadata after 300 ms.\n"
            )
    );
    check!(broker.records.lock().unwrap().is_empty());
    broker.broker.stop();
}
