//! `krabka log-dirs --describe` and `krabka get-offsets` against scripted
//! brokers, through the built binary.
//!
//! `DescribeCluster` and the cluster metadata name three brokers. Two are mock
//! brokers that answer `DescribeLogDirs`; the third is a port that refuses
//! connections, so the fan-out meets one failed broker.

mod support;

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        describe_cluster_request,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_log_dirs_request,
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
        list_offsets_request::{self, ListOffsetsRequest},
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    },
};
use serde_json::{Value, json};

use self::support::{MockBroker, Reply, respond};

const METADATA_VERSION: i16 = 12;
const DESCRIBE_LOG_DIRS_VERSION: i16 = 4;
const DESCRIBE_CLUSTER_VERSION: i16 = 1;
const LIST_OFFSETS_VERSION: i16 = 7;

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

fn metadata(brokers: &[(i32, u16)], topics: &[(&str, i32)]) -> Reply {
    respond(
        &MetadataResponse {
            brokers: brokers
                .iter()
                .map(|(node_id, port)| MetadataResponseBroker {
                    node_id: *node_id,
                    host: "127.0.0.1".into(),
                    port: i32::from(*port),
                    ..Default::default()
                })
                .collect(),
            topics: topics
                .iter()
                .map(|(name, partitions)| MetadataResponseTopic {
                    name: Some((*name).into()),
                    partitions: (0..*partitions)
                        .map(|partition_index| MetadataResponsePartition {
                            partition_index,
                            leader_id: partition_index + 1,
                            replica_nodes: vec![partition_index + 1],
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

fn log_dirs(dir: &str, topics: &[(&str, &[(i32, i64)])]) -> Reply {
    respond(
        &DescribeLogDirsResponse {
            results: vec![DescribeLogDirsResult {
                log_dir: dir.into(),
                topics: topics
                    .iter()
                    .map(|(name, partitions)| DescribeLogDirsTopic {
                        name: (*name).into(),
                        partitions: partitions
                            .iter()
                            .map(
                                |(partition_index, partition_size)| DescribeLogDirsPartition {
                                    partition_index: *partition_index,
                                    partition_size: *partition_size,
                                    ..Default::default()
                                },
                            )
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                total_bytes: -1,
                usable_bytes: -1,
                ..Default::default()
            }],
            ..Default::default()
        },
        DESCRIBE_LOG_DIRS_VERSION,
        describe_log_dirs_request::FLEXIBLE_MIN,
    )
}

fn cluster_description(brokers: &[(i32, u16)]) -> Reply {
    respond(
        &DescribeClusterResponse {
            endpoint_type: 1,
            controller_id: 1,
            brokers: brokers
                .iter()
                .map(|(broker_id, port)| DescribeClusterBroker {
                    broker_id: *broker_id,
                    host: "127.0.0.1".into(),
                    port: i32::from(*port),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        DESCRIBE_CLUSTER_VERSION,
        describe_cluster_request::FLEXIBLE_MIN,
    )
}

/// Broker 1's `ListOffsets` answer: each partition's offset is its
/// `-timestamp` for the sentinels -1 to -3, and no offset for a timestamp.
fn list_offsets(frame: &[u8]) -> Reply {
    let client_id = usize::try_from(i16::from_be_bytes([frame[0], frame[1]]).max(0)).unwrap();
    let flexible = LIST_OFFSETS_VERSION >= list_offsets_request::FLEXIBLE_MIN;
    let mut body = &frame[2 + client_id + usize::from(flexible)..];
    let request = ListOffsetsRequest::decode(&mut body, LIST_OFFSETS_VERSION).unwrap();
    respond(
        &ListOffsetsResponse {
            topics: request
                .topics
                .into_iter()
                .map(|topic| ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| ListOffsetsPartitionResponse {
                            partition_index: partition.partition_index,
                            offset: if partition.timestamp < 0 {
                                -partition.timestamp
                            } else {
                                -1
                            },
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        LIST_OFFSETS_VERSION,
        list_offsets_request::FLEXIBLE_MIN,
    )
}

/// A port with nothing listening on it.
fn refused_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The bootstrap broker, which answers from a handler that knows its own
/// port.
struct Bootstrap(krabka_client_core::MockBroker);

impl Bootstrap {
    fn address(&self) -> String {
        self.0.addr.to_string()
    }

    fn stop(self) {
        self.0.stop();
    }
}

/// Brokers 1 and 2 answer, broker 3 refuses. Broker 1 is the bootstrap and
/// answers the metadata, which names all three at their real ports.
async fn cluster() -> (Bootstrap, MockBroker) {
    let advertised = [
        (metadata_request::API_KEY, 0, METADATA_VERSION),
        (
            describe_log_dirs_request::API_KEY,
            1,
            DESCRIBE_LOG_DIRS_VERSION,
        ),
        (
            describe_cluster_request::API_KEY,
            0,
            DESCRIBE_CLUSTER_VERSION,
        ),
    ];
    let second = MockBroker::start(
        &advertised,
        BTreeMap::from([(
            (
                describe_log_dirs_request::API_KEY,
                DESCRIBE_LOG_DIRS_VERSION,
            ),
            log_dirs("/data/2", &[("orders", &[(1, 0)])]),
        )]),
    )
    .await;
    let second_port = port(&second);
    // The bootstrap's metadata must name its own port, which is known only
    // once it listens, so a relay forwards to a broker started with it.
    let own_port = Arc::new(Mutex::new(0_u16));
    let relay = Arc::clone(&own_port);
    let refused = refused_port();
    let first = krabka_client_core::MockBroker::start(move |api_key, version, _, frame| {
        let port = *relay.lock().unwrap();
        let reply = match (api_key, version) {
            (api_versions_request::API_KEY, _) => {
                let mut own = advertised.to_vec();
                own.push((list_offsets_request::API_KEY, 1, LIST_OFFSETS_VERSION));
                return Some(api_versions(&own));
            }
            (list_offsets_request::API_KEY, LIST_OFFSETS_VERSION) => list_offsets(frame),
            (metadata_request::API_KEY, METADATA_VERSION) => metadata(
                &[(1, port), (2, second_port), (3, refused)],
                &[("orders", 2), ("events", 1)],
            ),
            (describe_cluster_request::API_KEY, DESCRIBE_CLUSTER_VERSION) => {
                cluster_description(&[(1, port), (2, second_port), (3, refused)])
            }
            (describe_log_dirs_request::API_KEY, DESCRIBE_LOG_DIRS_VERSION) => {
                log_dirs("/data/1", &[("orders", &[(0, 152)]), ("events", &[(0, 7)])])
            }
            _ => Reply::Silent,
        };
        match reply {
            Reply::Respond(body) => Some(body),
            Reply::Silent => None,
        }
    })
    .await;
    *own_port.lock().unwrap() = first.addr.port();
    (Bootstrap(first), second)
}

fn port(broker: &MockBroker) -> u16 {
    broker
        .address()
        .rsplit_once(':')
        .unwrap()
        .1
        .parse()
        .unwrap()
}

fn api_versions(advertised: &[(i16, i16, i16)]) -> Vec<u8> {
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

fn args(bootstrap: &str, extra: &[&str]) -> Vec<String> {
    ["log-dirs", "--describe", "--bootstrap-server", bootstrap]
        .iter()
        .chain(extra)
        .map(|arg| (*arg).to_owned())
        .collect()
}

/// A command config whose `default.api.timeout.ms` bounds the retries of
/// the refused broker, which the admin client retries until that deadline,
/// as Kafka's does.
fn short_deadline() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        "request.timeout.ms=1000\ndefault.api.timeout.ms=2000\n",
    )
    .unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_broker_leaves_a_partial_document_and_exits_1() {
    let (first, second) = cluster().await;
    let config = short_deadline();
    let out = krabka(args(
        &first.address(),
        &["--command-config", config.path().to_str().unwrap()],
    ))
    .await;
    check!(out.code == Some(1));
    let lines = out.stdout.lines().collect::<Vec<_>>();
    check!(
        lines
            == [
                "Querying brokers for log directories information",
                "Received log directory information from brokers 1,2",
                concat!(
                    r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"orders-0","size":152,"offsetLag":0,"isFuture":false},"#,
                    r#"{"partition":"events-0","size":7,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/data/1"}]},"#,
                    r#"{"broker":2,"logDirs":[{"partitions":[{"partition":"orders-1","size":0,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/data/2"}]}],"version":1}"#,
                ),
            ]
    );
    check!(
        out.stderr
            .starts_with("ERROR: failed to describe the log directories of broker 3: ")
    );
    check!(out.stderr.lines().count() == 1);
    first.stop();
    second.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_and_topic_lists_narrow_the_document() {
    let (first, second) = cluster().await;
    let out = krabka(args(
        &first.address(),
        &["--broker-list", "1", "--topic-list", "events"],
    ))
    .await;
    check!((out.code, out.stderr.as_str()) == (Some(0), ""));
    check!(
        out.stdout.lines().last()
            == Some(concat!(
                r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"events-0","size":7,"offsetLag":0,"isFuture":false}],"#,
                r#""error":null,"logDir":"/data/1"}]}],"version":1}"#,
            ))
    );
    // Broker 2 was not asked: it saw no request at all.
    check!(second.received().is_empty());
    let missing = krabka(args(&first.address(), &["--broker-list", "1,9"])).await;
    check!(missing.code == Some(1));
    check!(missing.stdout.is_empty());
    check!(
        missing.stderr
            == "krabka log-dirs: ERROR: The given brokers do not exist from --broker-list: 9. Current existent brokers: 1,2,3\n"
    );
    first.stop();
    second.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_json_envelope_carries_the_document_and_the_failed_brokers() {
    let (first, second) = cluster().await;
    let mut argv = vec!["--output".to_owned(), "json".to_owned()];
    let config = short_deadline();
    argv.extend(args(
        &first.address(),
        &[
            "--broker-list",
            "2,3",
            "--command-config",
            config.path().to_str().unwrap(),
        ],
    ));
    let out = krabka(argv).await;
    check!(out.code == Some(1));
    let mut envelope: Value = serde_json::from_str(&out.stdout).unwrap();
    let failed = envelope["data"]["failed_brokers"].take();
    check!(failed[0]["broker"] == json!(3));
    check!(
        envelope
            == json!({"data": {
                "brokers": [{"broker": 2, "logDirs": [{"partitions": [{"partition": "orders-1", "size": 0, "offsetLag": 0, "isFuture": false}], "error": null, "logDir": "/data/2"}]}],
                "version": 1,
                "failed_brokers": null,
            }})
    );
    check!(out.stderr.is_empty());
    first.stop();
    second.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_refuses_without_confirmation_and_dry_run_moves_nothing() {
    let (first, second) = cluster().await;
    let alter = |extra: &[&str]| {
        [
            "log-dirs",
            "--alter",
            "--broker",
            "1",
            "--move",
            "orders:0=/data/1",
            "--bootstrap-server",
            &first.address(),
        ]
        .iter()
        .chain(extra)
        .map(|arg| (*arg).to_owned())
        .collect::<Vec<_>>()
    };
    let refused = krabka(alter(&[])).await;
    check!(refused.code == Some(2));
    check!(refused.stderr.contains("pass --yes"));
    let dry = krabka(alter(&["--dry-run"])).await;
    check!(dry.code == Some(0));
    check!(
        dry.stdout
            == "DRY RUN: no change was made.\nMoving replica orders-0 on broker 1 from /data/1 to /data/1.\n"
    );
    first.stop();
    second.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn get_offsets_prints_each_listed_offset_and_fails_a_failed_partition() {
    let (first, second) = cluster().await;
    let run = |extra: &[&str]| {
        ["get-offsets", "--bootstrap-server", &first.address()]
            .iter()
            .chain(extra)
            .map(|arg| (*arg).to_owned())
            .collect::<Vec<_>>()
    };
    // Broker 1 leads orders-0 and events-0 and answers ListOffsets. Broker 2
    // leads orders-1 and does not advertise ListOffsets, so the client fails
    // that partition with UNSUPPORTED_VERSION, as Kafka's does.
    let cases: [(&[&str], i32, &str, &str); 6] = [
        (
            &["--topic", "nope"],
            1,
            "",
            "krabka get-offsets: Could not match any topic-partitions with the specified filters\n",
        ),
        (
            &["--topic-partitions", "orders:0,events", "--time", "-2"],
            0,
            "events:0:2\norders:0:2\n",
            "",
        ),
        (
            &["--topic", "orders", "--partitions", "0", "--time", "-3"],
            0,
            "orders:0:3\n",
            "",
        ),
        (&["--topic", "events", "--time", "1700000000000"], 0, "", ""),
        (
            &["--topic", "ev.*", "--time", "latest"],
            0,
            "events:0:1\n",
            "",
        ),
        (
            &["--time", "soon"],
            1,
            "",
            "krabka get-offsets: Malformed time argument soon. Please use -1 or latest / -2 or earliest / -3 or max-timestamp / -4 or earliest-local / -5 or latest-tiered / -6 or earliest-pending-upload, or a specified long format timestamp\n",
        ),
    ];
    for (extra, code, stdout, stderr) in cases {
        let out = krabka(run(extra)).await;
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str()) == (Some(code), stdout, stderr),
            "{extra:?}"
        );
    }
    let failed = krabka(run(&["--topic", "orders"])).await;
    check!(
        (failed.code, failed.stdout.as_str(), failed.stderr.as_str())
            == (
                Some(1),
                "orders:0:1\n",
                "Skip getting offsets for topic-partition orders-1 due to error: org.apache.kafka.common.errors.UnsupportedVersionException: The version of API is not supported.\n"
            )
    );
    first.stop();
    second.stop();
}
