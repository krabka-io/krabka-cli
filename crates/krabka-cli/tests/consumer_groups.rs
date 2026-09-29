//! `krabka consumer-groups` against a scripted broker, through the built
//! binary.
//!
//! One broker is the bootstrap, the only cluster member, the leader of every
//! partition and the coordinator of every group. Its groups:
//!
//! - `orders-app`: a Stable consumer-protocol group (KIP-848) with member
//!   `m1`, which holds `orders-0` and is to hold `orders-0,1`. Committed
//!   offsets `orders-0` 10 and `orders-1` 3.
//! - `idle`: an Empty classic group, which `ConsumerGroupDescribe` does not
//!   know. Committed offset `orders-0` 4.
//! - `denied`: `ConsumerGroupDescribe` answers `GROUP_AUTHORIZATION_FAILED`.
//! - `nope`: no such group.
//!
//! `orders` has two partitions, with log start offsets 2 and 0, log end
//! offsets 15 and 8, and at the timestamp of `--to-datetime` offset 12 and no
//! record. The broker logs every mutation it receives, so each case can check
//! what a `--dry-run` did not send.

use std::{
    fmt::Write as _,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        common::consumer_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        },
        consumer_group_describe_request::{self, ConsumerGroupDescribeRequest},
        consumer_group_describe_response::{
            ConsumerGroupDescribeResponse, DescribedGroup as ConsumerGroup, Member,
        },
        delete_groups_request::{self, DeleteGroupsRequest},
        delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse},
        describe_cluster_request,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_groups_request::{self, DescribeGroupsRequest},
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup as ClassicGroup},
        find_coordinator_request::{self, FindCoordinatorRequest},
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        list_groups_request::{self, ListGroupsRequest},
        list_groups_response::{ListGroupsResponse, ListedGroup},
        list_offsets_request::{self, ListOffsetsRequest},
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_commit_request::{self, OffsetCommitRequest},
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
        offset_delete_request::{self, OffsetDeleteRequest},
        offset_delete_response::{
            OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
        },
        offset_fetch_request::{self, OffsetFetchRequest},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
    },
};

/// `2024-01-01T00:00:00.000` in Kafka epoch milliseconds.
const DATETIME_MS: i64 = 1_704_067_200_000;

/// The APIs the broker advertises, `(api_key, min, max)`.
const ADVERTISED: [(i16, i16, i16); 12] = [
    (metadata_request::API_KEY, 0, 12),
    (describe_cluster_request::API_KEY, 0, 1),
    (find_coordinator_request::API_KEY, 0, 4),
    (offset_fetch_request::API_KEY, 1, 8),
    (list_groups_request::API_KEY, 0, 5),
    (list_offsets_request::API_KEY, 1, 7),
    (consumer_group_describe_request::API_KEY, 0, 1),
    (describe_groups_request::API_KEY, 0, 5),
    (delete_groups_request::API_KEY, 0, 2),
    (offset_delete_request::API_KEY, 0, 0),
    (offset_commit_request::API_KEY, 2, 8),
    (api_versions_request::API_KEY, 0, 3),
];

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

/// An encoded response body for `version`, with the response header's
/// tagged-fields byte when the version is flexible.
fn respond<T: Encode>(message: &T, version: i16, flexible_min: i16) -> Vec<u8> {
    let mut body = Vec::new();
    if version >= flexible_min {
        body.push(0);
    }
    message.encode(&mut body, version).unwrap();
    body
}

/// The request body after the header's client id, and its tagged-fields
/// byte for a flexible version.
fn decode<T: for<'de> Decode<'de>>(frame: &[u8], version: i16, flexible_min: i16) -> T {
    let client_id = usize::try_from(i16::from_be_bytes([frame[0], frame[1]]).max(0)).unwrap();
    let mut body = &frame[2 + client_id + usize::from(version >= flexible_min)..];
    T::decode(&mut body, version).unwrap()
}

fn api_versions() -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: ADVERTISED
            .into_iter()
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut out = Vec::new();
    response.encode(&mut out, 0).unwrap();
    out
}

fn assignment(partitions: &[i32]) -> Assignment {
    Assignment {
        topic_partitions: vec![TopicPartitions {
            topic_name: "orders".into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn consumer_group(group_id: String) -> ConsumerGroup {
    match group_id.as_str() {
        "orders-app" => ConsumerGroup {
            group_id,
            group_state: "Stable".into(),
            group_epoch: 7,
            assignment_epoch: 7,
            assignor_name: "uniform".into(),
            members: vec![Member {
                member_id: "m1".into(),
                member_epoch: 5,
                client_id: "cli-1".into(),
                client_host: "/10.0.0.1".into(),
                subscribed_topic_names: vec!["orders".into()],
                assignment: assignment(&[0]),
                target_assignment: assignment(&[0, 1]),
                ..Default::default()
            }],
            ..Default::default()
        },
        "denied" => ConsumerGroup {
            group_id,
            error_code: 30,
            ..Default::default()
        },
        _ => ConsumerGroup {
            group_id,
            error_code: 69,
            ..Default::default()
        },
    }
}

fn classic_group(group_id: String) -> ClassicGroup {
    match group_id.as_str() {
        "idle" => ClassicGroup {
            group_id,
            group_state: "Empty".into(),
            protocol_type: "consumer".into(),
            ..Default::default()
        },
        _ => ClassicGroup {
            group_id,
            group_state: "Dead".into(),
            error_code: 69,
            ..Default::default()
        },
    }
}

/// The committed offsets of `group`.
fn committed(group: &str) -> Vec<(i32, i64)> {
    match group {
        "orders-app" => vec![(0, 10), (1, 3)],
        "idle" => vec![(0, 4)],
        _ => vec![],
    }
}

/// The offset of `orders-partition` at `timestamp`.
fn log_offset(partition: i32, timestamp: i64) -> i64 {
    match (partition, timestamp) {
        (0, -2) => 2,
        (0, -1) => 15,
        (0, DATETIME_MS) => 12,
        (1, -2) => 0,
        (1, -1) => 8,
        _ => -1,
    }
}

/// `(state, type)` of each listed group.
const LISTED: [(&str, &str, &str); 3] = [
    ("orders-app", "Stable", "Consumer"),
    ("idle", "Empty", "Classic"),
    ("denied", "Empty", "Classic"),
];

fn handle(
    port: u16,
    log: &Mutex<Vec<String>>,
    api_key: i16,
    version: i16,
    frame: &[u8],
) -> Vec<u8> {
    match api_key {
        delete_groups_request::API_KEY
        | offset_delete_request::API_KEY
        | offset_commit_request::API_KEY => mutate(log, api_key, version, frame),
        offset_fetch_request::API_KEY
        | list_groups_request::API_KEY
        | list_offsets_request::API_KEY
        | consumer_group_describe_request::API_KEY
        | describe_groups_request::API_KEY => lookup(api_key, version, frame),
        _ => cluster(port, api_key, version, frame),
    }
}

/// The cluster APIs: versions, metadata and coordinators.
fn cluster(port: u16, api_key: i16, version: i16, frame: &[u8]) -> Vec<u8> {
    match api_key {
        api_versions_request::API_KEY => api_versions(),
        metadata_request::API_KEY => respond(
            &MetadataResponse {
                brokers: vec![MetadataResponseBroker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(port),
                    ..Default::default()
                }],
                controller_id: 1,
                topics: vec![MetadataResponseTopic {
                    name: Some("orders".into()),
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
                }],
                ..Default::default()
            },
            version,
            metadata_request::FLEXIBLE_MIN,
        ),
        describe_cluster_request::API_KEY => respond(
            &DescribeClusterResponse {
                controller_id: 1,
                brokers: vec![DescribeClusterBroker {
                    broker_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(port),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version,
            describe_cluster_request::FLEXIBLE_MIN,
        ),
        find_coordinator_request::API_KEY => {
            let request: FindCoordinatorRequest =
                decode(frame, version, find_coordinator_request::FLEXIBLE_MIN);
            respond(
                &FindCoordinatorResponse {
                    coordinators: request
                        .coordinator_keys
                        .into_iter()
                        .map(|key| Coordinator {
                            key,
                            node_id: 1,
                            host: "127.0.0.1".into(),
                            port: i32::from(port),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                find_coordinator_request::FLEXIBLE_MIN,
            )
        }
        other => panic!("unexpected request: api key {other} v{version}"),
    }
}

/// The group and offset lookups.
fn lookup(api_key: i16, version: i16, frame: &[u8]) -> Vec<u8> {
    match api_key {
        offset_fetch_request::API_KEY => {
            let request: OffsetFetchRequest =
                decode(frame, version, offset_fetch_request::FLEXIBLE_MIN);
            let groups = request
                .groups
                .into_iter()
                .map(|group| OffsetFetchResponseGroup {
                    topics: vec![OffsetFetchResponseTopics {
                        name: "orders".into(),
                        partitions: committed(&group.group_id)
                            .into_iter()
                            .map(|(partition_index, committed_offset)| {
                                OffsetFetchResponsePartitions {
                                    partition_index,
                                    committed_offset,
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    group_id: group.group_id,
                    ..Default::default()
                })
                .collect();
            respond(
                &OffsetFetchResponse {
                    groups,
                    ..Default::default()
                },
                version,
                offset_fetch_request::FLEXIBLE_MIN,
            )
        }
        list_groups_request::API_KEY => {
            let request: ListGroupsRequest =
                decode(frame, version, list_groups_request::FLEXIBLE_MIN);
            let wanted = |filter: &[String], value: &str| {
                filter.is_empty() || filter.iter().any(|entry| entry.eq_ignore_ascii_case(value))
            };
            respond(
                &ListGroupsResponse {
                    groups: LISTED
                        .into_iter()
                        .filter(|(_, state, kind)| {
                            wanted(&request.states_filter, state)
                                && wanted(&request.types_filter, kind)
                        })
                        .map(|(group_id, group_state, group_type)| ListedGroup {
                            group_id: group_id.into(),
                            protocol_type: "consumer".into(),
                            group_state: group_state.into(),
                            group_type: group_type.into(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                list_groups_request::FLEXIBLE_MIN,
            )
        }
        list_offsets_request::API_KEY => {
            let request: ListOffsetsRequest =
                decode(frame, version, list_offsets_request::FLEXIBLE_MIN);
            respond(
                &ListOffsetsResponse {
                    topics: request
                        .topics
                        .into_iter()
                        .map(|topic| ListOffsetsTopicResponse {
                            partitions: topic
                                .partitions
                                .iter()
                                .map(|partition| ListOffsetsPartitionResponse {
                                    partition_index: partition.partition_index,
                                    offset: log_offset(
                                        partition.partition_index,
                                        partition.timestamp,
                                    ),
                                    ..Default::default()
                                })
                                .collect(),
                            name: topic.name,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                list_offsets_request::FLEXIBLE_MIN,
            )
        }
        consumer_group_describe_request::API_KEY => {
            let request: ConsumerGroupDescribeRequest = decode(
                frame,
                version,
                consumer_group_describe_request::FLEXIBLE_MIN,
            );
            respond(
                &ConsumerGroupDescribeResponse {
                    groups: request.group_ids.into_iter().map(consumer_group).collect(),
                    ..Default::default()
                },
                version,
                consumer_group_describe_request::FLEXIBLE_MIN,
            )
        }
        describe_groups_request::API_KEY => {
            let request: DescribeGroupsRequest =
                decode(frame, version, describe_groups_request::FLEXIBLE_MIN);
            respond(
                &DescribeGroupsResponse {
                    groups: request.groups.into_iter().map(classic_group).collect(),
                    ..Default::default()
                },
                version,
                describe_groups_request::FLEXIBLE_MIN,
            )
        }
        other => panic!("unexpected request: api key {other} v{version}"),
    }
}

/// The mutations, each logged.
fn mutate(log: &Mutex<Vec<String>>, api_key: i16, version: i16, frame: &[u8]) -> Vec<u8> {
    match api_key {
        delete_groups_request::API_KEY => {
            let request: DeleteGroupsRequest =
                decode(frame, version, delete_groups_request::FLEXIBLE_MIN);
            log.lock()
                .unwrap()
                .push(format!("DeleteGroups {}", request.groups_names.join(",")));
            respond(
                &DeleteGroupsResponse {
                    results: request
                        .groups_names
                        .into_iter()
                        .map(|group_id| DeletableGroupResult {
                            error_code: match group_id.as_str() {
                                "orders-app" => 68,
                                "nope" => 69,
                                _ => 0,
                            },
                            group_id,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                delete_groups_request::FLEXIBLE_MIN,
            )
        }
        offset_delete_request::API_KEY => {
            let request: OffsetDeleteRequest =
                decode(frame, version, offset_delete_request::FLEXIBLE_MIN);
            let partitions = request
                .topics
                .iter()
                .flat_map(|topic| {
                    topic.partitions.iter().map(move |partition| {
                        format!("{}-{}", topic.name, partition.partition_index)
                    })
                })
                .collect::<Vec<_>>();
            log.lock().unwrap().push(format!(
                "OffsetDelete {} {}",
                request.group_id,
                partitions.join(",")
            ));
            let (error_code, partition_code) = match request.group_id.as_str() {
                "nope" => (69, 0),
                "orders-app" => (0, 86),
                _ => (0, 0),
            };
            respond(
                &OffsetDeleteResponse {
                    error_code,
                    topics: request
                        .topics
                        .into_iter()
                        .map(|topic| OffsetDeleteResponseTopic {
                            name: topic.name,
                            partitions: topic
                                .partitions
                                .into_iter()
                                .map(|partition| OffsetDeleteResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code: partition_code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                offset_delete_request::FLEXIBLE_MIN,
            )
        }
        offset_commit_request::API_KEY => {
            let request: OffsetCommitRequest =
                decode(frame, version, offset_commit_request::FLEXIBLE_MIN);
            let offsets = request
                .topics
                .iter()
                .flat_map(|topic| {
                    topic.partitions.iter().map(move |partition| {
                        format!(
                            "{}-{}={}",
                            topic.name, partition.partition_index, partition.committed_offset
                        )
                    })
                })
                .collect::<Vec<_>>();
            log.lock().unwrap().push(format!(
                "OffsetCommit {} {}",
                request.group_id,
                offsets.join(",")
            ));
            respond(
                &OffsetCommitResponse {
                    topics: request
                        .topics
                        .into_iter()
                        .map(|topic| OffsetCommitResponseTopic {
                            name: topic.name,
                            partitions: topic
                                .partitions
                                .into_iter()
                                .map(|partition| OffsetCommitResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code: 0,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                offset_commit_request::FLEXIBLE_MIN,
            )
        }
        other => panic!("unexpected request: api key {other} v{version}"),
    }
}

/// The scripted broker and the log of the mutations it received.
struct Broker {
    inner: krabka_client_core::MockBroker,
    log: Arc<Mutex<Vec<String>>>,
}

impl Broker {
    async fn start() -> Self {
        let port = Arc::new(Mutex::new(0_u16));
        let seen = Arc::clone(&port);
        let log = Arc::new(Mutex::new(Vec::new()));
        let writes = Arc::clone(&log);
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, frame| {
            Some(handle(
                *seen.lock().unwrap(),
                &writes,
                api_key,
                version,
                frame,
            ))
        })
        .await;
        *port.lock().unwrap() = inner.addr.port();
        Self { inner, log }
    }

    /// The mutations received since the last call.
    fn mutations(&self) -> Vec<String> {
        std::mem::take(&mut *self.log.lock().unwrap())
    }

    fn port(&self) -> u16 {
        self.inner.addr.port()
    }

    fn args(&self, argv: &[&str]) -> Vec<String> {
        [
            "consumer-groups",
            "--bootstrap-server",
            &self.inner.addr.to_string(),
        ]
        .iter()
        .chain(argv)
        .map(|arg| (*arg).to_owned())
        .collect()
    }

    fn stop(self) {
        self.inner.stop();
    }
}

/// One invocation and everything it should produce.
struct Case {
    argv: &'static [&'static str],
    code: i32,
    stdout: String,
    stderr: String,
    mutations: Vec<&'static str>,
}

fn case(argv: &'static [&'static str], code: i32, stdout: &str, stderr: &str) -> Case {
    Case {
        argv,
        code,
        stdout: stdout.to_owned(),
        stderr: stderr.to_owned(),
        mutations: Vec::new(),
    }
}

async fn run_cases(broker: &Broker, cases: Vec<Case>) {
    for case in cases {
        let out = krabka(broker.args(case.argv)).await;
        check!(
            (
                out.code,
                out.stdout.as_str(),
                out.stderr.as_str(),
                broker.mutations()
            ) == (
                Some(case.code),
                case.stdout.as_str(),
                case.stderr.as_str(),
                case.mutations
                    .iter()
                    .map(|mutation| (*mutation).to_owned())
                    .collect::<Vec<_>>()
            ),
            "{:?}",
            case.argv
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_filters_by_state_and_type_as_kafka_does() {
    let broker = Broker::start().await;
    run_cases(
        &broker,
        vec![
            case(&["--list"], 0, "idle\norders-app\ndenied\n", ""),
            case(
                &["--list", "--state", "--type"],
                0,
                concat!(
                    "GROUP                     TYPE                 STATE               \n",
                    "idle                      Classic              Empty               \n",
                    "orders-app                Consumer             Stable              \n",
                    "denied                    Classic              Empty               \n",
                ),
                "",
            ),
            case(
                &["--list", "--state", "stable"],
                0,
                concat!(
                    "GROUP                     STATE               \n",
                    "orders-app                Stable              \n",
                ),
                "",
            ),
            case(
                &["--list", "--type", "classic"],
                0,
                concat!(
                    "GROUP                     TYPE                \n",
                    "idle                      Classic             \n",
                    "denied                    Classic             \n",
                ),
                "",
            ),
            case(
                &["--list", "--state", "bogus"],
                1,
                "",
                "krabka consumer-groups: Invalid state list 'bogus'. Valid states are: Dead, CompletingRebalance, Empty, Stable, Assigning, Reconciling, PreparingRebalance\n",
            ),
        ],
    )
    .await;
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_prints_offsets_lag_members_and_state() {
    let broker = Broker::start().await;
    let coordinator = format!("127.0.0.1:{}  (1)", broker.port());
    run_cases(
        &broker,
        vec![
            case(
                &["--describe", "--all-groups"],
                1,
                concat!(
                    "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID",
                    "\nidle            orders          0          4               15              11              -               -               -",
                    "\n",
                    "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID",
                    "\norders-app      orders          0          10              15              5               m1              /10.0.0.1       cli-1",
                    "\norders-app      orders          1          3               8               5               -               -               -",
                    "\n",
                ),
                concat!(
                    "Error: Executing consumer group command failed for group 'denied' due to org.apache.kafka.common.errors.GroupAuthorizationException: Group authorization failed.\n",
                    "\n",
                    "Consumer group 'idle' has no active members.\n",
                ),
            ),
            case(
                &["--describe", "--group", "nope"],
                1,
                "",
                "Error: Executing consumer group command failed for group 'nope' due to org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.\n",
            ),
            case(
                &["--describe", "--group", "orders-app", "--members", "--verbose"],
                0,
                concat!(
                    "\nGROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     CURRENT-EPOCH   CURRENT-ASSIGNMENT   TARGET-EPOCH    TARGET-ASSIGNMENT   \n",
                    "orders-app      m1              /10.0.0.1       cli-1           1               5               orders:0             7               orders:0,1          \n",
                ),
                "",
            ),
            case(
                &["--describe", "--group", "idle", "--members"],
                0,
                "\nGROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     \n",
                "\nConsumer group 'idle' has no active members.\n",
            ),
        ],
    )
    .await;
    run_cases(
        &broker,
        vec![Case {
            argv: &["--describe", "--group", "orders-app", "--group", "idle", "--state", "--verbose"],
            code: 0,
            stdout: format!(
                concat!(
                    "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                GROUP-EPOCH     TARGET-ASSIGNMENT-EPOCH   #MEMBERS",
                    "\nidle            {0:<25} -                    Empty                -               -                         0",
                    "\n",
                    "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                GROUP-EPOCH     TARGET-ASSIGNMENT-EPOCH   #MEMBERS",
                    "\norders-app      {0:<25} uniform              Stable               7               7                         1",
                    "\n",
                ),
                coordinator
            ),
            stderr: "\nConsumer group 'idle' has no active members.\n".to_owned(),
            mutations: Vec::new(),
        }],
    )
    .await;
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_asks_describes_under_dry_run_and_reports_each_group() {
    let broker = Broker::start().await;
    let partial = concat!(
        "\nError: Deletion of some consumer groups failed:\n",
        "* Group 'orders-app' could not be deleted due to: org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.\n",
        "\nThese consumer groups were deleted successfully: 'idle'\n",
    );
    run_cases(
        &broker,
        vec![
            case(
                &["--delete", "--group", "idle"],
                2,
                "",
                "krabka consumer-groups: refusing to prompt for confirmation on a non-interactive stdin; pass --yes to proceed\n",
            ),
            case(
                &["--delete", "--group", "orders-app", "--group", "idle", "--dry-run"],
                1,
                partial,
                "DRY RUN: no change was made.\n",
            ),
            Case {
                mutations: vec!["DeleteGroups idle,orders-app"],
                ..case(
                    &["--delete", "--group", "orders-app", "--group", "idle", "--yes"],
                    1,
                    partial,
                    "",
                )
            },
            Case {
                mutations: vec!["DeleteGroups idle"],
                ..case(
                    &["--delete", "--group", "idle", "--yes"],
                    0,
                    "Deletion of requested consumer groups ('idle') was successful.\n",
                    "",
                )
            },
        ],
    )
    .await;
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_offsets_reports_each_partition_as_kafka_does() {
    let broker = Broker::start().await;
    let succeeded = concat!(
        "Request succeeded for deleting offsets from group idle.\n",
        "\nTOPIC           PARTITION  STATUS         ",
        "\norders          0          Successful     ",
        "\norders          1          Successful     ",
        "\n",
    );
    run_cases(
        &broker,
        vec![
            case(
                &["--delete-offsets", "--group", "idle", "--topic", "orders", "--dry-run"],
                0,
                succeeded,
                "DRY RUN: no change was made.\n",
            ),
            Case {
                mutations: vec!["OffsetDelete idle orders-0,orders-1"],
                ..case(
                    &["--delete-offsets", "--group", "idle", "--topic", "orders", "--yes"],
                    0,
                    succeeded,
                    "",
                )
            },
            Case {
                mutations: vec!["OffsetDelete orders-app orders-1"],
                ..case(
                    &["--delete-offsets", "--group", "orders-app", "--topic", "orders:1", "--yes"],
                    1,
                    concat!(
                        "\nError: Encountered some partition-level error, see the follow-up details.\n",
                        "\nTOPIC           PARTITION  STATUS         ",
                        "\norders          1          Error: org.apache.kafka.common.errors.GroupSubscribedToTopicException: Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
                        "\n",
                    ),
                    "",
                )
            },
            Case {
                mutations: vec!["OffsetDelete nope orders-0"],
                ..case(
                    &["--delete-offsets", "--group", "nope", "--topic", "orders:0", "--yes"],
                    1,
                    concat!(
                        "\nError: The group id does not exist.\n",
                        "\nTOPIC           PARTITION  STATUS         ",
                        "\norders          0          Error: org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.",
                        "\n",
                    ),
                    "",
                )
            },
        ],
    )
    .await;
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_offsets_plans_from_the_log_and_commits_only_on_execute() {
    let broker = Broker::start().await;
    let table = |rows: &[(i32, i64)]| {
        let mut text = "\nGROUP           TOPIC           PARTITION  NEW-OFFSET".to_owned();
        for (partition, offset) in rows {
            let _ = write!(
                text,
                "\nidle            orders          {partition:<10} {offset}"
            );
        }
        text.push('\n');
        text
    };
    let dry = "DRY RUN: no change was made.\n";
    run_cases(
        &broker,
        vec![
            case(
                &["--reset-offsets", "--group", "idle", "--topic", "orders", "--to-earliest", "--dry-run"],
                0,
                &table(&[(0, 2), (1, 0)]),
                dry,
            ),
            Case {
                mutations: vec!["OffsetCommit idle orders-0=15,orders-1=8"],
                ..case(
                    &["--reset-offsets", "--group", "idle", "--topic", "orders", "--to-latest", "--execute"],
                    0,
                    &table(&[(0, 15), (1, 8)]),
                    "",
                )
            },
            case(
                &["--reset-offsets", "--group", "idle", "--topic", "orders:0", "--shift-by", "20", "--dry-run"],
                0,
                &table(&[(0, 15)]),
                "DRY RUN: no change was made.\nWARN New offset (24) is higher than latest offset for topic partition orders-0. Value will be set to 15\n",
            ),
            case(
                &["--reset-offsets", "--group", "idle", "--topic", "orders:1", "--to-offset", "-5", "--dry-run"],
                0,
                &table(&[(1, 0)]),
                "DRY RUN: no change was made.\nWARN New offset (-5) is lower than earliest offset for topic partition orders-1. Value will be set to 0\n",
            ),
            case(
                &["--reset-offsets", "--group", "idle", "--topic", "orders", "--to-datetime", "2024-01-01T00:00:00.000", "--dry-run"],
                0,
                &format!(
                    "\nWarn: Partition 1 from topic orders is empty. Falling back to latest known offset.\n{}",
                    table(&[(0, 12), (1, 8)])
                ),
                dry,
            ),
            case(
                &["--reset-offsets", "--group", "idle", "--all-topics", "--to-current", "--dry-run"],
                0,
                &table(&[(0, 4)]),
                dry,
            ),
            case(
                &["--reset-offsets", "--group", "orders-app", "--topic", "orders", "--to-earliest", "--execute"],
                0,
                concat!(
                    "\nError: Assignments can only be reset if the group 'orders-app' is inactive, but the current state is Stable.\n",
                    "\nGROUP           TOPIC           PARTITION  NEW-OFFSET\n",
                ),
                "",
            ),
            case(
                &["--reset-offsets", "--group", "idle", "--topic", "orders:7", "--to-offset", "1", "--execute"],
                1,
                "",
                "krabka consumer-groups: The partitions \"orders-7\" do not exist\n",
            ),
        ],
    )
    .await;
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_offsets_without_dry_run_or_execute_is_refused_by_clap() {
    let out = krabka(
        [
            "consumer-groups",
            "--reset-offsets",
            "--group",
            "g",
            "--topic",
            "t",
            "--to-earliest",
            "--bootstrap-server",
            "127.0.0.1:1",
        ]
        .map(ToOwned::to_owned)
        .to_vec(),
    )
    .await;
    check!(out.code == Some(2));
    check!(out.stdout.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_json_envelope_of_a_reset_names_each_new_offset() {
    let broker = Broker::start().await;
    let mut argv = vec!["--output".to_owned(), "json".to_owned()];
    argv.extend(broker.args(&[
        "--reset-offsets",
        "--group",
        "idle",
        "--topic",
        "orders:0",
        "--to-latest",
        "--dry-run",
    ]));
    let out = krabka(argv).await;
    check!(out.code == Some(0));
    check!(
        serde_json::from_str::<serde_json::Value>(&out.stdout).unwrap()
            == serde_json::json!({
                "data": {
                    "plans": [{"group": "idle", "topic": "orders", "partition": 0, "new_offset": 15}],
                    "error": null,
                },
                "dry_run": true,
            })
    );
    check!(broker.mutations().is_empty());
    broker.stop();
}
