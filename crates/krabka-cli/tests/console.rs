//! `krabka console-consumer` and `krabka console-producer`, run as the built
//! binary against a scripted broker, with stdout and stderr checked apart.

use std::{
    collections::BTreeSet,
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
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as HeartbeatTopicPartitions,
        consumer_group_heartbeat_request::{self, ConsumerGroupHeartbeatRequest},
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
        consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition},
        consumer_protocol_subscription::ConsumerProtocolSubscription,
        fetch_request::{self, FetchRequest, FetchTopic},
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        find_coordinator_request,
        find_coordinator_response::FindCoordinatorResponse,
        heartbeat_request,
        heartbeat_response::HeartbeatResponse,
        init_producer_id_request,
        init_producer_id_response::InitProducerIdResponse,
        join_group_request::{self, JoinGroupRequest},
        join_group_response::JoinGroupResponse,
        leave_group_request,
        leave_group_response::LeaveGroupResponse,
        list_offsets_request::{self, ListOffsetsRequest},
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request::{self, MetadataRequest},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_commit_request::{self, OffsetCommitRequest},
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
        offset_fetch_request::{self, OffsetFetchRequest},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
        },
        produce_request::{self, ProduceRequest},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
        sync_group_request::{self, SyncGroupRequest},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch, RecordHeader, RecordsPayload, TimestampType},
};
use serde_json::{Value, json};

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
const LIST_OFFSETS_VERSION: i16 = 7;
const OFFSET_COMMIT_VERSION: i16 = 8;
const CONSUMER_GROUP_HEARTBEAT_VERSION: i16 = 1;
/// `OFFSET_OUT_OF_RANGE`.
const OFFSET_OUT_OF_RANGE: i16 = 1;
/// The line that `ConsoleConsumer.maybePrintConsumerProtocolMessage` prints
/// on stderr for a subscribed run of the classic protocol.
const KIP_848_LINE: &str = "The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol";
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

/// The metadata of one broker that leads partition 0 of each of `topics`.
/// The broker advertises port 0, as a single in-process broker does, so every
/// client routes to the bootstrap address.
fn metadata_of(topics: &[(&str, Uuid)]) -> MetadataResponse {
    MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: 0,
            host: "127.0.0.1".into(),
            port: 0,
            ..Default::default()
        }],
        topics: topics
            .iter()
            .map(|(name, topic_id)| MetadataResponseTopic {
                name: Some((*name).into()),
                topic_id: *topic_id,
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 0,
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn metadata() -> Vec<u8> {
    encoded(
        &metadata_of(&[(TOPIC, TOPIC_ID)]),
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

/// What the scripted cluster holds: the topics, the log of each, and how
/// the group coordinator and the leader answer.
#[derive(Clone)]
struct Cluster {
    /// The topics in the metadata, in the order they appear.
    topics: Vec<(&'static str, Uuid)>,
    /// A topic that joins the metadata after this many `Metadata` requests.
    created_later: Option<((&'static str, Uuid), usize)>,
    /// The log start offset of every partition.
    log_start: i64,
    /// The records of partition 0 of every topic, from offset `log_start`.
    values: Vec<&'static str>,
    /// Batches of `LogAppendTime`.
    log_append_time: bool,
    /// The coordinator answers `OffsetFetch` with this committed offset.
    committed: i64,
    /// The leader does not answer `Fetch`.
    silent_fetch: bool,
    /// The topics of the last `JoinGroup`, which `SyncGroup` assigns.
    joined: Arc<Mutex<Vec<String>>>,
}

impl Default for Cluster {
    fn default() -> Self {
        Self {
            topics: vec![(TOPIC, TOPIC_ID)],
            created_later: None,
            log_start: 0,
            values: vec!["v0", "v1", "v2"],
            log_append_time: false,
            committed: -1,
            silent_fetch: false,
            joined: Arc::default(),
        }
    }
}

/// A request that the scripted cluster decoded, cut down to what the tests
/// check.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    /// `ListOffsets` for `(topic, partition, timestamp)`.
    ListOffsets(Vec<(String, i32, i64)>),
    /// `Fetch` of `(topic, partition, offset)`.
    Fetch(Vec<(String, i32, i64)>),
    /// `JoinGroup` with the topics of its first protocol.
    JoinGroup(Vec<String>),
    /// `ConsumerGroupHeartbeat` with its topics and its server assignor.
    ConsumerGroupHeartbeat(Option<Vec<String>>, Option<String>),
    /// `OffsetCommit` of `(topic, partition, offset)`.
    OffsetCommit(Vec<(String, i32, i64)>),
    /// Any other request, by API key.
    Other(i16),
}

/// The API versions that the scripted cluster serves.
const CLUSTER_APIS: [(i16, i16, i16); 11] = [
    (metadata_request::API_KEY, 0, METADATA_VERSION),
    (fetch_request::API_KEY, 4, FETCH_VERSION),
    (list_offsets_request::API_KEY, 1, LIST_OFFSETS_VERSION),
    (
        find_coordinator_request::API_KEY,
        0,
        FIND_COORDINATOR_VERSION,
    ),
    (join_group_request::API_KEY, 0, JOIN_GROUP_VERSION),
    (sync_group_request::API_KEY, 0, SYNC_GROUP_VERSION),
    (offset_fetch_request::API_KEY, 1, OFFSET_FETCH_VERSION),
    (offset_commit_request::API_KEY, 2, OFFSET_COMMIT_VERSION),
    (heartbeat_request::API_KEY, 0, HEARTBEAT_VERSION),
    (leave_group_request::API_KEY, 0, LEAVE_GROUP_VERSION),
    (
        consumer_group_heartbeat_request::API_KEY,
        0,
        CONSUMER_GROUP_HEARTBEAT_VERSION,
    ),
];

/// A running scripted cluster and the requests it decoded.
struct ClusterBroker {
    broker: krabka_client_core::MockBroker,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl ClusterBroker {
    fn address(&self) -> String {
        self.broker.addr.to_string()
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The decoded requests of one kind, `ApiVersions` and the rest left out.
    fn seen_where(&self, keep: impl Fn(&Seen) -> bool) -> Vec<Seen> {
        self.seen().into_iter().filter(|seen| keep(seen)).collect()
    }

    fn keys(&self) -> BTreeSet<i16> {
        self.seen()
            .iter()
            .map(|seen| match seen {
                Seen::ListOffsets(_) => list_offsets_request::API_KEY,
                Seen::Fetch(_) => fetch_request::API_KEY,
                Seen::JoinGroup(_) => join_group_request::API_KEY,
                Seen::ConsumerGroupHeartbeat(..) => consumer_group_heartbeat_request::API_KEY,
                Seen::OffsetCommit(_) => offset_commit_request::API_KEY,
                Seen::Other(key) => *key,
            })
            .collect()
    }

    fn stop(self) {
        self.broker.stop();
    }
}

/// The request body after the request header: the client id, then the
/// tagged fields of a flexible header.
fn request_body(body: &[u8], flexible: bool) -> &[u8] {
    let client_id = usize::try_from(i16::from_be_bytes([body[0], body[1]]).max(0)).unwrap();
    let rest = &body[2 + client_id..];
    if flexible { &rest[1..] } else { rest }
}

fn decode<'a, T: krabka_protocol::Decode<'a>>(
    body: &'a [u8],
    version: i16,
    flexible_min: i16,
) -> T {
    let mut rest = request_body(body, version >= flexible_min);
    T::decode(&mut rest, version).expect("a valid request")
}

/// A response body for `version`, with the response header's tagged-fields
/// byte when the version is flexible.
fn encoded<T: Encode>(message: &T, version: i16, flexible_min: i16) -> Vec<u8> {
    let mut body = Vec::new();
    if version >= flexible_min {
        body.push(0);
    }
    message
        .encode(&mut body, version)
        .expect("encode a canned response");
    body
}

/// One request to the scripted cluster, and how many `Metadata` requests
/// came before it.
#[derive(Clone, Copy)]
struct Asked<'a> {
    api_key: i16,
    version: i16,
    body: &'a [u8],
    requests: usize,
}

/// Starts a broker that plays a whole single-node cluster: metadata, the
/// leader of partition 0 of each topic, and the group coordinator for both
/// group protocols. Each request is decoded, answered as Kafka answers it,
/// and logged.
async fn cluster(cluster: Cluster) -> ClusterBroker {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    let api_versions = {
        let mut body = Vec::new();
        ApiVersionsResponse {
            api_keys: std::iter::once((api_versions_request::API_KEY, 0, 3))
                .chain(CLUSTER_APIS)
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
    let mut metadata_requests = 0;
    let broker = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
        let (seen, reply) = answer(&cluster, &mut metadata_requests, api_key, version, body);
        if api_key != api_versions_request::API_KEY {
            log.lock().unwrap().push(seen);
        }
        match api_key {
            k if k == api_versions_request::API_KEY => Some(api_versions.clone()),
            _ => reply,
        }
    })
    .await;
    ClusterBroker { broker, seen }
}

/// The topics of the metadata after `requests` earlier `Metadata` requests.
fn topics_now(cluster: &Cluster, requests: usize) -> Vec<(&'static str, Uuid)> {
    let mut topics = cluster.topics.clone();
    if let Some((topic, after)) = cluster.created_later
        && requests >= after
    {
        topics.push(topic);
    }
    topics
}

/// The name of a fetched topic: the name below `Fetch` v13, the name of the
/// id from v13.
fn topic_name(cluster: &Cluster, requests: usize, topic: &FetchTopic) -> String {
    if !topic.topic.is_empty() {
        return topic.topic.clone();
    }
    topics_now(cluster, requests)
        .iter()
        .find(|(_, id)| *id == topic.topic_id)
        .map_or_else(String::new, |(name, _)| (*name).to_owned())
}

/// The batch that holds `offset`, as a broker returns it: the whole log in
/// one batch from the log start, or nothing at the log end.
fn records_from(cluster: &Cluster, offset: i64) -> Vec<RecordBatch> {
    if offset >= log_end(cluster) {
        return Vec::new();
    }
    let mut batch = batch(cluster.log_start, &cluster.values);
    if cluster.log_append_time {
        batch.attributes = batch
            .attributes
            .with_timestamp_type(TimestampType::LogAppendTime);
        batch.max_timestamp = 9_000;
    }
    vec![batch]
}

fn log_end(cluster: &Cluster) -> i64 {
    cluster.log_start + i64::try_from(cluster.values.len()).unwrap()
}

/// The consumer-protocol assignment of partition 0 of every topic.
fn heartbeat_assignment(topics: &[(&str, Uuid)]) -> Assignment {
    Assignment {
        topic_partitions: topics
            .iter()
            .map(|(_, topic_id)| HeartbeatTopicPartitions {
                topic_id: *topic_id,
                partitions: vec![0],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The classic-protocol assignment of partition 0 of every topic.
fn classic_assignment(topics: &[String]) -> Vec<u8> {
    let mut bytes = 3_i16.to_be_bytes().to_vec();
    ConsumerProtocolAssignment {
        assigned_partitions: topics
            .iter()
            .map(|topic| TopicPartition {
                topic: topic.clone(),
                partitions: vec![0],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
    .encode(&mut bytes, 3)
    .unwrap();
    bytes
}

fn answer(
    cluster: &Cluster,
    metadata_requests: &mut usize,
    api_key: i16,
    version: i16,
    body: &[u8],
) -> (Seen, Option<Vec<u8>>) {
    let asked = Asked {
        api_key,
        version,
        body,
        requests: *metadata_requests,
    };
    let requests = asked.requests;
    match api_key {
        k if k == metadata_request::API_KEY => {
            answer_metadata(cluster, metadata_requests, requests, api_key, version, body)
        }
        k if k == list_offsets_request::API_KEY => answer_list_offsets(cluster, &asked),
        k if k == fetch_request::API_KEY => answer_fetch(cluster, &asked),
        k if k == find_coordinator_request::API_KEY => {
            let response = FindCoordinatorResponse {
                node_id: 0,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            };
            (
                Seen::Other(api_key),
                Some(encoded(
                    &response,
                    version,
                    find_coordinator_request::FLEXIBLE_MIN,
                )),
            )
        }
        k if k == join_group_request::API_KEY => answer_join_group(cluster, &asked),
        k if k == sync_group_request::API_KEY => answer_sync_group(cluster, &asked),
        k if k == consumer_group_heartbeat_request::API_KEY => {
            answer_consumer_group_heartbeat(cluster, &asked)
        }
        k if k == offset_fetch_request::API_KEY => answer_offset_fetch(cluster, &asked),
        k if k == offset_commit_request::API_KEY => answer_offset_commit(cluster, &asked),
        k if k == heartbeat_request::API_KEY => (
            Seen::Other(api_key),
            Some(encoded(
                &HeartbeatResponse::default(),
                version,
                heartbeat_request::FLEXIBLE_MIN,
            )),
        ),
        k if k == leave_group_request::API_KEY => (
            Seen::Other(api_key),
            Some(encoded(
                &LeaveGroupResponse::default(),
                version,
                leave_group_request::FLEXIBLE_MIN,
            )),
        ),
        _ => (Seen::Other(api_key), None),
    }
}

fn answer_metadata(
    cluster: &Cluster,
    metadata_requests: &mut usize,
    requests: usize,
    api_key: i16,
    version: i16,
    body: &[u8],
) -> (Seen, Option<Vec<u8>>) {
    *metadata_requests += 1;
    let request: MetadataRequest = decode(body, version, metadata_request::FLEXIBLE_MIN);
    let all = topics_now(cluster, requests);
    let topics = match &request.topics {
        None => all,
        Some(asked) => all
            .into_iter()
            .filter(|(name, id)| {
                asked
                    .iter()
                    .any(|topic| topic.name.as_deref() == Some(*name) || topic.topic_id == *id)
            })
            .collect(),
    };
    let reply = encoded(
        &metadata_of(&topics),
        version,
        metadata_request::FLEXIBLE_MIN,
    );
    (Seen::Other(api_key), Some(reply))
}

fn answer_list_offsets(cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked { version, body, .. } = *asked;
    let request: ListOffsetsRequest = decode(body, version, list_offsets_request::FLEXIBLE_MIN);
    let asked = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|partition| {
                (
                    topic.name.clone(),
                    partition.partition_index,
                    partition.timestamp,
                )
            })
        })
        .collect::<Vec<_>>();
    let response = ListOffsetsResponse {
        topics: request
            .topics
            .iter()
            .map(|topic| ListOffsetsTopicResponse {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| ListOffsetsPartitionResponse {
                        partition_index: partition.partition_index,
                        timestamp: -1,
                        // -2 is the log start, -1 the log end, and a
                        // timestamp the first record at or after it.
                        offset: if partition.timestamp == -2 {
                            cluster.log_start
                        } else if partition.timestamp == -1 {
                            log_end(cluster)
                        } else {
                            cluster.log_start + 1
                        },
                        leader_epoch: -1,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    (
        Seen::ListOffsets(asked),
        Some(encoded(
            &response,
            version,
            list_offsets_request::FLEXIBLE_MIN,
        )),
    )
}

fn answer_fetch(cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked {
        version,
        body,
        requests,
        ..
    } = *asked;
    let request: FetchRequest = decode(body, version, fetch_request::FLEXIBLE_MIN);
    let asked = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|partition| {
                (
                    topic_name(cluster, requests, topic),
                    partition.partition,
                    partition.fetch_offset,
                )
            })
        })
        .collect::<Vec<_>>();
    if cluster.silent_fetch {
        return (Seen::Fetch(asked), None);
    }
    let response = FetchResponse {
        responses: request
            .topics
            .iter()
            .map(|topic| FetchableTopicResponse {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        let offset = partition.fetch_offset;
                        if offset < cluster.log_start || offset > log_end(cluster) {
                            PartitionData {
                                partition_index: partition.partition,
                                error_code: OFFSET_OUT_OF_RANGE,
                                high_watermark: -1,
                                last_stable_offset: -1,
                                log_start_offset: -1,
                                ..Default::default()
                            }
                        } else {
                            PartitionData {
                                partition_index: partition.partition,
                                high_watermark: log_end(cluster),
                                last_stable_offset: log_end(cluster),
                                log_start_offset: cluster.log_start,
                                records: Some(RecordsPayload::V2(records_from(cluster, offset))),
                                ..Default::default()
                            }
                        }
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    (
        Seen::Fetch(asked),
        Some(encoded(&response, version, fetch_request::FLEXIBLE_MIN)),
    )
}

fn answer_join_group(cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked { version, body, .. } = *asked;
    let request: JoinGroupRequest = decode(body, version, join_group_request::FLEXIBLE_MIN);
    let topics = request
        .protocols
        .first()
        .map(|protocol| {
            let mut metadata = &protocol.metadata[..];
            let subscription_version = i16::from_be_bytes([metadata[0], metadata[1]]);
            metadata = &metadata[2..];
            ConsumerProtocolSubscription::decode(&mut metadata, subscription_version)
                .expect("a valid subscription")
                .topics
        })
        .unwrap_or_default();
    cluster.joined.lock().unwrap().clone_from(&topics);
    let response = JoinGroupResponse {
        generation_id: 1,
        protocol_type: Some("consumer".into()),
        protocol_name: request
            .protocols
            .first()
            .map(|protocol| protocol.name.clone()),
        leader: "other-member".into(),
        member_id: "member-1".into(),
        ..Default::default()
    };
    (
        Seen::JoinGroup(topics),
        Some(encoded(
            &response,
            version,
            join_group_request::FLEXIBLE_MIN,
        )),
    )
}

fn answer_sync_group(cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked {
        api_key,
        version,
        body,
        ..
    } = *asked;
    let request: SyncGroupRequest = decode(body, version, sync_group_request::FLEXIBLE_MIN);
    let topics = cluster.joined.lock().unwrap().clone();
    let response = SyncGroupResponse {
        protocol_type: request.protocol_type.clone(),
        protocol_name: request.protocol_name.clone(),
        assignment: classic_assignment(&topics).into(),
        ..Default::default()
    };
    (
        Seen::Other(api_key),
        Some(encoded(
            &response,
            version,
            sync_group_request::FLEXIBLE_MIN,
        )),
    )
}

fn answer_consumer_group_heartbeat(
    cluster: &Cluster,
    asked: &Asked<'_>,
) -> (Seen, Option<Vec<u8>>) {
    let Asked {
        version,
        body,
        requests,
        ..
    } = *asked;
    let request: ConsumerGroupHeartbeatRequest = decode(
        body,
        version,
        consumer_group_heartbeat_request::FLEXIBLE_MIN,
    );
    let leaving = request.member_epoch < 0;
    let response = ConsumerGroupHeartbeatResponse {
        member_id: Some(request.member_id.clone()),
        member_epoch: if leaving { request.member_epoch } else { 1 },
        heartbeat_interval_ms: 3_000,
        assignment: (!leaving).then(|| heartbeat_assignment(&topics_now(cluster, requests))),
        ..Default::default()
    };
    (
        Seen::ConsumerGroupHeartbeat(
            request.subscribed_topic_names.clone(),
            request.server_assignor.clone(),
        ),
        Some(encoded(
            &response,
            version,
            consumer_group_heartbeat_request::FLEXIBLE_MIN,
        )),
    )
}

fn answer_offset_fetch(cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked {
        api_key,
        version,
        body,
        ..
    } = *asked;
    let request: OffsetFetchRequest = decode(body, version, offset_fetch_request::FLEXIBLE_MIN);
    let response = OffsetFetchResponse {
        topics: request
            .topics
            .unwrap_or_default()
            .iter()
            .map(|topic| OffsetFetchResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|partition| OffsetFetchResponsePartition {
                        partition_index: *partition,
                        committed_offset: cluster.committed,
                        committed_leader_epoch: -1,
                        metadata: Some(String::new()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    (
        Seen::Other(api_key),
        Some(encoded(
            &response,
            version,
            offset_fetch_request::FLEXIBLE_MIN,
        )),
    )
}

fn answer_offset_commit(_cluster: &Cluster, asked: &Asked<'_>) -> (Seen, Option<Vec<u8>>) {
    let Asked { version, body, .. } = *asked;
    let request: OffsetCommitRequest = decode(body, version, offset_commit_request::FLEXIBLE_MIN);
    let committed = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|partition| {
                (
                    topic.name.clone(),
                    partition.partition_index,
                    partition.committed_offset,
                )
            })
        })
        .collect::<Vec<_>>();
    let response = OffsetCommitResponse {
        topics: request
            .topics
            .iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| OffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    (
        Seen::OffsetCommit(committed),
        Some(encoded(
            &response,
            version,
            offset_commit_request::FLEXIBLE_MIN,
        )),
    )
}

fn console_consumer(broker: &ClusterBroker, flags: &[&str]) -> Vec<String> {
    let address = broker.address();
    let mut argv = args(&["console-consumer", "--bootstrap-server", &address]);
    argv.extend(flags.iter().map(|flag| (*flag).to_owned()));
    argv
}

fn is_fetch(seen: &Seen) -> bool {
    matches!(seen, Seen::Fetch(_))
}

fn is_list_offsets(seen: &Seen) -> bool {
    matches!(seen, Seen::ListOffsets(_))
}

/// The `--partition` path assigns the partition and joins no group, and
/// `--offset` says where it starts: `earliest` and `latest` through
/// `ListOffsets` with timestamp -2 and -1, as `seekToBeginning` and
/// `seekToEnd` do, a number with no `ListOffsets` at all. The default is
/// `latest`, and `--from-beginning` is `earliest`.
#[tokio::test(flavor = "multi_thread")]
async fn the_partition_path_starts_where_offset_says_without_a_group() {
    type Case<'a> = (&'a [&'a str], &'a str, &'a [Seen], i64);
    let log_start = 5;
    let cases: [Case<'_>; 5] = [
        (
            &["--offset", "earliest", "--max-messages", "2"],
            "v0\nv1\n",
            &[Seen::ListOffsets(vec![(TOPIC.into(), 0, -2)])],
            log_start,
        ),
        (
            &["--from-beginning", "--max-messages", "1"],
            "v0\n",
            &[Seen::ListOffsets(vec![(TOPIC.into(), 0, -2)])],
            log_start,
        ),
        (
            &["--offset", "6", "--max-messages", "2"],
            "v1\nv2\n",
            &[],
            6,
        ),
        (
            &["--offset", "latest", "--timeout-ms", "300"],
            "",
            &[Seen::ListOffsets(vec![(TOPIC.into(), 0, -1)])],
            log_start + 3,
        ),
        (
            &["--timeout-ms", "300"],
            "",
            &[Seen::ListOffsets(vec![(TOPIC.into(), 0, -1)])],
            log_start + 3,
        ),
    ];
    for (flags, stdout, list_offsets, first_fetch) in cases {
        let broker = cluster(Cluster {
            log_start,
            ..Cluster::default()
        })
        .await;
        let argv = [&["--topic", TOPIC, "--partition", "0"][..], flags].concat();
        let out = run(console_consumer(&broker, &argv), b"").await;
        let count = stdout.lines().count();
        check!(
            (out.code, out.stdout.as_str(), out.stderr)
                == (
                    Some(0),
                    stdout,
                    format!("Processed a total of {count} messages\n")
                ),
            "{flags:?}"
        );
        check!(
            broker.seen_where(is_list_offsets) == list_offsets,
            "{flags:?}"
        );
        check!(
            broker.seen_where(is_fetch).first()
                == Some(&Seen::Fetch(vec![(TOPIC.into(), 0, first_fetch)])),
            "{flags:?}"
        );
        let keys = broker.keys();
        for group_api in [
            find_coordinator_request::API_KEY,
            join_group_request::API_KEY,
            consumer_group_heartbeat_request::API_KEY,
            offset_commit_request::API_KEY,
        ] {
            check!(!keys.contains(&group_api), "{flags:?}: {keys:?}");
        }
        broker.stop();
    }
}

/// An offset out of range on the `--partition` path resets by
/// `auto.offset.reset`, as the consumer's fetcher does: `earliest` to the log
/// start, `latest` to the log end, and `none` fails the run.
#[tokio::test(flavor = "multi_thread")]
async fn an_out_of_range_offset_resets_by_auto_offset_reset() {
    let cases = [
        (
            "earliest",
            Some(0),
            "v0\n",
            Some(Seen::Fetch(vec![(TOPIC.into(), 0, 5)])),
        ),
        (
            "latest",
            Some(0),
            "",
            Some(Seen::Fetch(vec![(TOPIC.into(), 0, 8)])),
        ),
        ("none", Some(1), "", None),
    ];
    for (reset, code, stdout, refetch) in cases {
        let broker = cluster(Cluster {
            log_start: 5,
            ..Cluster::default()
        })
        .await;
        let reset_property = format!("auto.offset.reset={reset}");
        let out = run(
            console_consumer(
                &broker,
                &[
                    "--topic",
                    TOPIC,
                    "--partition",
                    "0",
                    "--offset",
                    "2",
                    "--max-messages",
                    "1",
                    "--timeout-ms",
                    "1000",
                    "--command-property",
                    &reset_property,
                ],
            ),
            b"",
        )
        .await;
        check!(
            (out.code, out.stdout.as_str()) == (code, stdout),
            "{reset}: {}",
            out.stderr
        );
        let fetches = broker.seen_where(is_fetch);
        check!(
            fetches.first() == Some(&Seen::Fetch(vec![(TOPIC.into(), 0, 2)])),
            "{reset}"
        );
        check!(fetches.get(1) == refetch.as_ref(), "{reset}");
        if reset == "none" {
            check!(
                out.stderr
                    == "Processed a total of 0 messages\nkrabka console-consumer: Unknown error when running consumer: log truncation detected on orders-0: fetch offset 2 is past the leader's log; safe offset 5\n"
            );
        }
        broker.stop();
    }
}

/// `--timeout-ms` ends a run that receives nothing, whether the leader
/// answers with no records or does not answer, and the run still exits 0 as
/// the JVM tool does.
#[tokio::test(flavor = "multi_thread")]
async fn timeout_ms_bounds_a_run_that_receives_nothing() {
    for silent_fetch in [false, true] {
        let broker = cluster(Cluster {
            values: if silent_fetch { vec!["v0"] } else { Vec::new() },
            silent_fetch,
            ..Cluster::default()
        })
        .await;
        let started = std::time::Instant::now();
        let out = run(
            console_consumer(
                &broker,
                &[
                    "--topic",
                    TOPIC,
                    "--partition",
                    "0",
                    "--offset",
                    "earliest",
                    "--timeout-ms",
                    "300",
                ],
            ),
            b"",
        )
        .await;
        check!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "silent: {silent_fetch}"
        );
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str())
                == (Some(0), "", "Processed a total of 0 messages\n"),
            "silent: {silent_fetch}"
        );
        broker.stop();
    }
}

/// Every `--formatter-property` that the default formatter prints reaches
/// stdout in the JVM tool's order, and a `LogAppendTime` batch prints its
/// timestamp type as Kafka's `TimestampType` does.
#[tokio::test(flavor = "multi_thread")]
async fn the_formatter_prints_the_record_fields_it_is_asked_for() {
    let cases = [
        (
            false,
            "CreateTime:1000\tPartition:0\tOffset:0\th:x\tk0\tv0\nCreateTime:1001\tPartition:0\tOffset:1\th:x\tk1\tv1\n",
        ),
        (
            true,
            "LogAppendTime:9000\tPartition:0\tOffset:0\th:x\tk0\tv0\nLogAppendTime:9000\tPartition:0\tOffset:1\th:x\tk1\tv1\n",
        ),
    ];
    for (log_append_time, stdout) in cases {
        let broker = cluster(Cluster {
            log_append_time,
            ..Cluster::default()
        })
        .await;
        let out = run(
            console_consumer(
                &broker,
                &[
                    "--topic",
                    TOPIC,
                    "--partition",
                    "0",
                    "--offset",
                    "earliest",
                    "--max-messages",
                    "2",
                    "--formatter-property",
                    "print.timestamp=true",
                    "--formatter-property",
                    "print.key=true",
                    "--formatter-property",
                    "print.offset=true",
                    "--formatter-property",
                    "print.headers=true",
                    "--formatter-property",
                    "print.partition=true",
                ],
            ),
            b"",
        )
        .await;
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str())
                == (Some(0), stdout, "Processed a total of 2 messages\n"),
            "log append time: {log_append_time}"
        );
        broker.stop();
    }
}

/// Under `--output json` each record is one `{"data": ...}` line on stdout,
/// and stderr stays empty.
#[tokio::test(flavor = "multi_thread")]
async fn json_output_is_one_line_per_record() {
    let broker = cluster(Cluster::default()).await;
    let mut argv = args(&["--output", "json"]);
    argv.extend(console_consumer(
        &broker,
        &[
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "1",
            "--max-messages",
            "1",
        ],
    ));
    let out = run(argv, b"").await;
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

/// The issue's first acceptance check: with `2>/dev/null`, stdout holds
/// exactly one record and nothing else. The subscribed path joins the group,
/// and stderr carries the JVM tool's KIP-848 line and the record count.
#[tokio::test(flavor = "multi_thread")]
async fn max_messages_one_writes_exactly_one_record_to_stdout() {
    let broker = cluster(Cluster::default()).await;
    let out = run(
        console_consumer(
            &broker,
            &["--topic", TOPIC, "--from-beginning", "--max-messages", "1"],
        ),
        b"",
    )
    .await;
    check!(out.code == Some(0));
    check!(out.stdout == "v0\n");
    check!(out.stderr == format!("{KIP_848_LINE}\nProcessed a total of 1 messages\n"));
    // The classic protocol joins, and joins again with its member id.
    let joins = broker.seen_where(|seen| matches!(seen, Seen::JoinGroup(_)));
    check!(!joins.is_empty());
    check!(
        joins
            .iter()
            .all(|join| *join == Seen::JoinGroup(vec![TOPIC.into()])),
        "{joins:?}"
    );
    check!(broker.seen_where(is_list_offsets) == [Seen::ListOffsets(vec![(TOPIC.into(), 0, -2)])]);
    broker.stop();
}

/// `--max-messages n` stops after exactly `n` records, although the broker
/// served more, and a named group commits only what was printed, as
/// `resetUnconsumedOffsets` makes the JVM tool do.
#[tokio::test(flavor = "multi_thread")]
async fn max_messages_stops_after_exactly_n_records_and_commits_them() {
    let broker = cluster(Cluster::default()).await;
    let out = run(
        console_consumer(
            &broker,
            &[
                "--topic",
                TOPIC,
                "--from-beginning",
                "--max-messages",
                "2",
                "--group",
                "workers",
                "--formatter-property",
                "print.key=true",
            ],
        ),
        b"",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr)
            == (
                Some(0),
                "k0\tv0\nk1\tv1\n",
                format!("{KIP_848_LINE}\nProcessed a total of 2 messages\n")
            )
    );
    check!(
        broker.seen_where(|seen| matches!(seen, Seen::OffsetCommit(_)))
            == [Seen::OffsetCommit(vec![(TOPIC.into(), 0, 2)])]
    );
    broker.stop();
}

/// `--include` subscribes to a pattern, as `subscribe(Pattern)` does, so a
/// topic that is created after the command starts joins the subscription.
/// Here the only matching topic appears at the third metadata refresh.
#[tokio::test(flavor = "multi_thread")]
async fn include_subscribes_to_topics_created_later() {
    const LATER_ID: Uuid = Uuid([9; 16]);
    let broker = cluster(Cluster {
        topics: vec![("other", Uuid([8; 16]))],
        created_later: Some((("orders-eu", LATER_ID), 3)),
        ..Cluster::default()
    })
    .await;
    let out = run(
        console_consumer(
            &broker,
            &[
                "--include",
                "orders.*",
                "--from-beginning",
                "--max-messages",
                "1",
                "--timeout-ms",
                "20000",
                "--command-property",
                "metadata.max.age.ms=200",
            ],
        ),
        b"",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr)
            == (
                Some(0),
                "v0\n",
                format!("{KIP_848_LINE}\nProcessed a total of 1 messages\n")
            )
    );
    let joins = broker.seen_where(|seen| matches!(seen, Seen::JoinGroup(_)));
    check!(
        joins.last() == Some(&Seen::JoinGroup(vec!["orders-eu".into()])),
        "{joins:?}"
    );
    broker.stop();
}

/// `group.protocol=consumer` joins with `ConsumerGroupHeartbeat` (KIP-848),
/// sends `group.remote.assignor` as the server assignor, and prints no
/// KIP-848 line.
#[tokio::test(flavor = "multi_thread")]
async fn the_consumer_group_protocol_joins_with_consumer_group_heartbeat() {
    let broker = cluster(Cluster::default()).await;
    let out = run(
        console_consumer(
            &broker,
            &[
                "--topic",
                TOPIC,
                "--from-beginning",
                "--max-messages",
                "1",
                "--command-property",
                "group.protocol=consumer",
                "--command-property",
                "group.remote.assignor=uniform",
            ],
        ),
        b"",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr.as_str())
            == (Some(0), "v0\n", "Processed a total of 1 messages\n")
    );
    check!(
        broker
            .seen_where(|seen| matches!(seen, Seen::ConsumerGroupHeartbeat(..)))
            .first()
            == Some(&Seen::ConsumerGroupHeartbeat(
                Some(vec![TOPIC.into()]),
                Some("uniform".into())
            ))
    );
    check!(!broker.keys().contains(&join_group_request::API_KEY));
    broker.stop();
}

/// `auto.offset.reset=by_duration:<ISO duration>` resets a partition with no
/// committed offset by `ListOffsets` for the timestamp that long ago
/// (KIP-1106), and reads from the offset the broker returns.
#[tokio::test(flavor = "multi_thread")]
async fn by_duration_resets_through_list_offsets_for_a_timestamp() {
    let broker = cluster(Cluster::default()).await;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let out = run(
        console_consumer(
            &broker,
            &[
                "--topic",
                TOPIC,
                "--max-messages",
                "1",
                "--command-property",
                "auto.offset.reset=by_duration:PT1H",
            ],
        ),
        b"",
    )
    .await;
    check!(
        (out.code, out.stdout.as_str()) == (Some(0), "v1\n"),
        "{}",
        out.stderr
    );
    let list_offsets = broker.seen_where(is_list_offsets);
    let Some(Seen::ListOffsets(asked)) = list_offsets.first() else {
        panic!("no ListOffsets: {list_offsets:?}");
    };
    let an_hour_ago = i64::try_from(started.as_millis()).unwrap() - 3_600_000;
    check!(asked.len() == 1);
    let (topic, partition, timestamp) = asked[0].clone();
    check!((topic.as_str(), partition) == (TOPIC, 0));
    check!(
        (an_hour_ago - 60_000..an_hour_ago + 60_000).contains(&timestamp),
        "{timestamp}"
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

async fn produce_broker_with(metadata: Vec<u8>) -> ProduceBroker {
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

/// `--socket-buffer-size`, `send.buffer.bytes` and `receive.buffer.bytes`
/// size the producer's broker sockets, -1 included, and the records still
/// reach the broker.
#[tokio::test(flavor = "multi_thread")]
async fn socket_buffer_sizes_produce_as_the_defaults_do() {
    let cases: [&[&str]; 3] = [
        &["--socket-buffer-size", "65536"],
        &[
            "--command-property",
            "send.buffer.bytes=-1",
            "--command-property",
            "receive.buffer.bytes=-1",
        ],
        &[
            "--command-property",
            "send.buffer.bytes=262144",
            "--command-property",
            "receive.buffer.bytes=8192",
        ],
    ];
    for flags in cases {
        let broker = produce_broker().await;
        let address = broker.broker.addr.to_string();
        let argv = [
            "console-producer",
            "--bootstrap-server",
            &address,
            "--topic",
            TOPIC,
        ]
        .iter()
        .chain(flags)
        .copied()
        .collect::<Vec<_>>();
        let out = run(args(&argv), b"one\n").await;
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str()) == (Some(0), "", ""),
            "{flags:?}"
        );
        check!(
            *broker.records.lock().unwrap() == [(None, Some("one".into()), Vec::new())],
            "{flags:?}"
        );
        broker.broker.stop();
    }
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

/// A topic that never appears in the metadata fails its record after
/// `--max-block-ms`, as `KafkaProducer.waitOnMetadata` does, and nothing is
/// sent. With `--sync` the run stops with the JVM producer's message; without
/// it `ErrorLoggingCallback` logs the error and the run counts the failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_topic_fails_after_max_block_ms() {
    let no_topics = encoded(
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
    let cases: [(&[&str], &str); 2] = [
        (
            &["--sync"],
            "krabka console-producer: Topic orders not present in metadata after 300 ms.\n",
        ),
        (
            &[],
            "krabka console-producer: 1 of 1 records failed to send\n",
        ),
    ];
    for (flags, stderr) in cases {
        let broker = produce_broker_with(no_topics.clone()).await;
        let address = broker.broker.addr.to_string();
        let mut argv = args(&[
            "console-producer",
            "--bootstrap-server",
            &address,
            "--topic",
            TOPIC,
            "--max-block-ms",
            "300",
        ]);
        argv.extend(args(flags));
        let out = run(argv, b"lost\n").await;
        check!(
            (out.code, out.stdout.as_str(), out.stderr.as_str()) == (Some(1), "", stderr),
            "{flags:?}"
        );
        check!(broker.records.lock().unwrap().is_empty());
        broker.broker.stop();
    }
}
