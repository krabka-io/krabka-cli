//! `krabka topics` against a scripted broker: the requests that each action
//! sends, and the whole stdout, stderr and exit code that it produces.
//!
//! The broker answers every API from one cluster description, as a Kafka
//! broker would: `Metadata` and `DescribeCluster` list it,
//! `DescribeTopicPartitions` pages through it at the request's partition
//! limit and cursor, and `DescribeConfigs` gives each topic's entries with
//! their sources. It records the body of every request, so a test compares
//! the exact requests that the command sent.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_partitions_request::{
            CreatePartitionsAssignment, CreatePartitionsRequest, CreatePartitionsTopic,
        },
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
        create_topics_request::{
            CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
        },
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
        describe_topic_partitions_request::{
            Cursor as RequestCursor, DescribeTopicPartitionsRequest, TopicRequest,
        },
        describe_topic_partitions_response::{
            Cursor as ResponseCursor, DescribeTopicPartitionsResponse,
            DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
        },
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::{
            ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
            OngoingTopicReassignment,
        },
        metadata_request::MetadataRequest,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    },
    primitives::{uuid::Uuid, varint::get_uvarint},
};
use serde_json::json;

// `4IgIMEgZQYS5IcnSzjHAqw`, the ID that Kafka printed for `orders` in the
// run that the conformance suite captured.
const ORDERS_ID: [u8; 16] = [
    0xe0, 0x88, 0x08, 0x30, 0x48, 0x19, 0x41, 0x84, 0xb9, 0x21, 0xc9, 0xd2, 0xce, 0x31, 0xc0, 0xab,
];

// The brokers that `Metadata` and `DescribeCluster` list.
const LIVE_BROKERS: [i32; 2] = [1, 2];

// `ConfigSource` wire ids.
const DYNAMIC_TOPIC: i8 = 1;
const STATIC_BROKER: i8 = 4;
const DEFAULT: i8 = 5;

// One partition of the scripted cluster.
#[derive(Debug, Clone)]
struct Part {
    leader: i32,
    replicas: &'static [i32],
    isr: &'static [i32],
    elr: &'static [i32],
    last_known_elr: &'static [i32],
}

const fn part(leader: i32, replicas: &'static [i32], isr: &'static [i32]) -> Part {
    Part {
        leader,
        replicas,
        isr,
        elr: &[],
        last_known_elr: &[],
    }
}

// One topic of the scripted cluster.
#[derive(Debug, Clone)]
struct Topic {
    name: &'static str,
    id: [u8; 16],
    partitions: Vec<Part>,
    // Each config's name, value and source.
    configs: &'static [(&'static str, &'static str, i8)],
    // The error that `DescribeTopicPartitions` gives the topic.
    describe_error: i16,
}

fn topic(name: &'static str, id: [u8; 16], partitions: Vec<Part>) -> Topic {
    Topic {
        name,
        id,
        partitions,
        configs: &[
            ("cleanup.policy", "delete", DEFAULT),
            ("min.insync.replicas", "1", DEFAULT),
        ],
        describe_error: 0,
    }
}

// The cluster that most tests read:
//
// - `orders`: `min.insync.replicas=2` and `retention.ms=1000` set on the
//   topic. Partition 0 is healthy and at the minimum, partition 1 has no
//   leader, and partition 2 has one in-sync replica of two.
// - `alpha`: a reassignment adds broker 3 to partition 0.
// - `bravo`: one replica, in sync.
// - `__consumer_offsets`: `segment.bytes` from the broker's static config.
fn cluster() -> Vec<Topic> {
    vec![
        Topic {
            configs: &[
                ("cleanup.policy", "delete", DEFAULT),
                ("min.insync.replicas", "2", DYNAMIC_TOPIC),
                ("retention.ms", "1000", DYNAMIC_TOPIC),
            ],
            ..topic(
                "orders",
                ORDERS_ID,
                vec![
                    part(1, &[1, 2], &[1, 2]),
                    Part {
                        elr: &[2],
                        last_known_elr: &[1],
                        ..part(-1, &[2, 1], &[])
                    },
                    part(2, &[2, 1], &[2]),
                ],
            )
        },
        topic("alpha", [1; 16], vec![part(1, &[1, 2, 3], &[1, 2])]),
        topic("bravo", [2; 16], vec![part(2, &[2], &[2])]),
        Topic {
            configs: &[
                ("min.insync.replicas", "1", DEFAULT),
                ("segment.bytes", "104857600", STATIC_BROKER),
            ],
            ..topic("__consumer_offsets", [3; 16], vec![part(1, &[1], &[1])])
        },
    ]
}

fn metadata_answer(cluster: &[Topic], port: u16, request: &MetadataRequest) -> MetadataResponse {
    let wanted = |topic: &Topic| {
        request.topics.as_ref().is_none_or(|topics| {
            topics.iter().any(|wanted| {
                wanted.name.as_deref() == Some(topic.name) || wanted.topic_id == Uuid(topic.id)
            })
        })
    };
    MetadataResponse {
        brokers: LIVE_BROKERS
            .iter()
            .map(|id| MetadataResponseBroker {
                node_id: *id,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            })
            .collect(),
        controller_id: 1,
        topics: cluster
            .iter()
            .filter(|topic| wanted(topic))
            .map(|topic| MetadataResponseTopic {
                name: Some(topic.name.into()),
                topic_id: Uuid(topic.id),
                is_internal: topic.name.starts_with("__"),
                partitions: (0..)
                    .zip(&topic.partitions)
                    .map(|(index, part)| MetadataResponsePartition {
                        partition_index: index,
                        leader_id: part.leader,
                        replica_nodes: part.replicas.to_vec(),
                        isr_nodes: part.isr.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn cluster_answer(port: u16) -> DescribeClusterResponse {
    DescribeClusterResponse {
        cluster_id: "krabka".into(),
        controller_id: 1,
        brokers: LIVE_BROKERS
            .iter()
            .map(|id| DescribeClusterBroker {
                broker_id: *id,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn describe_partition(index: i32, part: &Part) -> DescribeTopicPartitionsResponsePartition {
    DescribeTopicPartitionsResponsePartition {
        partition_index: index,
        leader_id: part.leader,
        leader_epoch: 0,
        replica_nodes: part.replicas.to_vec(),
        isr_nodes: part.isr.to_vec(),
        eligible_leader_replicas: Some(part.elr.to_vec()),
        last_known_elr: Some(part.last_known_elr.to_vec()),
        ..Default::default()
    }
}

fn cursor(topic_name: &str, partition_index: i32) -> ResponseCursor {
    ResponseCursor {
        topic_name: topic_name.into(),
        partition_index,
        ..Default::default()
    }
}

// One page of `DescribeTopicPartitions`, as a Kafka broker cuts it: the
// requested topics in name order from the cursor, at most the limit of
// partitions, and a cursor at the first partition left out.
fn partitions_answer(
    cluster: &[Topic],
    request: &DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionsResponse {
    let (start_topic, start_partition) = request.cursor.as_ref().map_or((String::new(), 0), |c| {
        (c.topic_name.clone(), c.partition_index)
    });
    let mut names = request
        .topics
        .iter()
        .map(|topic| topic.name.clone())
        .filter(|name| *name >= start_topic)
        .collect::<Vec<_>>();
    names.sort();
    let mut left = usize::try_from(request.response_partition_limit).unwrap();
    let mut response = DescribeTopicPartitionsResponse::default();
    for (position, name) in names.iter().enumerate() {
        let error = cluster
            .iter()
            .find(|topic| topic.name == name)
            .map_or(3, |topic| topic.describe_error);
        let Some(topic) = cluster
            .iter()
            .find(|topic| topic.name == name && error == 0)
        else {
            response.topics.push(DescribeTopicPartitionsResponseTopic {
                error_code: error,
                name: Some(name.clone()),
                ..Default::default()
            });
            continue;
        };
        let first = if *name == start_topic {
            start_partition
        } else {
            0
        };
        let rest = (0..)
            .zip(&topic.partitions)
            .filter(|(index, _)| *index >= first)
            .collect::<Vec<_>>();
        let taken = rest.iter().take(left).collect::<Vec<_>>();
        left -= taken.len();
        response.topics.push(DescribeTopicPartitionsResponseTopic {
            name: Some(name.clone()),
            topic_id: Uuid(topic.id),
            is_internal: name.starts_with("__"),
            partitions: taken
                .iter()
                .map(|(index, part)| describe_partition(*index, part))
                .collect(),
            ..Default::default()
        });
        if let Some((next, _)) = rest.get(taken.len()) {
            response.next_cursor = Some(cursor(name, *next));
            break;
        }
        if left == 0 {
            response.next_cursor = names.get(position + 1).map(|next| cursor(next, 0));
            break;
        }
    }
    response
}

fn configs_answer(cluster: &[Topic], request: &DescribeConfigsRequest) -> DescribeConfigsResponse {
    DescribeConfigsResponse {
        results: request
            .resources
            .iter()
            .map(|resource| {
                let found = cluster
                    .iter()
                    .find(|topic| topic.name == resource.resource_name);
                DescribeConfigsResult {
                    error_code: if found.is_some() { 0 } else { 3 },
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: found
                        .map(|topic| {
                            topic
                                .configs
                                .iter()
                                .map(|(name, value, source)| DescribeConfigsResourceResult {
                                    name: (*name).into(),
                                    value: Some((*value).into()),
                                    config_source: *source,
                                    ..Default::default()
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    }
}

// The one ongoing reassignment: broker 3 joins `alpha` partition 0.
fn reassignments_answer(
    request: &ListPartitionReassignmentsRequest,
) -> ListPartitionReassignmentsResponse {
    let asked = request.topics.as_ref().is_none_or(|topics| {
        topics
            .iter()
            .any(|topic| topic.name == "alpha" && topic.partition_indexes.contains(&0))
    });
    ListPartitionReassignmentsResponse {
        topics: if asked {
            vec![OngoingTopicReassignment {
                name: "alpha".into(),
                partitions: vec![OngoingPartitionReassignment {
                    partition_index: 0,
                    replicas: vec![1, 2, 3],
                    adding_replicas: vec![3],
                    removing_replicas: vec![],
                    ..Default::default()
                }],
                ..Default::default()
            }]
        } else {
            Vec::new()
        },
        ..Default::default()
    }
}

// A request as the broker received it: API key, version, and the bytes
// after the correlation id.
type Request = (i16, i16, Vec<u8>);

// Answers one API: the broker's port, the request's version and bytes, to
// the response body, or `None` to send nothing.
type Handler = Box<dyn Fn(u16, i16, &[u8]) -> Option<Vec<u8>> + Send>;

// One API that the broker advertises and answers.
struct Api {
    key: i16,
    min: i16,
    max: i16,
    handler: Handler,
}

// Skips the rest of the request header, the nullable client id and the
// tagged fields of a flexible version, and decodes the body.
fn decode_request<R: ProtocolRequest + for<'de> Decode<'de>>(version: i16, bytes: &[u8]) -> R {
    let client_id_len = i16::from_be_bytes([bytes[0], bytes[1]]);
    let mut body = &bytes[2 + usize::try_from(client_id_len.max(0)).unwrap()..];
    if version >= R::FLEXIBLE_MIN {
        let fields = get_uvarint(&mut body).unwrap();
        assert!(fields == 0);
    }
    let request = R::decode(&mut body, version).unwrap();
    assert!(body.is_empty());
    request
}

fn encode_response<R: ProtocolRequest>(response: &R::Response, version: i16) -> Vec<u8>
where
    R::Response: Encode,
{
    let mut body = Vec::new();
    if version >= R::FLEXIBLE_MIN {
        body.push(0);
    }
    response.encode(&mut body, version).unwrap();
    body
}

// An API whose answer `answer` computes from the port and the decoded
// request.
fn api<R>(
    min: i16,
    max: i16,
    answer: impl Fn(u16, R) -> Option<R::Response> + Send + 'static,
) -> Api
where
    R: ProtocolRequest + for<'de> Decode<'de>,
    R::Response: Encode,
{
    Api {
        key: R::API_KEY,
        min,
        max,
        handler: Box::new(move |port, version, bytes| {
            answer(port, decode_request::<R>(version, bytes))
                .map(|response| encode_response::<R>(&response, version))
        }),
    }
}

// The reads of `cluster`: `Metadata`, `DescribeCluster`,
// `DescribeTopicPartitions`, `DescribeConfigs` and
// `ListPartitionReassignments`.
fn reads(cluster: &[Topic]) -> Vec<Api> {
    let (metadata, partitions, configs) = (cluster.to_vec(), cluster.to_vec(), cluster.to_vec());
    vec![
        api::<MetadataRequest>(0, 12, move |port, request| {
            Some(metadata_answer(&metadata, port, &request))
        }),
        api::<DescribeClusterRequest>(0, 1, |port, _| Some(cluster_answer(port))),
        api::<DescribeTopicPartitionsRequest>(0, 0, move |_, request| {
            Some(partitions_answer(&partitions, &request))
        }),
        api::<DescribeConfigsRequest>(1, 4, move |_, request| {
            Some(configs_answer(&configs, &request))
        }),
        api::<ListPartitionReassignmentsRequest>(0, 0, |_, request| {
            Some(reassignments_answer(&request))
        }),
    ]
}

fn created(results: &[(&str, i16, Option<&str>)]) -> Api {
    let results = results
        .iter()
        .map(|(name, error_code, message)| CreatableTopicResult {
            name: (*name).into(),
            error_code: *error_code,
            error_message: message.map(str::to_owned),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    api::<CreateTopicsRequest>(2, 7, move |_, _| {
        Some(CreateTopicsResponse {
            topics: results.clone(),
            ..Default::default()
        })
    })
}

fn deleted(results: &[(&str, i16)]) -> Api {
    let results = results
        .iter()
        .map(|(name, error_code)| DeletableTopicResult {
            name: Some((*name).into()),
            error_code: *error_code,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    api::<DeleteTopicsRequest>(1, 6, move |_, _| {
        Some(DeleteTopicsResponse {
            responses: results.clone(),
            ..Default::default()
        })
    })
}

fn partitions_created(results: &[(&str, i16, Option<&str>)]) -> Api {
    let results = results
        .iter()
        .map(|(name, error_code, message)| CreatePartitionsTopicResult {
            name: (*name).into(),
            error_code: *error_code,
            error_message: message.map(str::to_owned),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    api::<CreatePartitionsRequest>(0, 3, move |_, _| {
        Some(CreatePartitionsResponse {
            results: results.clone(),
            ..Default::default()
        })
    })
}

// A running scripted broker.
struct Broker {
    inner: krabka_client_core::MockBroker,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Broker {
    async fn start(apis: Vec<Api>) -> Self {
        let versions = ApiVersionsResponse {
            api_keys: std::iter::once((api_versions_request::API_KEY, 0, 3))
                .chain(apis.iter().map(|api| (api.key, api.min, api.max)))
                .map(|(api_key, min_version, max_version)| ApiVersion {
                    api_key,
                    min_version,
                    max_version,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut versions_body = Vec::new();
        versions.encode(&mut versions_body, 0).unwrap();
        let handlers = apis
            .into_iter()
            .map(|api| (api.key, api.handler))
            .collect::<BTreeMap<_, _>>();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let port = Arc::new(Mutex::new(0_u16));
        let seen = Arc::clone(&port);
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            log.lock().unwrap().push((api_key, version, body.to_vec()));
            if api_key == api_versions_request::API_KEY {
                return Some(versions_body.clone());
            }
            let port = *seen.lock().unwrap();
            handlers
                .get(&api_key)
                .and_then(|handler| handler(port, version, body))
        })
        .await;
        *port.lock().unwrap() = inner.addr.port();
        Self { inner, requests }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    // The API keys of the requests after `ApiVersions`, in order.
    fn sent(&self) -> Vec<i16> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(key, _, _)| *key)
            .filter(|key| *key != api_versions_request::API_KEY)
            .collect()
    }

    // The decoded requests of type `R`.
    fn decoded<R: ProtocolRequest + for<'de> Decode<'de>>(&self) -> Vec<R> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| *key == R::API_KEY)
            .map(|(_, version, body)| decode_request::<R>(*version, body))
            .collect()
    }

    fn stop(self) {
        self.inner.stop();
    }
}

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

// The lines `--describe` prints of the cluster.
const ORDERS: &str = "Topic: orders\tTopicId: 4IgIMEgZQYS5IcnSzjHAqw\tPartitionCount: 3\t\
                      ReplicationFactor: 2\tConfigs: min.insync.replicas=2,retention.ms=1000\n";
const ORDERS_0: &str = "\tTopic: orders\tPartition: 0\tLeader: 1\tReplicas: 1,2\tIsr: 1,2\tElr: \t\
                        LastKnownElr: \n";
const ORDERS_1: &str = "\tTopic: orders\tPartition: 1\tLeader: none\tReplicas: 2,1\tIsr: \tElr: \
                        2\tLastKnownElr: 1\n";
const ORDERS_2: &str = "\tTopic: orders\tPartition: 2\tLeader: 2\tReplicas: 2,1\tIsr: 2\tElr: \t\
                        LastKnownElr: \n";
const ALPHA: &str = "Topic: alpha\tTopicId: AQEBAQEBAQEBAQEBAQEBAQ\tPartitionCount: 1\t\
                     ReplicationFactor: 2\tConfigs: \n";
const ALPHA_0: &str = "\tTopic: alpha\tPartition: 0\tLeader: 1\tReplicas: 1,2,3\tIsr: 1,2\t\
                       Adding Replicas: 3\tRemoving Replicas: \tElr: \tLastKnownElr: \n";
const BRAVO: &str = "Topic: bravo\tTopicId: AgICAgICAgICAgICAgICAg\tPartitionCount: 1\t\
                     ReplicationFactor: 1\tConfigs: \n";
const BRAVO_0: &str = "\tTopic: bravo\tPartition: 0\tLeader: 2\tReplicas: 2\tIsr: 2\tElr: \t\
                       LastKnownElr: \n";
const OFFSETS: &str = "Topic: __consumer_offsets\tTopicId: AwMDAwMDAwMDAwMDAwMDAw\t\
                       PartitionCount: 1\tReplicationFactor: 1\tConfigs: \
                       segment.bytes=104857600\n";
const OFFSETS_0: &str = "\tTopic: __consumer_offsets\tPartition: 0\tLeader: 1\tReplicas: 1\t\
                         Isr: 1\tElr: \tLastKnownElr: \n";

// Every topic, in the order `kafka-topics --describe` prints them.
fn everything() -> String {
    [
        BRAVO, BRAVO_0, ORDERS, ORDERS_0, ORDERS_1, ORDERS_2, OFFSETS, OFFSETS_0, ALPHA, ALPHA_0,
    ]
    .concat()
}

// API keys, for the expected request sequences.
const METADATA: i16 = MetadataRequest::API_KEY;
const CLUSTER: i16 = DescribeClusterRequest::API_KEY;
const PARTITIONS: i16 = DescribeTopicPartitionsRequest::API_KEY;
const CONFIGS: i16 = DescribeConfigsRequest::API_KEY;
const REASSIGNMENTS: i16 = ListPartitionReassignmentsRequest::API_KEY;
const CREATE: i16 = CreateTopicsRequest::API_KEY;
const DELETE: i16 = DeleteTopicsRequest::API_KEY;
const ALTER: i16 = CreatePartitionsRequest::API_KEY;

// The requests of a `--describe` by name that needs one page.
const DESCRIBE: &[i16] = &[
    METADATA,
    CLUSTER,
    PARTITIONS,
    CONFIGS,
    CLUSTER,
    REASSIGNMENTS,
];

struct Case {
    name: &'static str,
    args: &'static [&'static str],
    apis: Vec<Api>,
    sent: &'static [i16],
    run: Run,
}

fn with(mut apis: Vec<Api>, api: Api) -> Vec<Api> {
    apis.push(api);
    apis
}

fn list_and_create_cases() -> Vec<Case> {
    let collision = "WARNING: Due to limitations in metric names, topics with a period ('.') or \
                     underscore ('_') could collide. To avoid issues it is best to use either, \
                     but not both.\n";
    vec![
        Case {
            name: "list prints every topic, sorted",
            args: &["--list"],
            apis: reads(&cluster()),
            sent: &[METADATA],
            run: run(0, "__consumer_offsets\nalpha\nbravo\norders\n", ""),
        },
        Case {
            name: "list filters by pattern and internal topics",
            args: &["--list", "--exclude-internal", "--topic", ".*a.*"],
            apis: reads(&cluster()),
            sent: &[METADATA],
            run: run(0, "alpha\nbravo\n", ""),
        },
        Case {
            name: "a list that matches nothing prints one empty line",
            args: &["--list", "--topic", "missing"],
            apis: reads(&cluster()),
            sent: &[METADATA],
            run: run(0, "\n", ""),
        },
        Case {
            name: "create",
            args: &["--create", "--topic", "payments", "--partitions", "3"],
            apis: vec![created(&[("payments", 0, None)])],
            sent: &[CREATE],
            run: run(0, "Created topic payments.\n", ""),
        },
        Case {
            name: "create warns about a colliding name",
            args: &["--create", "--topic", "my.topic_x"],
            apis: vec![created(&[("my.topic_x", 0, None)])],
            sent: &[CREATE],
            run: run(0, &format!("{collision}Created topic my.topic_x.\n"), ""),
        },
        Case {
            name: "create of an existing topic fails on stdout",
            args: &["--create", "--topic", "orders"],
            apis: vec![created(&[(
                "orders",
                36,
                Some("Topic 'orders' already exists."),
            )])],
            sent: &[CREATE],
            run: run(
                1,
                "Error while executing topic command : Topic 'orders' already exists.\n",
                "",
            ),
        },
        Case {
            name: "create --if-not-exists still sends the request and prints nothing",
            args: &["--create", "--topic", "orders", "--if-not-exists"],
            apis: vec![created(&[(
                "orders",
                36,
                Some("Topic 'orders' already exists."),
            )])],
            sent: &[CREATE],
            run: run(0, "", ""),
        },
        Case {
            name: "a partial create prints every row and exits 1",
            args: &["--create", "--topic", "a", "--topic", "b", "--topic", "c"],
            apis: vec![created(&[
                ("c", 0, None),
                ("b", 40, Some("Unknown topic config name: foo")),
                ("a", 0, None),
            ])],
            sent: &[CREATE],
            run: run(
                1,
                "Created topic a.\nError while executing topic command : Unknown topic config \
                 name: foo\nCreated topic c.\n",
                "",
            ),
        },
        Case {
            name: "create with a replica assignment",
            args: &[
                "--create",
                "--topic",
                "payments",
                "--replica-assignment",
                "1:2,2:1",
            ],
            apis: vec![created(&[("payments", 0, None)])],
            sent: &[CREATE],
            run: run(0, "Created topic payments.\n", ""),
        },
        Case {
            name: "an assignment the broker refuses fails on stdout",
            args: &[
                "--create",
                "--topic",
                "payments",
                "--replica-assignment",
                "7",
            ],
            apis: vec![created(&[("payments", 39, None)])],
            sent: &[CREATE],
            run: run(
                1,
                "Error while executing topic command : Replica assignment is invalid.\n",
                "",
            ),
        },
    ]
}

fn alter_and_delete_cases() -> Vec<Case> {
    let alter_reads = || reads(&cluster());
    vec![
        Case {
            name: "alter describes the topics and prints nothing on success",
            args: &["--alter", "--topic", "orders", "--partitions", "4"],
            apis: with(alter_reads(), partitions_created(&[("orders", 0, None)])),
            sent: &[METADATA, CLUSTER, PARTITIONS, ALTER],
            run: run(0, "", ""),
        },
        Case {
            name: "alter with a replica assignment for the new partitions",
            args: &[
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "5",
                "--replica-assignment",
                "1:2,2:1,1:2,2:1,1:2",
            ],
            apis: with(alter_reads(), partitions_created(&[("orders", 0, None)])),
            sent: &[METADATA, CLUSTER, PARTITIONS, ALTER],
            run: run(0, "", ""),
        },
        Case {
            name: "alter to fewer partitions fails with the broker's message",
            args: &["--alter", "--topic", "orders", "--partitions", "2"],
            apis: with(
                alter_reads(),
                partitions_created(&[(
                    "orders",
                    37,
                    Some(
                        "The topic orders currently has 3 partition(s); 2 would not be an \
                         increase.",
                    ),
                )]),
            ),
            sent: &[METADATA, CLUSTER, PARTITIONS, ALTER],
            run: run(
                1,
                "Error while executing topic command : The topic orders currently has 3 \
                 partition(s); 2 would not be an increase.\n",
                "",
            ),
        },
        Case {
            name: "alter with an assignment of a topic it cannot describe",
            args: &[
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "4",
                "--replica-assignment",
                "1,1,1,1",
            ],
            apis: reads(&[Topic {
                describe_error: 29,
                ..cluster().remove(0)
            }]),
            sent: &[METADATA, CLUSTER, PARTITIONS],
            run: run(
                1,
                "Error while executing topic command : java.util.concurrent.ExecutionException: \
                 org.apache.kafka.common.errors.TopicAuthorizationException: Topic \
                 authorization failed.\n",
                "",
            ),
        },
        Case {
            name: "alter of a missing topic fails before any mutation",
            args: &["--alter", "--topic", "missing", "--partitions", "2"],
            apis: alter_reads(),
            sent: &[METADATA],
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
            apis: alter_reads(),
            sent: &[METADATA],
            run: run(0, "", ""),
        },
        Case {
            name: "delete resolves the pattern and prints nothing",
            args: &["--delete", "--topic", "alpha|bravo", "--yes"],
            apis: with(alter_reads(), deleted(&[("alpha", 0), ("bravo", 0)])),
            sent: &[METADATA, DELETE],
            run: run(0, "", ""),
        },
        Case {
            name: "delete of a missing topic fails before any mutation",
            args: &["--delete", "--topic", "missing", "--yes"],
            apis: alter_reads(),
            sent: &[METADATA],
            run: run(
                1,
                "",
                "krabka topics: Topic 'missing' does not exist as expected\n",
            ),
        },
        Case {
            name: "delete --if-exists of a missing topic does nothing",
            args: &["--delete", "--topic", "missing", "--if-exists"],
            apis: alter_reads(),
            sent: &[METADATA],
            run: run(0, "", ""),
        },
        Case {
            name: "a delete dry run names the topics and sends no mutation",
            args: &["--delete", "--topic", "alpha|bravo", "--dry-run"],
            apis: alter_reads(),
            sent: &[METADATA],
            run: run(0, "DRY RUN: no change was made.\nalpha\nbravo\n", ""),
        },
    ]
}

fn describe_cases() -> Vec<Case> {
    // A topic whose configs lack `min.insync.replicas`, as no Kafka broker
    // answers, for the exception `kafka-topics` then throws.
    let bare = || {
        vec![Topic {
            configs: &[],
            ..topic("bare", [4; 16], vec![part(1, &[1], &[1])])
        }]
    };
    vec![
        Case {
            name: "describe prints every topic with its partitions",
            args: &["--describe"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &everything(), ""),
        },
        Case {
            name: "describe one topic",
            args: &["--describe", "--topic", "orders"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS, ORDERS_0, ORDERS_1, ORDERS_2].concat(), ""),
        },
        Case {
            name: "describe --exclude-internal",
            args: &["--describe", "--exclude-internal", "--topic", "orders|__.*"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS, ORDERS_0, ORDERS_1, ORDERS_2].concat(), ""),
        },
        Case {
            name: "--under-replicated-partitions",
            args: &["--describe", "--under-replicated-partitions"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS_1, ORDERS_2].concat(), ""),
        },
        Case {
            name: "--unavailable-partitions",
            args: &["--describe", "--unavailable-partitions"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, ORDERS_1, ""),
        },
        Case {
            name: "--under-min-isr-partitions",
            args: &["--describe", "--under-min-isr-partitions"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS_1, ORDERS_2].concat(), ""),
        },
        Case {
            name: "--at-min-isr-partitions",
            args: &["--describe", "--at-min-isr-partitions"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[BRAVO_0, ORDERS_0, OFFSETS_0].concat(), ""),
        },
        Case {
            name: "two partition selectors print the union",
            args: &[
                "--describe",
                "--topic",
                "orders",
                "--unavailable-partitions",
                "--at-min-isr-partitions",
            ],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS_0, ORDERS_1].concat(), ""),
        },
        Case {
            name: "--topics-with-overrides prints each topic with a non-default config",
            args: &["--describe", "--topics-with-overrides"],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[ORDERS, OFFSETS].concat(), ""),
        },
        Case {
            name: "describe by topic ID reads Metadata, which has no ELR",
            args: &["--describe", "--topic-id", "AQEBAQEBAQEBAQEBAQEBAQ"],
            apis: reads(&cluster()),
            sent: &[METADATA, METADATA, CONFIGS, CLUSTER, REASSIGNMENTS],
            run: run(
                0,
                &[
                    ALPHA,
                    "\tTopic: alpha\tPartition: 0\tLeader: 1\tReplicas: 1,2,3\tIsr: 1,2\t\
                     Adding Replicas: 3\tRemoving Replicas: \tElr: N/A\tLastKnownElr: N/A\n",
                ]
                .concat(),
                "",
            ),
        },
        Case {
            name: "the zero topic ID describes by name",
            args: &[
                "--describe",
                "--topic-id",
                "AAAAAAAAAAAAAAAAAAAAAA",
                "--topic",
                "bravo",
            ],
            apis: reads(&cluster()),
            sent: DESCRIBE,
            run: run(0, &[BRAVO, BRAVO_0].concat(), ""),
        },
        Case {
            name: "describe of a topic the broker refuses prints Kafka's exception",
            args: &["--describe", "--topic", "orders"],
            apis: reads(&[Topic {
                describe_error: 29,
                ..cluster().remove(0)
            }]),
            sent: &[METADATA, CLUSTER, PARTITIONS],
            run: run(
                1,
                "Error while executing topic command : Topic authorization failed.\n",
                "",
            ),
        },
        Case {
            name: "a min ISR selector without min.insync.replicas throws as Kafka's does",
            args: &["--describe", "--under-min-isr-partitions"],
            apis: reads(&bare()),
            sent: DESCRIBE,
            run: run(
                1,
                "Error while executing topic command : Cannot invoke \
                 \"org.apache.kafka.clients.admin.ConfigEntry.value()\" because the return value \
                 of \"org.apache.kafka.clients.admin.Config.get(String)\" is null\n",
                "",
            ),
        },
        Case {
            name: "a plain describe does not read min.insync.replicas",
            args: &["--describe"],
            apis: reads(&bare()),
            sent: DESCRIBE,
            run: run(
                0,
                "Topic: bare\tTopicId: BAQEBAQEBAQEBAQEBAQEBA\tPartitionCount: 1\t\
                 ReplicationFactor: 1\tConfigs: \n\tTopic: bare\tPartition: 0\tLeader: 1\t\
                 Replicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n",
                "",
            ),
        },
        Case {
            name: "describe of a missing topic fails with Kafka's message",
            args: &["--describe", "--topic", "missing"],
            apis: reads(&cluster()),
            sent: &[METADATA],
            run: run(
                1,
                "",
                "krabka topics: Topic 'missing' does not exist as expected\n",
            ),
        },
        Case {
            name: "describe of an unknown topic ID fails with Kafka's message",
            args: &["--describe", "--topic-id", "BQUFBQUFBQUFBQUFBQUFBQ"],
            apis: reads(&cluster()),
            sent: &[METADATA],
            run: run(
                1,
                "",
                "krabka topics: TopicId 'BQUFBQUFBQUFBQUFBQUFBQ' does not exist as expected\n",
            ),
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn each_action_sends_its_requests_and_prints_what_kafka_topics_prints() {
    let cases = list_and_create_cases()
        .into_iter()
        .chain(alter_and_delete_cases())
        .chain(describe_cases());
    for case in cases {
        let broker = Broker::start(case.apis).await;
        let outcome = krabka(topics(&broker.address(), case.args)).await;
        check!(
            (outcome, broker.sent()) == (case.run, case.sent.to_vec()),
            "{}",
            case.name
        );
        broker.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_follows_the_cursor_until_every_topic_is_complete() {
    let broker = Broker::start(reads(&cluster())).await;
    let outcome = krabka(topics(
        &broker.address(),
        &["--describe", "--partition-size-limit-per-response", "2"],
    ))
    .await;
    check!(outcome == run(0, &everything(), ""));
    let request = |names: &[&str], cursor: Option<(&str, i32)>| DescribeTopicPartitionsRequest {
        topics: names
            .iter()
            .map(|name| TopicRequest {
                name: (*name).into(),
                ..Default::default()
            })
            .collect(),
        response_partition_limit: 2,
        cursor: cursor.map(|(topic_name, partition_index)| RequestCursor {
            topic_name: topic_name.into(),
            partition_index,
            ..Default::default()
        }),
        ..Default::default()
    };
    // The first page ends after `alpha`, with a cursor at `bravo` partition
    // 0, which the client does not send back; the second ends inside
    // `orders`, and the third resumes it at partition 1.
    check!(
        broker.decoded::<DescribeTopicPartitionsRequest>()
            == vec![
                request(&["__consumer_offsets", "alpha", "bravo", "orders"], None),
                request(&["bravo", "orders"], None),
                request(&["orders"], Some(("orders", 1))),
            ]
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_assignments_reach_the_wire_as_kafka_topics_sends_them() {
    let broker = Broker::start(vec![created(&[("payments", 0, None)])]).await;
    krabka(topics(
        &broker.address(),
        &[
            "--create",
            "--topic",
            "payments",
            "--replica-assignment",
            "1:2, 2:1",
            "--config",
            "retention.ms=5",
        ],
    ))
    .await;
    let sent = broker
        .decoded::<CreateTopicsRequest>()
        .into_iter()
        .map(|request| request.topics)
        .collect::<Vec<_>>();
    let assignment = |partition_index, broker_ids: &[i32]| CreatableReplicaAssignment {
        partition_index,
        broker_ids: broker_ids.to_vec(),
        ..Default::default()
    };
    check!(
        sent == vec![vec![CreatableTopic {
            name: "payments".into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: vec![assignment(0, &[1, 2]), assignment(1, &[2, 1])],
            configs: vec![CreatableTopicConfig {
                name: "retention.ms".into(),
                value: Some("5".into()),
                ..Default::default()
            }],
            ..Default::default()
        }]]
    );
    broker.stop();

    // `orders` has three partitions, so the rows for partitions 3 and 4
    // are the new ones.
    let broker = Broker::start(with(
        reads(&cluster()),
        partitions_created(&[("orders", 0, None)]),
    ))
    .await;
    krabka(topics(
        &broker.address(),
        &[
            "--alter",
            "--topic",
            "orders",
            "--partitions",
            "5",
            "--replica-assignment",
            "1:2,2:1,1:2,2:3,3:1",
        ],
    ))
    .await;
    let sent = broker
        .decoded::<CreatePartitionsRequest>()
        .into_iter()
        .map(|request| request.topics)
        .collect::<Vec<_>>();
    let brokers = |broker_ids: &[i32]| CreatePartitionsAssignment {
        broker_ids: broker_ids.to_vec(),
        ..Default::default()
    };
    check!(
        sent == vec![vec![CreatePartitionsTopic {
            name: "orders".into(),
            count: 5,
            assignments: Some(vec![brokers(&[2, 3]), brokers(&[3, 1])]),
            ..Default::default()
        }]]
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_metadata_timeout_fails_through_the_output_layer_without_a_mutation() {
    let broker = Broker::start(vec![api::<MetadataRequest>(0, 12, |_, _| None)]).await;
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
    check!(broker.sent().iter().all(|api_key| *api_key == METADATA));
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
    let broker = Broker::start(Vec::new()).await;
    for (args, stderr) in cases {
        let outcome = krabka(topics(&broker.address(), args)).await;
        check!(outcome == run(1, "", stderr), "{args:?}");
    }
    check!(broker.requests.lock().unwrap().is_empty());
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_json_rendering_carries_every_row() {
    let broker = Broker::start(vec![created(&[
        ("b", 36, Some("Topic 'b' already exists.")),
        ("a", 0, None),
    ])])
    .await;
    let mut args = vec!["--output".to_owned(), "json".to_owned()];
    args.extend(topics(
        &broker.address(),
        &["--create", "--topic", "a", "--topic", "b"],
    ));
    let outcome = krabka(args).await;
    let expected = json!({"data": [
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
async fn the_json_describe_carries_the_selected_partitions() {
    let broker = Broker::start(reads(&cluster())).await;
    let mut args = vec!["--output".to_owned(), "json".to_owned()];
    args.extend(topics(
        &broker.address(),
        &[
            "--describe",
            "--topic",
            "orders",
            "--unavailable-partitions",
        ],
    ));
    let outcome = krabka(args).await;
    let expected = json!({"data": [{
        "topic": "orders",
        "topic_id": "4IgIMEgZQYS5IcnSzjHAqw",
        "partition_count": 3,
        "replication_factor": 2,
        "configs": {"min.insync.replicas": "2", "retention.ms": "1000"},
        "partitions": [{
            "partition": 1,
            "leader": null,
            "replicas": [2, 1],
            "isr": [],
            "adding_replicas": null,
            "removing_replicas": null,
            "elr": [2],
            "last_known_elr": [1],
        }],
    }]});
    check!(
        (
            outcome.code,
            serde_json::from_str::<serde_json::Value>(&outcome.stdout).unwrap(),
            outcome.stderr,
        ) == (Some(0), expected, String::new())
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_config_prints_kafkas_notice_on_stderr() {
    let broker = Broker::start(reads(&cluster())).await;
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
