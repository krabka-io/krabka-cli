//! `krabka configs` end to end: the binary against a scripted broker that
//! records the body of every request, so a test decodes the exact request
//! the command sent.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        alter_client_quotas_request::{
            AlterClientQuotasRequest, EntityData as AlterEntity, EntryData as AlterEntry, OpData,
        },
        alter_client_quotas_response::{
            AlterClientQuotasResponse, EntityData as ResultEntity, EntryData as AlterResult,
        },
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialDeletion,
        },
        alter_user_scram_credentials_response::{
            AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
        },
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::{
            DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
        },
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
            DescribeConfigsSynonym,
        },
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::{ConfigResource, ListConfigResourcesResponse},
        list_groups_request::ListGroupsRequest,
        list_groups_response::{ListGroupsResponse, ListedGroup},
        metadata_request::MetadataRequest,
        metadata_response::{MetadataResponse, MetadataResponseBroker, MetadataResponseTopic},
    },
    primitives::varint::get_uvarint,
};
use serde_json::{Value, json};

// One recorded request: its API key, its version, and the bytes after its
// correlation id.
type Request = (i16, i16, Vec<u8>);

// A broker that answers each API with the highest version the client
// supports, from a table of canned responses, and records every request.
struct Broker {
    inner: krabka_client_core::MockBroker,
    requests: Arc<Mutex<Vec<Request>>>,
}

// One canned response: the API it answers and its encoder, which takes the
// request's version and raw bytes.
type Canned = (i16, i16, Box<dyn Fn(i16, &[u8]) -> Vec<u8> + Send>);

fn canned<R: ProtocolRequest>(response: R::Response) -> Canned
where
    R::Response: Encode + Send + 'static,
{
    (
        R::API_KEY,
        R::MAX_VERSION,
        Box::new(move |version, _| encode_response::<R>(&response, version)),
    )
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

// A response that `answer` computes from the decoded request.
fn replier<R: ProtocolRequest + for<'de> Decode<'de>>(
    answer: impl Fn(R) -> R::Response + Send + 'static,
) -> Canned
where
    R::Response: Encode,
{
    (
        R::API_KEY,
        R::MAX_VERSION,
        Box::new(move |version, request| {
            encode_response::<R>(&answer(decode_request::<R>(version, request)), version)
        }),
    )
}

impl Broker {
    async fn start(replies: Vec<Canned>) -> Self {
        Self::start_with(|_| replies).await
    }

    // A broker whose replies may name its own port, which is known only
    // once it listens: `replies` gets a cell that holds the port by the time
    // any request arrives.
    async fn start_with(replies: impl FnOnce(Arc<AtomicU16>) -> Vec<Canned>) -> Self {
        let port = Arc::new(AtomicU16::new(0));
        let replies = replies(Arc::clone(&port));
        let versions = ApiVersionsResponse {
            api_keys: std::iter::once((api_versions_request::API_KEY, 3))
                .chain(replies.iter().map(|(key, max, _)| (*key, *max)))
                .map(|(api_key, max_version)| ApiVersion {
                    api_key,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut versions_body = Vec::new();
        versions.encode(&mut versions_body, 0).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let replies = replies
            .into_iter()
            .map(|(key, _, encode)| (key, encode))
            .collect::<BTreeMap<_, _>>();
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            log.lock().unwrap().push((api_key, version, body.to_vec()));
            if api_key == api_versions_request::API_KEY {
                return Some(versions_body.clone());
            }
            replies.get(&api_key).map(|encode| encode(version, body))
        })
        .await;
        port.store(inner.addr.port(), Ordering::SeqCst);
        Self { inner, requests }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    // The API keys of the requests after `ApiVersions`, in order.
    fn api_keys(&self) -> Vec<i16> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(key, _, _)| *key)
            .filter(|key| *key != api_versions_request::API_KEY)
            .collect()
    }

    // Every raw request, header bytes after the correlation id included.
    fn raw(&self) -> Vec<Vec<u8>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, body)| body.clone())
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

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: Vec<String>, rust_log: &'static str) -> Run {
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .env("RUST_LOG", rust_log)
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

fn configs(broker: &Broker, argv: &[&str]) -> Vec<String> {
    ["configs", "--bootstrap-server", &broker.address()]
        .iter()
        .chain(argv)
        .map(|arg| (*arg).to_owned())
        .collect()
}

fn json_output(argv: Vec<String>) -> Vec<String> {
    ["--output".to_owned(), "json".to_owned()]
        .into_iter()
        .chain(argv)
        .collect()
}

// `Metadata` that lists `topics`, and broker 1 at the mock's own port, so a
// call to broker 1 comes back to the mock.
fn metadata(port: &Arc<AtomicU16>, topics: &[&str]) -> Canned {
    let port = Arc::clone(port);
    let topics = topics
        .iter()
        .map(|name| MetadataResponseTopic {
            name: Some((*name).into()),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    (
        MetadataRequest::API_KEY,
        MetadataRequest::MAX_VERSION,
        Box::new(move |version, _| {
            encode_response::<MetadataRequest>(
                &MetadataResponse {
                    brokers: vec![MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: i32::from(port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }],
                    controller_id: 1,
                    topics: topics.clone(),
                    ..Default::default()
                },
                version,
            )
        }),
    )
}

// `DescribeCluster` that lists the brokers `ids`.
fn cluster(ids: &[i32]) -> Canned {
    canned::<DescribeClusterRequest>(DescribeClusterResponse {
        cluster_id: "cluster".into(),
        controller_id: ids[0],
        brokers: ids
            .iter()
            .map(|id| DescribeClusterBroker {
                broker_id: *id,
                host: "127.0.0.1".into(),
                port: 9092,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

// `ListConfigResources` that lists `names` of the resource type `kind`.
fn config_resources(kind: i8, names: &[&str]) -> Canned {
    canned::<ListConfigResourcesRequest>(ListConfigResourcesResponse {
        config_resources: names
            .iter()
            .map(|name| ConfigResource {
                resource_name: (*name).into(),
                resource_type: kind,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

// `ListGroups` that lists `groups`.
fn groups(ids: &[&str]) -> Canned {
    canned::<ListGroupsRequest>(ListGroupsResponse {
        groups: ids
            .iter()
            .map(|id| ListedGroup {
                group_id: (*id).into(),
                protocol_type: "consumer".into(),
                group_state: "Stable".into(),
                group_type: "consumer".into(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

fn config(
    name: &str,
    value: Option<&str>,
    source: i8,
    sensitive: bool,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.into(),
        value: value.map(Into::into),
        config_source: source,
        is_sensitive: sensitive,
        synonyms: vec![DescribeConfigsSynonym {
            name: name.into(),
            value: value.map(Into::into),
            source,
            ..Default::default()
        }],
        ..Default::default()
    }
}

// A config whose value comes from `source`, with a synonym at each of
// `synonyms`.
fn config_with(
    name: &str,
    value: Option<&str>,
    source: i8,
    synonyms: &[(i8, &str, Option<&str>)],
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.into(),
        value: value.map(Into::into),
        config_source: source,
        synonyms: synonyms
            .iter()
            .map(|(source, name, value)| DescribeConfigsSynonym {
                name: (*name).into(),
                value: value.map(Into::into),
                source: *source,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

// `DescribeConfigs` that answers each asked resource with `entries`.
fn describe_configs(entries: Vec<DescribeConfigsResourceResult>) -> Canned {
    replier::<DescribeConfigsRequest>(move |request: DescribeConfigsRequest| {
        DescribeConfigsResponse {
            results: request
                .resources
                .into_iter()
                .map(|resource| DescribeConfigsResult {
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                    configs: entries.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    })
}

// `IncrementalAlterConfigs` that accepts each resource.
fn altered_configs() -> Canned {
    replier::<IncrementalAlterConfigsRequest>(|request: IncrementalAlterConfigsRequest| {
        IncrementalAlterConfigsResponse {
            responses: request
                .resources
                .into_iter()
                .map(|resource| AlterConfigsResourceResponse {
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    })
}

fn describe_request(
    resource_type: i8,
    name: &str,
    include_synonyms: bool,
) -> DescribeConfigsRequest {
    DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type,
            resource_name: name.into(),
            configuration_keys: None,
            ..Default::default()
        }],
        include_synonyms,
        ..Default::default()
    }
}

fn alterable(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
    AlterableConfig {
        name: name.into(),
        config_operation: operation,
        value: value.map(Into::into),
        ..Default::default()
    }
}

fn alter_request(
    resource_type: i8,
    name: &str,
    configs: Vec<AlterableConfig>,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type,
            resource_name: name.into(),
            configs,
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    }
}

// The entries of a topic: two overrides, a sensitive override, and a
// default, whose synonyms show the broker-level values.
fn topic_entries() -> Vec<DescribeConfigsResourceResult> {
    vec![
        config_with(
            "retention.ms",
            Some("1000"),
            1,
            &[
                (1, "retention.ms", Some("1000")),
                (5, "log.retention.ms", None),
            ],
        ),
        config("cleanup.policy", Some("compact,delete"), 1, false),
        config("secret.thing", Some("hunter2"), 1, true),
        config("segment.bytes", Some("1073741824"), 5, false),
    ]
}

// One describe of a config resource: the broker's replies, given the cell of
// its port, the flags, the stdout, the APIs after `ApiVersions`, and the
// `DescribeConfigs` requests.
struct DescribeCase {
    name: &'static str,
    replies: fn(&Arc<AtomicU16>) -> Vec<Canned>,
    argv: &'static [&'static str],
    stdout: &'static str,
    api_keys: Vec<i16>,
    describes: Vec<DescribeConfigsRequest>,
}

const METADATA: i16 = MetadataRequest::API_KEY;
const DESCRIBE_CONFIGS: i16 = DescribeConfigsRequest::API_KEY;
const DESCRIBE_CLUSTER: i16 = DescribeClusterRequest::API_KEY;
const LIST_CONFIG_RESOURCES: i16 = ListConfigResourcesRequest::API_KEY;
const LIST_GROUPS: i16 = ListGroupsRequest::API_KEY;

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_each_config_resource_prints_the_jvm_lines() {
    let cases = vec![
        DescribeCase {
            name: "one topic",
            replies: |port| {
                vec![
                    metadata(port, &["t1", "other"]),
                    describe_configs(topic_entries()),
                ]
            },
            argv: &[
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                "t1",
            ],
            stdout: TOPIC_T1,
            api_keys: vec![METADATA, DESCRIBE_CONFIGS],
            describes: vec![describe_request(2, "t1", true)],
        },
        DescribeCase {
            name: "one topic with no configs",
            replies: |port| vec![metadata(port, &["t1"]), describe_configs(Vec::new())],
            argv: &["--describe", "--topic", "t1"],
            stdout: "Dynamic configs for topic t1 are:\n",
            api_keys: vec![METADATA, DESCRIBE_CONFIGS],
            describes: vec![describe_request(2, "t1", true)],
        },
        DescribeCase {
            name: "a missing topic",
            replies: |port| vec![metadata(port, &["t1"]), describe_configs(Vec::new())],
            argv: &["--describe", "--topic", "nope"],
            stdout: "The topic 'nope' doesn't exist and doesn't have dynamic config.\n",
            api_keys: vec![METADATA],
            describes: Vec::new(),
        },
        DescribeCase {
            name: "every topic, in the order of the JVM topic set",
            replies: |port| {
                vec![
                    metadata(port, &["t1", "orders", "payments"]),
                    describe_configs(Vec::new()),
                ]
            },
            argv: &["--describe", "--entity-type", "topics"],
            stdout: "Dynamic configs for topic payments are:\nDynamic configs for topic orders are:\nDynamic configs for topic t1 are:\n",
            api_keys: vec![
                METADATA,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
            ],
            describes: vec![
                describe_request(2, "payments", true),
                describe_request(2, "orders", true),
                describe_request(2, "t1", true),
            ],
        },
        DescribeCase {
            name: "every config of a topic, without the existence check",
            replies: |_| vec![describe_configs(topic_entries())],
            argv: &["--describe", "--topic", "nope", "--all"],
            stdout: "All configs for topic nope are:\n  cleanup.policy=compact,delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact,delete}\n  retention.ms=1000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=1000, DEFAULT_CONFIG:log.retention.ms=null}\n  secret.thing=null sensitive=true synonyms={DYNAMIC_TOPIC_CONFIG:secret.thing=null}\n  segment.bytes=1073741824 sensitive=false synonyms={DEFAULT_CONFIG:segment.bytes=1073741824}\n",
            api_keys: vec![DESCRIBE_CONFIGS],
            describes: vec![describe_request(2, "nope", true)],
        },
    ];
    run_describe_cases(cases).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_brokers_and_their_loggers_prints_the_jvm_lines() {
    let cases = vec![
        DescribeCase {
            name: "one broker, asked of that broker",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    cluster(&[1]),
                    describe_configs(vec![
                        config("log.cleaner.threads", Some("2"), 2, false),
                        config("log.retention.ms", Some("5"), 3, false),
                    ]),
                ]
            },
            argv: &[
                "--describe",
                "--entity-type",
                "brokers",
                "--entity-name",
                "1",
            ],
            stdout: "Dynamic configs for broker 1 are:\n  log.cleaner.threads=2 sensitive=false synonyms={DYNAMIC_BROKER_CONFIG:log.cleaner.threads=2}\n",
            api_keys: vec![DESCRIBE_CLUSTER, METADATA, DESCRIBE_CONFIGS],
            describes: vec![describe_request(4, "1", true)],
        },
        DescribeCase {
            name: "the default broker",
            replies: |_| {
                vec![
                    cluster(&[1]),
                    describe_configs(vec![
                        config("log.cleaner.threads", Some("2"), 2, false),
                        config("log.retention.ms", Some("5"), 3, false),
                    ]),
                ]
            },
            argv: &["--describe", "--broker-defaults"],
            stdout: "Default configs for brokers in the cluster are:\n  log.retention.ms=5 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=5}\n",
            api_keys: vec![DESCRIBE_CLUSTER, DESCRIBE_CONFIGS],
            describes: vec![describe_request(4, "", true)],
        },
        DescribeCase {
            name: "a broker that is not in the cluster",
            replies: |_| vec![cluster(&[1])],
            argv: &["--describe", "--broker", "9"],
            stdout: "The broker '9' doesn't exist and doesn't have dynamic config.\n",
            api_keys: vec![DESCRIBE_CLUSTER],
            describes: Vec::new(),
        },
        DescribeCase {
            name: "every broker, then the default",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    cluster(&[1]),
                    describe_configs(Vec::new()),
                ]
            },
            argv: &["--describe", "--entity-type", "brokers"],
            stdout: "Dynamic configs for broker 1 are:\nDefault configs for brokers in the cluster are:\n",
            api_keys: vec![
                DESCRIBE_CLUSTER,
                METADATA,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
            ],
            describes: vec![
                describe_request(4, "1", true),
                describe_request(4, "", true),
            ],
        },
        DescribeCase {
            name: "the loggers of a broker, every level",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    cluster(&[1]),
                    describe_configs(vec![
                        config("root", Some("INFO"), 5, false),
                        config("kafka.server", Some("DEBUG"), 6, false),
                    ]),
                ]
            },
            argv: &["--describe", "--broker-logger", "1"],
            stdout: "Dynamic configs for broker-logger 1 are:\n  kafka.server=DEBUG sensitive=false synonyms={DYNAMIC_BROKER_LOGGER_CONFIG:kafka.server=DEBUG}\n  root=INFO sensitive=false synonyms={DEFAULT_CONFIG:root=INFO}\n",
            api_keys: vec![DESCRIBE_CLUSTER, METADATA, DESCRIBE_CONFIGS],
            describes: vec![describe_request(8, "1", true)],
        },
    ];
    run_describe_cases(cases).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_subscriptions_and_groups_prints_the_jvm_lines() {
    let cases = vec![
        DescribeCase {
            name: "a client-metrics subscription",
            replies: |_| {
                vec![
                    config_resources(16, &["cm"]),
                    describe_configs(vec![config("interval.ms", Some("1000"), 7, false)]),
                ]
            },
            argv: &["--describe", "--client-metrics", "cm"],
            stdout: "Dynamic configs for client-metric cm are:\n  interval.ms=1000 sensitive=false synonyms={DYNAMIC_CLIENT_METRICS_CONFIG:interval.ms=1000}\n",
            api_keys: vec![LIST_CONFIG_RESOURCES, DESCRIBE_CONFIGS],
            describes: vec![describe_request(16, "cm", true)],
        },
        DescribeCase {
            name: "a missing client-metrics subscription",
            replies: |_| vec![config_resources(16, &["cm"])],
            argv: &[
                "--describe",
                "--entity-type",
                "client-metrics",
                "--entity-name",
                "nope",
            ],
            stdout: "The client-metric 'nope' doesn't exist and doesn't have dynamic config.\n",
            api_keys: vec![LIST_CONFIG_RESOURCES],
            describes: Vec::new(),
        },
        DescribeCase {
            name: "every client-metrics subscription",
            replies: |_| {
                vec![
                    config_resources(16, &["b", "a"]),
                    describe_configs(Vec::new()),
                ]
            },
            argv: &["--describe", "--entity-type", "client-metrics"],
            stdout: "Dynamic configs for client-metric b are:\nDynamic configs for client-metric a are:\n",
            api_keys: vec![LIST_CONFIG_RESOURCES, DESCRIBE_CONFIGS, DESCRIBE_CONFIGS],
            describes: vec![
                describe_request(16, "b", true),
                describe_request(16, "a", true),
            ],
        },
        DescribeCase {
            name: "a listed group",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    groups(&["g"]),
                    describe_configs(vec![config(
                        "consumer.session.timeout.ms",
                        Some("50000"),
                        8,
                        false,
                    )]),
                ]
            },
            argv: &["--describe", "--group", "g"],
            stdout: "Dynamic configs for group g are:\n  consumer.session.timeout.ms=50000 sensitive=false synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}\n",
            api_keys: vec![METADATA, LIST_GROUPS, DESCRIBE_CONFIGS],
            describes: vec![describe_request(32, "g", true)],
        },
        DescribeCase {
            name: "a group that has configs and no members",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    groups(&[]),
                    config_resources(32, &["idle"]),
                    describe_configs(Vec::new()),
                ]
            },
            argv: &["--describe", "--group", "idle"],
            stdout: "Dynamic configs for group idle are:\n",
            api_keys: vec![
                METADATA,
                LIST_GROUPS,
                LIST_CONFIG_RESOURCES,
                DESCRIBE_CONFIGS,
            ],
            describes: vec![describe_request(32, "idle", true)],
        },
        DescribeCase {
            name: "a missing group",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    groups(&["g"]),
                    config_resources(32, &[]),
                ]
            },
            argv: &["--describe", "--group", "nope"],
            stdout: "The group 'nope' doesn't exist and doesn't have dynamic config.\n",
            api_keys: vec![METADATA, LIST_GROUPS, LIST_CONFIG_RESOURCES],
            describes: Vec::new(),
        },
        DescribeCase {
            name: "every group, listed and configured, in Scala set order",
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    groups(&["g1", "g2", "g3"]),
                    config_resources(32, &["g4", "g5", "g1"]),
                    describe_configs(Vec::new()),
                ]
            },
            argv: &["--describe", "--entity-type", "groups"],
            stdout: "Dynamic configs for group g2 are:\nDynamic configs for group g1 are:\nDynamic configs for group g5 are:\nDynamic configs for group g3 are:\nDynamic configs for group g4 are:\n",
            api_keys: vec![
                METADATA,
                LIST_GROUPS,
                LIST_CONFIG_RESOURCES,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
                DESCRIBE_CONFIGS,
            ],
            describes: ["g2", "g1", "g5", "g3", "g4"]
                .iter()
                .map(|group| describe_request(32, group, true))
                .collect(),
        },
    ];
    run_describe_cases(cases).await;
}

async fn run_describe_cases(cases: Vec<DescribeCase>) {
    for case in cases {
        let broker = Broker::start_with(|port| (case.replies)(&port)).await;
        let run = krabka(configs(&broker, case.argv), "off").await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), case.stdout, ""),
            "{}",
            case.name
        );
        check!(broker.api_keys() == case.api_keys, "{}", case.name);
        check!(
            broker.decoded::<DescribeConfigsRequest>() == case.describes,
            "{}",
            case.name
        );
    }
}

// The describe of topic t1 with the entries of `topic_entries`.
const TOPIC_T1: &str = "Dynamic configs for topic t1 are:\n  cleanup.policy=compact,delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact,delete}\n  retention.ms=1000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=1000, DEFAULT_CONFIG:log.retention.ms=null}\n  secret.thing=null sensitive=true synonyms={DYNAMIC_TOPIC_CONFIG:secret.thing=null}\n";

#[tokio::test(flavor = "multi_thread")]
async fn describe_in_json_withholds_a_sensitive_value() {
    let broker = Broker::start_with(|port| {
        vec![
            metadata(&port, &["t1"]),
            describe_configs(vec![
                config("retention.ms", Some("1000"), 1, false),
                config("secret.thing", Some("hunter2"), 1, true),
            ]),
        ]
    })
    .await;
    let run = krabka(
        json_output(configs(&broker, &["--describe", "--topic", "t1"])),
        "trace",
    )
    .await;
    let data: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(
        data == json!({"data": [{
            "entity_type": "topics",
            "entity_name": "t1",
            "exists": true,
            "configs": [
                {
                    "name": "retention.ms",
                    "value": "1000",
                    "sensitive": false,
                    "synonyms": [{"source": "DYNAMIC_TOPIC_CONFIG", "name": "retention.ms", "value": "1000"}],
                },
                {
                    "name": "secret.thing",
                    "value": null,
                    "sensitive": true,
                    "synonyms": [{"source": "DYNAMIC_TOPIC_CONFIG", "name": "secret.thing", "value": null}],
                },
            ],
        }]})
    );
    check!(!run.stderr.contains("hunter2"));
}

// One alter of a config resource: the replies, the flags, the stdout, and
// the `IncrementalAlterConfigs` request.
struct AlterCase {
    replies: fn(&Arc<AtomicU16>) -> Vec<Canned>,
    argv: &'static [&'static str],
    stdout: &'static str,
    request: IncrementalAlterConfigsRequest,
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_of_each_config_resource_sends_deletes_then_sets() {
    let cases = vec![
        AlterCase {
            replies: |_| vec![altered_configs()],
            argv: &[
                "--alter",
                "--entity-type",
                "topics",
                "--entity-name",
                "t1",
                "--add-config",
                "retention.ms=1000,cleanup.policy=[compact,delete]",
                "--delete-config",
                "segment.bytes",
            ],
            stdout: "Completed updating config for topic t1.\n",
            request: alter_request(
                2,
                "t1",
                vec![
                    alterable("segment.bytes", 1, Some("")),
                    alterable("cleanup.policy", 0, Some("compact,delete")),
                    alterable("retention.ms", 0, Some("1000")),
                ],
            ),
        },
        AlterCase {
            replies: |_| vec![altered_configs()],
            argv: &[
                "--alter",
                "--entity-type",
                "brokers",
                "--entity-default",
                "--add-config",
                "log.cleaner.threads=2",
            ],
            stdout: "Completed updating default config for brokers in the cluster.\n",
            request: alter_request(4, "", vec![alterable("log.cleaner.threads", 0, Some("2"))]),
        },
        AlterCase {
            replies: |port| vec![metadata(port, &[]), altered_configs()],
            argv: &[
                "--alter",
                "--broker",
                "1",
                "--delete-config",
                "log.cleaner.threads",
            ],
            stdout: "Completed updating config for broker 1.\n",
            request: alter_request(4, "1", vec![alterable("log.cleaner.threads", 1, Some(""))]),
        },
        AlterCase {
            replies: |_| vec![altered_configs()],
            argv: &[
                "--alter",
                "--group",
                "g",
                "--add-config",
                "consumer.session.timeout.ms=50000",
            ],
            stdout: "Completed updating config for group g.\n",
            request: alter_request(
                32,
                "g",
                vec![alterable("consumer.session.timeout.ms", 0, Some("50000"))],
            ),
        },
        AlterCase {
            replies: |_| vec![altered_configs()],
            argv: &[
                "--alter",
                "--client-metrics",
                "cm",
                "--add-config",
                "metrics=[a,b],interval.ms=1000",
            ],
            stdout: "Completed updating config for client-metric cm.\n",
            request: alter_request(
                16,
                "cm",
                vec![
                    alterable("interval.ms", 0, Some("1000")),
                    alterable("metrics", 0, Some("a,b")),
                ],
            ),
        },
        AlterCase {
            replies: |port| {
                vec![
                    metadata(port, &[]),
                    describe_configs(vec![
                        config("kafka.server", Some("INFO"), 5, false),
                        config("kafka.log", Some("INFO"), 5, false),
                    ]),
                    altered_configs(),
                ]
            },
            argv: &[
                "--alter",
                "--broker-logger",
                "1",
                "--add-config",
                "kafka.server=DEBUG",
                "--delete-config",
                "kafka.log",
            ],
            stdout: "Completed updating config for broker-logger 1.\n",
            request: alter_request(
                8,
                "1",
                vec![
                    alterable("kafka.log", 1, Some("")),
                    alterable("kafka.server", 0, Some("DEBUG")),
                ],
            ),
        },
    ];
    for case in cases {
        let broker = Broker::start_with(|port| (case.replies)(&port)).await;
        let run = krabka(configs(&broker, case.argv), "off").await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), case.stdout, ""),
            "{:?}",
            case.argv
        );
        check!(
            broker.decoded::<IncrementalAlterConfigsRequest>()
                == std::slice::from_ref(&case.request),
            "{:?}",
            case.argv
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broker_logger_the_broker_does_not_have_is_refused_before_any_change() {
    let broker = Broker::start_with(|port| {
        vec![
            metadata(&port, &[]),
            describe_configs(vec![config("kafka.server", Some("INFO"), 5, false)]),
            altered_configs(),
        ]
    })
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--broker-logger",
                "1",
                "--add-config",
                "kafka.server=DEBUG,no.such=WARN",
                "--delete-config",
                "missing",
            ],
        ),
        "off",
    )
    .await;
    check!(
        (run.code, run.stderr.as_str())
            == (
                Some(1),
                "krabka configs: Invalid broker logger(s): missing,no.such\n"
            )
    );
    check!(broker.decoded::<DescribeConfigsRequest>() == [describe_request(8, "1", false)]);
    check!(
        broker
            .decoded::<IncrementalAlterConfigsRequest>()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_topic_alter_fails_with_the_kafka_error() {
    let broker = Broker::start(vec![canned::<IncrementalAlterConfigsRequest>(
        IncrementalAlterConfigsResponse {
            responses: vec![AlterConfigsResourceResponse {
                error_code: 40,
                error_message: Some("Unknown topic config name: bogus.config".into()),
                resource_type: 2,
                resource_name: "t1".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        json_output(configs(
            &broker,
            &["--alter", "--topic", "t1", "--add-config", "bogus.config=1"],
        )),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    let error: Value = serde_json::from_str(&run.stderr).unwrap();
    check!(
        error
            == json!({"error": {
                "code": 1,
                "message": "IncrementalAlterConfigs failed: INVALID_CONFIG (40): Unknown topic config name: bogus.config",
            }})
    );
}

// One quota entity of a response: each entity type and name.
type Entity<'a> = &'a [(&'a str, Option<&'a str>)];

fn quota_entity(entity: Entity) -> Vec<EntityData> {
    entity
        .iter()
        .map(|(entity_type, name)| EntityData {
            entity_type: (*entity_type).into(),
            entity_name: name.map(Into::into),
            ..Default::default()
        })
        .collect()
}

// `DescribeClientQuotas` that answers each entity with its values.
fn described_quotas(entries: &[(Entity, &[(&str, f64)])]) -> Canned {
    canned::<DescribeClientQuotasRequest>(DescribeClientQuotasResponse {
        entries: Some(
            entries
                .iter()
                .map(|(entity, values)| EntryData {
                    entity: quota_entity(entity),
                    values: values
                        .iter()
                        .map(|(key, value)| ValueData {
                            key: (*key).into(),
                            value: *value,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
        ),
        ..Default::default()
    })
}

fn user_quotas(values: &[(&str, f64)]) -> Canned {
    if values.is_empty() {
        described_quotas(&[])
    } else {
        described_quotas(&[(&[("user", Some("alice"))], values)])
    }
}

// A filter component: the entity type, the match type (0 exact, 1 default,
// 2 any), and the name.
fn component(entity_type: &str, match_type: i8, name: Option<&str>) -> ComponentData {
    ComponentData {
        entity_type: entity_type.into(),
        match_type,
        match_: name.map(Into::into),
        ..Default::default()
    }
}

fn filter(components: Vec<ComponentData>) -> DescribeClientQuotasRequest {
    DescribeClientQuotasRequest {
        components,
        strict: true,
        ..Default::default()
    }
}

fn quota_filter() -> DescribeClientQuotasRequest {
    filter(vec![component("user", 0, Some("alice"))])
}

// The current quotas, the flags, and the ops that the alter sends.
type QuotaCase<'a> = (&'a [(&'a str, f64)], &'a [&'a str], Vec<OpData>);

// `AlterClientQuotas` that accepts each entity.
fn altered_quotas() -> Canned {
    replier::<AlterClientQuotasRequest>(|request: AlterClientQuotasRequest| {
        AlterClientQuotasResponse {
            entries: request
                .entries
                .into_iter()
                .map(|entry| AlterResult {
                    entity: entry
                        .entity
                        .into_iter()
                        .map(|entity| ResultEntity {
                            entity_type: entity.entity_type,
                            entity_name: entity.entity_name,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    })
}

fn op(key: &str, value: f64, remove: bool) -> OpData {
    OpData {
        key: key.into(),
        value,
        remove,
        ..Default::default()
    }
}

fn alter_quotas_request(entity: Entity, ops: Vec<OpData>) -> AlterClientQuotasRequest {
    AlterClientQuotasRequest {
        entries: vec![AlterEntry {
            entity: entity
                .iter()
                .map(|(entity_type, name)| AlterEntity {
                    entity_type: (*entity_type).into(),
                    entity_name: name.map(Into::into),
                    ..Default::default()
                })
                .collect(),
            ops,
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_user_quotas_sets_each_added_quota_and_removes_each_deleted_one() {
    let cases: [QuotaCase; 3] = [
        (
            &[],
            &["--add-config", "consumer_byte_rate=1024"],
            vec![op("consumer_byte_rate", 1024.0, false)],
        ),
        (
            &[("consumer_byte_rate", 1024.0), ("producer_byte_rate", 5.0)],
            &["--delete-config", "consumer_byte_rate"],
            vec![op("consumer_byte_rate", 0.0, true)],
        ),
        // Kafka sends a quota that already has the value, too.
        (
            &[("consumer_byte_rate", 1024.0)],
            &[
                "--add-config",
                "consumer_byte_rate=1024,producer_byte_rate=2e7",
            ],
            vec![
                op("producer_byte_rate", 2.0e7, false),
                op("consumer_byte_rate", 1024.0, false),
            ],
        ),
    ];
    for (current, flags, ops) in cases {
        let broker = Broker::start(vec![user_quotas(current), altered_quotas()]).await;
        let argv = [
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
            ],
            flags,
        ]
        .concat();
        let run = krabka(configs(&broker, &argv), "off").await;
        check!(
            (run.code, run.stdout.as_str())
                == (Some(0), "Completed updating config for user alice.\n"),
            "{flags:?}"
        );
        check!(broker.decoded::<DescribeClientQuotasRequest>() == [quota_filter()]);
        check!(
            broker.decoded::<AlterClientQuotasRequest>()
                == [alter_quotas_request(&[("user", Some("alice"))], ops)],
            "{flags:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_quotas_of_each_entity_form_sends_its_entity() {
    let cases: [(
        &[&str],
        DescribeClientQuotasRequest,
        AlterClientQuotasRequest,
        &str,
    ); 6] = [
        (
            &[
                "--alter",
                "--client",
                "c1",
                "--add-config",
                "request_percentage=50",
            ],
            filter(vec![component("client-id", 0, Some("c1"))]),
            alter_quotas_request(
                &[("client-id", Some("c1"))],
                vec![op("request_percentage", 50.0, false)],
            ),
            "Completed updating config for client c1.\n",
        ),
        (
            &[
                "--alter",
                "--client-defaults",
                "--add-config",
                "request_percentage=50",
            ],
            filter(vec![component("client-id", 1, None)]),
            alter_quotas_request(
                &[("client-id", None)],
                vec![op("request_percentage", 50.0, false)],
            ),
            "Completed updating default config for clients in the cluster.\n",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-default",
                "--add-config",
                "producer_byte_rate=10",
            ],
            filter(vec![component("user", 1, None)]),
            alter_quotas_request(
                &[("user", None)],
                vec![op("producer_byte_rate", 10.0, false)],
            ),
            "Completed updating default config for users in the cluster.\n",
        ),
        (
            &[
                "--alter",
                "--user",
                "alice",
                "--client",
                "c1",
                "--add-config",
                "consumer_byte_rate=5",
            ],
            filter(vec![
                component("client-id", 0, Some("c1")),
                component("user", 0, Some("alice")),
            ]),
            alter_quotas_request(
                &[("client-id", Some("c1")), ("user", Some("alice"))],
                vec![op("consumer_byte_rate", 5.0, false)],
            ),
            "Completed updating config for client c1.\n",
        ),
        (
            &[
                "--alter",
                "--ip",
                "127.0.0.1",
                "--add-config",
                "connection_creation_rate=10",
            ],
            filter(vec![component("ip", 0, Some("127.0.0.1"))]),
            alter_quotas_request(
                &[("ip", Some("127.0.0.1"))],
                vec![op("connection_creation_rate", 10.0, false)],
            ),
            "Completed updating config for ip 127.0.0.1.\n",
        ),
        (
            &[
                "--alter",
                "--ip-defaults",
                "--add-config",
                "connection_creation_rate=10",
            ],
            filter(vec![component("ip", 1, None)]),
            alter_quotas_request(
                &[("ip", None)],
                vec![op("connection_creation_rate", 10.0, false)],
            ),
            "Completed updating default config for ips in the cluster.\n",
        ),
    ];
    for (argv, describe, alter, stdout) in cases {
        let broker = Broker::start(vec![described_quotas(&[]), altered_quotas()]).await;
        let run = krabka(configs(&broker, argv), "off").await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), stdout, ""),
            "{argv:?}"
        );
        check!(
            broker.decoded::<DescribeClientQuotasRequest>() == [describe],
            "{argv:?}"
        );
        check!(
            broker.decoded::<AlterClientQuotasRequest>() == [alter],
            "{argv:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_quotas_of_each_entity_form_prints_the_kafka_lines() {
    let cases: [(&[&str], DescribeClientQuotasRequest, Canned, &str); 4] = [
        (
            &["--describe", "--client-defaults"],
            filter(vec![component("client-id", 1, None)]),
            described_quotas(&[(&[("client-id", None)], &[("request_percentage", 50.0)])]),
            "Quota configs for the default client-id are request_percentage=50.0\n",
        ),
        (
            &["--describe", "--user", "alice", "--client", "c1"],
            filter(vec![
                component("client-id", 0, Some("c1")),
                component("user", 0, Some("alice")),
            ]),
            described_quotas(&[(
                &[("user", Some("alice")), ("client-id", Some("c1"))],
                &[("consumer_byte_rate", 5.0)],
            )]),
            "Quota configs for user-principal 'alice', client-id 'c1' are consumer_byte_rate=5.0\n",
        ),
        (
            &["--describe", "--ip", "127.0.0.1"],
            filter(vec![component("ip", 0, Some("127.0.0.1"))]),
            described_quotas(&[(
                &[("ip", Some("127.0.0.1"))],
                &[("connection_creation_rate", 10.0)],
            )]),
            "Quota configs for ip '127.0.0.1' are connection_creation_rate=10.0\n",
        ),
        (
            &["--describe", "--entity-type", "ips"],
            filter(vec![component("ip", 2, None)]),
            described_quotas(&[]),
            "",
        ),
    ];
    for (argv, describe, reply, stdout) in cases {
        let broker = Broker::start(vec![reply]).await;
        let run = krabka(configs(&broker, argv), "off").await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), stdout, ""),
            "{argv:?}"
        );
        check!(
            broker.decoded::<DescribeClientQuotasRequest>() == [describe],
            "{argv:?}"
        );
        check!(
            broker.api_keys() == [DescribeClientQuotasRequest::API_KEY],
            "{argv:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_quota_the_user_does_not_have_is_refused_before_any_change() {
    let broker = Broker::start(vec![
        user_quotas(&[("consumer_byte_rate", 1.0)]),
        altered_quotas(),
    ])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--user",
                "alice",
                "--delete-config",
                "producer_byte_rate",
            ],
        ),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stderr == "krabka configs: Invalid config(s): producer_byte_rate\n");
    check!(broker.api_keys() == [DescribeClientQuotasRequest::API_KEY]);
}

fn altered_scram() -> Canned {
    canned::<AlterUserScramCredentialsRequest>(AlterUserScramCredentialsResponse {
        results: vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            ..Default::default()
        }],
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_upsert_sends_a_salted_password_and_the_plaintext_goes_nowhere() {
    const PLAINTEXT: &str = "plaintext-hunter2";
    let broker = Broker::start(vec![altered_scram()]).await;
    let spec = format!("SCRAM-SHA-512=[iterations=8192,password={PLAINTEXT}]");
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                &spec,
            ],
        ),
        "trace",
    )
    .await;
    check!(run.code == Some(0));
    check!(run.stdout == "Completed updating config for user alice.\n");
    check!(!run.stdout.contains(PLAINTEXT));
    check!(!run.stderr.contains(PLAINTEXT));
    check!(!broker.raw().iter().any(|bytes| {
        bytes
            .windows(PLAINTEXT.len())
            .any(|window| window == PLAINTEXT.as_bytes())
    }));
    let requests = broker.decoded::<AlterUserScramCredentialsRequest>();
    assert!(requests.len() == 1);
    let request = &requests[0];
    check!(request.deletions.is_empty());
    assert!(request.upsertions.len() == 1);
    let upsertion = &request.upsertions[0];
    check!(
        (
            upsertion.name.as_str(),
            upsertion.mechanism,
            upsertion.iterations
        ) == ("alice", 2, 8192)
    );
    check!(upsertion.salt.len() == 16);
    let expected = krabka_security::pbkdf2_salted(
        PLAINTEXT.as_bytes(),
        krabka_security::SaslMechanism::ScramSha512,
        8192,
        &upsertion.salt,
    );
    check!(upsertion.salted_password.as_ref() == expected.as_slice());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_delete_sends_the_mechanism_of_the_key() {
    let cases = [("SCRAM-SHA-256", 1), ("SCRAM-SHA-512", 2)];
    for (key, mechanism) in cases {
        let broker = Broker::start(vec![altered_scram()]).await;
        let run = krabka(
            configs(
                &broker,
                &["--alter", "--user", "alice", "--delete-config", key],
            ),
            "off",
        )
        .await;
        check!(run.code == Some(0), "{key}");
        check!(
            broker.decoded::<AlterUserScramCredentialsRequest>()
                == [AlterUserScramCredentialsRequest {
                    deletions: vec![ScramCredentialDeletion {
                        name: "alice".into(),
                        mechanism,
                        ..Default::default()
                    }],
                    upsertions: Vec::new(),
                    ..Default::default()
                }],
            "{key}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_delete_the_broker_refuses_fails_with_its_error() {
    let broker = Broker::start(vec![canned::<AlterUserScramCredentialsRequest>(
        AlterUserScramCredentialsResponse {
            results: vec![AlterUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 91,
                error_message: Some(
                    "Attempt to delete a user credential that does not exist".into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--user",
                "alice",
                "--delete-config",
                "SCRAM-SHA-512",
            ],
        ),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(
        run.stderr
            == "krabka configs: AlterUserScramCredentials failed: RESOURCE_NOT_FOUND (91): Attempt to delete a user credential that does not exist\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_user_prints_quotas_then_scram_credentials() {
    let scram = |results: Vec<DescribeUserScramCredentialsResult>| {
        canned::<DescribeUserScramCredentialsRequest>(DescribeUserScramCredentialsResponse {
            results,
            ..Default::default()
        })
    };
    let cases = [
        (
            &[
                ("consumer_byte_rate", 1024.0),
                ("producer_byte_rate", 2.0e7),
            ][..],
            vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                credential_infos: vec![
                    CredentialInfo {
                        mechanism: 1,
                        iterations: 8192,
                        ..Default::default()
                    },
                    CredentialInfo {
                        mechanism: 2,
                        iterations: 4096,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            "Quota configs for user-principal 'alice' are consumer_byte_rate=1024.0, producer_byte_rate=2.0E7\nSCRAM credential configs for user-principal 'alice' are SCRAM-SHA-256=iterations=8192, SCRAM-SHA-512=iterations=4096\n",
        ),
        (
            &[][..],
            vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 91,
                ..Default::default()
            }],
            "",
        ),
    ];
    for (quotas, results, expected) in cases {
        let broker = Broker::start(vec![user_quotas(quotas), scram(results)]).await;
        let run = krabka(
            configs(
                &broker,
                &[
                    "--describe",
                    "--entity-type",
                    "users",
                    "--entity-name",
                    "alice",
                ],
            ),
            "off",
        )
        .await;
        check!((run.code, run.stdout.as_str()) == (Some(0), expected));
        check!(broker.decoded::<DescribeClientQuotasRequest>() == [quota_filter()]);
        check!(
            broker.decoded::<DescribeUserScramCredentialsRequest>()
                == [DescribeUserScramCredentialsRequest {
                    users: Some(vec![UserName {
                        name: "alice".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_every_user_lists_every_quota_then_every_scram_credential() {
    let broker = Broker::start(vec![
        described_quotas(&[
            (&[("user", Some("bob"))], &[("consumer_byte_rate", 1.0)]),
            (&[("user", None)], &[("producer_byte_rate", 2.0)]),
            (&[("user", Some("alice"))], &[("request_percentage", 3.0)]),
        ]),
        canned::<DescribeUserScramCredentialsRequest>(DescribeUserScramCredentialsResponse {
            results: vec![
                DescribeUserScramCredentialsResult {
                    user: "bob".into(),
                    credential_infos: vec![CredentialInfo {
                        mechanism: 2,
                        iterations: 4096,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescribeUserScramCredentialsResult {
                    user: "carol".into(),
                    error_code: 31,
                    error_message: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
    ])
    .await;
    let run = krabka(
        configs(&broker, &["--describe", "--entity-type", "users"]),
        "off",
    )
    .await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "Quota configs for the default user-principal are producer_byte_rate=2.0\nQuota configs for user-principal 'alice' are request_percentage=3.0\nQuota configs for user-principal 'bob' are consumer_byte_rate=1.0\nSCRAM credential configs for user-principal 'bob' are SCRAM-SHA-512=iterations=4096\nError retrieving SCRAM credential configs for user-principal 'carol': ExecutionException: org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.\n"
            )
    );
    check!(
        broker.decoded::<DescribeClientQuotasRequest>()
            == [filter(vec![component("user", 2, None)])]
    );
    check!(
        broker.decoded::<DescribeUserScramCredentialsRequest>()
            == [DescribeUserScramCredentialsRequest {
                users: Some(Vec::new()),
                ..Default::default()
            }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_without_incremental_alter_configs_fails_with_the_kafka_message() {
    // `UNSUPPORTED_VERSION`, which Kafka raises as the
    // `UnsupportedVersionException` that `alterConfig` rewords.
    let broker = Broker::start(vec![replier::<IncrementalAlterConfigsRequest>(
        |request: IncrementalAlterConfigsRequest| IncrementalAlterConfigsResponse {
            responses: request
                .resources
                .into_iter()
                .map(|resource| AlterConfigsResourceResponse {
                    error_code: 35,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--group",
                "g",
                "--add-config",
                "consumer.session.timeout.ms=50000",
            ],
        ),
        "off",
    )
    .await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(1),
                "",
                "krabka configs: The INCREMENTAL_ALTER_CONFIGS API is not supported by the cluster. The API is supported starting from version 2.3.0. You may want to use an older version of this tool to interact with your cluster, or upgrade your brokers to version 2.3.0 or newer to avoid this error.\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn help_lists_the_kafka_configs_flags() {
    let run = krabka(vec!["configs".into(), "--help".into()], "off").await;
    check!(run.code == Some(0));
    for flag in [
        "--describe",
        "--alter",
        "--entity-type",
        "--entity-name",
        "--entity-default",
        "--add-config",
        "--add-config-file",
        "--delete-config",
        "--all",
        "--bootstrap-server",
        "--bootstrap-controller",
        "--command-config",
        "--topic",
        "--client",
        "--client-defaults",
        "--user",
        "--user-defaults",
        "--broker",
        "--broker-defaults",
        "--broker-logger",
        "--ip",
        "--ip-defaults",
        "--group",
        "--client-metrics",
    ] {
        check!(
            run.stdout.split_whitespace().any(|word| word == flag),
            "{flag}"
        );
    }
    for entity_type in [
        "topics",
        "clients",
        "users",
        "brokers",
        "broker-loggers",
        "ips",
        "client-metrics",
        "groups",
    ] {
        check!(run.stdout.contains(entity_type), "{entity_type}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alter_with_nothing_to_change_reports_the_entity_as_kafka_does() {
    let cases: [(&[&str], &str); 3] = [
        (
            &["--alter", "--client", "c1", "--add-config", ","],
            "Completed updating config for client c1.\n",
        ),
        (
            &["--alter", "--user", "alice", "--add-config", ","],
            "Completed updating config for user alice.\n",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-default",
                "--add-config",
                ",",
            ],
            "Completed updating default config for users in the cluster.\n",
        ),
    ];
    for (argv, expected) in cases {
        let broker = Broker::start(Vec::new()).await;
        let run = krabka(configs(&broker, argv), "off").await;
        check!(
            (run.code, run.stdout.as_str()) == (Some(0), expected),
            "{argv:?}"
        );
        check!(broker.api_keys().is_empty(), "{argv:?}");
    }
}
