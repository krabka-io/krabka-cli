//! `krabka metadata-quorum`, `krabka cluster`, `krabka leader-election` and
//! `krabka reassign-partitions`, run as the binary against a scripted broker
//! that records the body of every request, so a test decodes the exact
//! requests the command sent.

use std::{
    collections::BTreeMap,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        add_raft_voter_request::{AddRaftVoterRequest, Listener},
        add_raft_voter_response::AddRaftVoterResponse,
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
        alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
        },
        alter_replica_log_dirs_response::{
            AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
            AlterReplicaLogDirsResponse,
        },
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        common::describe_quorum_response::replica_state::ReplicaState,
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_log_dirs_request::{DescribableLogDirTopic, DescribeLogDirsRequest},
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::{DescribeQuorumResponse, PartitionData, TopicData},
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
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
        remove_raft_voter_request::RemoveRaftVoterRequest,
        remove_raft_voter_response::RemoveRaftVoterResponse,
        unregister_broker_request::UnregisterBrokerRequest,
        unregister_broker_response::UnregisterBrokerResponse,
    },
    primitives::{uuid::Uuid, varint::get_uvarint},
};
use serde_json::{Value, json};

const CLUSTER_ID: &str = "5L6g3nShT-eMCtK--X86sw";
const DIRECTORY_2: &str = "AAAAAAAAAAAAAAAAAAAAAg";
const BROKER_RESOURCE: i8 = 4;
const TOPIC_RESOURCE: i8 = 2;
const SET: i8 = 0;
const DELETE: i8 = 1;

// ---------------------------------------------------------------------------
// The scripted broker.

// One recorded request: its API key, its version, and the bytes after its
// correlation id.
type Request = (i16, i16, Vec<u8>);

// The answer to one API: its highest version, and the encoder of the response
// from the request's version and raw bytes.
type Canned = (i16, i16, Box<dyn Fn(i16, &[u8]) -> Vec<u8> + Send>);

fn canned<R: ProtocolRequest>(response: R::Response) -> Canned
where
    R::Response: Encode + Send + 'static,
{
    canned_at::<R>(R::LATEST_STABLE_VERSION, response)
}

// A canned response of an API that the broker advertises up to `max`.
fn canned_at<R: ProtocolRequest>(max: i16, response: R::Response) -> Canned
where
    R::Response: Encode + Send + 'static,
{
    (
        R::API_KEY,
        max,
        Box::new(move |version, _| encode_response::<R>(&response, version)),
    )
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
        R::LATEST_STABLE_VERSION,
        Box::new(move |version, request| {
            encode_response::<R>(&answer(decode_request::<R>(version, request)), version)
        }),
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

struct Broker {
    inner: krabka_client_core::MockBroker,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Broker {
    // A broker that answers `Metadata` with itself as brokers 1 and 2 at its
    // own port and with `topics`, and every other API from `replies`. An API
    // with no row is not advertised.
    async fn start(topics: Vec<MetadataResponseTopic>, replies: Vec<Canned>) -> Self {
        let mut advertised = vec![
            (api_versions_request::API_KEY, 3),
            (MetadataRequest::API_KEY, 12),
        ];
        advertised.extend(replies.iter().map(|(key, max, _)| (*key, *max)));
        let versions = ApiVersionsResponse {
            api_keys: advertised
                .into_iter()
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
        let own_port = Arc::new(Mutex::new(0_u16));
        let port = Arc::clone(&own_port);
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            log.lock().unwrap().push((api_key, version, body.to_vec()));
            match api_key {
                api_versions_request::API_KEY => Some(versions_body.clone()),
                MetadataRequest::API_KEY => Some(encode_response::<MetadataRequest>(
                    &MetadataResponse {
                        brokers: (1..=2)
                            .map(|node_id| MetadataResponseBroker {
                                node_id,
                                host: "127.0.0.1".into(),
                                port: i32::from(*port.lock().unwrap()),
                                ..Default::default()
                            })
                            .collect(),
                        cluster_id: Some(CLUSTER_ID.into()),
                        controller_id: 1,
                        topics: topics.clone(),
                        ..Default::default()
                    },
                    version,
                )),
                _ => replies.get(&api_key).map(|encode| encode(version, body)),
            }
        })
        .await;
        *own_port.lock().unwrap() = inner.addr.port();
        Self { inner, requests }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
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

    // The `IncrementalAlterConfigs` requests, by their first resource: the
    // client sends each broker's resources to that broker at the same time,
    // so they arrive in any order.
    fn config_alterations(&self) -> Vec<IncrementalAlterConfigsRequest> {
        let mut requests = self.decoded::<IncrementalAlterConfigsRequest>();
        requests.sort_by_key(|request| {
            request
                .resources
                .first()
                .map(|resource| (resource.resource_type, resource.resource_name.clone()))
        });
        requests
    }

    // How many requests of `api_key` the broker received.
    fn count(&self, api_key: i16) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| *key == api_key)
            .count()
    }
}

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

fn write(dir: &Path, name: &str, text: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path.to_str().unwrap().to_owned()
}

fn directory(last: u8) -> Uuid {
    let mut bytes = [0; 16];
    bytes[15] = last;
    Uuid(bytes)
}

fn cluster_broker(
    id: i32,
    host: &str,
    rack: Option<&str>,
    is_fenced: bool,
) -> DescribeClusterBroker {
    DescribeClusterBroker {
        broker_id: id,
        host: host.into(),
        port: 9092,
        rack: rack.map(Into::into),
        is_fenced,
        ..Default::default()
    }
}

// `DescribeCluster` naming `brokers`.
fn describe_cluster(brokers: Vec<DescribeClusterBroker>) -> Canned {
    canned::<DescribeClusterRequest>(DescribeClusterResponse {
        endpoint_type: 1,
        cluster_id: CLUSTER_ID.into(),
        controller_id: brokers.first().map_or(-1, |broker| broker.broker_id),
        brokers,
        cluster_authorized_operations: i32::MIN,
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// metadata-quorum

fn replica(id: i32, last: u8, log_end_offset: i64) -> ReplicaState {
    ReplicaState {
        replica_id: id,
        replica_directory_id: directory(last),
        log_end_offset,
        last_fetch_timestamp: 1_790_000_001_000,
        last_caught_up_timestamp: 1_790_000_001_000,
        ..Default::default()
    }
}

fn describe_quorum() -> Canned {
    canned::<DescribeQuorumRequest>(DescribeQuorumResponse {
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
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_prints_kafkas_status_and_replication_reports() {
    let broker = Broker::start(
        Vec::new(),
        vec![
            describe_quorum(),
            describe_cluster(vec![cluster_broker(1, "b1", None, false)]),
        ],
    )
    .await;
    let address = broker.address();
    let base = [
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "describe",
    ];
    let cases = [
        (
            "--status",
            format!(
                "ClusterId:              {CLUSTER_ID}\n\
                 LeaderId:               1\n\
                 LeaderEpoch:            3\n\
                 HighWatermark:          42\n\
                 MaxFollowerLag:         5\n\
                 MaxFollowerLagTimeMs:   0\n\
                 CurrentVoters:          [{{\"id\": 1, \"directoryId\": \"AAAAAAAAAAAAAAAAAAAAAQ\"}}, \
                 {{\"id\": 2, \"directoryId\": \"{DIRECTORY_2}\"}}]\n\
                 CurrentObservers:       [{{\"id\": 3}}]\n"
            ),
        ),
        (
            "--replication",
            "NodeId\tDirectoryId           \tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus  \t\n\
             1     \tAAAAAAAAAAAAAAAAAAAAAQ\t45          \t0  \t1790000001000     \t1790000001000        \tLeader  \t\n\
             2     \tAAAAAAAAAAAAAAAAAAAAAg\t40          \t5  \t1790000001000     \t1790000001000        \tFollower\t\n\
             3     \tAAAAAAAAAAAAAAAAAAAAAA\t44          \t1  \t1790000001000     \t1790000001000        \tObserver\t\n"
                .to_owned(),
        ),
    ];
    for (flag, expected) in cases {
        let run = krabka(&[&base[..], &[flag]].concat()).await;
        check!(
            (run.code, run.stdout, run.stderr) == (Some(0), expected, String::new()),
            "{flag}"
        );
    }
    // `--status` asks `DescribeCluster` for broker endpoints.
    check!(
        broker.decoded::<DescribeClusterRequest>()
            == [DescribeClusterRequest {
                endpoint_type: 1,
                ..Default::default()
            }]
    );
    let json = krabka(&[&["--output", "json"][..], &base[..], &["--status"]].concat()).await;
    let payload: Value = serde_json::from_str(&json.stdout).unwrap();
    check!(payload["data"]["cluster_id"] == json!(CLUSTER_ID));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quorum_request_the_broker_drops_fails_with_exit_1_and_no_report() {
    let broker = krabka_client_core::MockBroker::start(|api_key, _, _, _| {
        (api_key == api_versions_request::API_KEY).then(|| {
            let mut body = Vec::new();
            ApiVersionsResponse {
                api_keys: [
                    (api_versions_request::API_KEY, 3),
                    (DescribeQuorumRequest::API_KEY, 2),
                ]
                .into_iter()
                .map(|(api_key, max_version)| ApiVersion {
                    api_key,
                    max_version,
                    ..Default::default()
                })
                .collect(),
                ..Default::default()
            }
            .encode(&mut body, 0)
            .unwrap();
            body
        })
    })
    .await;
    let address = broker.addr.to_string();
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

fn remove_voter(error_code: i16) -> Canned {
    canned::<RemoveRaftVoterRequest>(RemoveRaftVoterResponse {
        error_code,
        error_message: (error_code != 0).then(|| "Voter set does not contain 2".into()),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_controller_confirms_dry_runs_and_sends_a_null_cluster_id() {
    let broker = Broker::start(Vec::new(), vec![remove_voter(0)]).await;
    let address = broker.address();
    let base = [
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "remove-controller",
        "-i",
        "2",
        "-d",
        DIRECTORY_2,
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
    check!(broker.count(RemoveRaftVoterRequest::API_KEY) == 0);
    let done = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!(
        (done.code, done.stdout.as_str())
            == (
                Some(0),
                "Removed  KRaft controller 2 with directory id AAAAAAAAAAAAAAAAAAAAAg\n"
            )
    );
    check!(
        broker.decoded::<RemoveRaftVoterRequest>()
            == [RemoveRaftVoterRequest {
                cluster_id: None,
                voter_id: 2,
                voter_directory_id: directory(2),
                ..Default::default()
            }]
    );

    let failing = Broker::start(Vec::new(), vec![remove_voter(127)]).await;
    let address = failing.address();
    let failed = krabka(&[
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "remove-controller",
        "-i",
        "2",
        "-d",
        DIRECTORY_2,
        "--yes",
    ])
    .await;
    check!(
        (failed.code, failed.stdout.as_str(), failed.stderr.as_str())
            == (
                Some(1),
                "",
                "krabka metadata-quorum: RemoveRaftVoter failed: VOTER_NOT_FOUND (127): Voter \
                 set does not contain 2\n"
            )
    );
}

/// A controller config, and the `meta.properties` of its metadata log dir.
fn controller_config(dir: &Path) -> String {
    let log_dir = dir.join("meta");
    std::fs::create_dir(&log_dir).unwrap();
    std::fs::write(
        log_dir.join("meta.properties"),
        "version=1\ncluster.id=5L6g3nShT-eMCtK--X86sw\nnode.id=7\n\
         directory.id=AAAAAAAAAAAAAAAAAAAABw\n",
    )
    .unwrap();
    write(
        dir,
        "controller.properties",
        &format!(
            "node.id=7\nprocess.roles=controller\nlog.dirs={}\ncontroller.listener.names=CONTROLLER\n\
             listeners=CONTROLLER://controller-7:9093\n",
            log_dir.display()
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn add_controller_sends_the_identity_of_the_controller_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = controller_config(dir.path());
    let broker = Broker::start(
        Vec::new(),
        vec![canned::<AddRaftVoterRequest>(
            AddRaftVoterResponse::default(),
        )],
    )
    .await;
    let address = broker.address();
    let base = [
        "metadata-quorum",
        "--bootstrap-server",
        &address,
        "--command-config",
        &config,
        "add-controller",
    ];
    let dry = krabka(&[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\nDRY RUN of adding controller 7 with directory id \
                 AAAAAAAAAAAAAAAAAAAABw and endpoints: CONTROLLER://controller-7:9093\n"
            )
    );
    check!(broker.count(AddRaftVoterRequest::API_KEY) == 0);
    let done = krabka(&base).await;
    check!(
        (done.code, done.stdout.as_str(), done.stderr.as_str())
            == (
                Some(0),
                "Added controller 7 with directory id AAAAAAAAAAAAAAAAAAAABw and endpoints: \
                 CONTROLLER://controller-7:9093\n",
                ""
            )
    );
    let sent = broker.decoded::<AddRaftVoterRequest>();
    let timeout_ms = sent.first().map_or(0, |request| request.timeout_ms);
    check!(timeout_ms > 0);
    check!(
        sent == [AddRaftVoterRequest {
            cluster_id: None,
            timeout_ms,
            voter_id: 7,
            voter_directory_id: directory(7),
            listeners: vec![Listener {
                name: "CONTROLLER".into(),
                host: "controller-7".into(),
                port: 9093,
                ..Default::default()
            }],
            ack_when_committed: true,
            ..Default::default()
        }]
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

// ---------------------------------------------------------------------------
// cluster

fn unregister(error_code: i16) -> Canned {
    canned::<UnregisterBrokerRequest>(UnregisterBrokerResponse {
        error_code,
        error_message: (error_code != 0).then(|| "Cluster authorization failed.".into()),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_unregister_confirms_dry_runs_and_reports_as_kafka_cluster_does() {
    let broker = Broker::start(Vec::new(), vec![unregister(0)]).await;
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
    check!(broker.count(UnregisterBrokerRequest::API_KEY) == 0);
    let done = krabka(&[&base[..], &["--yes"]].concat()).await;
    check!((done.code, done.stdout.as_str()) == (Some(0), "Broker 5 is no longer registered.\n"));
    check!(
        broker.decoded::<UnregisterBrokerRequest>()
            == [UnregisterBrokerRequest {
                broker_id: 5,
                ..Default::default()
            }]
    );

    let failing = Broker::start(Vec::new(), vec![unregister(31)]).await;
    let address = failing.address();
    let failed = krabka(&["cluster", "unregister", "-b", &address, "-i", "5", "--yes"]).await;
    check!(
        (failed.code, failed.stderr.as_str())
            == (
                Some(1),
                "krabka cluster: UnregisterBroker failed: CLUSTER_AUTHORIZATION_FAILED (31): \
                 Cluster authorization failed.\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_id_and_list_endpoints_print_what_kafka_cluster_prints() {
    let brokers = vec![
        cluster_broker(2, "b.example", Some("r1"), true),
        cluster_broker(1, "a", None, false),
    ];
    let broker = Broker::start(Vec::new(), vec![describe_cluster(brokers)]).await;
    let address = broker.address();
    let endpoints = "ID         HOST      PORT       RACK STATE      ENDPOINT_TYPE  \n\
                     1          a         9092       null unfenced   broker         \n\
                     2          b.example 9092       r1 fenced     broker         \n";
    let cases: [(&[&str], String); 3] = [
        (&["cluster-id"], format!("Cluster ID: {CLUSTER_ID}\n")),
        (&["list-endpoints"], endpoints.to_owned()),
        (
            &["list-endpoints", "--include-fenced-brokers"],
            endpoints.to_owned(),
        ),
    ];
    for (action, expected) in cases {
        let run = krabka(&[&["cluster"], action, &["-b", &address]].concat()).await;
        check!(
            (run.code, run.stdout, run.stderr) == (Some(0), expected, String::new()),
            "{action:?}"
        );
    }
    let request = |include_fenced_brokers| DescribeClusterRequest {
        endpoint_type: 1,
        include_fenced_brokers,
        ..Default::default()
    };
    check!(
        broker.decoded::<DescribeClusterRequest>()
            == [request(false), request(false), request(true)]
    );

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
async fn fenced_brokers_on_an_old_cluster_print_kafkas_unsupported_version_message() {
    let broker = Broker::start(
        Vec::new(),
        vec![canned_at::<DescribeClusterRequest>(
            1,
            DescribeClusterResponse::default(),
        )],
    )
    .await;
    let address = broker.address();
    let run = krabka(&[
        "cluster",
        "list-endpoints",
        "-b",
        &address,
        "--include-fenced-brokers",
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "Attempted to write a non-default includeFencedBrokers at version 1\n"
            )
    );
    check!(broker.count(DescribeClusterRequest::API_KEY) == 0);
}

// ---------------------------------------------------------------------------
// leader-election

type PartitionAnswer<'a> = (i32, i16, Option<&'a str>);

fn election_answer(results: &[(&str, &[PartitionAnswer<'_>])]) -> ElectLeadersResponse {
    ElectLeadersResponse {
        replica_election_results: results
            .iter()
            .map(|(topic, partitions)| ReplicaElectionResult {
                topic: (*topic).into(),
                partition_result: partitions
                    .iter()
                    .map(|(partition_id, error_code, message)| PartitionResult {
                        partition_id: *partition_id,
                        error_code: *error_code,
                        error_message: message.map(Into::into),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn topic_partitions(entries: &[(&str, &[i32])]) -> Vec<TopicPartitions> {
    entries
        .iter()
        .map(|(topic, partitions)| TopicPartitions {
            topic: (*topic).into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_election_sends_each_selection_and_prints_kafkas_lines() {
    let dir = tempfile::tempdir().unwrap();
    let file = write(
        dir.path(),
        "election.json",
        r#"{"partitions": [{"topic": "foo", "partition": 1}, {"topic": "bar", "partition": 0}]}"#,
    );
    let empty = write(dir.path(), "empty.json", r#"{"partitions": []}"#);
    let answer = election_answer(&[
        (
            "foo",
            &[(0, 0, None), (1, 84, Some("Leader election not needed"))],
        ),
        ("bar", &[(0, 0, None)]),
    ]);
    let broker = Broker::start(Vec::new(), vec![canned::<ElectLeadersRequest>(answer)]).await;
    let address = broker.address();
    let succeeded = "Successfully completed leader election (PREFERRED) for partitions bar-0, \
                     foo-0\nValid replica already elected for partitions foo-1\n";
    let cases: [(&[&str], Option<Vec<TopicPartitions>>); 4] = [
        (
            &["--election-type", "preferred", "--all-topic-partitions"],
            None,
        ),
        (
            &[
                "--election-type",
                "PREFERRED",
                "--topic",
                "foo",
                "--partition",
                "1",
            ],
            Some(topic_partitions(&[("foo", &[1])])),
        ),
        (
            &["--election-type", "preferred", "--path-to-json-file", &file],
            Some(topic_partitions(&[("bar", &[0]), ("foo", &[1])])),
        ),
        (
            &[
                "--election-type",
                "preferred",
                "--path-to-json-file",
                &empty,
            ],
            Some(Vec::new()),
        ),
    ];
    for (argv, topic_partitions) in cases {
        let run =
            krabka(&[&["leader-election", "--bootstrap-server", &address], argv].concat()).await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), succeeded, ""),
            "{argv:?}"
        );
        let sent = broker.decoded::<ElectLeadersRequest>().pop().unwrap();
        check!(
            sent == ElectLeadersRequest {
                election_type: 0,
                topic_partitions,
                timeout_ms: sent.timeout_ms,
                ..Default::default()
            },
            "{argv:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_election_prints_the_exception_and_exits_1() {
    let answer = election_answer(&[("nope", &[(0, 3, Some("No such topic as nope"))])]);
    let broker = Broker::start(Vec::new(), vec![canned::<ElectLeadersRequest>(answer)]).await;
    let address = broker.address();
    let run = krabka(&[
        "leader-election",
        "--bootstrap-server",
        &address,
        "--election-type",
        "unclean",
        "--topic",
        "nope",
        "--partition",
        "0",
    ])
    .await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(1),
                "Error completing leader election (UNCLEAN) for partition: nope-0: \
                 org.apache.kafka.common.errors.UnknownTopicOrPartitionException: No such topic \
                 as nope\n",
                "1 replica(s) could not be elected\n"
            )
    );
    check!(
        broker
            .decoded::<ElectLeadersRequest>()
            .iter()
            .map(|request| (request.election_type, request.topic_partitions.clone()))
            .collect::<Vec<_>>()
            == [(1, Some(topic_partitions(&[("nope", &[0])])))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_election_validates_the_file_before_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let file = write(
        dir.path(),
        "election.json",
        r#"{"partitions": [{"topic": "foo", "partition": 1}, {"topic": "foo", "partition": 1}]}"#,
    );
    let cases: [(&[&str], &str); 2] = [
        (
            &["--election-type", "preferred", "--path-to-json-file", &file],
            "krabka leader-election: Replica election data contains duplicate partitions: \
             [foo-1]\n",
        ),
        (
            &[
                "--election-type",
                "preferred",
                "--all-topic-partitions",
                "--topic",
                "foo",
            ],
            "krabka leader-election: One and only one of the following options is required: \
             topic, all-topic-partitions, path-to-json-file\n",
        ),
    ];
    for (argv, message) in cases {
        let run = krabka(
            &[
                &["leader-election", "--bootstrap-server", "127.0.0.1:1"],
                argv,
            ]
            .concat(),
        )
        .await;
        check!(
            (run.code, run.stderr.as_str()) == (Some(1), message),
            "{argv:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// reassign-partitions

/// Topic `foo`: partitions 0 and 1 on brokers 1,2.
fn foo_topic() -> Vec<MetadataResponseTopic> {
    vec![MetadataResponseTopic {
        name: Some("foo".into()),
        partitions: (0..2)
            .map(|partition_index| MetadataResponsePartition {
                partition_index,
                replica_nodes: vec![1, 2],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }]
}

/// `ListPartitionReassignments`, with an active reassignment of `foo-1`
/// when `active`.
fn list_reassignments(active: bool) -> Canned {
    canned::<ListPartitionReassignmentsRequest>(ListPartitionReassignmentsResponse {
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
    })
}

/// `AlterPartitionReassignments`, answering each partition it names with
/// `error_code`.
fn alter_reassignments(error_code: i16) -> Canned {
    replier::<AlterPartitionReassignmentsRequest>(move |request| {
        AlterPartitionReassignmentsResponse {
            allow_replication_factor_change: request.allow_replication_factor_change,
            responses: request
                .topics
                .iter()
                .map(|topic| ReassignableTopicResponse {
                    name: topic.name.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| ReassignablePartitionResponse {
                            partition_index: partition.partition_index,
                            error_code,
                            error_message: None,
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

/// `IncrementalAlterConfigs`, answering each resource it names.
fn alter_configs() -> Canned {
    replier::<IncrementalAlterConfigsRequest>(|request| IncrementalAlterConfigsResponse {
        responses: request
            .resources
            .iter()
            .map(|resource| AlterConfigsResourceResponse {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// `DescribeLogDirs`: every replica it asks for lives in `/data/a`, with a
/// future replica in `/data/b` when `moving`.
fn describe_log_dirs(moving: bool) -> Canned {
    replier::<DescribeLogDirsRequest>(move |request| {
        let topics = |future: bool| {
            request
                .topics
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|topic| DescribeLogDirsTopic {
                    name: topic.topic,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition_index| DescribeLogDirsPartition {
                            partition_index,
                            is_future_key: future,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect()
        };
        let log_dir = |path: &str, future| DescribeLogDirsResult {
            log_dir: path.into(),
            topics: topics(future),
            total_bytes: -1,
            usable_bytes: -1,
            ..Default::default()
        };
        let mut results = vec![log_dir("/data/a", false)];
        if moving {
            results.push(log_dir("/data/b", true));
        }
        DescribeLogDirsResponse {
            results,
            ..Default::default()
        }
    })
}

/// `AlterReplicaLogDirs`, answering each partition it names.
fn alter_log_dirs() -> Canned {
    replier::<AlterReplicaLogDirsRequest>(|request| AlterReplicaLogDirsResponse {
        results: request
            .dirs
            .iter()
            .flat_map(|dir| &dir.topics)
            .map(|topic| AlterReplicaLogDirTopicResult {
                topic_name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition_index| AlterReplicaLogDirPartitionResult {
                        partition_index: *partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// A cluster whose only live broker is broker 1, the scripted broker, with
/// topic `foo`. Broker 2 holds replicas but is not live, so only broker 1's
/// replica log dirs are read, as Kafka reads only live nodes'.
async fn reassignment_broker(active: bool, moving: bool, alter_error: i16) -> Broker {
    Broker::start(
        foo_topic(),
        vec![
            describe_cluster(vec![cluster_broker(1, "127.0.0.1", None, false)]),
            list_reassignments(active),
            alter_reassignments(alter_error),
            alter_configs(),
            describe_log_dirs(moving),
            alter_log_dirs(),
        ],
    )
    .await
}

async fn reassign(broker: &Broker, argv: &[&str]) -> Run {
    let address = broker.address();
    krabka(
        &[
            &["reassign-partitions", "--bootstrap-server", &address][..],
            argv,
        ]
        .concat(),
    )
    .await
}

fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
    AlterableConfig {
        name: name.into(),
        config_operation: operation,
        value: value.map(Into::into),
        ..Default::default()
    }
}

fn resource(resource_type: i8, name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type,
        resource_name: name.into(),
        configs,
        ..Default::default()
    }
}

fn alter_request(resources: Vec<AlterConfigsResource>) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_prints_active_reassignments_with_adding_and_removing_replicas() {
    for (active, expected) in [
        (
            true,
            "Current partition reassignments:\nfoo-1: replicas: 1,2,3. adding: 3. removing: 1.\n",
        ),
        (false, "No partition reassignments found.\n"),
    ] {
        let broker = reassignment_broker(active, false, 0).await;
        let out = reassign(&broker, &["--list"]).await;
        check!((out.code, out.stdout.as_str()) == (Some(0), expected));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_reads_the_cluster_and_the_replica_log_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let topics = write(
        dir.path(),
        "topics.json",
        r#"{"version":1,"topics":[{"topic":"bar"}]}"#,
    );
    // `bar` has one replica per partition, on broker 2, so the proposal onto
    // broker 1 alone is deterministic.
    let bar = vec![MetadataResponseTopic {
        name: Some("bar".into()),
        partitions: (0..2)
            .map(|partition_index| MetadataResponsePartition {
                partition_index,
                replica_nodes: vec![2],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }];
    let broker = Broker::start(
        bar,
        vec![
            describe_cluster(vec![
                cluster_broker(1, "127.0.0.1", Some("r1"), false),
                cluster_broker(2, "127.0.0.1", Some("r2"), false),
            ]),
            describe_log_dirs(false),
        ],
    )
    .await;
    let out = reassign(
        &broker,
        &[
            "--generate",
            "--topics-to-move-json-file",
            &topics,
            "--broker-list",
            "1",
        ],
    )
    .await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr.as_str())
            == (
                Some(0),
                "Current partition replica assignment\n\
                 {\"version\":1,\"partitions\":[{\"topic\":\"bar\",\"partition\":0,\"replicas\":[2],\"log_dirs\":[\"/data/a\"]},\
                 {\"topic\":\"bar\",\"partition\":1,\"replicas\":[2],\"log_dirs\":[\"/data/a\"]}]}\n\n\
                 Proposed partition reassignment configuration\n\
                 {\"version\":1,\"partitions\":[{\"topic\":\"bar\",\"partition\":0,\"replicas\":[1],\"log_dirs\":[\"any\"]},\
                 {\"topic\":\"bar\",\"partition\":1,\"replicas\":[1],\"log_dirs\":[\"any\"]}]}\n",
                ""
            )
    );
    check!(
        broker.decoded::<DescribeLogDirsRequest>()
            == [DescribeLogDirsRequest {
                topics: Some(vec![DescribableLogDirTopic {
                    topic: "bar".into(),
                    partitions: vec![0, 1],
                    ..Default::default()
                }]),
                ..Default::default()
            }]
    );
    check!(
        broker.decoded::<DescribeClusterRequest>()
            == [DescribeClusterRequest {
                endpoint_type: 1,
                ..Default::default()
            }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_refuses_brokers_where_only_some_have_racks() {
    let dir = tempfile::tempdir().unwrap();
    let topics = write(
        dir.path(),
        "topics.json",
        r#"{"version":1,"topics":[{"topic":"foo"}]}"#,
    );
    let broker = Broker::start(
        foo_topic(),
        vec![
            describe_cluster(vec![
                cluster_broker(1, "127.0.0.1", Some("r1"), false),
                cluster_broker(2, "127.0.0.1", None, false),
            ]),
            describe_log_dirs(false),
        ],
    )
    .await;
    let out = reassign(
        &broker,
        &[
            "--generate",
            "--topics-to-move-json-file",
            &topics,
            "--broker-list",
            "1,2",
        ],
    )
    .await;
    check!(
        (out.code, out.stderr.as_str())
            == (
                Some(1),
                "krabka reassign-partitions: Not all brokers have rack information. Add \
                 --disable-rack-aware in command line to make replica assignment without rack \
                 information.\n"
            )
    );
}

const PLAN: &str = r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[2,1],"log_dirs":["any","any"]}]}"#;
const ROLLBACK: &str = "Current partition replica assignment\n\n\
    {\"version\":1,\"partitions\":[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2],\"log_dirs\":[\"/data/a\",\"/data/a\"]}]}\n\n\
    Save this to use as the --reassignment-json-file option during rollback\n";
const WARNING: &str = "Warning: You must run --verify periodically, until the reassignment \
                       completes, to ensure the throttle is removed.\n";

/// A cluster whose live brokers are 1 and 2, for plans that name both.
async fn two_broker_cluster(alter_error: i16) -> Broker {
    Broker::start(
        foo_topic(),
        vec![
            describe_cluster(vec![
                cluster_broker(1, "127.0.0.1", None, false),
                cluster_broker(2, "127.0.0.1", None, false),
            ]),
            list_reassignments(false),
            alter_reassignments(alter_error),
            alter_configs(),
            describe_log_dirs(false),
            alter_log_dirs(),
        ],
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn execute_writes_throttles_then_the_reassignment() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(dir.path(), "plan.json", PLAN);
    let broker = two_broker_cluster(0).await;
    let base = [
        "--execute",
        "--reassignment-json-file",
        &plan,
        "--throttle",
        "1000",
    ];
    let refused = reassign(&broker, &base).await;
    check!(refused.code == Some(2));
    let rollback = ROLLBACK;
    let dry = reassign(&broker, &[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout)
            == (
                Some(0),
                format!(
                    "DRY RUN: no change was made.\n{rollback}{WARNING}The inter-broker throttle \
                     limit was set to 1000 B/s\nSuccessfully started partition reassignment for \
                     foo-0\n"
                )
            )
    );
    check!(broker.count(AlterPartitionReassignmentsRequest::API_KEY) == 0);
    check!(broker.count(IncrementalAlterConfigsRequest::API_KEY) == 0);

    let done = reassign(&broker, &[&base[..], &["--yes"]].concat()).await;
    check!(
        (done.code, done.stdout, done.stderr)
            == (
                Some(0),
                format!(
                    "{rollback}{WARNING}The inter-broker throttle limit was set to 1000 B/s\n\
                     Successfully started partition reassignment for foo-0\n"
                ),
                String::new()
            )
    );
    let rate = |name| config(name, SET, Some("1000"));
    let broker_rates = |id| {
        resource(
            BROKER_RESOURCE,
            id,
            vec![
                rate("leader.replication.throttled.rate"),
                rate("follower.replication.throttled.rate"),
            ],
        )
    };
    check!(
        broker.config_alterations()
            == [
                alter_request(vec![resource(
                    TOPIC_RESOURCE,
                    "foo",
                    vec![
                        config(
                            "leader.replication.throttled.replicas",
                            SET,
                            Some("0:1,0:2")
                        ),
                        config("follower.replication.throttled.replicas", SET, Some("")),
                    ]
                )]),
                alter_request(vec![broker_rates("1")]),
                alter_request(vec![broker_rates("2")]),
            ]
    );
    check!(
        broker.decoded::<AlterPartitionReassignmentsRequest>()
            == [AlterPartitionReassignmentsRequest {
                timeout_ms: 10_000,
                allow_replication_factor_change: true,
                topics: vec![ReassignableTopic {
                    name: "foo".into(),
                    partitions: vec![ReassignablePartition {
                        partition_index: 0,
                        replicas: Some(vec![2, 1]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn execute_refuses_unknown_brokers_and_existing_reassignments() {
    let dir = tempfile::tempdir().unwrap();
    let unknown = write(
        dir.path(),
        "unknown.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,9]}]}"#,
    );
    let plan = write(dir.path(), "plan.json", PLAN);
    let cases = [
        (
            false,
            unknown,
            "krabka reassign-partitions: Unknown broker id 9\n",
        ),
        (
            true,
            plan,
            "krabka reassign-partitions: Cannot execute because there is an existing partition \
             assignment.  Use --additional to override this and create a new partition \
             assignment in addition to the existing one. The --additional flag can also be used \
             to change the throttle by resubmitting the current reassignment.\n",
        ),
    ];
    for (active, file, message) in cases {
        let broker = reassignment_broker(active, false, 0).await;
        let out = reassign(
            &broker,
            &["--execute", "--reassignment-json-file", &file, "--yes"],
        )
        .await;
        check!((out.code, out.stderr.as_str()) == (Some(1), message));
        check!(broker.count(AlterPartitionReassignmentsRequest::API_KEY) == 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn execute_sends_allow_replication_factor_change_and_reports_partition_errors() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(dir.path(), "plan.json", PLAN);
    let broker = two_broker_cluster(38).await;
    let out = reassign(
        &broker,
        &[
            "--execute",
            "--reassignment-json-file",
            &plan,
            "--disallow-replication-factor-change",
            "--yes",
        ],
    )
    .await;
    check!(
        (out.code, out.stdout)
            == (
                Some(1),
                format!(
                    "{ROLLBACK}Error reassigning partition(s):\nfoo-0: Replication factor is \
                     below 1 or larger than the number of available brokers.\n"
                )
            )
    );
    check!(
        broker
            .decoded::<AlterPartitionReassignmentsRequest>()
            .iter()
            .map(|request| request.allow_replication_factor_change)
            .collect::<Vec<_>>()
            == [false]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn execute_moves_replicas_between_log_dirs_with_the_log_dir_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2],"log_dirs":["/data/b","any"]}]}"#,
    );
    let broker = two_broker_cluster(0).await;
    let out = reassign(
        &broker,
        &[
            "--execute",
            "--reassignment-json-file",
            &plan,
            "--replica-alter-log-dirs-throttle",
            "500",
            "--yes",
        ],
    )
    .await;
    check!(
        (out.code, out.stdout, out.stderr)
            == (
                Some(0),
                format!(
                    "{ROLLBACK}{WARNING}The replica-alter-dir throttle limit was set to 500 B/s\n\
                     Successfully started partition reassignment for foo-0\n\
                     Successfully started moving log directory to /data/b for replica foo-0 with \
                     broker 1 \n"
                ),
                String::new()
            )
    );
    check!(
        broker.config_alterations()
            == [alter_request(vec![resource(
                BROKER_RESOURCE,
                "1",
                vec![config(
                    "replica.alter.log.dirs.io.max.bytes.per.second",
                    SET,
                    Some("500")
                )]
            )])]
    );
    check!(
        broker.decoded::<AlterReplicaLogDirsRequest>()
            == [AlterReplicaLogDirsRequest {
                dirs: vec![AlterReplicaLogDir {
                    path: "/data/b".into(),
                    topics: vec![AlterReplicaLogDirTopic {
                        name: "foo".into(),
                        partitions: vec![0],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
}

/// The requests of Kafka's `clearAllThrottles`, in the order of
/// [`Broker::config_alterations`]: the topics, then each broker.
fn clear_requests(brokers: &[&str], topics: &[&str]) -> Vec<IncrementalAlterConfigsRequest> {
    let delete = |names: &[&str]| {
        names
            .iter()
            .map(|name| config(name, DELETE, None))
            .collect::<Vec<_>>()
    };
    let mut requests = vec![alter_request(
        topics
            .iter()
            .map(|topic| {
                resource(
                    TOPIC_RESOURCE,
                    topic,
                    delete(&[
                        "leader.replication.throttled.replicas",
                        "follower.replication.throttled.replicas",
                    ]),
                )
            })
            .collect(),
    )];
    // A `BROKER` resource goes to its own broker, in a request of its own.
    requests.extend(brokers.iter().map(|id| {
        alter_request(vec![resource(
            BROKER_RESOURCE,
            id,
            delete(&[
                "leader.replication.throttled.rate",
                "follower.replication.throttled.rate",
                "replica.alter.log.dirs.io.max.bytes.per.second",
            ]),
        )])
    }));
    requests
}

/// Whether a reassignment is active, the plan, the extra flags, and the
/// expected exit code, stdout and config requests.
type VerifyCase<'a> = (
    bool,
    &'a str,
    &'a [&'a str],
    i32,
    String,
    Vec<IncrementalAlterConfigsRequest>,
);

#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_progress_and_clears_throttles_once_done() {
    let dir = tempfile::tempdir().unwrap();
    let moving = write(
        dir.path(),
        "moving.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]},{"topic":"foo","partition":1,"replicas":[2,3]}]}"#,
    );
    let done = write(
        dir.path(),
        "done.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"#,
    );
    let status_done = "Status of partition reassignment:\nReassignment of partition foo-0 is \
                       completed.\n\n";
    let cleared = clear_requests(&["1", "2"], &["foo"]);
    let cases: [VerifyCase<'_>; 3] = [
        (
            true,
            &moving,
            &[],
            1,
            "Status of partition reassignment:\nReassignment of partition foo-0 is \
             completed.\nReassignment of partition foo-1 is still in progress.\n\n"
                .to_owned(),
            Vec::new(),
        ),
        (
            false,
            &done,
            &["--preserve-throttles"],
            0,
            status_done.to_owned(),
            Vec::new(),
        ),
        (
            false,
            &done,
            &[],
            0,
            format!(
                "{status_done}Clearing broker-level throttles on brokers 1,2\nClearing \
                 topic-level throttles on topic foo\n"
            ),
            cleared,
        ),
    ];
    for (active, file, extra, code, expected, alters) in cases {
        let broker = reassignment_broker(active, false, 0).await;
        let argv = [&["--verify", "--reassignment-json-file", file][..], extra].concat();
        let out = reassign(&broker, &argv).await;
        check!((out.code, out.stdout) == (Some(code), expected), "{argv:?}");
        check!(broker.config_alterations() == alters, "{argv:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_log_dir_moves() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2],"log_dirs":["/data/b","any"]}]}"#,
    );
    let cases = [
        (
            true,
            1,
            "Reassignment of replica foo-0-1 is still in progress.\n".to_owned(),
        ),
        (
            false,
            0,
            "Partition foo-0 on broker 1 is not being moved from log dir /data/a to /data/b.\n\
             Clearing broker-level throttles on brokers 1,2\nClearing topic-level throttles on \
             topic foo\n"
                .to_owned(),
        ),
    ];
    for (moving, code, report) in cases {
        let broker = reassignment_broker(false, moving, 0).await;
        let out = reassign(&broker, &["--verify", "--reassignment-json-file", &plan]).await;
        check!(
            (out.code, out.stdout)
                == (
                    Some(code),
                    format!(
                        "Status of partition reassignment:\nReassignment of partition foo-0 is \
                         completed.\n{report}"
                    )
                )
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_confirms_dry_runs_and_cancels_only_active_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]},{"topic":"foo","partition":1,"replicas":[2,1]}]}"#,
    );
    let broker = reassignment_broker(true, false, 0).await;
    let base = ["--cancel", "--reassignment-json-file", &plan];
    // The plan names brokers 1 and 2, which the metadata has; Kafka clears
    // the throttles of every broker that the plan names.
    let refused = reassign(&broker, &base).await;
    check!(refused.code == Some(2));
    let cleared = "Clearing broker-level throttles on brokers 1,2\nClearing topic-level \
                   throttles on topic foo\n";
    let dry = reassign(&broker, &[&base[..], &["--dry-run"]].concat()).await;
    check!(
        (dry.code, dry.stdout)
            == (
                Some(0),
                format!(
                    "DRY RUN: no change was made.\nSuccessfully cancelled partition reassignment \
                     for: foo-1\nNone of the specified partition moves are active.{cleared}"
                )
            )
    );
    check!(broker.count(AlterPartitionReassignmentsRequest::API_KEY) == 0);
    check!(broker.count(IncrementalAlterConfigsRequest::API_KEY) == 0);
    let done = reassign(&broker, &[&base[..], &["--yes"]].concat()).await;
    check!(
        (done.code, done.stdout)
            == (
                Some(0),
                format!(
                    "Successfully cancelled partition reassignment for: foo-1\nNone of the \
                     specified partition moves are active.{cleared}"
                )
            )
    );
    check!(
        broker.decoded::<AlterPartitionReassignmentsRequest>()
            == [AlterPartitionReassignmentsRequest {
                timeout_ms: 10_000,
                allow_replication_factor_change: true,
                topics: vec![ReassignableTopic {
                    name: "foo".into(),
                    partitions: vec![ReassignablePartition {
                        partition_index: 1,
                        replicas: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
    check!(broker.config_alterations() == clear_requests(&["1", "2"], &["foo"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_moves_an_active_log_dir_move_back() {
    let dir = tempfile::tempdir().unwrap();
    let plan = write(
        dir.path(),
        "plan.json",
        r#"{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2],"log_dirs":["/data/b","any"]}]}"#,
    );
    let broker = reassignment_broker(false, true, 0).await;
    let out = reassign(
        &broker,
        &[
            "--cancel",
            "--reassignment-json-file",
            &plan,
            "--preserve-throttles",
            "--yes",
        ],
    )
    .await;
    check!(
        (out.code, out.stdout.as_str())
            == (
                Some(0),
                "None of the specified partition reassignments are active.\nSuccessfully started \
                 moving log directory to /data/a for replica foo-0 with broker 1 \n"
            )
    );
    check!(
        broker
            .decoded::<AlterReplicaLogDirsRequest>()
            .iter()
            .map(|request| request.dirs[0].path.clone())
            .collect::<Vec<_>>()
            == ["/data/a"]
    );
    check!(broker.count(IncrementalAlterConfigsRequest::API_KEY) == 0);
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
        let out = krabka(&[&["reassign-partitions"][..], argv].concat()).await;
        check!(
            (out.code, out.stderr) == (Some(1), format!("krabka reassign-partitions: {message}\n")),
            "{argv:?}"
        );
    }
}
