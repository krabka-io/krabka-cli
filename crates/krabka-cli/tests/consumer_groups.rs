//! `krabka consumer-groups` against a scripted broker, through the built
//! binary: `--list`, `--describe` with a failing group, and
//! `--reset-offsets --dry-run`.
//!
//! One broker is the bootstrap, the only cluster member and the coordinator of
//! every group. `OffsetFetch` answers per group: `orders-app` has committed
//! offsets, `denied` fails with `GROUP_AUTHORIZATION_FAILED`.

use std::{
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        find_coordinator_request::{self, FindCoordinatorRequest},
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        list_groups_request,
        list_groups_response::{ListGroupsResponse, ListedGroup},
        metadata_request,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_fetch_request::{self, OffsetFetchRequest},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
    },
};

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

const METADATA_VERSION: i16 = 12;
const FIND_COORDINATOR_VERSION: i16 = 4;
const OFFSET_FETCH_VERSION: i16 = 8;
const LIST_GROUPS_VERSION: i16 = 4;

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

fn api_versions() -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: [
            (api_versions_request::API_KEY, 0, 3),
            (metadata_request::API_KEY, 0, METADATA_VERSION),
            (
                find_coordinator_request::API_KEY,
                0,
                FIND_COORDINATOR_VERSION,
            ),
            (offset_fetch_request::API_KEY, 1, OFFSET_FETCH_VERSION),
            (list_groups_request::API_KEY, 0, LIST_GROUPS_VERSION),
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
    };
    let mut out = Vec::new();
    response.encode(&mut out, 0).unwrap();
    out
}

fn handle(port: u16, api_key: i16, version: i16, frame: &[u8]) -> Option<Vec<u8>> {
    Some(match (api_key, version) {
        (api_versions_request::API_KEY, _) => api_versions(),
        (metadata_request::API_KEY, METADATA_VERSION) => respond(
            &MetadataResponse {
                brokers: vec![MetadataResponseBroker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(port),
                    ..Default::default()
                }],
                topics: vec![MetadataResponseTopic {
                    name: Some("orders".into()),
                    partitions: (0..2)
                        .map(|partition_index| MetadataResponsePartition {
                            partition_index,
                            replica_nodes: vec![1],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            METADATA_VERSION,
            metadata_request::FLEXIBLE_MIN,
        ),
        (find_coordinator_request::API_KEY, FIND_COORDINATOR_VERSION) => {
            let request: FindCoordinatorRequest = decode(frame, FIND_COORDINATOR_VERSION, true);
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
                FIND_COORDINATOR_VERSION,
                find_coordinator_request::FLEXIBLE_MIN,
            )
        }
        (offset_fetch_request::API_KEY, OFFSET_FETCH_VERSION) => {
            let request: OffsetFetchRequest = decode(frame, OFFSET_FETCH_VERSION, true);
            let groups = request
                .groups
                .into_iter()
                .map(|group| match group.group_id.as_str() {
                    "denied" => OffsetFetchResponseGroup {
                        group_id: group.group_id,
                        error_code: 30,
                        ..Default::default()
                    },
                    _ => OffsetFetchResponseGroup {
                        group_id: group.group_id,
                        topics: vec![OffsetFetchResponseTopics {
                            name: "orders".into(),
                            partitions: [(0, 10), (1, -1)]
                                .into_iter()
                                .map(|(partition_index, committed_offset)| {
                                    OffsetFetchResponsePartitions {
                                        partition_index,
                                        committed_offset,
                                        committed_leader_epoch: -1,
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                })
                .collect();
            respond(
                &OffsetFetchResponse {
                    groups,
                    ..Default::default()
                },
                OFFSET_FETCH_VERSION,
                offset_fetch_request::FLEXIBLE_MIN,
            )
        }
        (list_groups_request::API_KEY, LIST_GROUPS_VERSION) => respond(
            &ListGroupsResponse {
                groups: ["orders-app", "denied"]
                    .into_iter()
                    .map(|group_id| ListedGroup {
                        group_id: group_id.into(),
                        protocol_type: "consumer".into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            LIST_GROUPS_VERSION,
            list_groups_request::FLEXIBLE_MIN,
        ),
        _ => return None,
    })
}

/// The request body after the header's client id, and its tagged-fields
/// byte for a flexible version.
fn decode<T: for<'de> Decode<'de>>(frame: &[u8], version: i16, flexible: bool) -> T {
    let client_id = usize::try_from(i16::from_be_bytes([frame[0], frame[1]]).max(0)).unwrap();
    let mut body = &frame[2 + client_id + usize::from(flexible)..];
    T::decode(&mut body, version).unwrap()
}

async fn broker() -> krabka_client_core::MockBroker {
    let port = Arc::new(Mutex::new(0_u16));
    let seen = Arc::clone(&port);
    let broker = krabka_client_core::MockBroker::start(move |api_key, version, _, frame| {
        handle(*seen.lock().unwrap(), api_key, version, frame)
    })
    .await;
    *port.lock().unwrap() = broker.addr.port();
    broker
}

fn args(broker: &krabka_client_core::MockBroker, argv: &[&str]) -> Vec<String> {
    [
        "consumer-groups",
        "--bootstrap-server",
        &broker.addr.to_string(),
    ]
    .iter()
    .chain(argv)
    .map(|arg| (*arg).to_owned())
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn list_names_every_group_the_brokers_report() {
    let broker = broker().await;
    let out = krabka(args(&broker, &["--list"])).await;
    check!(
        (out.code, out.stdout.as_str(), out.stderr.as_str())
            == (Some(0), "orders-app\ndenied\n", "")
    );
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_all_groups_prints_the_others_when_one_fails() {
    let broker = broker().await;
    let out = krabka(args(
        &broker,
        &["--describe", "--all-groups", "--request-timeout-ms", "2000"],
    ))
    .await;
    check!(out.code == Some(1));
    check!(
        out.stdout
            == concat!(
                "\n",
                "GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID\n",
                "orders-app      orders          0          10              -               -               -               -               -\n",
            )
    );
    let stderr = out.stderr.lines().collect::<Vec<_>>();
    check!(
        stderr[0]
            == "Error: Executing consumer group command failed for group 'denied' due to OffsetFetch failed: GROUP_AUTHORIZATION_FAILED (30): group=denied"
    );
    check!(stderr[1].contains("AdminClient::describe_consumer_groups"));
    broker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_offsets_dry_run_prints_kafkas_table_and_commits_nothing() {
    let broker = broker().await;
    let out = krabka(args(
        &broker,
        &[
            "--reset-offsets",
            "--group",
            "orders-app",
            "--topic",
            "orders",
            "--to-current",
            "--dry-run",
        ],
    ))
    .await;
    // orders-1 has no committed offset, so --to-current needs its log-end
    // offset, which needs ListOffsets.
    check!(out.code == Some(1));
    check!(
        out.stderr
            == "krabka consumer-groups: reading log offsets (ListOffsets timestamp -1) is not supported by this build: it needs AdminClient::list_offsets, which the pinned krabka-client-rs revision does not have\n"
    );
    let dry = krabka(args(
        &broker,
        &[
            "--reset-offsets",
            "--group",
            "orders-app",
            "--topic",
            "orders:0",
            "--shift-by",
            "-3",
            "--dry-run",
        ],
    ))
    .await;
    check!(dry.code == Some(0));
    check!(
        dry.stdout
            == "\nGROUP           TOPIC           PARTITION  NEW-OFFSET\norders-app      orders          0          7\n"
    );
    let notices = dry.stderr.lines().collect::<Vec<_>>();
    check!(notices[0] == "DRY RUN: no change was made.");
    check!(
        notices[1]
            .starts_with("WARN New offsets are not checked against the log start and end offsets")
    );
    let missing = krabka(args(
        &broker,
        &[
            "--reset-offsets",
            "--group",
            "orders-app",
            "--topic",
            "orders:7",
            "--to-offset",
            "1",
            "--execute",
        ],
    ))
    .await;
    check!(
        (missing.code, missing.stderr.as_str())
            == (
                Some(1),
                "krabka consumer-groups: The partitions \"orders-7\" do not exist\n"
            )
    );
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
async fn delete_refuses_as_unsupported_before_touching_the_broker() {
    let broker = broker().await;
    let out = krabka(args(
        &broker,
        &[
            "--delete",
            "--group",
            "orders-app",
            "--yes",
            "--output",
            "json",
        ],
    ))
    .await;
    check!(out.code == Some(1));
    check!(
        serde_json::from_str::<serde_json::Value>(&out.stderr).unwrap()
            == serde_json::json!({"error": {"code": 1, "message": "--delete is not supported by this build: it needs AdminClient::delete_consumer_groups, which the pinned krabka-client-rs revision does not have"}})
    );
    broker.stop();
}
