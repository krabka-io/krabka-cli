//! `krabka features` against a scripted broker that keeps finalized feature
//! levels, so an update and a later `describe` see the same cluster.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{
            ApiVersion, ApiVersionsResponse, FinalizedFeatureKey, SupportedFeatureKey,
        },
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponseBroker},
        update_features_request::{self, FeatureUpdateKey, UpdateFeaturesRequest},
        update_features_response::{UpdatableFeatureResult, UpdateFeaturesResponse},
    },
};
use serde_json::{Value, json};

const UPDATE_FEATURES_VERSION: i16 = 1;

/// The cluster: supported ranges, finalized levels, and what it received.
#[derive(Default)]
struct Cluster {
    supported: BTreeMap<String, (i16, i16)>,
    finalized: BTreeMap<String, i16>,
    epoch: i64,
    /// Every `UpdateFeatures` request, decoded.
    updates: Vec<UpdateFeaturesRequest>,
    /// Every `(api_key, version)` received.
    received: Vec<(i16, i16)>,
    /// A top-level error to answer `UpdateFeatures` with.
    top_level_error: Option<(i16, String)>,
    /// Per-feature errors to answer `UpdateFeatures` with: the controller
    /// applies no update when any feature fails.
    feature_errors: BTreeMap<String, (i16, String)>,
    /// The node id of this broker, and the port of the broker's own listener,
    /// which `Metadata` names.
    node: Option<(i32, u16)>,
}

struct Broker {
    inner: krabka_client_core::MockBroker,
    cluster: Arc<Mutex<Cluster>>,
}

impl Broker {
    async fn start(cluster: Cluster) -> Self {
        let cluster = Arc::new(Mutex::new(cluster));
        let state = Arc::clone(&cluster);
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            let mut cluster = state.lock().unwrap();
            cluster.received.push((api_key, version));
            match api_key {
                api_versions_request::API_KEY => Some(api_versions(&cluster, version)),
                metadata_request::API_KEY => Some(metadata(&cluster, version)),
                update_features_request::API_KEY => {
                    Some(update_features(&mut cluster, version, body))
                }
                _ => None,
            }
        })
        .await;
        let port = inner.addr.port();
        if let Some((_, own)) = &mut cluster.lock().unwrap().node {
            *own = port;
        }
        Self { inner, cluster }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    /// Every `UpdateFeatures` request, with its `timeout_ms` checked and set
    /// to the 5s deadline of the command: the admin client sends the time
    /// that remains of that deadline, as Kafka's
    /// `Call.calcTimeoutMsRemainingAsInt` does.
    fn update_requests(&self) -> Vec<UpdateFeaturesRequest> {
        let mut requests = self.cluster.lock().unwrap().updates.clone();
        for request in &mut requests {
            assert!((4_000..=5_000).contains(&request.timeout_ms));
            request.timeout_ms = 5_000;
        }
        requests
    }

    fn received(&self) -> Vec<(i16, i16)> {
        self.cluster.lock().unwrap().received.clone()
    }
}

fn api_versions(cluster: &Cluster, version: i16) -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: update_features_request::API_KEY,
                min_version: 0,
                max_version: UPDATE_FEATURES_VERSION,
                ..Default::default()
            },
            ApiVersion {
                api_key: metadata_request::API_KEY,
                min_version: 12,
                max_version: 12,
                ..Default::default()
            },
        ],
        supported_features: cluster
            .supported
            .iter()
            .map(|(name, (min, max))| SupportedFeatureKey {
                name: name.clone(),
                min_version: *min,
                max_version: *max,
                ..Default::default()
            })
            .collect(),
        finalized_features: cluster
            .finalized
            .iter()
            .map(|(name, level)| FinalizedFeatureKey {
                name: name.clone(),
                min_version_level: *level,
                max_version_level: *level,
                ..Default::default()
            })
            .collect(),
        finalized_features_epoch: cluster.epoch,
        ..Default::default()
    };
    let mut body = Vec::new();
    response.encode(&mut body, version).unwrap();
    body
}

// `Metadata` that names this broker, when it has a node id.
fn metadata(cluster: &Cluster, version: i16) -> Vec<u8> {
    let response = MetadataResponse {
        brokers: cluster
            .node
            .iter()
            .map(|(node_id, port)| MetadataResponseBroker {
                node_id: *node_id,
                host: "127.0.0.1".into(),
                port: i32::from(*port),
                ..Default::default()
            })
            .collect(),
        controller_id: cluster.node.map_or(-1, |(node_id, _)| node_id),
        ..Default::default()
    };
    // Metadata is flexible from v9: the response header has a tagged fields
    // byte.
    let mut body = vec![0];
    response.encode(&mut body, version).unwrap();
    body
}

// The request body after the header's client id and tagged fields.
fn request_body(body: &[u8]) -> &[u8] {
    let length = i16::from_be_bytes([body[0], body[1]]);
    let client_id = usize::try_from(length.max(0)).unwrap();
    &body[2 + client_id + 1..]
}

fn update_features(cluster: &mut Cluster, version: i16, body: &[u8]) -> Vec<u8> {
    let request = UpdateFeaturesRequest::decode(&mut request_body(body), version).unwrap();
    let response = if let Some((code, message)) = cluster.top_level_error.clone() {
        UpdateFeaturesResponse {
            error_code: code,
            error_message: Some(message),
            ..Default::default()
        }
    } else {
        let results = request
            .feature_updates
            .iter()
            .map(|update| {
                let (error_code, error_message) = cluster
                    .feature_errors
                    .get(&update.feature)
                    .map_or((0, None), |(code, message)| (*code, Some(message.clone())));
                UpdatableFeatureResult {
                    feature: update.feature.clone(),
                    error_code,
                    error_message,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();
        let failed = results.iter().any(|result| result.error_code != 0);
        if !request.validate_only && !failed {
            for update in &request.feature_updates {
                cluster
                    .finalized
                    .insert(update.feature.clone(), update.max_version_level);
            }
            cluster.epoch += 1;
        }
        UpdateFeaturesResponse {
            results,
            ..Default::default()
        }
    };
    cluster.updates.push(request);
    // UpdateFeatures is flexible from v0: the response header has a tagged
    // fields byte.
    let mut out = vec![0];
    response.encode(&mut out, version).unwrap();
    out
}

fn cluster() -> Cluster {
    Cluster {
        supported: BTreeMap::from([
            ("metadata.version".to_owned(), (7, 25)),
            ("group.version".to_owned(), (0, 1)),
            ("transaction.version".to_owned(), (0, 1)),
        ]),
        finalized: BTreeMap::from([("metadata.version".to_owned(), 21)]),
        epoch: 7,
        ..Cluster::default()
    }
}

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

fn features(broker: &Broker, rest: &[&str]) -> Vec<String> {
    [
        "features",
        "--bootstrap-server",
        &broker.address(),
        "--timeout",
        "5s",
    ]
    .iter()
    .chain(rest)
    .map(|arg| (*arg).to_owned())
    .collect()
}

fn describe_line(feature: &str, min: &str, max: &str, finalized: &str, epoch: i64) -> String {
    format!(
        "Feature: {feature:<40}  SupportedMinVersion: {min:<15}  SupportedMaxVersion: {max:<15}  FinalizedVersionLevel: {finalized:<15}  Epoch: {epoch}"
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_prints_the_kafka_features_table() {
    let broker = Broker::start(cluster()).await;
    let run = krabka(features(&broker, &["describe"])).await;
    let expected = [
        describe_line("group.version", "0", "1", "0", 7),
        describe_line("metadata.version", "3.3-IV3", "4.0-IV3", "3.9-IV0", 7),
        describe_line("transaction.version", "0", "1", "0", 7),
    ]
    .map(|line| line + "\n")
    .concat();
    check!((run.code, run.stdout, run.stderr) == (Some(0), expected, String::new()));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_of_group_version_reads_back_through_describe() {
    let broker = Broker::start(cluster()).await;
    let upgrade = krabka(features(
        &broker,
        &["upgrade", "--feature", "group.version=1"],
    ))
    .await;
    check!(
        (upgrade.code, upgrade.stdout.as_str()) == (Some(0), "group.version was upgraded to 1.\n")
    );
    assert!(
        broker.update_requests()
            == vec![UpdateFeaturesRequest {
                timeout_ms: 5_000,
                feature_updates: vec![FeatureUpdateKey {
                    feature: "group.version".into(),
                    max_version_level: 1,
                    allow_downgrade: false,
                    upgrade_type: 1,
                    ..Default::default()
                }],
                validate_only: false,
                ..Default::default()
            }]
    );

    let describe = krabka(features(&broker, &["--output", "json", "describe"])).await;
    let report: Value = serde_json::from_str(&describe.stdout).unwrap();
    check!(
        report
            == json!({"data": {
                "features": [
                    {"feature": "group.version", "supported_min_version": 0, "supported_max_version": 1, "finalized_version_level": 1},
                    {"feature": "metadata.version", "supported_min_version": 7, "supported_max_version": 25, "finalized_version_level": 21},
                    {"feature": "transaction.version", "supported_min_version": 0, "supported_max_version": 1, "finalized_version_level": 0},
                ],
                "finalized_features_epoch": 8,
            }})
    );
}

fn key(feature: &str, max_version_level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    FeatureUpdateKey {
        feature: feature.into(),
        max_version_level,
        upgrade_type,
        ..Default::default()
    }
}

fn request(validate_only: bool, feature_updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        timeout_ms: 5_000,
        feature_updates,
        validate_only,
        ..Default::default()
    }
}

/// One case of an update: its name, the subcommand's argv, the per-feature
/// errors of the controller, the request it must receive, the exit code,
/// stdout and stderr, and the finalized levels afterwards.
type UpdateCase = (
    &'static str,
    Vec<&'static str>,
    BTreeMap<String, (i16, String)>,
    UpdateFeaturesRequest,
    (Option<i32>, &'static str, &'static str),
    &'static [(&'static str, i16)],
);

/// `upgrade`, `downgrade` and `disable`, with and without `--dry-run` and
/// `--unsafe`, send the `UpdateFeatures` request of `FeatureCommand.update`
/// and print its report from the controller's answer for each feature. A
/// dry run sets `validateOnly`, prints "can be" or "Can not" lines on stdout
/// and leaves the finalized levels alone.
#[tokio::test(flavor = "multi_thread")]
async fn updates_send_kafka_features_request_and_print_its_report() {
    let invalid = |message: &str| (95, message.to_owned());
    let cases: Vec<UpdateCase> = vec![
        (
            "dry-run upgrade",
            vec!["upgrade", "--feature", "group.version=1", "--dry-run"],
            BTreeMap::new(),
            request(true, vec![key("group.version", 1, 1)]),
            (
                Some(0),
                "group.version can be upgraded to 1.\n",
                "DRY RUN: no change was made.\n",
            ),
            &[("metadata.version", 21)],
        ),
        (
            "dry-run disable",
            vec!["disable", "--feature", "group.version", "--dry-run"],
            BTreeMap::new(),
            request(true, vec![key("group.version", 0, 2)]),
            (
                Some(0),
                "group.version can be disabled.\n",
                "DRY RUN: no change was made.\n",
            ),
            &[("metadata.version", 21)],
        ),
        (
            "dry-run whose validate-only answer fails one feature",
            vec![
                "upgrade",
                "--feature",
                "group.version=1",
                "--feature",
                "transaction.version=1",
                "--dry-run",
            ],
            BTreeMap::from([(
                "transaction.version".to_owned(),
                invalid(
                    "Invalid update version 1 for feature transaction.version. Broker only supports versions 0-0",
                ),
            )]),
            request(
                true,
                vec![key("group.version", 1, 1), key("transaction.version", 1, 1)],
            ),
            (
                Some(1),
                "group.version can be upgraded to 1.\n\
                 Can not upgrade transaction.version to 1. Invalid update version 1 for feature transaction.version. Broker only supports versions 0-0\n",
                "DRY RUN: no change was made.\n1 out of 2 operation(s) failed.\n",
            ),
            &[("metadata.version", 21)],
        ),
        (
            "unsafe downgrade",
            vec!["downgrade", "--unsafe", "--feature", "metadata.version=20"],
            BTreeMap::new(),
            request(false, vec![key("metadata.version", 20, 3)]),
            (Some(0), "metadata.version was downgraded to 20.\n", ""),
            &[("metadata.version", 20)],
        ),
        (
            "safe downgrade",
            vec!["downgrade", "--feature", "metadata.version=20"],
            BTreeMap::new(),
            request(false, vec![key("metadata.version", 20, 2)]),
            (Some(0), "metadata.version was downgraded to 20.\n", ""),
            &[("metadata.version", 20)],
        ),
        (
            "unsafe disable",
            vec!["disable", "--unsafe", "--feature", "group.version"],
            BTreeMap::new(),
            request(false, vec![key("group.version", 0, 3)]),
            (Some(0), "group.version was disabled.\n", ""),
            &[("group.version", 0), ("metadata.version", 21)],
        ),
        (
            "an update the controller refuses",
            vec!["downgrade", "--feature", "metadata.version=20"],
            BTreeMap::from([(
                "metadata.version".to_owned(),
                invalid(
                    "Invalid metadata.version 20. Refusing to perform the requested downgrade because it might delete metadata information.",
                ),
            )]),
            request(false, vec![key("metadata.version", 20, 2)]),
            (
                Some(1),
                "Could not downgrade metadata.version to 20. Invalid metadata.version 20. Refusing to perform the requested downgrade because it might delete metadata information.\n",
                "1 out of 1 operation(s) failed.\n",
            ),
            &[("metadata.version", 21)],
        ),
    ];
    for (case, argv, feature_errors, expected_request, expected_output, finalized) in cases {
        let broker = Broker::start(Cluster {
            feature_errors,
            ..cluster()
        })
        .await;
        let run = krabka(features(&broker, &argv)).await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == expected_output,
            "{case}"
        );
        check!(broker.update_requests() == vec![expected_request], "{case}");
        let finalized = finalized
            .iter()
            .map(|(name, level)| ((*name).to_owned(), *level))
            .collect::<BTreeMap<_, _>>();
        check!(
            broker.cluster.lock().unwrap().finalized == finalized,
            "{case}"
        );
    }
}

/// `upgrade --release-version 4.3 --dry-run` validates every feature that
/// Kafka 4.3.1 maps 4.3 to, `kraft.version` included, and prints Kafka's
/// report on stdout with krabka's dry-run marker on stderr.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_version_dry_run_validates_every_feature_of_the_release() {
    let broker = Broker::start(cluster()).await;
    let run = krabka(features(
        &broker,
        &["upgrade", "--release-version", "4.3", "--dry-run"],
    ))
    .await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(0),
                "eligible.leader.replicas.version can be upgraded to 1.\n\
                 group.version can be upgraded to 1.\n\
                 kraft.version can be upgraded to 1.\n\
                 metadata.version can be upgraded to 30.\n\
                 share.version can be upgraded to 1.\n\
                 streams.version can be upgraded to 1.\n\
                 transaction.version can be upgraded to 2.\n",
                "DRY RUN: no change was made.\n",
            )
    );
    check!(
        broker.update_requests()
            == vec![request(
                true,
                vec![
                    key("eligible.leader.replicas.version", 1, 1),
                    key("group.version", 1, 1),
                    key("kraft.version", 1, 1),
                    key("metadata.version", 30, 1),
                    key("share.version", 1, 1),
                    key("streams.version", 1, 1),
                    key("transaction.version", 2, 1),
                ],
            )]
    );
}

/// A dry run applies nothing: a later `describe` reads the levels from
/// before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_leaves_the_finalized_levels_alone() {
    let broker = Broker::start(cluster()).await;
    let run = krabka(features(
        &broker,
        &["upgrade", "--feature", "group.version=1", "--dry-run"],
    ))
    .await;
    check!(run.code == Some(0));
    check!(
        broker.cluster.lock().unwrap().finalized
            == BTreeMap::from([("metadata.version".to_owned(), 21)])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_json_report_carries_the_marker_and_each_result() {
    let broker = Broker::start(Cluster {
        feature_errors: BTreeMap::from([("transaction.version".to_owned(), (95, "no".to_owned()))]),
        ..cluster()
    })
    .await;
    let run = krabka(features(
        &broker,
        &[
            "--output",
            "json",
            "upgrade",
            "--feature",
            "group.version=1",
            "--feature",
            "transaction.version=1",
            "--dry-run",
        ],
    ))
    .await;
    let report: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(run.code == Some(1));
    check!(
        report
            == json!({
                "data": {
                    "operation": "upgrade",
                    "updates": [
                        {"feature": "group.version", "level": 1, "upgrade_type": "UPGRADE", "error": null},
                        {"feature": "transaction.version", "level": 1, "upgrade_type": "UPGRADE", "error": {"code": 95, "name": "INVALID_UPDATE_VERSION", "message": "no"}},
                    ],
                    "failures": 1,
                },
                "dry_run": true,
            })
    );
}

/// `describe --node-id` asks the node that the metadata names, as
/// `DescribeFeaturesOptions.nodeId` does.
#[tokio::test(flavor = "multi_thread")]
async fn describe_node_id_asks_that_node() {
    let broker = Broker::start(Cluster {
        node: Some((3, 0)),
        ..cluster()
    })
    .await;
    let run = krabka(features(&broker, &["describe", "--node-id", "3"])).await;
    let expected = [
        describe_line("group.version", "0", "1", "0", 7),
        describe_line("metadata.version", "3.3-IV3", "4.0-IV3", "3.9-IV0", 7),
        describe_line("transaction.version", "0", "1", "0", 7),
    ]
    .map(|line| line + "\n")
    .concat();
    check!((run.code, run.stdout, run.stderr) == (Some(0), expected, String::new()));
    check!(
        broker
            .received()
            .iter()
            .any(|(api_key, _)| *api_key == metadata_request::API_KEY)
    );
}

/// `describe --node-id` of a node that the metadata does not name times
/// out, as Kafka's call to a missing node does.
#[tokio::test(flavor = "multi_thread")]
async fn describe_node_id_of_an_unknown_node_times_out() {
    let broker = Broker::start(Cluster {
        node: Some((3, 0)),
        ..cluster()
    })
    .await;
    let mut argv = features(&broker, &["describe", "--node-id", "9"]);
    argv[4] = "1s".into();
    let run = krabka(argv).await;
    check!((run.code, run.stdout.as_str()) == (Some(1), ""));
    check!(
        run.stderr
            .starts_with("krabka features: ApiVersions failed: REQUEST_TIMED_OUT (7)")
    );
}

/// A finalized-features epoch that the node reports as negative prints "-".
#[tokio::test(flavor = "multi_thread")]
async fn describe_without_an_epoch_prints_a_dash() {
    let broker = Broker::start(Cluster {
        epoch: -1,
        ..cluster()
    })
    .await;
    let run = krabka(features(&broker, &["describe"])).await;
    check!(run.code == Some(0));
    check!(
        run.stdout.lines().next()
            == Some(
                "Feature: group.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 0                Epoch: -"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_updates_are_refused_before_any_request() {
    let known = {
        let mut names = krabka_metadata::feature_registry()
            .iter()
            .map(|feature| feature.name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.join(", ")
    };
    let cases: Vec<(Vec<&str>, String)> = vec![
        (
            vec!["upgrade", "--feature", "foo.bar=1"],
            format!("Unsupported feature: foo.bar. Supported features are: {known}"),
        ),
        (
            vec!["upgrade", "--feature", "group.version=9"],
            "feature group.version=9 is outside the supported range 0..=1".into(),
        ),
        (
            vec![
                "upgrade",
                "--release-version",
                "4.0",
                "--feature",
                "metadata.version=20",
            ],
            "Can not specify `release-version` with other feature flags.".into(),
        ),
        (
            vec!["upgrade"],
            "You must specify at least one feature to upgrade".into(),
        ),
    ];
    for (argv, message) in cases {
        let broker = Broker::start(cluster()).await;
        let run = krabka(features(&broker, &argv)).await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr)
                == (Some(1), "", format!("krabka features: {message}\n")),
            "{argv:?}"
        );
        check!(broker.received().is_empty(), "{argv:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_top_level_error_fails_every_feature() {
    let broker = Broker::start(Cluster {
        top_level_error: Some((
            89,
            "The update failed for all features since the following feature had an error: nope"
                .into(),
        )),
        ..cluster()
    })
    .await;
    let run = krabka(features(
        &broker,
        &[
            "upgrade",
            "--feature",
            "group.version=1",
            "--feature",
            "transaction.version=1",
        ],
    ))
    .await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(1),
                "Could not upgrade group.version to 1. The update failed for all features since the following feature had an error: nope\n\
                 Could not upgrade transaction.version to 1. The update failed for all features since the following feature had an error: nope\n",
                "2 out of 2 operation(s) failed.\n",
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn version_mapping_and_feature_dependencies_need_no_broker() {
    let run = krabka(vec![
        "features".into(),
        "version-mapping".into(),
        "--release-version".into(),
        "4.0".into(),
    ])
    .await;
    let level = krabka_metadata::metadata_version::from_version_string("4.0")
        .unwrap()
        .feature_level();
    check!(run.code == Some(0));
    check!(run.stdout.lines().next() == Some(format!("metadata.version={level} (4.0)").as_str()));

    let run = krabka(vec![
        "features".into(),
        "feature-dependencies".into(),
        "--feature".into(),
        "group.version=9".into(),
    ])
    .await;
    check!(
        (run.code, run.stderr)
            == (
                Some(1),
                "krabka features: No feature:group.version with feature level 9\n".to_owned()
            )
    );
}
