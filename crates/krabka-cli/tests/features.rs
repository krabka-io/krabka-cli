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
                update_features_request::API_KEY => {
                    Some(update_features(&mut cluster, version, body))
                }
                _ => None,
            }
        })
        .await;
        Self { inner, cluster }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    fn update_requests(&self) -> Vec<UpdateFeaturesRequest> {
        self.cluster.lock().unwrap().updates.clone()
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
        for update in &request.feature_updates {
            cluster
                .finalized
                .insert(update.feature.clone(), update.max_version_level);
        }
        cluster.epoch += 1;
        UpdateFeaturesResponse {
            results: request
                .feature_updates
                .iter()
                .map(|update| UpdatableFeatureResult {
                    feature: update.feature.clone(),
                    ..Default::default()
                })
                .collect(),
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

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_sends_no_update_and_reports_what_would_happen() {
    let cases: &[(&[&str], Option<i32>, &str)] = &[
        (
            &["upgrade", "--feature", "group.version=1", "--dry-run"],
            Some(0),
            "DRY RUN: no change was made.\ngroup.version can be upgraded to 1.\n",
        ),
        (
            &["disable", "--feature", "group.version", "--dry-run"],
            Some(0),
            "DRY RUN: no change was made.\ngroup.version can be disabled.\n",
        ),
        (
            &["upgrade", "--feature", "transaction.version=2", "--dry-run"],
            Some(1),
            "DRY RUN: no change was made.\nCan not upgrade transaction.version to 2. The update failed for all features since the following feature had an error: Invalid update version 2 for feature transaction.version. Broker only supports versions 0-1\n",
        ),
        (
            &["downgrade", "--feature", "metadata.version=22", "--dry-run"],
            Some(1),
            "DRY RUN: no change was made.\nCan not downgrade metadata.version to 22. The update failed for all features since the following feature had an error: Invalid update version 22 for feature metadata.version. Can't downgrade to a newer version.\n",
        ),
    ];
    for (argv, code, stdout) in cases {
        let broker = Broker::start(cluster()).await;
        let run = krabka(features(&broker, argv)).await;
        check!(
            (run.code, run.stdout.as_str()) == (*code, *stdout),
            "{argv:?}"
        );
        check!(broker.update_requests().is_empty(), "{argv:?}");
        check!(
            broker
                .received()
                .iter()
                .all(|(api_key, _)| *api_key == api_versions_request::API_KEY),
            "{argv:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_json_report_carries_the_marker() {
    let broker = Broker::start(cluster()).await;
    let run = krabka(features(
        &broker,
        &[
            "--output",
            "json",
            "upgrade",
            "--release-version",
            "3.9",
            "--dry-run",
        ],
    ))
    .await;
    let report: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(report["dry_run"] == json!(true));
    check!(report["data"]["operation"] == json!("upgrade"));
    check!(broker.update_requests().is_empty());
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
    check!(run.code == Some(1));
    check!(
        run.stdout
            == "Could not upgrade group.version to 1. The update failed for all features since the following feature had an error: nope\n\
                Could not upgrade transaction.version to 1. The update failed for all features since the following feature had an error: nope\n"
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
