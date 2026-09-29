use assert2::{assert, check};
use clap::Parser;
use krabka_client_admin::{FeatureRange, KafkaError};

use super::*;

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    features: FeaturesArgs,
}

fn parse(argv: &[&str]) -> Result<FeaturesArgs, clap::Error> {
    Cli::try_parse_from(std::iter::once("kafka-features").chain(argv.iter().copied()))
        .map(|cli| cli.features)
}

fn plan_of(command_line: &[&str]) -> Result<Plan, String> {
    let args = parse(command_line).expect("argv parses");
    match args.command {
        FeaturesCommand::Upgrade(args) => Plan::upgrade_or_downgrade(
            Op::Upgrade,
            args.metadata.as_deref(),
            args.release_version.as_deref(),
            &args.feature,
            UpgradeType::Upgrade,
            args.dry_run,
        ),
        FeaturesCommand::Downgrade(args) => Plan::upgrade_or_downgrade(
            Op::Downgrade,
            args.metadata.as_deref(),
            args.release_version.as_deref(),
            &args.feature,
            downgrade_type(args.unsafe_downgrade),
            args.dry_run,
        ),
        FeaturesCommand::Disable(args) => Plan::disable(
            &args.feature,
            downgrade_type(args.unsafe_downgrade),
            args.dry_run,
        ),
        other => panic!("not an update: {other:?}"),
    }
    .and_then(|plan| plan.validate().map(|()| plan))
}

fn update(level: i16, upgrade_type: UpgradeType) -> FeatureUpdate {
    FeatureUpdate::new(level, upgrade_type).unwrap()
}

fn updates(rows: &[(&str, i16, UpgradeType)]) -> BTreeMap<String, FeatureUpdate> {
    rows.iter()
        .map(|(name, level, upgrade_type)| ((*name).to_owned(), update(*level, *upgrade_type)))
        .collect()
}

const UP: UpgradeType = UpgradeType::Upgrade;
const SAFE: UpgradeType = UpgradeType::SafeDowngrade;
const UNSAFE: UpgradeType = UpgradeType::UnsafeDowngrade;

#[test]
fn flags_map_to_the_updates_kafka_features_sends() {
    let known = {
        let mut names = feature_registry()
            .iter()
            .map(|feature| feature.name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.join(", ")
    };
    let plan = |op, notices: &[&str], updates, dry_run| {
        Ok(Plan {
            op,
            notices: notices.iter().map(|notice| (*notice).to_owned()).collect(),
            updates,
            dry_run,
        })
    };
    let cases: Vec<(&str, Vec<&str>, Result<Plan, String>)> = vec![
        (
            "upgrade --feature repeated",
            vec!["upgrade", "--feature", "group.version=1", "--feature", "transaction.version = 2"],
            plan(Op::Upgrade, &[], updates(&[("group.version", 1, UP), ("transaction.version", 2, UP)]), false),
        ),
        (
            "upgrade --release-version skips level-0 features",
            vec!["upgrade", "--release-version", "4.0", "--dry-run"],
            // `kafka-features version-mapping --release-version 4.0` at
            // Kafka 4.3.1, less its level-0 features.
            plan(
                Op::Upgrade,
                &[],
                updates(&[
                    ("metadata.version", 25, UP),
                    ("kraft.version", 1, UP),
                    ("transaction.version", 2, UP),
                    ("group.version", 1, UP),
                ]),
                true,
            ),
        ),
        (
            "downgrade --release-version keeps level-0 features",
            vec!["downgrade", "--release-version", "3.7"],
            // `kafka-features version-mapping --release-version 3.7` at
            // Kafka 4.3.1.
            plan(
                Op::Downgrade,
                &[],
                updates(&[
                    ("metadata.version", 19, SAFE),
                    ("kraft.version", 0, SAFE),
                    ("transaction.version", 0, SAFE),
                    ("group.version", 0, SAFE),
                    ("eligible.leader.replicas.version", 0, SAFE),
                    ("share.version", 0, SAFE),
                    ("streams.version", 0, SAFE),
                ]),
                false,
            ),
        ),
        (
            "upgrade --metadata prints the deprecation notice",
            vec!["upgrade", "--metadata", "4.0"],
            plan(
                Op::Upgrade,
                &[METADATA_FLAG_NOTICE],
                updates(&[(METADATA_VERSION_FEATURE, resolve_release("4.0").unwrap().feature_level(), UP)]),
                false,
            ),
        ),
        (
            "downgrade without --unsafe is safe",
            vec!["downgrade", "--feature", "group.version=0"],
            plan(Op::Downgrade, &[], updates(&[("group.version", 0, SAFE)]), false),
        ),
        (
            "downgrade --unsafe",
            vec!["downgrade", "--unsafe", "--feature", "group.version=0"],
            plan(Op::Downgrade, &[], updates(&[("group.version", 0, UNSAFE)]), false),
        ),
        (
            "disable --unsafe",
            vec!["disable", "--unsafe", "--feature", "group.version"],
            plan(Op::Disable, &[], updates(&[("group.version", 0, UNSAFE)]), false),
        ),
        (
            "disable is level 0",
            vec!["disable", "--feature", "share.version", "--feature", "streams.version", "--dry-run"],
            plan(Op::Disable, &[], updates(&[("share.version", 0, SAFE), ("streams.version", 0, SAFE)]), true),
        ),
        (
            "kraft.version is an ordinary update, as in Kafka",
            vec!["upgrade", "--feature", "kraft.version=1"],
            plan(Op::Upgrade, &[], updates(&[("kraft.version", 1, UP)]), false),
        ),
        (
            "release-version with a feature",
            vec!["upgrade", "--release-version", "4.0", "--feature", "metadata.version=20"],
            Err("Can not specify `release-version` with other feature flags.".into()),
        ),
        (
            "release-version with metadata",
            vec!["downgrade", "--release-version", "4.0", "--metadata", "3.9"],
            Err("Can not specify `release-version` with other feature flags.".into()),
        ),
        (
            "a feature twice",
            vec!["upgrade", "--feature", "group.version=1", "--feature", "group.version=1"],
            Err("Feature group.version was specified more than once.".into()),
        ),
        (
            "disable a feature twice",
            vec!["disable", "--feature", "group.version", "--feature", "group.version"],
            Err("Feature group.version was specified more than once.".into()),
        ),
        (
            "no equals sign",
            vec!["upgrade", "--feature", "group.version"],
            Err("Can't parse feature=level string group.version: equals sign not found.".into()),
        ),
        (
            "upgrade to level 0",
            vec!["upgrade", "--feature", "group.version=0"],
            Err("The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.".into()),
        ),
        (
            "a negative level",
            vec!["downgrade", "--feature", "group.version=-1"],
            Err("Cannot specify a negative version level.".into()),
        ),
        (
            "nothing to upgrade",
            vec!["upgrade"],
            Err("You must specify at least one feature to upgrade".into()),
        ),
        (
            "nothing to disable",
            vec!["disable"],
            Err("You must specify at least one feature to disable".into()),
        ),
        (
            "an empty feature name",
            vec!["upgrade", "--feature", " =1"],
            Err("Provided feature can not be empty.".into()),
        ),
        (
            "an unknown feature",
            vec!["upgrade", "--feature", "foo.bar=1"],
            Err(format!("Unsupported feature: foo.bar. Supported features are: {known}")),
        ),
        (
            "a level out of range",
            vec!["upgrade", "--feature", "group.version=5"],
            Err("feature group.version=5 is outside the supported range 0..=1".into()),
        ),
        (
            "an unknown release",
            vec!["upgrade", "--release-version", "2.8"],
            Err(resolve_release("2.8").unwrap_err()),
        ),
    ];
    for (case, argv, expected) in cases {
        check!(plan_of(&argv) == expected, "{case}");
    }
}

#[test]
fn the_command_line_is_kafka_features_command_line() {
    let accepted: &[&[&str]] = &[
        &["--bootstrap-server", "b:9092", "describe"],
        &[
            "--bootstrap-controller",
            "c:9093",
            "--command-config",
            "admin.properties",
            "describe",
        ],
        &["--bootstrap-server", "b:9092", "describe", "--node-id", "1"],
        &[
            "--bootstrap-server",
            "b:9092",
            "upgrade",
            "--metadata",
            "4.0",
            "--dry-run",
        ],
        &[
            "--bootstrap-server",
            "b:9092",
            "downgrade",
            "--feature",
            "a=1",
            "--unsafe",
            "--dry-run",
        ],
        &[
            "--bootstrap-server",
            "b:9092",
            "disable",
            "--feature",
            "a",
            "--unsafe",
        ],
        &["version-mapping"],
        &["version-mapping", "--release-version", "4.0"],
        &["version-mapping", "--all"],
        &["feature-dependencies", "--feature", "group.version=1"],
    ];
    for argv in accepted {
        check!(parse(argv).is_ok(), "{argv:?}");
    }
    let refused: &[&[&str]] = &[
        // The connection flags belong to the tool, not to the subcommand.
        &["describe", "--bootstrap-server", "b:9092"],
        &[
            "--bootstrap-server",
            "b:9092",
            "--bootstrap-controller",
            "c:9093",
            "describe",
        ],
        &["--bootstrap-server", "b:9092", "upgrade", "--unsafe"],
        &["--bootstrap-server", "b:9092"],
        &["version-mapping", "--all", "--release-version", "4.0"],
    ];
    for argv in refused {
        check!(parse(argv).is_err(), "{argv:?}");
    }
}

fn metadata(
    supported: &[(&str, i16, i16)],
    finalized: &[(&str, i16)],
    epoch: Option<i64>,
) -> FeatureMetadata {
    FeatureMetadata {
        supported: supported
            .iter()
            .map(|(name, min, max)| FeatureRange {
                name: (*name).to_owned(),
                min_version: *min,
                max_version: *max,
            })
            .collect(),
        finalized: finalized
            .iter()
            .map(|(name, level)| FeatureRange {
                name: (*name).to_owned(),
                min_version: *level,
                max_version: *level,
            })
            .collect(),
        finalized_features_epoch: epoch,
    }
}

#[test]
fn describe_prints_kafka_features_layout() {
    let cluster = metadata(
        &[
            ("transaction.version", 0, 2),
            ("metadata.version", 7, 25),
            ("share.version", 0, 1),
        ],
        &[("metadata.version", 25), ("transaction.version", 2)],
        Some(320),
    );
    let result = render_describe(&cluster);
    check!(
        result.human
            == vec![
                "Feature: metadata.version                          SupportedMinVersion: 3.3-IV3          SupportedMaxVersion: 4.0-IV3          FinalizedVersionLevel: 4.0-IV3          Epoch: 320",
                "Feature: share.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 0                Epoch: 320",
                "Feature: transaction.version                       SupportedMinVersion: 0                SupportedMaxVersion: 2                FinalizedVersionLevel: 2                Epoch: 320",
            ]
    );
    check!(
        result.data
            == json!({
                "features": [
                    {"feature": "metadata.version", "supported_min_version": 7, "supported_max_version": 25, "finalized_version_level": 25},
                    {"feature": "share.version", "supported_min_version": 0, "supported_max_version": 1, "finalized_version_level": 0},
                    {"feature": "transaction.version", "supported_min_version": 0, "supported_max_version": 2, "finalized_version_level": 2},
                ],
                "finalized_features_epoch": 320,
            })
    );
}

#[test]
fn describe_without_an_epoch_prints_a_dash() {
    let result = render_describe(&metadata(&[("group.version", 0, 1)], &[], None));
    check!(
        result.human
            == vec![
                "Feature: group.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 0                Epoch: -",
            ]
    );
    check!(result.data["finalized_features_epoch"] == Value::Null);
}

#[test]
fn the_report_uses_kafka_features_wording() {
    let row_error = |message: &str| RowError {
        code: 95,
        name: "INVALID_UPDATE_VERSION",
        message: message.to_owned(),
    };
    let requested = updates(&[("group.version", 1, UP), ("transaction.version", 2, UP)]);
    let cases = [
        (
            Op::Upgrade,
            false,
            None,
            vec![
                "group.version was upgraded to 1.",
                "transaction.version was upgraded to 2.",
            ],
        ),
        (
            Op::Upgrade,
            true,
            None,
            vec![
                "group.version can be upgraded to 1.",
                "transaction.version can be upgraded to 2.",
            ],
        ),
        (
            Op::Downgrade,
            false,
            Some("no."),
            vec![
                "Could not downgrade group.version to 1. no.",
                "Could not downgrade transaction.version to 2. no.",
            ],
        ),
        (
            Op::Downgrade,
            true,
            Some("no."),
            vec![
                "Can not downgrade group.version to 1. no.",
                "Can not downgrade transaction.version to 2. no.",
            ],
        ),
        (
            Op::Disable,
            false,
            None,
            vec![
                "group.version was disabled.",
                "transaction.version was disabled.",
            ],
        ),
        (
            Op::Disable,
            true,
            Some("no."),
            vec![
                "Can not disable group.version. no.",
                "Can not disable transaction.version. no.",
            ],
        ),
    ];
    for (op, dry_run, error, expected) in cases {
        let plan = Plan {
            op,
            notices: Vec::new(),
            updates: requested.clone(),
            dry_run,
        };
        let result = plan.report(|_| error.map(row_error));
        check!(
            (result.human.clone(), result.failed, result.notices.clone())
                == (
                    expected
                        .iter()
                        .map(|line| (*line).to_owned())
                        .collect::<Vec<_>>(),
                    error.is_some(),
                    error
                        .map(|_| vec!["2 out of 2 operation(s) failed.".to_owned()])
                        .unwrap_or_default(),
                ),
            "{op:?} dry_run={dry_run}"
        );
    }
}

#[test]
fn the_report_json_names_each_update_and_its_error() {
    let plan = Plan {
        op: Op::Downgrade,
        notices: vec![METADATA_FLAG_NOTICE.to_owned()],
        updates: updates(&[("group.version", 0, SAFE), ("share.version", 0, SAFE)]),
        dry_run: false,
    };
    let result = plan.report(|name| {
        (name == "share.version").then(|| RowError {
            code: 95,
            name: "INVALID_UPDATE_VERSION",
            message: "bad".into(),
        })
    });
    assert!(
        result
            == CommandResult::rows(
                vec![
                    METADATA_FLAG_NOTICE.to_owned(),
                    "group.version was downgraded to 0.".to_owned(),
                    "Could not downgrade share.version to 0. bad".to_owned(),
                ],
                json!({
                    "operation": "downgrade",
                    "updates": [
                        {"feature": "group.version", "level": 0, "upgrade_type": "SAFE_DOWNGRADE", "error": null},
                        {"feature": "share.version", "level": 0, "upgrade_type": "SAFE_DOWNGRADE", "error": {"code": 95, "name": "INVALID_UPDATE_VERSION", "message": "bad"}},
                    ],
                    "failures": 1,
                }),
                true,
            )
            .with_notices(vec!["1 out of 2 operation(s) failed.".to_owned()])
    );
}

#[test]
fn results_reach_each_feature_as_feature_command_reads_them() {
    let requested = updates(&[("group.version", 1, UP), ("transaction.version", 2, UP)]);
    let row = |code, name, message: &str| {
        Some(RowError {
            code,
            name,
            message: message.to_owned(),
        })
    };
    let cases = [
        (
            "every feature succeeds",
            Ok(BTreeMap::from([
                ("group.version".to_owned(), Ok(())),
                ("transaction.version".to_owned(), Ok(())),
            ])),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                ("transaction.version".to_owned(), None),
            ]),
        ),
        (
            "one feature fails with the controller's message",
            Ok(BTreeMap::from([
                ("group.version".to_owned(), Ok(())),
                (
                    "transaction.version".to_owned(),
                    Err(KafkaError {
                        code: 95,
                        name: "UNKNOWN",
                        message: Some("nope".into()),
                    }),
                ),
            ])),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                (
                    "transaction.version".to_owned(),
                    row(95, "INVALID_UPDATE_VERSION", "nope"),
                ),
            ]),
        ),
        (
            "an error with no message takes Kafka's default message",
            Ok(BTreeMap::from([
                ("group.version".to_owned(), Ok(())),
                (
                    "transaction.version".to_owned(),
                    Err(KafkaError {
                        code: 95,
                        name: "UNKNOWN",
                        message: None,
                    }),
                ),
            ])),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                (
                    "transaction.version".to_owned(),
                    row(
                        95,
                        "INVALID_UPDATE_VERSION",
                        KafkaException::for_code(95).message(),
                    ),
                ),
            ]),
        ),
        (
            "a feature with no result",
            Ok(BTreeMap::from([("group.version".to_owned(), Ok(()))])),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                (
                    "transaction.version".to_owned(),
                    row(
                        -1,
                        "UNKNOWN_SERVER_ERROR",
                        "The controller response did not contain a result for feature transaction.version",
                    ),
                ),
            ]),
        ),
        (
            "a failed call fails every feature",
            Err(AdminError::Broker {
                api: "UpdateFeatures",
                code: 7,
                name: "REQUEST_TIMED_OUT",
                message: Some("timed out".into()),
            }),
            BTreeMap::from([
                (
                    "group.version".to_owned(),
                    row(7, "REQUEST_TIMED_OUT", "timed out"),
                ),
                (
                    "transaction.version".to_owned(),
                    row(7, "REQUEST_TIMED_OUT", "timed out"),
                ),
            ]),
        ),
    ];
    for (case, outcome, expected) in cases {
        check!(
            outcome_errors(&requested, outcome).ok() == Some(expected),
            "{case}"
        );
    }
    check!(outcome_errors(&requested, Err(AdminError::Protocol("lost".into()))).is_err());
}

#[tokio::test]
async fn a_negative_node_id_fails_before_connecting() {
    // An address nothing listens on: reaching it would fail differently.
    let error = parse(&[
        "--bootstrap-server",
        "127.0.0.1:1",
        "describe",
        "--node-id",
        "-1",
    ])
    .unwrap()
    .run()
    .await
    .unwrap_err();
    check!(error.to_string() == "Invalid node id -1: must be non-negative.");
}
