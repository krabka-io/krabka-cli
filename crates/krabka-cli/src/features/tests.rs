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
            UpgradeType::downgrade(args.unsafe_downgrade),
            args.dry_run,
        ),
        FeaturesCommand::Disable(args) => Plan::disable(
            &args.feature,
            UpgradeType::downgrade(args.unsafe_downgrade),
            args.dry_run,
        ),
        other => panic!("not an update: {other:?}"),
    }
    .and_then(|plan| plan.validate().map(|()| plan))
}

fn updates(rows: &[(&str, i16, UpgradeType)]) -> BTreeMap<String, Update> {
    rows.iter()
        .map(|(name, level, upgrade_type)| {
            (
                (*name).to_owned(),
                Update {
                    level: *level,
                    upgrade_type: *upgrade_type,
                },
            )
        })
        .collect()
}

fn release_updates(release: &str, upgrade_type: UpgradeType) -> BTreeMap<String, Update> {
    let level = resolve_release(release).unwrap().feature_level();
    std::iter::once((
        METADATA_VERSION_FEATURE.to_owned(),
        Update {
            level,
            upgrade_type,
        },
    ))
    .chain(production_features().into_iter().filter_map(|feature| {
        let default = feature.default_level(level);
        (upgrade_type != UpgradeType::Upgrade || default > 0).then(|| {
            (
                feature.name().to_owned(),
                Update {
                    level: default,
                    upgrade_type,
                },
            )
        })
    }))
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
            plan(Op::Upgrade, &[], release_updates("4.0", UP), true),
        ),
        (
            "downgrade --release-version keeps level-0 features",
            vec!["downgrade", "--release-version", "3.7"],
            plan(Op::Downgrade, &[], release_updates("3.7", SAFE), false),
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
    epoch: i64,
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
        320,
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
    let result = render_describe(&metadata(&[("group.version", 0, 1)], &[], -1));
    check!(
        result.human
            == vec![
                "Feature: group.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 0                Epoch: -",
            ]
    );
    check!(result.data["finalized_features_epoch"] == Value::Null);
}

fn dependencies_for_tests(name: &str, level: i16) -> &'static [(&'static str, i16)] {
    match (name, level) {
        ("group.version", 1) => &[("transaction.version", 2)],
        _ => &[],
    }
}

#[test]
fn a_dry_run_predicts_the_controller_verdict() {
    let cluster = metadata(
        &[
            ("metadata.version", 7, 25),
            ("group.version", 0, 1),
            ("transaction.version", 0, 2),
            ("kraft.version", 0, 1),
            ("streams.version", 0, 0),
        ],
        &[
            ("metadata.version", 20),
            ("group.version", 1),
            ("kraft.version", 1),
        ],
        5,
    );
    let failed = |feature: &str, level: i16, message: &str| {
        Some(format!(
            "The update failed for all features since the following feature had an error: Invalid update version {level} for feature {feature}. {message}"
        ))
    };
    let cases = [
        (
            "an upgrade inside the range",
            updates(&[("transaction.version", 2, UP), ("metadata.version", 25, UP)]),
            None,
        ),
        (
            "a level the cluster does not support",
            updates(&[("transaction.version", 3, UP)]),
            failed(
                "transaction.version",
                3,
                "Broker only supports versions 0-2",
            ),
        ),
        (
            "a feature the cluster disables",
            updates(&[("streams.version", 1, UP)]),
            failed(
                "streams.version",
                1,
                "Broker does not support this feature.",
            ),
        ),
        (
            "a feature the cluster does not know",
            updates(&[("share.version", 1, UP)]),
            failed("share.version", 1, "Broker does not support this feature."),
        ),
        (
            "an upgrade to a lower level",
            updates(&[("metadata.version", 19, UP)]),
            failed(
                "metadata.version",
                19,
                "Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.",
            ),
        ),
        (
            "a downgrade to a higher level",
            updates(&[("metadata.version", 21, SAFE)]),
            failed(
                "metadata.version",
                21,
                "Can't downgrade to a newer version.",
            ),
        ),
        (
            "a kraft.version downgrade",
            updates(&[("kraft.version", 0, SAFE)]),
            failed(
                "kraft.version",
                0,
                "Can't downgrade the version of this feature.",
            ),
        ),
        (
            "an unmet dependency",
            updates(&[("group.version", 1, UP), ("transaction.version", 1, UP)]),
            failed(
                "group.version",
                1,
                "group.version could not be set to 1 because it depends on transaction.version level 2",
            ),
        ),
        (
            "a dependency met by the same request",
            updates(&[("group.version", 1, UP), ("transaction.version", 2, UP)]),
            None,
        ),
    ];
    for (case, requested, expected) in cases {
        check!(
            predict_failure(&requested, &cluster, dependencies_for_tests) == expected,
            "{case}"
        );
    }
}

#[test]
fn the_report_uses_kafka_features_wording() {
    let row_error = |message: &str| RowError {
        code: Some(89),
        name: Some("INVALID_UPDATE_VERSION"),
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
            (result.human.clone(), result.failed)
                == (
                    expected
                        .iter()
                        .map(|line| (*line).to_owned())
                        .collect::<Vec<_>>(),
                    error.is_some()
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
            code: Some(89),
            name: Some("INVALID_UPDATE_VERSION"),
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
                        {"feature": "share.version", "level": 0, "upgrade_type": "SAFE_DOWNGRADE", "error": {"code": 89, "name": "INVALID_UPDATE_VERSION", "message": "bad"}},
                    ],
                    "failures": 1,
                }),
                true,
            )
    );
}

#[test]
fn response_errors_reach_each_feature_as_the_admin_client_reports_them() {
    let requested = updates(&[("group.version", 1, UP), ("transaction.version", 2, UP)]);
    let invalid = |message: Option<&str>| KafkaError {
        code: 89,
        name: "INVALID_UPDATE_VERSION",
        message: message.map(str::to_owned),
    };
    let row = |code, name, message: &str| {
        Some(RowError {
            code,
            name,
            message: message.to_owned(),
        })
    };
    let cases = [
        (
            "a v2 success has no rows",
            Ok(Vec::new()),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                ("transaction.version".to_owned(), None),
            ]),
        ),
        (
            "per-feature rows",
            Ok(vec![
                FeatureUpdateOutcome {
                    name: "group.version".into(),
                    error: None,
                },
                FeatureUpdateOutcome {
                    name: "transaction.version".into(),
                    error: Some(invalid(Some("nope"))),
                },
            ]),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                (
                    "transaction.version".to_owned(),
                    row(Some(89), Some("INVALID_UPDATE_VERSION"), "nope"),
                ),
            ]),
        ),
        (
            "a missing row",
            Ok(vec![FeatureUpdateOutcome {
                name: "group.version".into(),
                error: None,
            }]),
            BTreeMap::from([
                ("group.version".to_owned(), None),
                (
                    "transaction.version".to_owned(),
                    row(
                        None,
                        None,
                        "The controller response did not contain a result for feature transaction.version",
                    ),
                ),
            ]),
        ),
        (
            "a top-level error fails every feature",
            Err(AdminError::Broker {
                api: "UpdateFeatures",
                code: 89,
                name: "INVALID_UPDATE_VERSION",
                message: Some("all bad".into()),
            }),
            BTreeMap::from([
                (
                    "group.version".to_owned(),
                    row(Some(89), Some("INVALID_UPDATE_VERSION"), "all bad"),
                ),
                (
                    "transaction.version".to_owned(),
                    row(Some(89), Some("INVALID_UPDATE_VERSION"), "all bad"),
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
async fn unsupported_sub_features_fail_before_connecting() {
    let cases: &[(&[&str], &str)] = &[
        (
            &["describe", "--node-id", "1"],
            "describe --node-id is not supported by this build: the pinned krabka-client-admin cannot send DescribeFeatures to one node",
        ),
        (
            &["describe", "--node-id", "-1"],
            "Invalid node id -1: must be non-negative.",
        ),
        (
            &["downgrade", "--unsafe", "--feature", "group.version=0"],
            "--unsafe is not supported by this build: the pinned krabka-client-admin cannot send an UNSAFE_DOWNGRADE feature update",
        ),
        (
            &["disable", "--unsafe", "--feature", "group.version"],
            "--unsafe is not supported by this build: the pinned krabka-client-admin cannot send an UNSAFE_DOWNGRADE feature update",
        ),
    ];
    for (argv, expected) in cases {
        // An address nothing listens on: reaching it would fail differently.
        let argv = ["--bootstrap-server", "127.0.0.1:1"]
            .iter()
            .chain(argv.iter())
            .copied()
            .collect::<Vec<_>>();
        let error = parse(&argv).unwrap().run().await.unwrap_err();
        check!(error.to_string() == *expected, "{argv:?}");
    }
}
