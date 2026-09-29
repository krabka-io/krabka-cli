use assert2::{assert, check};

use super::*;

fn level_of(release: &str) -> Result<i16, String> {
    resolve_release(release).map(MetadataVersion::feature_level)
}

fn highest_in(short: &str) -> i16 {
    metadata_versions()
        .filter(|version| version.short() == short)
        .map(MetadataVersion::feature_level)
        .max()
        .expect("the release is in the table")
}

#[test]
fn a_release_resolves_as_kafka_resolves_it() {
    let cases = [
        ("4.0", Ok(highest_in("4.0"))),
        ("4.0-IV1", Ok(23)),
        ("3.8.1", Ok(highest_in("3.8"))),
        ("3.8.1.7", Ok(highest_in("3.8"))),
        ("3.3-IV3", Ok(METADATA_VERSION_MIN)),
    ];
    for (release, expected) in cases {
        check!(level_of(release) == expected, "{release}");
    }
}

#[test]
fn an_unknown_release_lists_every_known_version() {
    let known = metadata_versions()
        .map(MetadataVersion::ivn)
        .collect::<Vec<_>>()
        .join(", ");
    for release in ["2.8", "4.0-IV9", "banana"] {
        check!(
            level_of(release)
                == Err(format!(
                    "Unknown metadata.version '{release}'. Supported metadata.version are: {known}"
                ))
        );
    }
}

#[test]
fn production_features_follow_kafka_order_and_omit_metadata_version() {
    let names = production_features()
        .iter()
        .map(|feature| feature.name())
        .collect::<Vec<_>>();
    let mut registered = feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != METADATA_VERSION_FEATURE)
        .collect::<Vec<_>>();
    registered.sort_by_key(|name| kafka_rank(name));
    assert!(names == registered);
    check!(
        names
            .windows(2)
            .all(|pair| kafka_rank(pair[0]) <= kafka_rank(pair[1]))
    );
    check!(names.first() == Some(&"kraft.version"));
}

#[test]
fn version_mapping_prints_each_feature_default_at_the_release() {
    for release in [None, Some("4.0"), Some("3.8.1"), Some("3.5-IV2")] {
        let version = release.map_or_else(latest_production_metadata_version, |release| {
            resolve_release(release).unwrap()
        });
        let echoed = release.map_or_else(|| version.ivn().to_owned(), str::to_owned);
        let expected = std::iter::once(format!(
            "metadata.version={} ({echoed})",
            version.feature_level()
        ))
        .chain(production_features().iter().map(|feature| {
            format!(
                "{}={}",
                feature.name(),
                feature.default_level(version.feature_level())
            )
        }))
        .collect::<Vec<_>>();
        check!(
            version_mapping(release).unwrap().human() == expected,
            "{release:?}"
        );
    }
}

#[test]
fn version_mapping_json_carries_the_same_values() {
    let mapping = version_mapping(Some("4.0")).unwrap();
    let version = resolve_release("4.0").unwrap();
    check!(
        mapping.json()
            == json!({
                "release_version": "4.0",
                "metadata_version": {"level": version.feature_level(), "name": version.ivn()},
                "features": production_features()
                    .iter()
                    .map(|feature| json!({
                        "feature": feature.name(),
                        "level": feature.default_level(version.feature_level()),
                    }))
                    .collect::<Vec<_>>(),
            })
    );
}

#[test]
fn version_mapping_table_has_one_row_per_level() {
    let table = version_mapping_table();
    let rows = metadata_versions()
        .map(|version| version_mapping(Some(version.ivn())).unwrap())
        .collect::<Vec<_>>();
    check!(table.human == rows.iter().map(VersionMapping::row).collect::<Vec<_>>());
    check!(table.data == json!(rows.iter().map(VersionMapping::json).collect::<Vec<_>>()));
    check!(rows.len() == metadata_versions().count());
}

#[test]
fn dependency_queries_parse_in_each_tool_wording() {
    let cases = [
        (
            Dialect::Features,
            " group.version = 1 ",
            Ok(("group.version".to_owned(), 1)),
        ),
        (
            Dialect::Storage,
            " group.version = 1 ",
            Ok(("group.version".to_owned(), 1)),
        ),
        (
            Dialect::Features,
            "group.version",
            Err("Can't parse feature=level string group.version: equals sign not found.".to_owned()),
        ),
        (
            Dialect::Storage,
            "group.version",
            Err(
                "Invalid feature format: group.version. Expected format: 'feature=version' (e.g. 'group.version=1')"
                    .to_owned(),
            ),
        ),
        (
            Dialect::Features,
            "group.version=x",
            Err(
                "Can't parse feature=level string group.version=x: unable to parse x as a short."
                    .to_owned(),
            ),
        ),
        (
            Dialect::Storage,
            "group.version=x",
            Err("Invalid version format: x for feature group.version".to_owned()),
        ),
        (
            Dialect::Features,
            "group.version=40000",
            Err(
                "Can't parse feature=level string group.version=40000: unable to parse 40000 as a short."
                    .to_owned(),
            ),
        ),
    ];
    for (dialect, spec, expected) in cases {
        check!(
            parse_dependency_query(spec, dialect) == expected,
            "{dialect:?} {spec}"
        );
    }
}

#[test]
fn dependency_lookups_reject_what_kafka_rejects() {
    let cases = [
        (Dialect::Features, "foo", 1, "Unknown feature: foo"),
        (Dialect::Storage, "foo", 1, "Unknown feature: foo"),
        (
            Dialect::Features,
            "metadata.version",
            99,
            "Unknown metadata.version 99",
        ),
        (
            Dialect::Storage,
            "metadata.version",
            6,
            "Unknown metadata.version 6",
        ),
        (
            Dialect::Features,
            "group.version",
            9,
            "No feature:group.version with feature level 9",
        ),
        (
            Dialect::Storage,
            "group.version",
            9,
            "Feature level 9 is not supported for feature group.version",
        ),
    ];
    for (dialect, name, level, expected) in cases {
        check!(
            feature_dependencies(name, level, dialect) == Err(expected.to_owned()),
            "{dialect:?} {name}={level}"
        );
    }
}

#[test]
fn a_known_feature_level_reports_its_declared_dependencies() {
    for feature in production_features() {
        let (min, max) = feature.supported_range();
        for level in min..=max {
            let declared = feature
                .dependencies(level)
                .iter()
                .map(|(name, level)| Dependency {
                    feature: (*name).to_owned(),
                    level: *level,
                })
                .collect();
            check!(
                feature_dependencies(feature.name(), level, Dialect::Features)
                    == Ok(FeatureDependencies {
                        feature: feature.name().to_owned(),
                        level,
                        dependencies: declared,
                    })
            );
        }
    }
}

#[test]
fn dependencies_render_as_kafka_prints_them() {
    let latest = latest_production_metadata_version();
    let cases = [
        (
            FeatureDependencies {
                feature: METADATA_VERSION_FEATURE.into(),
                level: latest.feature_level(),
                dependencies: Vec::new(),
            },
            vec![format!(
                "metadata.version={} ({}) has no dependencies.",
                latest.feature_level(),
                latest.ivn()
            )],
        ),
        (
            FeatureDependencies {
                feature: "group.version".into(),
                level: 1,
                dependencies: Vec::new(),
            },
            vec!["group.version=1 has no dependencies.".to_owned()],
        ),
        (
            FeatureDependencies {
                feature: "eligible.leader.replicas.version".into(),
                level: 1,
                dependencies: vec![
                    Dependency {
                        feature: METADATA_VERSION_FEATURE.into(),
                        level: 23,
                    },
                    Dependency {
                        feature: "group.version".into(),
                        level: 1,
                    },
                ],
            },
            vec![
                "eligible.leader.replicas.version=1 requires:".to_owned(),
                "    metadata.version=23 (4.0-IV1)".to_owned(),
                "    group.version=1".to_owned(),
            ],
        ),
    ];
    for (dependencies, expected) in cases {
        check!(dependencies.human() == expected);
    }
}

#[test]
fn the_whole_graph_has_one_row_per_feature_level() {
    let expected = metadata_versions()
        .map(|version| (METADATA_VERSION_FEATURE.to_owned(), version.feature_level()))
        .chain(production_features().iter().flat_map(|feature| {
            let (min, max) = feature.supported_range();
            (min..=max).map(|level| (feature.name().to_owned(), level))
        }))
        .collect::<Vec<_>>();
    let graph = all_feature_dependencies();
    check!(
        graph
            .iter()
            .map(|row| (row.feature.clone(), row.level))
            .collect::<Vec<_>>()
            == expected
    );
    for row in &graph {
        let declared = feature(&row.feature).map_or(0, |f| f.dependencies(row.level).len());
        check!(
            row.dependencies.len() == declared,
            "{}={}",
            row.feature,
            row.level
        );
    }
}

#[test]
fn level_to_string_names_metadata_versions_only() {
    let latest = latest_production_metadata_version();
    let cases = [
        (
            METADATA_VERSION_FEATURE,
            latest.feature_level(),
            latest.ivn().to_owned(),
        ),
        (METADATA_VERSION_FEATURE, 0, "UNKNOWN 0".to_owned()),
        ("group.version", 1, "1".to_owned()),
    ];
    for (feature, level, expected) in cases {
        check!(level_to_string(feature, level) == expected);
    }
}
