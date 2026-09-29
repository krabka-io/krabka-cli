//! `krabka storage` through the binary.

use std::{path::Path, process::Command};

use assert2::{assert, check};
use krabka_metadata::{
    feature_registry,
    metadata_version::{
        self, KRAFT_VERSION_FEATURE, METADATA_VERSION_FEATURE, METADATA_VERSION_MAX,
        METADATA_VERSION_MIN,
    },
};
use serde_json::{Value, json};

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn krabka(args: &[&str]) -> Run {
    let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
        .args(args)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    Run {
        code: out.status.code(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

fn production_features() -> Vec<&'static str> {
    let order = [
        "kraft.version",
        "transaction.version",
        "group.version",
        "eligible.leader.replicas.version",
        "share.version",
        "streams.version",
    ];
    let mut names = feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != METADATA_VERSION_FEATURE)
        .collect::<Vec<_>>();
    names.sort_by_key(|name| {
        order
            .iter()
            .position(|known| known == name)
            .unwrap_or(usize::MAX)
    });
    names
}

fn mapping_lines(level: i16, release: &str) -> Vec<String> {
    std::iter::once(format!("metadata.version={level} ({release})"))
        .chain(production_features().into_iter().map(|name| {
            let feature = krabka_metadata::feature(name).unwrap();
            format!("{name}={}", feature.default_level(level))
        }))
        .collect()
}

#[test]
fn version_mapping_prints_each_feature_default_at_the_release() {
    // With no --release-version, kafka-storage maps MetadataVersion.LATEST_PRODUCTION.
    let latest =
        metadata_version::from_feature_level(krabka_format::LATEST_PRODUCTION_METADATA_VERSION)
            .unwrap();
    let cases = [
        (vec!["--release-version", "4.0"], "4.0"),
        (vec!["-r", "3.8.1"], "3.8.1"),
        (Vec::new(), latest.ivn()),
    ];
    for (flags, release) in cases {
        let level = if flags.is_empty() {
            krabka_format::LATEST_PRODUCTION_METADATA_VERSION
        } else {
            let key = release.split('.').take(2).collect::<Vec<_>>().join(".");
            metadata_version::from_version_string(&key)
                .unwrap()
                .feature_level()
        };
        let argv = ["storage", "version-mapping"]
            .into_iter()
            .chain(flags.iter().copied())
            .collect::<Vec<_>>();
        let run = krabka(&argv);
        check!(
            (
                run.code,
                run.stdout.lines().map(str::to_owned).collect::<Vec<_>>()
            ) == (Some(0), mapping_lines(level, release)),
            "{flags:?}"
        );
    }
}

#[test]
fn version_mapping_all_prints_one_row_per_level() {
    let run = krabka(&["storage", "version-mapping", "--all"]);
    let expected = (METADATA_VERSION_MIN..=METADATA_VERSION_MAX)
        .filter_map(metadata_version::from_feature_level)
        .map(|version| mapping_lines(version.feature_level(), version.ivn()).join(" "))
        .collect::<Vec<_>>();
    check!(run.code == Some(0));
    check!(run.stdout.lines().map(str::to_owned).collect::<Vec<_>>() == expected);
}

#[test]
fn an_unknown_release_fails_with_kafka_message() {
    let run = krabka(&["storage", "version-mapping", "--release-version", "2.8"]);
    check!(run.code == Some(1));
    check!(run.stderr.starts_with(
        "krabka storage: Unknown metadata.version '2.8'. Supported metadata.version are: 3.3-IV3, "
    ));
}

#[test]
fn feature_dependencies_renders_the_whole_graph() {
    let run = krabka(&["storage", "feature-dependencies"]);
    let mut expected = (METADATA_VERSION_MIN..=METADATA_VERSION_MAX)
        .filter_map(metadata_version::from_feature_level)
        .map(|version| {
            format!(
                "metadata.version={} ({}) has no dependencies.",
                version.feature_level(),
                version.ivn()
            )
        })
        .collect::<Vec<_>>();
    for name in production_features() {
        let feature = krabka_metadata::feature(name).unwrap();
        let (min, max) = feature.supported_range();
        for level in min..=max {
            let dependencies = feature.dependencies(level);
            if dependencies.is_empty() {
                expected.push(format!("{name}={level} has no dependencies."));
            } else {
                expected.push(format!("{name}={level} requires:"));
                // Kafka's `FeatureCommand` names a metadata.version
                // dependency's release version too.
                expected.extend(dependencies.iter().map(|(dependency, min)| {
                    match metadata_version::from_feature_level(*min) {
                        Some(version) if *dependency == METADATA_VERSION_FEATURE => {
                            format!("    {dependency}={min} ({})", version.ivn())
                        }
                        _ => format!("    {dependency}={min}"),
                    }
                }));
            }
        }
    }
    check!(run.code == Some(0));
    check!(run.stdout.lines().map(str::to_owned).collect::<Vec<_>>() == expected);
}

#[test]
fn feature_dependencies_answers_each_query_in_kafka_storage_wording() {
    let cases: &[(&[&str], Option<i32>, &str, &str)] = &[
        (
            &["--feature", "group.version=1", "-f", "kraft.version=1"],
            Some(0),
            "group.version=1 has no dependencies.\nkraft.version=1 has no dependencies.\n",
            "",
        ),
        (
            &["--feature", "group.version=9"],
            Some(1),
            "",
            "krabka storage: Feature level 9 is not supported for feature group.version\n",
        ),
        (
            &["--feature", "group.version"],
            Some(1),
            "",
            "krabka storage: Invalid feature format: group.version. Expected format: 'feature=version' (e.g. 'group.version=1')\n",
        ),
        (
            &["--feature", "foo=1"],
            Some(1),
            "",
            "krabka storage: Unknown feature: foo\n",
        ),
    ];
    for (flags, code, stdout, stderr) in cases {
        let argv = ["storage", "feature-dependencies"]
            .iter()
            .chain(flags.iter())
            .copied()
            .collect::<Vec<_>>();
        let run = krabka(&argv);
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (*code, *stdout, *stderr),
            "{flags:?}"
        );
    }
}

#[test]
fn random_uuid_prints_a_kafka_uuid_that_format_accepts_as_the_cluster_id() {
    let first = krabka(&["storage", "random-uuid"]);
    let second = krabka(&["storage", "random-uuid"]);
    let id = first.stdout.trim_end_matches('\n');
    check!((first.code, first.stderr.as_str()) == (Some(0), ""));
    check!(first.stdout == format!("{id}\n"));
    check!(id.len() == 22);
    check!(
        id.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    );
    check!(id.parse::<krabka_ids::KafkaUuid>().is_ok());
    check!(first.stdout != second.stdout);

    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a");
    let format = krabka(&[
        "storage",
        "format",
        "--log-dir",
        dir.to_str().unwrap(),
        "--cluster-id",
        id,
    ]);
    assert!(format.code == Some(0), "{}", format.stderr);
    check!(meta_properties(&dir)["cluster_id"] == json!(id));
}

fn meta_properties(dir: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(dir.join("meta.properties.json")).unwrap()).unwrap()
}

#[test]
fn info_reads_back_a_directory_that_storage_format_wrote() {
    let root = tempfile::tempdir().unwrap();
    let formatted = root.path().join("formatted");
    let unformatted = root.path().join("unformatted");
    std::fs::create_dir(&unformatted).unwrap();
    // Kafka's base64 Uuid form, which `kafka-storage format` takes.
    let cluster_id = "XzoePA17S1OaTjotmm97EA";
    let format = krabka(&[
        "storage",
        "format",
        "--log-dir",
        formatted.to_str().unwrap(),
        "--cluster-id",
        cluster_id,
        "--release-version",
        "3.9",
    ]);
    assert!(format.code == Some(0), "{}", format.stderr);
    let meta = meta_properties(&formatted);

    let run = krabka(&[
        "--output",
        "json",
        "storage",
        "info",
        "--log-dir",
        &format!("{},{}", formatted.display(), unformatted.display()),
    ]);
    let level = metadata_version::from_version_string("3.9")
        .unwrap()
        .feature_level();
    let features = feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != KRAFT_VERSION_FEATURE)
        .map(|name| {
            let feature = krabka_metadata::feature(name).unwrap();
            (name.to_owned(), json!(feature.default_level(level)))
        })
        .collect::<serde_json::Map<_, _>>();
    check!(
        run.code == Some(1),
        "an unformatted directory is a problem, as in kafka-storage"
    );
    check!(
        serde_json::from_str::<Value>(&run.stdout).unwrap()
            == json!({"data": {
                "directories": [
                    {
                        "path": formatted.display().to_string(),
                        "status": "formatted",
                        "cluster_id": cluster_id,
                        "directory_id": meta["directory_id"],
                        "version": meta["version"],
                        "features": features,
                    },
                    {"path": unformatted.display().to_string(), "status": "unformatted"},
                ],
                "problems": [format!("{} is not formatted.", unformatted.display())],
            }})
    );
}

#[test]
fn info_reads_the_directories_of_a_kafka_config_file() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let format = krabka(&["storage", "format", "--log-dir", a.to_str().unwrap()]);
    assert!(format.code == Some(0), "{}", format.stderr);
    let config = root.path().join("server.properties");
    std::fs::write(&config, format!("log.dirs={}\n", a.display())).unwrap();
    let meta = meta_properties(&a);

    let run = krabka(&["storage", "info", "-c", config.to_str().unwrap()]);
    let lines = run.stdout.lines().collect::<Vec<_>>();
    check!(run.code == Some(0));
    check!(lines[..3] == ["Found log directory:", &format!("  {}", a.display()), ""]);
    check!(
        lines[3]
            == format!(
                "Found metadata: {{cluster.id={}, directory.id={}, version={}}}",
                meta["cluster_id"].as_str().unwrap(),
                meta["directory_id"].as_str().unwrap(),
                meta["version"]
            )
    );
    check!(lines[4].starts_with("Found features: {"));
}

#[test]
fn storage_format_exit_codes_reach_the_process_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("occupied"), b"x").unwrap();
    let run = krabka(&[
        "storage",
        "format",
        "--log-dir",
        dir.path().to_str().unwrap(),
    ]);
    check!(run.code == Some(3));
}

#[test]
fn help_lists_the_storage_and_features_subcommands() {
    let top = krabka(&["--help"]);
    check!(top.stdout.contains("\n  storage "));
    check!(top.stdout.contains("\n  features "));
    let storage = krabka(&["storage", "--help"]);
    for subcommand in [
        "info",
        "format",
        "version-mapping",
        "feature-dependencies",
        "random-uuid",
    ] {
        check!(
            storage.stdout.contains(&format!("\n  {subcommand} ")),
            "{subcommand}"
        );
    }
    let features = krabka(&["features", "--help"]);
    for subcommand in [
        "describe",
        "upgrade",
        "downgrade",
        "disable",
        "version-mapping",
        "feature-dependencies",
    ] {
        check!(
            features.stdout.contains(&format!("\n  {subcommand} ")),
            "{subcommand}"
        );
    }
}

#[test]
fn info_prints_kafka_storage_s_report_for_every_directory_that_one_format_wrote() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    let cluster_id = "XzoePA17S1OaTjotmm97EA";
    let format = krabka(&[
        "storage",
        "format",
        "--log-dir",
        a.to_str().unwrap(),
        "--log-dir",
        b.to_str().unwrap(),
        "--cluster-id",
        cluster_id,
        "--release-version",
        "4.0",
    ]);
    assert!(format.code == Some(0), "{}", format.stderr);
    let (meta_a, meta_b) = (meta_properties(&a), meta_properties(&b));
    check!(meta_a["cluster_id"] == meta_b["cluster_id"]);
    check!(meta_a["directory_id"] != meta_b["directory_id"]);
    check!(meta_a["version"] == json!(krabka_format::META_PROPERTIES_VERSION));

    // A repeated `--log-dir` and a comma-separated one name the same set.
    let repeated = krabka(&[
        "storage",
        "info",
        "--log-dir",
        b.to_str().unwrap(),
        "--log-dir",
        a.to_str().unwrap(),
    ]);
    let listed = krabka(&[
        "storage",
        "info",
        "--log-dir",
        &format!("{},{}", b.display(), a.display()),
    ]);
    let level = metadata_version::from_version_string("4.0")
        .unwrap()
        .feature_level();
    let features = feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != KRAFT_VERSION_FEATURE)
        .map(|name| {
            let feature = krabka_metadata::feature(name).unwrap();
            (name, feature.default_level(level))
        })
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(name, level)| format!("{name}={level}"))
        .collect::<Vec<_>>()
        .join(", ");
    let expected = format!(
        "Found log directories:\n  {}\n  {}\n\n\
         Found metadata: {{cluster.id={cluster_id}, directory.id={}, version={}}}\n\
         Found features: {{{features}}}\n\n",
        a.display(),
        b.display(),
        meta_a["directory_id"].as_str().unwrap(),
        krabka_format::META_PROPERTIES_VERSION,
    );
    for run in [repeated, listed] {
        check!((run.code, run.stdout, run.stderr) == (Some(0), expected.clone(), String::new()));
    }
}
