use assert2::{assert, check};
use krabka_metadata::{FeatureLevelRecord, to_kafka_record};

use super::*;

fn properties(text: &str) -> Properties {
    Properties::parse(text.as_bytes()).unwrap()
}

#[test]
fn config_directories_follow_kafka_precedence() {
    let cases = [
        ("log.dirs=/b, /a,/b\nlog.dir=/ignored\n", vec!["/a", "/b"]),
        ("log.dir=/single\n", vec!["/single"]),
        ("", vec![DEFAULT_LOG_DIR]),
        (
            "log.dirs=/data\nmetadata.log.dir=/meta\n",
            vec!["/data", "/meta"],
        ),
        ("log.dirs=/data\nmetadata.log.dir=/data\n", vec!["/data"]),
    ];
    for (text, expected) in cases {
        check!(
            config_directories(&properties(text))
                == expected.into_iter().map(PathBuf::from).collect::<Vec<_>>(),
            "{text:?}"
        );
    }
}

fn encode(records: &[MetadataRecord]) -> Vec<u8> {
    records
        .iter()
        .flat_map(|record| {
            let payload = to_kafka_record(record).unwrap().value.unwrap();
            let length = u32::try_from(payload.len()).unwrap().to_le_bytes();
            length.into_iter().chain(payload.to_vec())
        })
        .collect()
}

fn level(name: &str, level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: name.to_owned(),
        level,
    })
}

#[test]
fn bootstrap_records_decode_and_reject_truncation() {
    let records = vec![level("metadata.version", 21), level("group.version", 1)];
    let bytes = encode(&records);
    check!(decode_records(&bytes) == Ok(records));
    check!(decode_records(&[]) == Ok(Vec::new()));
    check!(decode_records(&bytes[..2]) == Err("truncated length prefix".to_owned()));
    check!(decode_records(&bytes[..6]) == Err("truncated record body".to_owned()));
}

#[test]
fn feature_levels_cover_every_registered_feature_but_kraft_version() {
    let levels = feature_levels(&[level("metadata.version", 21), level("group.version", 1)]);
    let expected = feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != KRAFT_VERSION_FEATURE)
        .map(|name| {
            let level = match name {
                "metadata.version" => 21,
                "group.version" => 1,
                _ => 0,
            };
            (name.to_owned(), level)
        })
        .collect::<BTreeMap<_, _>>();
    assert!(levels == expected);
}

fn formatted(dir: &Path, cluster_id: &str, directory_id: &str, records: &[MetadataRecord]) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join(META_PROPERTIES),
        serde_json::to_vec(
            &json!({"cluster_id": cluster_id, "directory_id": directory_id, "version": 2}),
        )
        .unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join(BOOTSTRAP_RECORDS), encode(records)).unwrap();
}

fn features_line(records: &[MetadataRecord]) -> String {
    format!(
        "Found features: {}",
        braces(
            feature_levels(records)
                .iter()
                .map(|(name, level)| format!("{name}={level}"))
        )
    )
}

#[test]
fn info_reports_each_directory_as_kafka_storage_does() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    let c = root.path().join("c");
    let file = root.path().join("file");
    let missing = root.path().join("missing");
    let records = [level("metadata.version", 21)];
    formatted(&a, "cluster-1", "dir-a", &records);
    formatted(&b, "cluster-1", "dir-b", &records);
    formatted(&c, "cluster-2", "dir-c", &records);
    std::fs::write(&file, b"x").unwrap();
    let unformatted = root.path().join("unformatted");
    std::fs::create_dir(&unformatted).unwrap();
    let show = |path: &Path| path.display().to_string();
    let metadata_line = "Found metadata: {cluster.id=cluster-1, directory.id=dir-a, version=2}";

    let cases: Vec<(Vec<PathBuf>, Vec<String>, bool)> = vec![
        (
            vec![a.clone()],
            vec![
                "Found log directory:".into(),
                format!("  {}", show(&a)),
                String::new(),
                metadata_line.into(),
                features_line(&records),
                String::new(),
            ],
            false,
        ),
        (
            vec![a.clone(), b.clone()],
            vec![
                "Found log directories:".into(),
                format!("  {}", show(&a)),
                format!("  {}", show(&b)),
                String::new(),
                metadata_line.into(),
                features_line(&records),
                String::new(),
            ],
            false,
        ),
        (
            vec![unformatted.clone()],
            vec![
                "Found log directory:".into(),
                format!("  {}", show(&unformatted)),
                String::new(),
                "Found problem:".into(),
                format!("  {} is not formatted.", show(&unformatted)),
                String::new(),
            ],
            true,
        ),
        (
            vec![a.clone(), c.clone(), file.clone(), missing.clone()],
            vec![
                "Found log directories:".into(),
                format!("  {}", show(&a)),
                format!("  {}", show(&c)),
                String::new(),
                metadata_line.into(),
                features_line(&records),
                String::new(),
                "Found problems:".into(),
                "  Mismatched cluster IDs between storage directories.".into(),
                format!("  {} is not a directory", show(&file)),
                format!("  {} does not exist", show(&missing)),
                String::new(),
            ],
            true,
        ),
        (Vec::new(), vec!["No directories specified.".into()], false),
    ];
    for (directories, expected, failed) in cases {
        let result = info(&directories);
        check!(
            (result.human, result.failed) == (expected, failed),
            "{directories:?}"
        );
    }
}

#[test]
fn info_json_reports_each_directory_status() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let unformatted = root.path().join("u");
    let broken = root.path().join("broken");
    let records = [level("metadata.version", 21)];
    formatted(&a, "cluster-1", "dir-a", &records);
    std::fs::create_dir(&unformatted).unwrap();
    std::fs::create_dir(&broken).unwrap();
    std::fs::write(broken.join(META_PROPERTIES), b"{").unwrap();
    let broken_meta = broken.join(META_PROPERTIES);
    let parse_error = serde_json::from_slice::<Value>(b"{").unwrap_err();
    let broken_message = format!("Error loading {}: {parse_error}", broken_meta.display());

    let result = info(&[a.clone(), broken.clone(), unformatted.clone()]);
    assert!(
        result.data
            == json!({
                "directories": [
                    {
                        "path": a.display().to_string(),
                        "status": "formatted",
                        "cluster_id": "cluster-1",
                        "directory_id": "dir-a",
                        "version": 2,
                        "features": feature_levels(&records),
                    },
                    {"path": broken.display().to_string(), "status": "unreadable", "error": broken_message},
                    {"path": unformatted.display().to_string(), "status": "unformatted"},
                ],
                "problems": [
                    broken_message,
                    format!("{} is not formatted.", unformatted.display()),
                ],
            })
    );
}
