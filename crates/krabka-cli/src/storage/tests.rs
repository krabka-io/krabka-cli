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

// Ids in Kafka's form, as `krabka format` writes them.
const CLUSTER_1: &str = "XzoePA17S1OaTjotmm97EA";
const CLUSTER_2: &str = "7xlAzeKzTD6RO1SSi9ocBQ";
const DIR_A: &str = "xK42y6AyRZqoNte2CoM0Jw";
const DIR_B: &str = "A5GA8TV7RYGs6-glHuyBWw";
const DIR_C: &str = "AQIDBAUGBwgJCgsMDQ4PEA";

fn write_meta(dir: &Path, meta: &str) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(META_PROPERTIES), meta).unwrap();
}

fn formatted(dir: &Path, cluster_id: &str, directory_id: &str, records: &[MetadataRecord]) {
    write_meta(
        dir,
        &format!(
            "#\n#Thu Feb 29 12:34:56 UTC 2024\ncluster.id={cluster_id}\n\
             directory.id={directory_id}\nnode.id=1\nversion=1\n"
        ),
    );
    std::fs::write(dir.join(BOOTSTRAP_RECORDS), encode(records)).unwrap();
}

/// A data directory: `krabka format` writes the bootstrap records only into
/// the metadata log directory.
fn data_dir(dir: &Path, cluster_id: &str, directory_id: &str) {
    formatted(dir, cluster_id, directory_id, &[]);
    std::fs::remove_file(dir.join(BOOTSTRAP_RECORDS)).unwrap();
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
    formatted(&a, CLUSTER_1, DIR_A, &records);
    formatted(&b, CLUSTER_1, DIR_B, &records);
    formatted(&c, CLUSTER_2, DIR_C, &records);
    let data = root.path().join("data");
    data_dir(&data, CLUSTER_1, DIR_B);
    std::fs::write(&file, b"x").unwrap();
    let unformatted = root.path().join("unformatted");
    std::fs::create_dir(&unformatted).unwrap();
    let other_node = root.path().join("other-node");
    write_meta(
        &other_node,
        &format!(
            "#\n#Thu Feb 29 12:34:56 UTC 2024\ncluster.id={CLUSTER_1}\n\
             directory.id={DIR_C}\nnode.id=2\nversion=1\n"
        ),
    );
    let show = |path: &Path| path.display().to_string();
    let metadata_line = format!(
        "Found metadata: {{cluster.id={CLUSTER_1}, directory.id={DIR_A}, node.id=1, version=1}}"
    );

    let cases: Vec<(Vec<PathBuf>, Vec<String>, bool)> = vec![
        (
            vec![a.clone()],
            vec![
                "Found log directory:".into(),
                format!("  {}", show(&a)),
                String::new(),
                metadata_line.clone(),
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
                metadata_line.clone(),
                features_line(&records),
                String::new(),
            ],
            false,
        ),
        (
            vec![data.clone()],
            vec![
                "Found log directory:".into(),
                format!("  {}", show(&data)),
                String::new(),
                format!(
                    "Found metadata: {{cluster.id={CLUSTER_1}, directory.id={DIR_B}, node.id=1, \
                     version=1}}"
                ),
                String::new(),
            ],
            false,
        ),
        (
            vec![data.clone(), a.clone()],
            vec![
                "Found log directories:".into(),
                format!("  {}", show(&data)),
                format!("  {}", show(&a)),
                String::new(),
                format!(
                    "Found metadata: {{cluster.id={CLUSTER_1}, directory.id={DIR_B}, node.id=1, \
                     version=1}}"
                ),
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
                metadata_line.clone(),
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
        (
            vec![a.clone(), other_node.clone()],
            vec![
                "Found log directories:".into(),
                format!("  {}", show(&a)),
                format!("  {}", show(&other_node)),
                String::new(),
                metadata_line.clone(),
                features_line(&records),
                String::new(),
                "Found problem:".into(),
                "  Mismatched node IDs between storage directories.".into(),
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
    formatted(&a, CLUSTER_1, DIR_A, &records);
    std::fs::create_dir(&unformatted).unwrap();
    std::fs::create_dir(&broken).unwrap();
    std::fs::write(broken.join(META_PROPERTIES), b"version=x\n").unwrap();
    let broken_meta = broken.join(META_PROPERTIES);
    let broken_message = format!(
        "Error loading {}: Invalid meta.properties version string 'x'",
        broken_meta.display()
    );

    let result = info(&[a.clone(), broken.clone(), unformatted.clone()]);
    assert!(
        result.data
            == json!({
                "directories": [
                    {
                        "path": a.display().to_string(),
                        "status": "formatted",
                        "cluster_id": CLUSTER_1,
                        "node_id": 1,
                        "directory_id": DIR_A,
                        "version": META_PROPERTIES_VERSION,
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

#[test]
fn a_meta_properties_file_that_krabka_refuses_is_a_problem() {
    let root = tempfile::tempdir().unwrap();
    let cases = [
        (
            format!("cluster.id={CLUSTER_1}\nnode.id=1\nversion=2\n"),
            "Unknown meta.properties version number 2".to_owned(),
        ),
        (
            format!("cluster.id={CLUSTER_1}\nnode.id=1\n"),
            "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node \
             writes"
                .to_owned(),
        ),
        (
            "node.id=1\nversion=1\n".to_owned(),
            "cluster.id was not found.".to_owned(),
        ),
        (
            "cluster.id=5f3a1e3c-0d7b-4b53-9a4e-3a2d9a6f7b10\nnode.id=1\nversion=1\n".to_owned(),
            format!(
                "Unable to read cluster.id as a Uuid: {}",
                "5f3a1e3c-0d7b-4b53-9a4e-3a2d9a6f7b10"
                    .parse::<ClusterId>()
                    .unwrap_err()
            ),
        ),
    ];
    for (index, (meta, reason)) in cases.into_iter().enumerate() {
        let dir = root.path().join(index.to_string());
        write_meta(&dir, &meta);
        let message = format!(
            "Error loading {}: {reason}",
            dir.join(META_PROPERTIES).display()
        );
        check!(inspect(&dir) == LogDir::Unreadable(message), "{meta:?}");
    }
}

#[test]
fn random_uuid_prints_one_kafka_uuid() {
    let id = KafkaUuid(uuid::Uuid::from_u128(
        0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
    ));
    let result = random_uuid(id);
    check!(
        (result.human, result.data, result.failed)
            == (
                vec!["AQIDBAUGBwgJCgsMDQ4PEA".to_owned()],
                json!({"uuid": "AQIDBAUGBwgJCgsMDQ4PEA"}),
                false,
            )
    );
}
