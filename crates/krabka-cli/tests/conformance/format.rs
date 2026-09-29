//! The `format` matrix: `krabka format`, and `krabka storage format`, which
//! runs the same formatter, against `kafka-storage format`.
//!
//! The two command lines differ by design, so each row carries the argument
//! vector of each tool. `kafka-storage format` reads its directories from a
//! `server.properties` file that the harness writes for it, and `krabka
//! format` takes them as `--log-dir`.
//!
//! The expected-difference list is [`DIVERGENCES`], which mirrors the
//! divergence table in krabka-broker's `docs/format-divergences.md` row for
//! row. Each matrix row names the table rows it exercises, and may differ from
//! Kafka only in the layers that those table rows allow. A difference that
//! the table does not list fails the run, unless [`DEFECTS`] records it as a
//! `krabka-format` defect, which then fails as stale once it is fixed.
//!
//! The resolved state of a row is what the directories hold afterwards: the
//! cluster id and every finalized feature level above zero, per directory.
//! The harness reads it from the files itself. For `kafka-storage format` it
//! reads `meta.properties`, and the bootstrap checkpoint through
//! `kafka-dump-log`; for `krabka format` it reads `meta.properties.json` and
//! decodes `bootstrap.records.bin`. Neither side is read from the output of
//! the command under test.

use std::{collections::BTreeMap, fmt::Write as _, path::Path};

use assert2::{assert, check};
use krabka_metadata::{MetadataRecord, from_kafka_record};
use krabka_protocol::records::Record;

use super::{
    Outcome, check_outcomes,
    oracle::{
        Clean, Declared, Difference, Expect, Layer, OUTCOME, OUTCOME_AND_STATE, Oracle, Scrub,
        Tool, Verdict, View, judge, view,
    },
};

/// The divergence table of `docs/format-divergences.md` in krabka-broker, at
/// the revision this workspace pins, one entry per table row: its `Topic`
/// cell, the layers in which the row allows `krabka format` to differ, and
/// the table's reason. A row that says **Matches** allows no layer.
#[rustfmt::skip]
pub const DIVERGENCES: &[(&str, Difference)] = &[
    ("Cluster id form", Difference::intended(OUTCOME_AND_STATE, "Kafka stores any string as cluster.id; krabka accepts only a string that decodes to 16 bytes, and normalises the hyphenated form to the base64 form")),
    ("Reserved cluster ids", Difference::intended(&[], "Matches")),
    ("`--cluster-id` required", Difference::intended(OUTCOME_AND_STATE, "a single-node format is one command; without --cluster-id krabka keeps the id of a formatted directory or generates one")),
    ("Directory ids", Difference::intended(&[], "Matches")),
    ("`--directory-id`", Difference::intended(OUTCOME_AND_STATE, "sets the metadata log directory's id, which an orchestrator has to know before the format runs; kafka-storage has no such flag")),
    ("`--config`", Difference::intended(OUTCOME, "krabka's broker does not read server.properties, so the formatter takes no --config")),
    ("`unstable.feature.versions.enable`", Difference::intended(OUTCOME, "follows from --config: krabka takes it as --unstable-feature-versions-enable")),
    ("Log directories", Difference::intended(OUTCOME, "follows from --config: krabka takes the directories as --log-dir")),
    ("Directory with foreign files and no `meta.properties`", Difference::intended(OUTCOME_AND_STATE, "krabka refuses with exit 3, so that a mistyped path does not seed a directory full of another program's data")),
    ("Already formatted, no `--ignore-formatted`", Difference::intended(&[Layer::Exit], "Matches, except for the exit code")),
    ("`--ignore-formatted` over a mixed set", Difference::intended(&[Layer::Stdout], "Matches, and krabka prints a line on stdout for each skipped directory")),
    ("Cluster id disagrees with a formatted directory", Difference::intended(&[Layer::Exit], "Matches the message; Kafka prints a stack trace because it does not catch the exception")),
    ("`meta.properties` file", Difference::intended(&[Layer::Stderr], "krabka writes JSON meta.properties.json, so a message that names the file names that one")),
    ("Bootstrap metadata", Difference::intended(&[], "different files for the same records; the resolved state reads the records from each")),
    ("Dynamic quorum snapshot", Difference::intended(&[], "krabka's metadata log directory layout; the snapshot holds the same records")),
    ("Write order", Difference::intended(&[], "Matches the marker-last order")),
    ("Progress output", Difference::intended(&[Layer::Stdout], "the directory lines match; Kafka's Bootstrap metadata line prints Java toString output, and krabka adds a summary line")),
    ("Exit codes", Difference::intended(&[Layer::Exit], "krabka exits 2, 3, 4 or 5 by cause where Kafka exits 1, so that an orchestrator can act on the cause")),
];

/// Differences from `kafka-storage format` that the divergence table does not
/// list: defects of `krabka-format`, recorded so that the run passes today and
/// fails the day each is fixed.
#[rustfmt::skip]
pub const DEFECTS: &[(&str, Difference)] = &[
    ("unknown-feature", Difference::defect(&[Layer::Stderr], "krabka lists metadata.version among the supported features; Kafka's list omits it, since --feature metadata.version is refused")),
    ("feature-level-out-of-range", Difference::defect(&[Layer::Stderr], "krabka words it `feature transaction.version=9 is outside the supported range 0..=2`; Kafka says `No feature:transaction.version with feature level 9`")),
];

/// What the directories of a row hold before it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Before {
    /// Nothing.
    Empty,
    /// Formatted by the same tool, with cluster id [`ID`].
    Formatted,
    /// The metadata directory holds one file that no formatter wrote.
    Foreign,
}

/// One row of the format matrix.
#[derive(Debug, Clone, Copy)]
pub struct FormatRow {
    pub name: &'static str,
    /// The arguments of `kafka-storage format` after `--config <file>`.
    pub jvm: &'static [&'static str],
    /// The arguments of `krabka format` after `--log-dir <dirs>`.
    pub krabka: &'static [&'static str],
    pub expect: Expect,
    /// How many log directories the node has.
    pub dirs: usize,
    pub before: Before,
    /// The rows of the divergence table that this row exercises.
    pub topics: &'static [&'static str],
    pub scrub: &'static [Scrub],
}

const fn format_row(
    name: &'static str,
    jvm: &'static [&'static str],
    krabka: &'static [&'static str],
    expect: Expect,
    topics: &'static [&'static str],
) -> FormatRow {
    FormatRow {
        name,
        jvm,
        krabka,
        expect,
        dirs: 1,
        before: Before::Empty,
        topics,
        scrub: &[],
    }
}

/// The cluster id of most rows.
const ID: &str = "AQIDBAUGBwgJCgsMDQ4PEA";

const PROGRESS: &[&str] = &["Progress output", "Bootstrap metadata", "Directory ids"];
const REFUSED: &[&str] = &["Exit codes"];

#[rustfmt::skip]
pub const MATRIX: &[FormatRow] = &[
    // Resolved state: the same release gives the same feature levels.
    format_row("default-release", &["-t", ID], &["--cluster-id", ID], Expect::Accepted, PROGRESS),
    format_row("release-3.3", &["-t", ID, "--release-version", "3.3"], &["--cluster-id", ID, "--release-version", "3.3"], Expect::Accepted, PROGRESS),
    format_row("release-3.9", &["-t", ID, "--release-version", "3.9"], &["--cluster-id", ID, "--release-version", "3.9"], Expect::Accepted, PROGRESS),
    format_row("release-4.0", &["-t", ID, "--release-version", "4.0"], &["--cluster-id", ID, "--release-version", "4.0"], Expect::Accepted, PROGRESS),
    format_row("release-4.2-IV1", &["-t", ID, "--release-version", "4.2-IV1"], &["--cluster-id", ID, "--release-version", "4.2-IV1"], Expect::Accepted, PROGRESS),
    format_row("release-short-flag", &["-t", ID, "-r", "4.1"], &["--cluster-id", ID, "--release-version", "4.1"], Expect::Accepted, PROGRESS),
    format_row("feature-override", &["-t", ID, "--feature", "transaction.version=0"], &["--cluster-id", ID, "--feature", "transaction.version=0"], Expect::Accepted, PROGRESS),
    format_row("release-and-feature", &["-t", ID, "--release-version", "4.0", "--feature", "group.version=0"], &["--cluster-id", ID, "--release-version", "4.0", "--feature", "group.version=0"], Expect::Accepted, PROGRESS),
    format_row("old-release-new-feature", &["-t", ID, "--release-version", "3.7", "--feature", "transaction.version=2"], &["--cluster-id", ID, "--release-version", "3.7", "--feature", "transaction.version=2"], Expect::Accepted, PROGRESS),
    FormatRow { dirs: 2, ..format_row("two-directories", &["-t", ID], &["--cluster-id", ID], Expect::Accepted, &["Progress output", "Bootstrap metadata", "Directory ids", "Log directories"]) },
    // Refused feature and release arguments.
    format_row("unknown-release", &["-t", ID, "--release-version", "9.9"], &["--cluster-id", ID, "--release-version", "9.9"], Expect::Rejected, REFUSED),
    format_row("unstable-release", &["-t", ID, "--release-version", "4.4"], &["--cluster-id", ID, "--release-version", "4.4"], Expect::Rejected, &["Exit codes", "`unstable.feature.versions.enable`"]),
    format_row("unstable-metadata-version", &["-t", ID, "--feature", "metadata.version=31"], &["--cluster-id", ID, "--feature", "metadata.version=31"], Expect::Rejected, &["Exit codes", "`unstable.feature.versions.enable`"]),
    format_row("release-and-metadata-version", &["-t", ID, "--release-version", "4.0", "--feature", "metadata.version=25"], &["--cluster-id", ID, "--release-version", "4.0", "--feature", "metadata.version=25"], Expect::Rejected, REFUSED),
    format_row("unknown-feature", &["-t", ID, "--feature", "bogus.version=1"], &["--cluster-id", ID, "--feature", "bogus.version=1"], Expect::Rejected, REFUSED),
    format_row("feature-level-out-of-range", &["-t", ID, "--feature", "transaction.version=9"], &["--cluster-id", ID, "--feature", "transaction.version=9"], Expect::Rejected, REFUSED),
    // Cluster ids.
    format_row("reserved-cluster-id", &["-t", "AAAAAAAAAAAAAAAAAAAAAA"], &["--cluster-id", "AAAAAAAAAAAAAAAAAAAAAA"], Expect::Accepted, &["Reserved cluster ids", "Progress output"]),
    format_row("hyphenated-cluster-id", &["-t", "01020304-0506-0708-090a-0b0c0d0e0f10"], &["--cluster-id", "01020304-0506-0708-090a-0b0c0d0e0f10"], Expect::Accepted, &["Cluster id form", "Progress output"]),
    format_row("free-form-cluster-id", &["-t", "foo"], &["--cluster-id", "foo"], Expect::Accepted, &["Cluster id form", "Progress output"]),
    FormatRow { scrub: &[Scrub::Uuid], ..format_row("no-cluster-id", &[], &[], Expect::Rejected, &["`--cluster-id` required"]) },
    format_row("directory-id", &["-t", ID, "--directory-id", "BQIDBAUGBwgJCgsMDQ4PEA"], &["--cluster-id", ID, "--directory-id", "BQIDBAUGBwgJCgsMDQ4PEA"], Expect::Rejected, &["`--directory-id`"]),
    // Directories that already hold something.
    FormatRow { before: Before::Formatted, ..format_row("already-formatted", &["-t", ID], &["--cluster-id", ID], Expect::Rejected, &["Already formatted, no `--ignore-formatted`", "Exit codes"]) },
    FormatRow { before: Before::Formatted, ..format_row("ignore-formatted", &["-t", ID, "--ignore-formatted"], &["--cluster-id", ID, "--ignore-formatted"], Expect::Accepted, &["`--ignore-formatted` over a mixed set"]) },
    FormatRow { before: Before::Formatted, ..format_row("cluster-id-disagrees", &["-t", "BQIDBAUGBwgJCgsMDQ4PEA", "--ignore-formatted"], &["--cluster-id", "BQIDBAUGBwgJCgsMDQ4PEA", "--ignore-formatted"], Expect::Rejected, &["Cluster id disagrees with a formatted directory", "`meta.properties` file", "Exit codes"]) },
    FormatRow { before: Before::Foreign, ..format_row("foreign-files", &["-t", ID], &["--cluster-id", ID], Expect::Accepted, &["Directory with foreign files and no `meta.properties`"]) },
];

/// The names of the matrix rows.
pub fn row_names() -> Vec<&'static str> {
    MATRIX.iter().map(|row| row.name).collect()
}

/// The names of the rows with a declared defect.
pub fn declared_names() -> Vec<&'static str> {
    DEFECTS.iter().map(|(name, _)| *name).collect()
}

/// Everything declared for `row`: the divergences of the table rows it
/// exercises that allow a difference, and its defects.
pub fn declared(row: &FormatRow) -> Option<Declared> {
    let divergences = row.topics.iter().flat_map(|topic| {
        DIVERGENCES
            .iter()
            .filter(move |(name, _)| name == topic)
            .map(|(_, difference)| *difference)
    });
    let defects = DEFECTS
        .iter()
        .filter(|(name, _)| *name == row.name)
        .map(|(_, difference)| *difference);
    Declared::merge(
        divergences
            .chain(defects)
            .filter(|difference| !difference.layers.is_empty()),
    )
}

/// The finalized feature levels above zero, one `name=level` line each, in
/// name order: the form in which both sides' bootstrap records are compared.
/// Kafka writes no record for a feature at level 0.
fn levels(levels: impl IntoIterator<Item = (String, i16)>) -> String {
    levels
        .into_iter()
        .filter(|(_, level)| *level != 0)
        .collect::<BTreeMap<_, _>>()
        .iter()
        .fold(String::new(), |mut text, (name, level)| {
            let _ = writeln!(text, "{name}={level}");
            text
        })
}

/// The feature levels of the `FEATURE_LEVEL_RECORD` payloads that
/// `kafka-dump-log --cluster-metadata-decoder` prints.
fn dumped_levels(dump: &str) -> Vec<(String, i16)> {
    dump.lines()
        .filter_map(|line| {
            let data = line
                .split_once("\"type\":\"FEATURE_LEVEL_RECORD\"")?
                .1
                .split_once("\"data\":")?
                .1;
            let value: serde_json::Value =
                serde_json::from_str(data.strip_suffix('}').unwrap_or(data)).ok()?;
            let name = value.get("name")?.as_str()?.to_owned();
            let level = i16::try_from(value.get("featureLevel")?.as_i64()?).ok()?;
            Some((name, level))
        })
        .collect()
}

/// The feature levels of `bootstrap.records.bin`: `u32` little-endian
/// length-prefixed record values.
fn record_levels(mut bytes: &[u8]) -> Vec<(String, i16)> {
    let mut found = Vec::new();
    while let Some((prefix, rest)) = bytes.split_first_chunk::<4>() {
        let length = usize::try_from(u32::from_le_bytes(*prefix)).expect("a record length");
        let (payload, rest) = rest.split_at(length);
        let record = Record {
            value: Some(payload.to_vec().into()),
            ..Record::default()
        };
        if let Ok(MetadataRecord::V1FeatureLevel(feature)) = from_kafka_record(&record) {
            found.push((feature.name, feature.level));
        }
        bytes = rest;
    }
    found
}

/// What one directory holds, as `kafka-storage format` left it.
fn read_jvm_dir(oracle: &Oracle, dir: &Path) -> String {
    let Ok(meta) = std::fs::read_to_string(dir.join("meta.properties")) else {
        return "unformatted\n".to_owned();
    };
    let cluster = meta
        .lines()
        .find_map(|line| line.strip_prefix("cluster.id="))
        .unwrap_or_default();
    let checkpoint = dir.join("bootstrap.checkpoint").display().to_string();
    let dump = oracle.jvm(
        "kafka-dump-log.sh",
        &[
            "--cluster-metadata-decoder".to_owned(),
            "--files".to_owned(),
            checkpoint,
        ],
        false,
        "",
    );
    assert!(dump.exit == Some(0), "{dump:?}");
    format!(
        "cluster.id={cluster}\n{}",
        levels(dumped_levels(&dump.stdout))
    )
}

/// What one directory holds, as `krabka format` left it.
fn read_krabka_dir(dir: &Path) -> String {
    let Ok(meta) = std::fs::read(dir.join("meta.properties.json")) else {
        return "unformatted\n".to_owned();
    };
    let meta: serde_json::Value = serde_json::from_slice(&meta).expect("meta.properties.json");
    let cluster = meta
        .get("cluster_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let records = std::fs::read(dir.join("bootstrap.records.bin")).unwrap_or_default();
    format!("cluster.id={cluster}\n{}", levels(record_levels(&records)))
}

/// The directories of one tool's run of a row.
struct Node {
    root: std::path::PathBuf,
    dirs: Vec<std::path::PathBuf>,
}

impl Node {
    fn new(oracle: &Oracle, own: &str, dirs: usize) -> Self {
        let root = oracle.work().join(own);
        std::fs::create_dir_all(&root).expect("create the row's directory");
        let dirs = (0..dirs)
            .map(|index| root.join(format!("d{index}")))
            .collect();
        Self { root, dirs }
    }

    fn joined(&self) -> String {
        self.dirs
            .iter()
            .map(|dir| dir.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Writes the `server.properties` that `kafka-storage format` reads.
    fn properties(&self) -> String {
        let path = self.root.join("server.properties");
        let body = format!(
            "process.roles=broker,controller\nnode.id=1\ncontroller.quorum.voters=1@localhost:9093\n\
             controller.listener.names=CONTROLLER\nlisteners=PLAINTEXT://:9092,CONTROLLER://:9093\n\
             log.dirs={}\n",
            self.joined()
        );
        std::fs::write(&path, body).expect("write server.properties");
        path.display().to_string()
    }
}

fn to_strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}

fn run_row(
    oracle: &Oracle,
    subcommand: &[&str],
    prefix: &str,
    index: usize,
    row: &FormatRow,
) -> Outcome {
    let [jvm, krabka] = [Tool::Jvm, Tool::Krabka].map(|tool| {
        let own = match tool {
            Tool::Jvm => format!("{prefix}-jvm-{index:02}"),
            Tool::Krabka => format!("{prefix}-krb-{index:02}"),
        };
        let node = Node::new(oracle, &own, row.dirs);
        let run = |args: &[&str]| match tool {
            Tool::Jvm => {
                let mut argv = vec![
                    "format".to_owned(),
                    "--config".to_owned(),
                    node.properties(),
                ];
                argv.extend(to_strings(args));
                oracle.jvm("kafka-storage.sh", &argv, false, "")
            }
            Tool::Krabka => {
                let mut argv = vec!["--log-dir".to_owned(), node.joined()];
                argv.extend(to_strings(args));
                oracle.krabka(subcommand, &argv, false, &[], "")
            }
        };
        let args = match tool {
            Tool::Jvm => row.jvm,
            Tool::Krabka => row.krabka,
        };
        match row.before {
            Before::Empty => {}
            Before::Formatted => {
                let first = match tool {
                    Tool::Jvm => run(&["-t", ID]),
                    Tool::Krabka => run(&["--cluster-id", ID]),
                };
                assert!(first.exit == Some(0), "{}: {first:?}", row.name);
            }
            Before::Foreign => {
                std::fs::create_dir_all(&node.dirs[0]).expect("create the metadata directory");
                std::fs::write(node.dirs[0].join("stray.txt"), "not a log")
                    .expect("write a stray file");
            }
        }
        let capture = run(args);
        let resolved = node
            .dirs
            .iter()
            .enumerate()
            .fold(String::new(), |mut text, (at, dir)| {
                let state = match tool {
                    Tool::Jvm => read_jvm_dir(oracle, dir),
                    Tool::Krabka => read_krabka_dir(dir),
                };
                let _ = write!(text, "[d{at}]\n{state}");
                text
            });
        let rename = [(node.root.display().to_string(), "{node}")];
        let clean = Clean {
            rename: &rename,
            scrub: row.scrub,
        };
        // Both subcommands run the same formatter, which prefixes its messages
        // with `krabka format: `.
        view(row.expect, &capture, tool, "format", &clean, &resolved)
    });
    Outcome {
        name: row.name,
        expect: row.expect,
        declared: declared(row),
        jvm,
        krabka,
    }
}

/// Runs the matrix once under `krabka format` and once under `krabka storage
/// format` against a fresh oracle, and checks every row.
pub fn run() {
    let oracle = Oracle::start();
    for (subcommand, prefix) in [
        (&["format"][..], "fmt"),
        (&["storage", "format"][..], "sfmt"),
    ] {
        let outcomes = MATRIX
            .iter()
            .enumerate()
            .map(|(index, row)| run_row(&oracle, subcommand, prefix, index, row))
            .collect::<Vec<_>>();
        let command = subcommand.join(" ");
        check_outcomes(&command, &outcomes);
        super::check_not_vacuous(&outcomes);
        check_resolved_not_vacuous(&outcomes);
    }
}

/// A changed feature level in the resolved state fails the comparison, even
/// where the table allows the printed output to differ, and the release rows
/// resolve to real levels rather than to two empty readings.
fn check_resolved_not_vacuous(outcomes: &[Outcome]) {
    let release = outcomes
        .iter()
        .find(|outcome| outcome.name == "release-4.0")
        .expect("the matrix has the release-4.0 row");
    check!(
        release.jvm.resolved
            == "[d0]\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\ngroup.version=1\nmetadata.version=25\ntransaction.version=2\n"
    );
    let changed = View {
        resolved: release
            .krabka
            .resolved
            .replace("transaction.version=2", "transaction.version=1"),
        ..release.krabka.clone()
    };
    check!(changed != release.krabka);
    check!(matches!(
        judge(release.expect, release.declared.as_ref(), &release.jvm, &changed),
        Verdict::Undeclared { layers, .. } if layers == [Layer::Resolved]
    ));
}

/// Every row cites table rows that [`DIVERGENCES`] holds, and every table row
/// is cited by some row, so the matrix exercises the whole table.
#[test]
fn every_row_cites_the_divergence_table() {
    for row in MATRIX {
        for topic in row.topics {
            check!(
                DIVERGENCES.iter().any(|(name, _)| name == topic),
                "{} cites {topic}, which the table does not hold",
                row.name
            );
        }
    }
    let exercised = [
        "`--config`",
        "Write order",
        "Dynamic quorum snapshot",
        "`meta.properties` file",
    ];
    for (topic, _) in DIVERGENCES {
        check!(
            MATRIX.iter().any(|row| row.topics.contains(topic)) || exercised.contains(topic),
            "no row exercises {topic}"
        );
    }
}

/// A row that cites only table rows that say **Matches** has no declaration,
/// so any difference on it fails.
#[test]
fn a_matching_table_row_declares_nothing() {
    let row = format_row(
        "r",
        &[],
        &[],
        Expect::Accepted,
        &["Reserved cluster ids", "Directory ids"],
    );
    assert!(declared(&row) == None);
    let row = format_row(
        "r",
        &[],
        &[],
        Expect::Rejected,
        &["Reserved cluster ids", "Exit codes"],
    );
    assert!(declared(&row).map(|declared| declared.layers) == Some(vec![Layer::Exit]));
}

#[test]
fn both_readers_resolve_the_same_levels() {
    let dump = r#"| offset: 1 CreateTime: 1 keySize: -1 valueSize: 23 sequence: -1 headerKeys: [] payload: {"type":"FEATURE_LEVEL_RECORD","version":0,"data":{"name":"metadata.version","featureLevel":25}}
| offset: 2 CreateTime: 1 keySize: -1 valueSize: 26 sequence: -1 headerKeys: [] payload: {"type":"FEATURE_LEVEL_RECORD","version":0,"data":{"name":"transaction.version","featureLevel":2}}
| offset: 4 CreateTime: 1 keySize: 4 valueSize: 3 sequence: -1 headerKeys: [] SnapshotFooter {"version":0}"#;
    let records = krabka_metadata::feature_registry()
        .iter()
        .filter_map(|feature| {
            let level = match feature.name() {
                "metadata.version" => 25,
                "transaction.version" => 2,
                "kraft.version" => return None,
                _ => 0,
            };
            Some(MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: feature.name().to_owned(),
                    level,
                },
            ))
        })
        .map(|record| {
            let value = krabka_metadata::to_kafka_record(&record)
                .expect("encode")
                .value
                .expect("a value");
            let mut framed = u32::try_from(value.len())
                .expect("a length")
                .to_le_bytes()
                .to_vec();
            framed.extend_from_slice(&value);
            framed
        })
        .collect::<Vec<_>>()
        .concat();
    let want = "metadata.version=25\ntransaction.version=2\n";
    assert!(levels(dumped_levels(dump)) == want);
    assert!(levels(record_levels(&records)) == want);
}
