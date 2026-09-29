//! Conformance of every `krabka` command with the `kafka-*` tool it copies,
//! against a Kafka oracle.
//!
//! Every comparison case needs a Docker daemon and is `#[ignore]`d. Run them
//! with `cargo nextest run -p krabka-cli --test conformance --run-ignored
//! all`, or with `bazel test --config=docker //crates/krabka-cli:all`, which
//! loads the digest-pinned oracle image first. See `support/oracle.rs` for the
//! topology, the pinned image, and what a passing row proves.
//!
//! Each command has a named argument matrix. Each row runs once under the
//! JVM tool in the container and once under `krabka` on the host, and the two
//! runs are compared in four categories:
//!
//! 1. Flag surface: a row that the JVM tool accepts is accepted by `krabka`,
//!    and a row that it refuses is refused.
//! 2. Exit code: on each refused row the exit codes agree.
//! 3. Output: on each accepted row, stdout and stderr agree byte for byte; on
//!    each refused row, the message agrees.
//! 4. Resolved state: where a row changes the cluster or the disk, what it
//!    holds afterwards agrees, as a neutral reader sees it. The reader is the
//!    JVM tool, or for a formatted directory the harness itself, and never
//!    the output of the command under test.
//!
//! Each matrix declares every row where the tools differ, the layers that
//! differ, and why. An undeclared difference fails, and so does a declared
//! one that no longer shows. [`COVERAGE`] names a comparison for every
//! built-in subcommand, and [`every_built_in_command_has_a_matrix`] reads the
//! subcommands from clap's own command tree. A new command without a matrix
//! then fails a test that needs no Docker.

#[path = "conformance/format.rs"]
mod format;
#[path = "conformance/matrices.rs"]
mod matrices;
#[path = "support/oracle.rs"]
mod oracle;
#[path = "conformance/topics.rs"]
mod topics;

use std::collections::BTreeSet;

use assert2::{assert, check};
use clap::CommandFactory;

use self::oracle::{
    Capture, Clean, Declared, Difference, Expect, KAFKA_VERSION, Oracle, Scrub, Tool, Verdict,
    View, judge, remove_column, view, wanted,
};

/// One JVM tool invocation that prepares, restores or reads back a row.
#[derive(Debug, Clone, Copy)]
pub struct Step {
    pub script: &'static str,
    pub args: &'static [&'static str],
    /// Whether `--bootstrap-server <oracle>` comes first.
    pub bootstrap: bool,
    pub stdin: &'static str,
}

/// A [`Step`] that runs `script` against the oracle's bootstrap server.
const fn step(script: &'static str, args: &'static [&'static str]) -> Step {
    Step {
        script,
        args,
        bootstrap: true,
        stdin: "",
    }
}

/// One argument vector of a command's matrix.
///
/// In every argument, file body and step, `{t}` stands for a name of the
/// row's own, which differs between the two tools so that a mutation by one
/// does not change what the other sees; `{f}` for a file of the row's own;
/// `{w}` for the work directory that both tools see; and `{b}` for the
/// oracle's bootstrap address. The comparison reads both tools' names as
/// `{t}`.
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub name: &'static str,
    pub args: &'static [&'static str],
    pub expect: Expect,
    /// Whether both tools get `--bootstrap-server <oracle>` first.
    pub bootstrap: bool,
    pub stdin: &'static str,
    /// Environment for `krabka` only.
    pub env: &'static [(&'static str, &'static str)],
    /// The body of `{f}`, written for each tool before it runs.
    pub file: &'static str,
    /// Run for each tool before the row, and expected to succeed.
    pub before: &'static [Step],
    /// Run for each tool after the row and its read-back, and expected to
    /// succeed: the restore of a cluster-wide change.
    pub after: &'static [Step],
    /// The read-back whose stdout is the row's resolved state.
    pub resolve: Option<Step>,
    /// Rewrites of volatile output.
    pub scrub: &'static [Scrub],
}

/// A row with no preparation, read-back or rewrite.
const fn row(name: &'static str, args: &'static [&'static str], expect: Expect) -> Row {
    Row {
        name,
        args,
        expect,
        bootstrap: true,
        stdin: "",
        env: &[],
        file: "",
        before: &[],
        after: &[],
        resolve: None,
        scrub: &[],
    }
}

/// A command's matrix.
pub struct Matrix {
    /// The JVM tool, a script in `/opt/kafka/bin`.
    pub script: &'static str,
    /// The `krabka` subcommand.
    pub command: &'static str,
    /// The prefix of the rows' own names.
    pub prefix: &'static str,
    pub rows: &'static [Row],
    /// Every row where the two tools differ. A row may be named more than
    /// once, and its declarations merge.
    pub differences: &'static [(&'static str, Difference)],
}

/// How a built-in subcommand is compared with its JVM tool.
pub enum Coverage {
    /// Through a [`Matrix`] and the generic runner.
    Matrix(&'static Matrix),
    /// Through the `topics` matrix, which has phases of its own.
    Topics,
    /// Through the `format` matrix and the divergence table.
    Format,
    /// The command has no JVM counterpart.
    NoCounterpart(&'static str),
}

/// A row's own name for `tool`.
fn own_name(prefix: &str, index: usize, tool: Tool) -> String {
    match tool {
        Tool::Jvm => format!("{prefix}-jvm-{index:02}"),
        Tool::Krabka => format!("{prefix}-krb-{index:02}"),
    }
}

/// What a row's placeholders stand for, for one tool.
struct Place<'a> {
    own: &'a str,
    file: &'a str,
    work: &'a str,
    bootstrap: &'a str,
}

impl Place<'_> {
    fn fill(&self, text: &str) -> String {
        text.replace("{t}", self.own)
            .replace("{f}", self.file)
            .replace("{w}", self.work)
            .replace("{b}", self.bootstrap)
    }

    fn fill_all(&self, args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| self.fill(arg)).collect()
    }
}

fn run_step(oracle: &Oracle, place: &Place<'_>, step: &Step) -> Capture {
    oracle.jvm(
        step.script,
        &place.fill_all(step.args),
        step.bootstrap,
        &place.fill(step.stdin),
    )
}

/// One row's views from both tools.
pub struct Outcome {
    pub name: &'static str,
    pub expect: Expect,
    pub declared: Option<Declared>,
    pub jvm: View,
    pub krabka: View,
}

impl Outcome {
    fn verdict(&self) -> Verdict {
        judge(self.expect, self.declared.as_ref(), &self.jvm, &self.krabka)
    }
}

/// The declarations for `name` in `differences`, merged.
fn declared(differences: &[(&str, Difference)], name: &str) -> Option<Declared> {
    Declared::merge(
        differences
            .iter()
            .filter(|(row, _)| *row == name)
            .map(|(_, difference)| *difference),
    )
}

fn run_row(oracle: &Oracle, matrix: &Matrix, index: usize, row: &Row) -> Outcome {
    let work = oracle.work().display().to_string();
    let bootstrap = oracle.bootstrap();
    let [jvm, krabka] = [Tool::Jvm, Tool::Krabka].map(|tool| {
        let own = own_name(matrix.prefix, index, tool);
        let file = format!("{work}/{own}.in");
        let place = Place {
            own: &own,
            file: &file,
            work: &work,
            bootstrap: &bootstrap,
        };
        for before in row.before {
            let prepared = run_step(oracle, &place, before);
            assert!(
                prepared.exit == Some(0),
                "{}: {before:?}: {prepared:?}",
                row.name
            );
        }
        if !row.file.is_empty() {
            std::fs::write(&file, place.fill(row.file)).expect("write the row's file");
        }
        let args = place.fill_all(row.args);
        let stdin = place.fill(row.stdin);
        let capture = match tool {
            Tool::Jvm => oracle.jvm(matrix.script, &args, row.bootstrap, &stdin),
            Tool::Krabka => oracle.krabka(&[matrix.command], &args, row.bootstrap, row.env, &stdin),
        };
        let resolved = row
            .resolve
            .map(|read| run_step(oracle, &place, &read).stdout)
            .unwrap_or_default();
        for after in row.after {
            let restored = run_step(oracle, &place, after);
            assert!(
                restored.exit == Some(0),
                "{}: {after:?}: {restored:?}",
                row.name
            );
        }
        let rename = [(own.clone(), "{t}"), (work.clone(), "{w}")];
        let clean = Clean {
            rename: &rename,
            scrub: row.scrub,
        };
        view(
            row.expect,
            &capture,
            tool,
            matrix.command,
            &clean,
            &resolved,
        )
    });
    Outcome {
        name: row.name,
        expect: row.expect,
        declared: declared(matrix.differences, row.name),
        jvm,
        krabka,
    }
}

/// The topics, records and group that the generic matrices read.
#[rustfmt::skip]
const FIXTURES: &[Step] = &[
    step("kafka-topics.sh", &["--create", "--topic", "fx-alpha", "--partitions", "2"]),
    step("kafka-topics.sh", &["--create", "--topic", "fx-bravo"]),
    Step {
        stdin: "k1:v1\nk2:v2\n",
        ..step("kafka-console-producer.sh", &["--topic", "fx-alpha", "--reader-property", "parse.key=true", "--reader-property", "key.separator=:"])
    },
    Step {
        stdin: "a\nb\nc\n",
        ..step("kafka-console-producer.sh", &["--topic", "fx-bravo"])
    },
    step("kafka-consumer-groups.sh", &["--group", "fx-group", "--topic", "fx-bravo", "--reset-offsets", "--to-earliest", "--execute"]),
];

fn prepare(oracle: &Oracle) {
    let work = oracle.work().display().to_string();
    let bootstrap = oracle.bootstrap();
    let place = Place {
        own: "",
        file: "",
        work: &work,
        bootstrap: &bootstrap,
    };
    for fixture in FIXTURES {
        let prepared = run_step(oracle, &place, fixture);
        assert!(prepared.exit == Some(0), "{fixture:?}: {prepared:?}");
    }
}

/// Runs `matrix` against a fresh oracle.
fn run_matrix(matrix: &Matrix) -> Vec<Outcome> {
    let oracle = Oracle::start();
    prepare(&oracle);
    matrix
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| run_row(&oracle, matrix, index, row))
        .collect()
}

/// Checks every outcome of `command` against the verdict it passes with, in
/// one comparison of the failing rows, so that a run reports all of them.
pub fn check_outcomes(command: &str, outcomes: &[Outcome]) {
    let failing = |verdict: &dyn Fn(&Outcome) -> Verdict| {
        outcomes
            .iter()
            .filter(|outcome| outcome.verdict() != wanted(outcome.declared.as_ref()))
            .map(|outcome| (outcome.name, verdict(outcome)))
            .collect::<Vec<_>>()
    };
    let got = failing(&Outcome::verdict);
    let want = failing(&|outcome| wanted(outcome.declared.as_ref()));
    check!(got == want, "{command} against Kafka {KAFKA_VERSION}");
}

/// The comparison fails on each deliberately wrong expectation, so it does
/// not pass by construction: an expected exit code that the JVM tool does
/// not return, and an expected stdout shape with a column removed.
pub fn check_not_vacuous(outcomes: &[Outcome]) {
    if let Some(refused) = outcomes
        .iter()
        .find(|outcome| outcome.expect == Expect::Rejected && outcome.verdict() == Verdict::Agree)
    {
        check!(matches!(
            judge(Expect::Accepted, None, &refused.jvm, &refused.krabka),
            Verdict::Mislabeled { expected: 0, .. }
        ));
    }
    if let Some(table) = outcomes.iter().find(|outcome| {
        outcome.expect == Expect::Accepted
            && outcome.verdict() == Verdict::Agree
            && outcome
                .jvm
                .stdout
                .lines()
                .any(|line| line.split_whitespace().count() > 2)
    }) {
        let expected = View {
            stdout: remove_column(&table.jvm.stdout, 1),
            ..table.jvm.clone()
        };
        check!(matches!(
            judge(table.expect, None, &expected, &table.krabka),
            Verdict::Undeclared { .. }
        ));
    }
}

fn run(command: &str, coverage: &Coverage) {
    match coverage {
        Coverage::Matrix(matrix) => {
            let outcomes = run_matrix(matrix);
            check_outcomes(command, &outcomes);
            check_not_vacuous(&outcomes);
        }
        Coverage::Topics => topics::run(),
        Coverage::Format => format::run(),
        Coverage::NoCounterpart(reason) => panic!("{command} has no JVM counterpart: {reason}"),
    }
}

/// Declares [`COVERAGE`], with one `#[ignore]`d test per compared command,
/// so that a coverage entry cannot exist without the test that runs it.
macro_rules! coverage {
    (
        compared { $($test:ident: $command:literal => $coverage:expr,)* }
        uncompared { $($other:literal => $reason:literal,)* }
    ) => {
        /// How every built-in subcommand is compared, by its clap name.
        const COVERAGE: &[(&str, Coverage)] = &[
            $(($command, $coverage),)*
            $(($other, Coverage::NoCounterpart($reason)),)*
        ];

        $(
            #[test]
            #[ignore = "needs a Docker daemon to run the Kafka oracle"]
            fn $test() {
                run($command, &$coverage);
            }
        )*
    };
}

coverage! {
    compared {
        acls_matches_kafka_acls: "acls" => Coverage::Matrix(&matrices::ACLS),
        cluster_matches_kafka_cluster: "cluster" => Coverage::Matrix(&matrices::CLUSTER),
        configs_matches_kafka_configs: "configs" => Coverage::Matrix(&matrices::CONFIGS),
        console_consumer_matches_kafka_console_consumer: "console-consumer" => Coverage::Matrix(&matrices::CONSOLE_CONSUMER),
        console_producer_matches_kafka_console_producer: "console-producer" => Coverage::Matrix(&matrices::CONSOLE_PRODUCER),
        consumer_groups_matches_kafka_consumer_groups: "consumer-groups" => Coverage::Matrix(&matrices::CONSUMER_GROUPS),
        delegation_tokens_matches_kafka_delegation_tokens: "delegation-tokens" => Coverage::Matrix(&matrices::DELEGATION_TOKENS),
        delete_records_matches_kafka_delete_records: "delete-records" => Coverage::Matrix(&matrices::DELETE_RECORDS),
        features_matches_kafka_features: "features" => Coverage::Matrix(&matrices::FEATURES),
        format_matches_kafka_storage_format: "format" => Coverage::Format,
        get_offsets_matches_kafka_get_offsets: "get-offsets" => Coverage::Matrix(&matrices::GET_OFFSETS),
        leader_election_matches_kafka_leader_election: "leader-election" => Coverage::Matrix(&matrices::LEADER_ELECTION),
        log_dirs_matches_kafka_log_dirs: "log-dirs" => Coverage::Matrix(&matrices::LOG_DIRS),
        metadata_quorum_matches_kafka_metadata_quorum: "metadata-quorum" => Coverage::Matrix(&matrices::METADATA_QUORUM),
        reassign_partitions_matches_kafka_reassign_partitions: "reassign-partitions" => Coverage::Matrix(&matrices::REASSIGN_PARTITIONS),
        storage_matches_kafka_storage: "storage" => Coverage::Matrix(&matrices::STORAGE),
        topics_matches_kafka_topics: "topics" => Coverage::Topics,
        transactions_matches_kafka_transactions: "transactions" => Coverage::Matrix(&matrices::TRANSACTIONS),
    }
    uncompared {
        "gres" => "krabka's own tenant registry and range layout; Kafka ships no such tool",
    }
}

/// Every built-in subcommand, as clap's command tree names it, has a
/// [`COVERAGE`] entry, and every entry names a built-in subcommand.
#[test]
fn every_built_in_command_has_a_matrix() {
    let built_in = krabka_cli::Cli::command()
        .get_subcommands()
        .map(|command| command.get_name().to_owned())
        .filter(|name| name != "help")
        .collect::<BTreeSet<_>>();
    let covered = COVERAGE
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    assert!(built_in == covered);
}

/// Every row name is unique within its matrix, every declared difference
/// names a row of its matrix, and every matrix drives the `krabka`
/// subcommand it is registered under.
#[test]
fn every_declaration_names_a_row_of_its_matrix() {
    for (command, coverage) in COVERAGE {
        let (rows, differences) = match coverage {
            Coverage::Matrix(matrix) => {
                check!(matrix.command == *command);
                (
                    matrix.rows.iter().map(|row| row.name).collect::<Vec<_>>(),
                    matrix
                        .differences
                        .iter()
                        .map(|(name, _)| *name)
                        .collect::<Vec<_>>(),
                )
            }
            Coverage::Topics => (topics::row_names(), topics::declared_names()),
            Coverage::Format => (format::row_names(), format::declared_names()),
            Coverage::NoCounterpart(_) => continue,
        };
        let unique = rows.iter().collect::<BTreeSet<_>>();
        check!(unique.len() == rows.len(), "{command} repeats a row name");
        for name in differences {
            check!(
                rows.contains(&name),
                "{command} declares {name}, which is not a row"
            );
        }
    }
}
