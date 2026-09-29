//! The `topics` matrix: `krabka topics` against `kafka-topics`.
//!
//! It runs in two phases around a cluster-wide change, and it creates its
//! own fixtures, so it has a runner of its own rather than the generic one.
//! The comparison itself is the generic one.

use assert2::{assert, check};

use super::{
    Outcome, check_outcomes,
    oracle::{Clean, Declared, Difference, Expect, Layer, OUTCOME, Oracle, Tool, Verdict, view},
};

/// When a row runs. Phase B runs after the oracle's cluster-level
/// `min.insync.replicas` is removed, which Kafka allows once ELR is off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    A,
    B,
}

/// One argument vector.
///
/// `{t}` in an argument stands for a topic name of the row's own, which
/// differs between the two tools so that a mutation by one does not change
/// what the other sees. The comparison reads both names as `{t}`.
struct Row {
    name: &'static str,
    args: &'static [&'static str],
    expect: Expect,
    phase: Phase,
    /// Whether both tools get `--bootstrap-server <oracle>`.
    bootstrap: bool,
    /// `kafka-topics` argument vectors run for each tool's `{t}` before the
    /// row, and expected to succeed.
    before: &'static [&'static [&'static str]],
    /// Environment for `krabka` only.
    env: &'static [(&'static str, &'static str)],
}

const fn row(name: &'static str, args: &'static [&'static str], expect: Expect) -> Row {
    Row {
        name,
        args,
        expect,
        phase: Phase::A,
        bootstrap: true,
        before: &[],
        env: &[],
    }
}

const fn after(
    before: &'static [&'static [&'static str]],
    name: &'static str,
    args: &'static [&'static str],
    expect: Expect,
) -> Row {
    Row {
        before,
        ..row(name, args, expect)
    }
}

const CREATE_T: &[&[&str]] = &[&["--create", "--topic", "{t}"]];

/// The topics that every read-only row reads, with their `--create`
/// arguments.
const FIXTURES: &[&[&str]] = &[
    &["--topic", "fx-alpha", "--config", "retention.ms=1000"],
    &["--topic", "fx-bravo", "--partitions", "2"],
    &[
        "--topic",
        "fx-charlie",
        "--partitions",
        "3",
        "--config",
        "retention.ms=5",
        "--config",
        "cleanup.policy=compact",
    ],
    &["--topic", "fx-delta"],
    &["--topic", "fx-echo", "--config", "max.message.bytes=2048"],
    &["--topic", "fx-foxtrot", "--config", "segment.ms=600000"],
];

#[rustfmt::skip]
const MATRIX: &[Row] = &[
    // Read-only rows first, while the topic set is the fixtures alone.
    row("list", &["--list"], Expect::Accepted),
    row("list-exclude-internal", &["--list", "--exclude-internal"], Expect::Accepted),
    row("list-regex", &["--list", "--topic", "fx-.*"], Expect::Accepted),
    row("list-equals-form", &["--list", "--topic=fx-alpha"], Expect::Accepted),
    row("list-comma-list", &["--list", "--topic", "fx-alpha,fx-bravo"], Expect::Accepted),
    row("list-quoted", &["--list", "--topic", "'fx-a.*'"], Expect::Accepted),
    row("list-no-match", &["--list", "--topic", "missing"], Expect::Accepted),
    row("list-twice", &["--list", "--list"], Expect::Accepted),
    row("list-delete-config", &["--list", "--topic", "fx-.*", "--delete-config", "retention.ms"], Expect::Accepted),
    row("list-invalid-regex", &["--list", "--topic", "["], Expect::Rejected),
    row("describe-overrides", &["--describe", "--topics-with-overrides", "--topic", "fx-.*"], Expect::Accepted),
    row("describe-topic", &["--describe", "--topic", "fx-bravo"], Expect::Accepted),
    row("describe-all", &["--describe"], Expect::Accepted),
    row("describe-exclude-internal", &["--describe", "--exclude-internal", "--topic", "fx-.*"], Expect::Accepted),
    row("describe-under-replicated", &["--describe", "--under-replicated-partitions"], Expect::Accepted),
    row("describe-unavailable", &["--describe", "--unavailable-partitions"], Expect::Accepted),
    row("describe-under-min-isr", &["--describe", "--under-min-isr-partitions"], Expect::Accepted),
    row("describe-at-min-isr", &["--describe", "--at-min-isr-partitions", "--topic", "fx-alpha"], Expect::Accepted),
    row("describe-size-limit", &["--describe", "--topic", "fx-charlie", "--partition-size-limit-per-response", "1"], Expect::Accepted),
    row("describe-zero-topic-id", &["--describe", "--topic-id", "AAAAAAAAAAAAAAAAAAAAAA"], Expect::Accepted),
    row("describe-missing-if-exists", &["--describe", "--topic", "missing", "--if-exists"], Expect::Accepted),
    row("describe-unknown-topic-id-if-exists", &["--describe", "--topic-id", "AQEBAQEBAQEBAQEBAQEBAQ", "--if-exists"], Expect::Accepted),
    row("describe-missing", &["--describe", "--topic", "missing"], Expect::Rejected),
    row("describe-unknown-topic-id", &["--describe", "--topic-id", "AQEBAQEBAQEBAQEBAQEBAQ"], Expect::Rejected),
    row("describe-bad-topic-id", &["--describe", "--topic-id", "nonsense"], Expect::Rejected),
    row("describe-long-topic-id", &["--describe", "--topic-id", "AAAAAAAAAAAAAAAAAAAAAAAAAAAA"], Expect::Rejected),
    row("describe-if-exists-without-topic", &["--describe", "--if-exists"], Expect::Rejected),
    row("describe-bad-size-limit", &["--describe", "--partition-size-limit-per-response", "x"], Expect::Rejected),
    // Command-line checks, which refuse before any request.
    row("no-action", &[], Expect::Rejected),
    row("two-actions", &["--list", "--describe"], Expect::Rejected),
    row("create-without-topic", &["--create"], Expect::Rejected),
    row("delete-without-topic", &["--delete"], Expect::Rejected),
    row("alter-without-partitions", &["--alter", "--topic", "fx-alpha"], Expect::Rejected),
    row("alter-with-config", &["--alter", "--topic", "fx-alpha", "--partitions", "5", "--config", "retention.ms=1"], Expect::Rejected),
    row("describe-with-config", &["--describe", "--config", "retention.ms=1"], Expect::Rejected),
    row("describe-with-partitions", &["--describe", "--partitions", "3"], Expect::Rejected),
    row("alter-with-replication-factor", &["--alter", "--topic", "{t}", "--partitions", "3", "--replication-factor", "1"], Expect::Rejected),
    row("list-with-replica-assignment", &["--list", "--replica-assignment", "1"], Expect::Rejected),
    row("create-assignment-and-partitions", &["--create", "--topic", "{t}", "--partitions", "1", "--replica-assignment", "1"], Expect::Rejected),
    row("create-assignment-and-factor", &["--create", "--topic", "{t}", "--replication-factor", "1", "--replica-assignment", "1"], Expect::Rejected),
    row("list-under-replicated", &["--list", "--under-replicated-partitions"], Expect::Rejected),
    row("list-unavailable", &["--list", "--unavailable-partitions"], Expect::Rejected),
    row("list-under-min-isr", &["--list", "--under-min-isr-partitions"], Expect::Rejected),
    row("list-at-min-isr", &["--list", "--at-min-isr-partitions"], Expect::Rejected),
    row("list-overrides", &["--list", "--topics-with-overrides"], Expect::Rejected),
    row("unavailable-and-overrides", &["--describe", "--unavailable-partitions", "--topics-with-overrides"], Expect::Rejected),
    row("overrides-and-at-min-isr", &["--describe", "--topics-with-overrides", "--at-min-isr-partitions"], Expect::Rejected),
    row("create-if-exists", &["--create", "--topic", "{t}", "--if-exists"], Expect::Rejected),
    row("list-if-not-exists", &["--list", "--if-not-exists"], Expect::Rejected),
    row("delete-exclude-internal", &["--delete", "--topic", "{t}", "--exclude-internal"], Expect::Rejected),
    row("create-two-topics", &["--create", "--topic", "{t}-a", "--topic", "{t}-b"], Expect::Rejected),
    row("alter-two-partition-counts", &["--alter", "--topic", "{t}", "--partitions", "5", "--partitions", "6"], Expect::Rejected),
    row("create-partitions-not-a-number", &["--create", "--topic", "{t}", "--partitions", "abc"], Expect::Rejected),
    row("create-zero-partitions", &["--create", "--topic", "{t}", "--partitions", "0"], Expect::Rejected),
    row("create-negative-partitions", &["--create", "--topic", "{t}", "--partitions", "-1"], Expect::Rejected),
    row("create-factor-too-large", &["--create", "--topic", "{t}", "--replication-factor", "40000"], Expect::Rejected),
    row("create-negative-factor", &["--create", "--topic", "{t}", "--replication-factor", "-1"], Expect::Rejected),
    row("create-config-without-value", &["--create", "--topic", "{t}", "--config", "retention.ms"], Expect::Rejected),
    row("create-unknown-config", &["--create", "--topic", "{t}", "--config", "foo=bar"], Expect::Rejected),
    row("create-config-not-a-number", &["--create", "--topic", "{t}", "--config", "retention.ms=abc"], Expect::Rejected),
    row("create-duplicate-replica", &["--create", "--topic", "{t}", "--replica-assignment", "1:1"], Expect::Rejected),
    row("create-uneven-assignment", &["--create", "--topic", "{t}", "--replica-assignment", "1,1:2"], Expect::Rejected),
    row("create-assignment-not-a-number", &["--create", "--topic", "{t}", "--replica-assignment", "a"], Expect::Rejected),
    Row { bootstrap: false, ..row("no-bootstrap", &["--list"], Expect::Rejected) },
    row("unknown-flag", &["--list", "--bogus"], Expect::Rejected),
    row("flag-without-value", &["--list", "--topic"], Expect::Rejected),
    row("single-dash-flag", &["-list"], Expect::Accepted),
    row("help", &["--help"], Expect::Rejected),
    row("version", &["--version"], Expect::Accepted),
    row("dry-run", &["--create", "--topic", "{t}", "--dry-run"], Expect::Rejected),
    // Rows that change the cluster, each on topics of its own.
    row("create", &["--create", "--topic", "{t}"], Expect::Accepted),
    row("create-with-everything", &["--create", "--topic", "{t}", "--partitions", "3", "--replication-factor", "1", "--config", "retention.ms=1000", "--config", " cleanup.policy = compact "], Expect::Accepted),
    row("create-colliding-name", &["--create", "--topic", "{t}.x"], Expect::Accepted),
    row("create-with-assignment", &["--create", "--topic", "{t}", "--replica-assignment", "1,1"], Expect::Accepted),
    row("create-existing", &["--create", "--topic", "fx-alpha"], Expect::Rejected),
    row("create-existing-if-not-exists", &["--create", "--topic", "fx-alpha", "--if-not-exists"], Expect::Accepted),
    row("create-invalid-name", &["--create", "--topic", "bad name"], Expect::Rejected),
    row("create-factor-above-brokers", &["--create", "--topic", "{t}", "--replication-factor", "2"], Expect::Rejected),
    // kafka-topics checks the range before it sends the request; krabka
    // leaves it to the broker, which refuses with the same message.
    row("create-config-out-of-range", &["--create", "--topic", "{t}", "--config", "retention.ms=-5"], Expect::Rejected),
    after(CREATE_T, "alter", &["--alter", "--topic", "{t}", "--partitions", "4"], Expect::Accepted),
    after(CREATE_T, "alter-no-increase", &["--alter", "--topic", "{t}", "--partitions", "1"], Expect::Rejected),
    after(CREATE_T, "alter-with-assignment", &["--alter", "--topic", "{t}", "--partitions", "2", "--replica-assignment", "1,1"], Expect::Accepted),
    row("alter-missing", &["--alter", "--topic", "missing", "--partitions", "2"], Expect::Rejected),
    row("alter-missing-if-exists", &["--alter", "--topic", "missing", "--partitions", "2", "--if-exists"], Expect::Accepted),
    after(CREATE_T, "delete-without-confirmation", &["--delete", "--topic", "{t}"], Expect::Accepted),
    Row { env: &[("KRABKA_ASSUME_YES", "true")], ..after(CREATE_T, "delete", &["--delete", "--topic", "{t}"], Expect::Accepted) },
    row("delete-missing", &["--delete", "--topic", "missing"], Expect::Rejected),
    row("delete-missing-if-exists", &["--delete", "--topic", "missing", "--if-exists"], Expect::Accepted),
    // With no cluster-level `min.insync.replicas`, only topic overrides are
    // non-default configs.
    Row { phase: Phase::B, ..row("describe-overrides-topic-configs", &["--describe", "--topics-with-overrides", "--topic", "fx-.*"], Expect::Accepted) },
    Row { phase: Phase::B, ..row("describe-overrides-one", &["--describe", "--topics-with-overrides", "--topic", "fx-charlie", "--exclude-internal"], Expect::Accepted) },
    Row { phase: Phase::B, ..row("describe-overrides-none", &["--describe", "--topics-with-overrides", "--topic", "fx-bravo"], Expect::Accepted) },
];

/// Every row where the two tools differ, with the reason.
const EXPECTED_DIFFERENCES: &[(&str, Difference)] = &[
    (
        "alter-with-config",
        Difference::intended(
            &[Layer::UnorderedStderr],
            "kafka-topics prints the option combination from a Set.of, whose order changes \
             between JVM runs",
        ),
    ),
    (
        "create-two-topics",
        Difference::intended(
            OUTCOME,
            "krabka accepts --topic more than once and creates each topic; kafka-topics refuses a \
             second value",
        ),
    ),
    (
        "unknown-flag",
        Difference::intended(
            OUTCOME,
            "clap refuses an unknown flag with its own message and exit 2, the exit code krabka \
             uses for a command line that does not parse",
        ),
    ),
    (
        "flag-without-value",
        Difference::intended(
            OUTCOME,
            "clap refuses a flag without its value with its own message and exit 2",
        ),
    ),
    (
        "single-dash-flag",
        Difference::intended(
            OUTCOME,
            "joptsimple accepts a long option after one dash; clap reads -list as short options",
        ),
    ),
    (
        "help",
        Difference::intended(
            OUTCOME,
            "krabka prints clap's help on stdout and exits 0; kafka-topics prints joptsimple's \
             help on stderr and exits 1",
        ),
    ),
    (
        "version",
        Difference::intended(
            OUTCOME,
            "krabka reports its version at `krabka --version`, not per subcommand",
        ),
    ),
    (
        "dry-run",
        Difference::intended(
            OUTCOME,
            "--dry-run is a krabka addition that kafka-topics does not know",
        ),
    ),
    (
        "delete-without-confirmation",
        Difference::intended(
            OUTCOME,
            "krabka asks before it deletes, and refuses on a non-interactive stdin without --yes \
             or KRABKA_ASSUME_YES; the row `delete` shows the same output once confirmed",
        ),
    ),
];

fn declared(name: &str) -> Option<Declared> {
    super::declared(EXPECTED_DIFFERENCES, name)
}

/// The names of the matrix rows.
pub fn row_names() -> Vec<&'static str> {
    MATRIX.iter().map(|row| row.name).collect()
}

/// The names of the rows with a declared difference.
pub fn declared_names() -> Vec<&'static str> {
    EXPECTED_DIFFERENCES.iter().map(|(name, _)| *name).collect()
}

/// The row's own topic name for `tool`.
fn own_topic(index: usize, tool: Tool) -> String {
    match tool {
        Tool::Jvm => format!("ct-jvm-{index:02}"),
        Tool::Krabka => format!("ct-krb-{index:02}"),
    }
}

fn substitute(args: &[&str], topic: &str) -> Vec<String> {
    args.iter().map(|arg| arg.replace("{t}", topic)).collect()
}

fn run_row(oracle: &Oracle, index: usize, row: &Row) -> Outcome {
    let [jvm, krabka] = [Tool::Jvm, Tool::Krabka].map(|tool| {
        let topic = own_topic(index, tool);
        for before in row.before {
            let prepared = oracle.jvm("kafka-topics.sh", &substitute(before, &topic), true, "");
            assert!(prepared.exit == Some(0), "{}: {prepared:?}", row.name);
        }
        let args = substitute(row.args, &topic);
        let capture = match tool {
            Tool::Jvm => oracle.jvm("kafka-topics.sh", &args, row.bootstrap, ""),
            Tool::Krabka => oracle.krabka(&["topics"], &args, row.bootstrap, row.env, ""),
        };
        let rename = [(topic, "{t}")];
        let clean = Clean {
            rename: &rename,
            scrub: &[],
        };
        view(row.expect, &capture, tool, "topics", &clean, "")
    });
    Outcome {
        name: row.name,
        expect: row.expect,
        declared: declared(row.name),
        jvm,
        krabka,
    }
}

fn prepare_fixtures(oracle: &Oracle) {
    for fixture in FIXTURES {
        let args = std::iter::once("--create")
            .chain(fixture.iter().copied())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let created = oracle.jvm("kafka-topics.sh", &args, true, "");
        assert!(created.exit == Some(0), "{created:?}");
    }
}

/// Removes the cluster-level `min.insync.replicas`, which Kafka refuses
/// while ELR is on, and waits until topics without overrides show none.
fn remove_cluster_min_isr(oracle: &Oracle) {
    let disable = [
        "--bootstrap-server",
        &oracle.bootstrap(),
        "disable",
        "--feature",
        "eligible.leader.replicas.version",
    ]
    .map(str::to_owned);
    let disabled = oracle.jvm("kafka-features.sh", &disable, false, "");
    assert!(disabled.exit == Some(0), "{disabled:?}");
    let delete = [
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-default",
        "--delete-config",
        "min.insync.replicas",
    ]
    .map(str::to_owned);
    let deleted = oracle.jvm("kafka-configs.sh", &delete, true, "");
    assert!(deleted.exit == Some(0), "{deleted:?}");
    let probe = [
        "--describe",
        "--topics-with-overrides",
        "--topic",
        "fx-bravo",
    ]
    .map(str::to_owned);
    for _ in 0..30 {
        if oracle
            .jvm("kafka-topics.sh", &probe, true, "")
            .stdout
            .is_empty()
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    panic!("the cluster-level min.insync.replicas did not go");
}

/// Runs the matrix against a fresh oracle and checks every row.
pub fn run() {
    let oracle = Oracle::start();
    prepare_fixtures(&oracle);
    let mut outcomes = Vec::new();
    for phase in [Phase::A, Phase::B] {
        if phase == Phase::B {
            remove_cluster_min_isr(&oracle);
        }
        for (index, row) in MATRIX.iter().enumerate() {
            if row.phase == phase {
                outcomes.push(run_row(&oracle, index, row));
            }
        }
    }
    check_outcomes("topics", &outcomes);
    super::check_not_vacuous(&outcomes);

    // The harness reports a declared difference as expected, and the same
    // views without the declaration as a failure, so the difference is real
    // and the declaration is what lets it pass.
    let bogus = outcomes
        .iter()
        .find(|outcome| outcome.name == "unknown-flag")
        .expect("the matrix has the unknown-flag row");
    check!(bogus.verdict() == Verdict::ExpectedDifference);
    check!(matches!(
        super::oracle::judge(bogus.expect, None, &bogus.jvm, &bogus.krabka),
        Verdict::Undeclared { .. }
    ));

    // One changed byte in an agreeing row fails the comparison.
    let listed = outcomes
        .iter()
        .find(|outcome| outcome.name == "list-regex")
        .expect("the matrix has the list-regex row");
    let mut changed = listed.jvm.clone();
    changed.stdout = changed.stdout.replacen("fx-alpha", "fx-alphb", 1);
    check!(changed != listed.jvm);
    check!(matches!(
        super::oracle::judge(listed.expect, None, &changed, &listed.krabka),
        Verdict::Undeclared { .. }
    ));
}
