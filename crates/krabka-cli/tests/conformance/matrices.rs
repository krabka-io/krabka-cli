//! The argument matrix of each command that the generic runner drives.
//!
//! Every row was run against the pinned `apache/kafka:4.3.1` oracle. The
//! fixtures that every matrix reads are `fx-alpha` (two partitions, one
//! keyed record in each), `fx-bravo` (one partition, three records) and the
//! empty group `fx-group` on `fx-bravo`; see `FIXTURES` in `conformance.rs`.
//!
//! Where a row changes the cluster, its `resolve` step reads the result back
//! through the JVM tool, and that read-back is the row's resolved state.

use super::{
    Matrix, Row, Step,
    oracle::{Difference, Expect, Layer, OUTCOME, Scrub},
    row, step,
};

const ACCEPTED: Expect = Expect::Accepted;
const REJECTED: Expect = Expect::Rejected;

/// `KRABKA_ASSUME_YES`, which answers krabka's confirmation prompt where the
/// JVM tool asks none.
const YES: &[(&str, &str)] = &[("KRABKA_ASSUME_YES", "true")];

/// A row that runs `before` for each tool first.
const fn after(before: &'static [Step], row: Row) -> Row {
    Row { before, ..row }
}

/// A row whose resolved state is what `read` prints afterwards.
const fn resolved(read: Step, row: Row) -> Row {
    Row {
        resolve: Some(read),
        ..row
    }
}

/// A row without `--bootstrap-server`.
const fn offline(row: Row) -> Row {
    Row {
        bootstrap: false,
        ..row
    }
}

/// The command-line refusals common to every joptsimple tool: clap has its
/// own words for them, and exits 2.
const CLAP_UNKNOWN: Difference = Difference::intended(
    OUTCOME,
    "clap refuses an unknown flag with its own message and exit 2, the exit code krabka uses for \
     a command line that does not parse",
);
const CLAP_HELP: Difference = Difference::intended(
    OUTCOME,
    "krabka prints clap's help on stdout and exits 0; the JVM tool prints its own help and exits \
     1 or 0",
);
const CLAP_MISSING: Difference = Difference::intended(
    OUTCOME,
    "clap refuses a missing required argument or subcommand in its own words, with exit 2",
);
const USAGE_EXIT: Difference = Difference::intended(
    &[Layer::Exit],
    "krabka exits 2 for a command line that its own checks refuse, where the JVM tool exits 1; \
     the message is the JVM tool's",
);
const DRY_RUN: Difference = Difference::intended(
    OUTCOME,
    "--dry-run is a krabka addition that the JVM tool does not know",
);
const PER_COMMAND_VERSION: Difference = Difference::intended(
    OUTCOME,
    "krabka reports its version at `krabka --version`, not per subcommand",
);

// ---------------------------------------------------------------------------
// acls
// ---------------------------------------------------------------------------

const LIST_T_ACLS: Step = step("kafka-acls.sh", &["--list", "--topic", "{t}"]);

#[rustfmt::skip]
pub const ACLS: Matrix = Matrix {
    script: "kafka-acls.sh",
    command: "acls",
    prefix: "ca",
    rows: &[
        row("list-empty", &["--list"], ACCEPTED),
        after(&[step("kafka-acls.sh", &["--add", "--allow-principal", "User:reader", "--operation", "Read", "--topic", "{t}"])],
            row("list-topic", &["--list", "--topic", "{t}"], ACCEPTED)),
        after(&[step("kafka-acls.sh", &["--add", "--allow-principal", "User:{t}", "--operation", "Describe", "--group", "{t}"])],
            row("list-principal", &["--list", "--principal", "User:{t}"], ACCEPTED)),
        resolved(LIST_T_ACLS, row("add-read", &["--add", "--allow-principal", "User:alice", "--operation", "Read", "--topic", "{t}"], ACCEPTED)),
        resolved(LIST_T_ACLS, row("add-deny-host", &["--add", "--deny-principal", "User:mallory", "--deny-host", "10.0.0.1", "--operation", "Write", "--topic", "{t}"], ACCEPTED)),
        resolved(LIST_T_ACLS, row("add-producer", &["--add", "--allow-principal", "User:bob", "--producer", "--topic", "{t}"], ACCEPTED)),
        resolved(step("kafka-acls.sh", &["--list", "--topic", "{t}", "--resource-pattern-type", "prefixed"]),
            row("add-prefixed", &["--add", "--allow-principal", "User:carol", "--operation", "All", "--topic", "{t}", "--resource-pattern-type", "prefixed"], ACCEPTED)),
        Row { env: YES, ..after(&[step("kafka-acls.sh", &["--add", "--allow-principal", "User:dave", "--operation", "Read", "--topic", "{t}"])],
            resolved(LIST_T_ACLS, row("remove-forced", &["--remove", "--allow-principal", "User:dave", "--operation", "Read", "--topic", "{t}", "--force"], ACCEPTED))) },
        resolved(LIST_T_ACLS, row("add-dry-run", &["--add", "--allow-principal", "User:erin", "--operation", "Read", "--topic", "{t}", "--dry-run"], REJECTED)),
        row("no-action", &[], REJECTED),
        row("two-actions", &["--list", "--add"], REJECTED),
        row("bad-operation", &["--add", "--allow-principal", "User:x", "--operation", "Bogus", "--topic", "{t}"], REJECTED),
        row("add-without-principal", &["--add", "--operation", "Read", "--topic", "{t}"], REJECTED),
        row("add-without-resource", &["--add", "--allow-principal", "User:x", "--operation", "Read"], REJECTED),
        row("unknown-flag", &["--list", "--bogus"], REJECTED),
        row("help", &["--help"], REJECTED),
        row("version", &["--version"], ACCEPTED),
    ],
    differences: &[
        ("add-dry-run", DRY_RUN),
        ("add-dry-run", Difference::intended(&[Layer::Resolved], "the dry run adds nothing; kafka-acls refuses the row and adds nothing either, so only the refusal differs")),
        ("no-action", USAGE_EXIT),
        ("two-actions", USAGE_EXIT),
        ("bad-operation", USAGE_EXIT),
        ("bad-operation", Difference::intended(&[Layer::Stderr], "krabka lists the operations in AclOperation code order; kafka-acls prints a Java Set, whose order is its hash order")),
        ("add-without-principal", USAGE_EXIT),
        ("add-without-resource", USAGE_EXIT),
        ("unknown-flag", CLAP_UNKNOWN),
        ("help", CLAP_HELP),
        ("version", PER_COMMAND_VERSION),
    ],
};

// ---------------------------------------------------------------------------
// cluster
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const CLUSTER: Matrix = Matrix {
    script: "kafka-cluster.sh",
    command: "cluster",
    prefix: "cl",
    rows: &[
        offline(row("cluster-id", &["cluster-id", "--bootstrap-server", "{b}"], ACCEPTED)),
        offline(row("list-endpoints", &["list-endpoints", "--bootstrap-server", "{b}"], ACCEPTED)),
        offline(row("cluster-id-without-bootstrap", &["cluster-id"], REJECTED)),
        offline(row("unregister-without-id", &["unregister", "--bootstrap-server", "{b}"], REJECTED)),
        Row { env: YES, ..offline(row("unregister-unknown", &["unregister", "--bootstrap-server", "{b}", "--id", "5"], REJECTED)) },
        offline(row("unregister-unconfirmed", &["unregister", "--bootstrap-server", "{b}", "--id", "5"], REJECTED)),
        offline(row("no-subcommand", &[], REJECTED)),
        offline(row("unknown-subcommand", &["bogus"], REJECTED)),
    ],
    differences: &[
        ("cluster-id-without-bootstrap", Difference::intended(&[Layer::Stderr], "argparse4j prints its usage and `one of the arguments --bootstrap-server/-b --bootstrap-controller/-C is required`; krabka names the two flags in one sentence")),
        ("unregister-without-id", CLAP_MISSING),
        ("unregister-unknown", Difference::intended(&[Layer::Stderr], "kafka-cluster prints the uncaught ExecutionException; krabka prints the broker's error code and message")),
        ("unregister-unconfirmed", Difference::intended(OUTCOME, "krabka asks before it unregisters a broker, and refuses on a non-interactive stdin without --yes; the row `unregister-unknown` shows the confirmed run")),
        ("no-subcommand", CLAP_MISSING),
        ("unknown-subcommand", CLAP_MISSING),
    ],
};

// ---------------------------------------------------------------------------
// configs
// ---------------------------------------------------------------------------

const DESCRIBE_T_TOPIC: Step = step(
    "kafka-configs.sh",
    &[
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        "{t}",
    ],
);
const CREATE_T: Step = step("kafka-topics.sh", &["--create", "--topic", "{t}"]);

#[rustfmt::skip]
pub const CONFIGS: Matrix = Matrix {
    script: "kafka-configs.sh",
    command: "configs",
    prefix: "cf",
    rows: &[
        row("describe-topic", &["--describe", "--entity-type", "topics", "--entity-name", "fx-alpha"], ACCEPTED),
        row("describe-all-topics", &["--describe", "--entity-type", "topics"], ACCEPTED),
        row("describe-missing-topic", &["--describe", "--entity-type", "topics", "--entity-name", "missing"], ACCEPTED),
        row("describe-broker", &["--describe", "--entity-type", "brokers", "--entity-name", "1"], ACCEPTED),
        row("describe-broker-default", &["--describe", "--entity-type", "brokers", "--entity-default"], ACCEPTED),
        row("describe-users", &["--describe", "--entity-type", "users"], ACCEPTED),
        row("describe-clients", &["--describe", "--entity-type", "clients"], ACCEPTED),
        row("describe-group", &["--describe", "--entity-type", "groups", "--entity-name", "fx-group"], ACCEPTED),
        row("describe-client-metrics", &["--describe", "--entity-type", "client-metrics"], ACCEPTED),
        row("describe-topic-shorthand", &["--describe", "--topic", "fx-alpha"], ACCEPTED),
        after(&[CREATE_T], resolved(DESCRIBE_T_TOPIC, row("add-config", &["--alter", "--entity-type", "topics", "--entity-name", "{t}", "--add-config", "retention.ms=1000,cleanup.policy=compact"], ACCEPTED))),
        after(&[CREATE_T, step("kafka-configs.sh", &["--alter", "--entity-type", "topics", "--entity-name", "{t}", "--add-config", "retention.ms=1000"])],
            resolved(DESCRIBE_T_TOPIC, row("delete-config", &["--alter", "--entity-type", "topics", "--entity-name", "{t}", "--delete-config", "retention.ms"], ACCEPTED))),
        Row { file: "segment.ms=600000\nmax.message.bytes=2048\n", ..after(&[CREATE_T],
            resolved(DESCRIBE_T_TOPIC, row("add-config-file", &["--alter", "--entity-type", "topics", "--entity-name", "{t}", "--add-config-file", "{f}"], ACCEPTED))) },
        resolved(step("kafka-configs.sh", &["--describe", "--entity-type", "users", "--entity-name", "{t}"]),
            row("add-user-quota", &["--alter", "--entity-type", "users", "--entity-name", "{t}", "--add-config", "producer_byte_rate=1024"], ACCEPTED)),
        after(&[CREATE_T], resolved(DESCRIBE_T_TOPIC, row("add-config-invalid", &["--alter", "--entity-type", "topics", "--entity-name", "{t}", "--add-config", "retention.ms=abc"], REJECTED))),
        row("alter-without-change", &["--alter", "--entity-type", "topics", "--entity-name", "fx-alpha"], REJECTED),
        row("bad-entity-type", &["--describe", "--entity-type", "bogus"], REJECTED),
        row("no-action", &["--entity-type", "topics"], REJECTED),
        row("unknown-flag", &["--describe", "--bogus"], REJECTED),
        row("help", &["--help"], REJECTED),
    ],
    differences: &[
        ("add-config-invalid", Difference::intended(&[Layer::Stderr], "kafka-configs prints `Error while executing config command with args ...` and the uncaught exception; krabka prints the broker's error code and message")),
        ("unknown-flag", CLAP_UNKNOWN),
        ("help", CLAP_HELP),
    ],
};

// ---------------------------------------------------------------------------
// console-consumer
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const CONSOLE_CONSUMER: Matrix = Matrix {
    script: "kafka-console-consumer.sh",
    command: "console-consumer",
    prefix: "cc",
    rows: &[
        row("partition-earliest", &["--topic", "fx-bravo", "--partition", "0", "--offset", "earliest", "--max-messages", "3"], ACCEPTED),
        row("partition-offset", &["--topic", "fx-bravo", "--partition", "0", "--offset", "1", "--max-messages", "2"], ACCEPTED),
        row("print-key", &["--topic", "fx-bravo", "--partition", "0", "--offset", "earliest", "--max-messages", "1", "--formatter-property", "print.key=true"], ACCEPTED),
        row("print-partition-offset", &["--topic", "fx-bravo", "--partition", "0", "--offset", "earliest", "--max-messages", "1", "--formatter-property", "print.partition=true", "--formatter-property", "print.offset=true"], ACCEPTED),
        row("deprecated-property", &["--topic", "fx-bravo", "--partition", "0", "--offset", "earliest", "--max-messages", "1", "--property", "print.key=true"], ACCEPTED),
        row("no-topic", &[], REJECTED),
        row("offset-without-partition", &["--topic", "fx-bravo", "--offset", "earliest"], REJECTED),
        row("bad-offset", &["--topic", "fx-bravo", "--partition", "0", "--offset", "bogus"], REJECTED),
        row("unknown-flag", &["--topic", "fx-bravo", "--bogus"], REJECTED),
    ],
    differences: &[
        ("deprecated-property", Difference::intended(&[Layer::Stdout, Layer::Stderr], "kafka-console-consumer prints the --property deprecation warning on stdout, ahead of the records; krabka prints it on stderr, so that stdout holds only records")),
        ("no-topic", USAGE_EXIT),
        ("offset-without-partition", USAGE_EXIT),
        ("bad-offset", USAGE_EXIT),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// console-producer
// ---------------------------------------------------------------------------

const READ_T: Step = step(
    "kafka-console-consumer.sh",
    &[
        "--topic",
        "{t}",
        "--partition",
        "0",
        "--offset",
        "earliest",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
        "--formatter-property",
        "print.key=true",
    ],
);

#[rustfmt::skip]
pub const CONSOLE_PRODUCER: Matrix = Matrix {
    script: "kafka-console-producer.sh",
    command: "console-producer",
    prefix: "cp",
    rows: &[
        Row { stdin: "a\nb\nc\n", ..after(&[CREATE_T], resolved(READ_T, row("values", &["--topic", "{t}"], ACCEPTED))) },
        Row { stdin: "k1:v1\nk2:v2\nk3:v3\n", ..after(&[CREATE_T], resolved(READ_T,
            row("keyed", &["--topic", "{t}", "--reader-property", "parse.key=true", "--reader-property", "key.separator=:"], ACCEPTED))) },
        Row { stdin: "k1|v1\nk2|v2\nk3|v3\n", ..after(&[CREATE_T], resolved(READ_T,
            row("keyed-deprecated", &["--topic", "{t}", "--property", "parse.key=true", "--property", "key.separator=|"], ACCEPTED))) },
        row("no-topic", &[], REJECTED),
        row("unknown-flag", &["--topic", "fx-bravo", "--bogus"], REJECTED),
    ],
    differences: &[
        ("keyed-deprecated", Difference::intended(&[Layer::Stdout, Layer::Stderr], "kafka-console-producer prints the --property deprecation warning on stdout; krabka prints it on stderr")),
        ("no-topic", USAGE_EXIT),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// consumer-groups
// ---------------------------------------------------------------------------

const DESCRIBE_T_GROUP: Step = step(
    "kafka-consumer-groups.sh",
    &["--describe", "--group", "{t}", "--offsets"],
);
const GROUP_T: Step = step(
    "kafka-consumer-groups.sh",
    &[
        "--group",
        "{t}",
        "--topic",
        "fx-bravo",
        "--reset-offsets",
        "--to-earliest",
        "--execute",
    ],
);

#[rustfmt::skip]
pub const CONSUMER_GROUPS: Matrix = Matrix {
    script: "kafka-consumer-groups.sh",
    command: "consumer-groups",
    prefix: "cg",
    rows: &[
        row("list", &["--list"], ACCEPTED),
        row("list-state", &["--list", "--state"], ACCEPTED),
        row("describe-offsets", &["--describe", "--group", "fx-group"], ACCEPTED),
        row("describe-members", &["--describe", "--group", "fx-group", "--members"], ACCEPTED),
        row("describe-state", &["--describe", "--group", "fx-group", "--state"], ACCEPTED),
        row("describe-all-groups", &["--describe", "--all-groups", "--offsets"], ACCEPTED),
        resolved(DESCRIBE_T_GROUP, row("reset-to-offset", &["--group", "{t}", "--topic", "fx-bravo", "--reset-offsets", "--to-offset", "1", "--execute"], ACCEPTED)),
        resolved(DESCRIBE_T_GROUP, row("reset-dry-run", &["--group", "{t}", "--topic", "fx-bravo", "--reset-offsets", "--to-latest", "--dry-run"], ACCEPTED)),
        row("reset-without-mode", &["--group", "fx-group", "--topic", "fx-bravo", "--reset-offsets", "--to-offset", "1"], ACCEPTED),
        Row { env: YES, ..after(&[GROUP_T], resolved(step("kafka-consumer-groups.sh", &["--list"]),
            row("delete", &["--delete", "--group", "{t}"], ACCEPTED))) },
        Row { env: YES, ..row("delete-missing", &["--delete", "--group", "missing"], ACCEPTED) },
        row("describe-missing", &["--describe", "--group", "missing"], ACCEPTED),
        row("describe-without-group", &["--describe"], REJECTED),
        row("no-action", &["--group", "fx-group"], REJECTED),
        row("unknown-flag", &["--list", "--bogus"], REJECTED),
    ],
    differences: &[
        ("describe-without-group", Difference::intended(&[Layer::UnorderedStderr], "kafka-consumer-groups prints the options of --describe from a Set.of, whose order changes between JVM runs")),
        ("no-action", CLAP_MISSING),
        ("reset-dry-run", Difference::intended(&[Layer::Stderr], "krabka marks a dry run with `DRY RUN: no change was made.` on stderr")),
        ("reset-without-mode", Difference::intended(OUTCOME, "krabka requires --dry-run or --execute with --reset-offsets, as Kafka 5.0 will; Kafka 4.3.1 warns and dry-runs")),
        ("delete-missing", Difference::intended(&[Layer::Surface], "krabka exits 1 when a group cannot be deleted; kafka-consumer-groups prints the same `Error: ...` report and exits 0")),
        ("describe-missing", Difference::intended(OUTCOME, "krabka prints the failure on stderr and exits 1; kafka-consumer-groups prints `Error: ...` on stdout with the stack trace on stderr, and exits 0")),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// delegation-tokens
// ---------------------------------------------------------------------------

/// The oracle listens on PLAINTEXT, where a broker refuses every delegation
/// token request, so every request row is refused by the broker; the rows
/// still compare the command line, the request and the rendered refusal.
#[rustfmt::skip]
pub const DELEGATION_TOKENS: Matrix = Matrix {
    script: "kafka-delegation-tokens.sh",
    command: "delegation-tokens",
    prefix: "dt",
    rows: &[
        Row { file: "bootstrap.servers={b}\n", ..row("describe", &["--describe", "--command-config", "{f}"], REJECTED) },
        Row { file: "bootstrap.servers={b}\n", ..row("create", &["--create", "--max-life-time-period", "-1", "--command-config", "{f}"], REJECTED) },
        Row { file: "bootstrap.servers={b}\n", ..row("no-action", &["--command-config", "{f}"], REJECTED) },
        row("no-command-config", &["--describe"], REJECTED),
        row("unknown-flag", &["--describe", "--bogus"], REJECTED),
    ],
    differences: &[
        ("describe", Difference::intended(&[Layer::Stderr], "kafka-delegation-tokens prints `Calling describe token operation ...` on stdout and the uncaught exception; krabka prints the broker's error code and message")),
        ("create", Difference::intended(&[Layer::Stderr], "kafka-delegation-tokens prints `Calling create token operation ...` on stdout and the uncaught exception; krabka prints the broker's error code and message")),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// delete-records
// ---------------------------------------------------------------------------

const PRODUCE_T: Step = Step {
    stdin: "a\nb\nc\n",
    ..step("kafka-console-producer.sh", &["--topic", "{t}"])
};

#[rustfmt::skip]
pub const DELETE_RECORDS: Matrix = Matrix {
    script: "kafka-delete-records.sh",
    command: "delete-records",
    prefix: "dr",
    rows: &[
        Row { env: YES, file: r#"{"partitions":[{"topic":"{t}","partition":0,"offset":1}],"version":1}"#,
            ..after(&[CREATE_T, PRODUCE_T], resolved(step("kafka-get-offsets.sh", &["--topic", "{t}", "--time", "-2"]),
                row("delete-below", &["--offset-json-file", "{f}"], ACCEPTED))) },
        Row { env: YES, file: r#"{"partitions":[{"topic":"{t}","partition":0,"offset":9}],"version":1}"#,
            ..after(&[CREATE_T, PRODUCE_T], row("offset-out-of-range", &["--offset-json-file", "{f}"], ACCEPTED)) },
        Row { file: r#"{"partitions":[{"topic":"{t}","partition":0,"offset":1}],"version":1}"#,
            ..after(&[CREATE_T, PRODUCE_T], resolved(step("kafka-get-offsets.sh", &["--topic", "{t}", "--time", "-2"]),
                row("delete-unconfirmed", &["--offset-json-file", "{f}"], ACCEPTED))) },
        row("no-file", &[], REJECTED),
        row("unknown-flag", &["--bogus"], REJECTED),
    ],
    differences: &[
        ("offset-out-of-range", Difference::intended(&[Layer::Surface, Layer::Stderr], "krabka exits 1 when a partition fails, where kafka-delete-records reports the failure in its table and exits 0; the JVM admin client also logs it on stderr as a log4j ERROR line")),
        ("delete-unconfirmed", Difference::intended(&[Layer::Surface, Layer::Stdout, Layer::Stderr, Layer::Resolved], "krabka asks before it deletes records, and refuses on a non-interactive stdin without --yes; the row `delete-below` shows the confirmed run")),
        ("no-file", CLAP_MISSING),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// features
// ---------------------------------------------------------------------------

const DESCRIBE_FEATURES: Step = step("kafka-features.sh", &["describe"]);
const EPOCH: &[Scrub] = &[Scrub::Field("Epoch:")];

#[rustfmt::skip]
pub const FEATURES: Matrix = Matrix {
    script: "kafka-features.sh",
    command: "features",
    prefix: "fe",
    rows: &[
        Row { scrub: EPOCH, ..row("describe", &["describe"], ACCEPTED) },
        row("version-mapping-default", &["version-mapping"], ACCEPTED),
        row("version-mapping-3.3", &["version-mapping", "--release-version", "3.3"], ACCEPTED),
        row("version-mapping-4.0", &["version-mapping", "--release-version", "4.0"], ACCEPTED),
        row("version-mapping-unknown", &["version-mapping", "--release-version", "9.9"], REJECTED),
        row("feature-dependencies", &["feature-dependencies", "--feature", "transaction.version=2"], ACCEPTED),
        row("upgrade-release-dry-run", &["upgrade", "--release-version", "4.3", "--dry-run"], ACCEPTED),
        row("downgrade-dry-run", &["downgrade", "--feature", "group.version=0", "--dry-run"], ACCEPTED),
        row("upgrade-unknown-feature", &["upgrade", "--feature", "bogus.version=1", "--dry-run"], REJECTED),
        Row {
            resolve: Some(DESCRIBE_FEATURES),
            scrub: EPOCH,
            after: &[step("kafka-features.sh", &["upgrade", "--feature", "streams.version=1"])],
            ..row("disable", &["disable", "--feature", "streams.version"], ACCEPTED)
        },
        Row {
            resolve: Some(DESCRIBE_FEATURES),
            scrub: EPOCH,
            before: &[step("kafka-features.sh", &["disable", "--feature", "streams.version"])],
            ..row("upgrade", &["upgrade", "--feature", "streams.version=1"], ACCEPTED)
        },
        row("no-subcommand", &[], REJECTED),
        row("unknown-flag", &["describe", "--bogus"], REJECTED),
    ],
    differences: &[
        ("version-mapping-default", Difference::defect(&[Layer::Stdout], "krabka prints kraft.version=0 where Kafka's release mapping gives kraft.version=1")),
        ("version-mapping-4.0", Difference::defect(&[Layer::Stdout], "krabka prints kraft.version=0 where Kafka's mapping for 4.0 gives kraft.version=1")),
        ("version-mapping-unknown", Difference::defect(&[Layer::Stderr], "krabka lists Kafka trunk's 4.4-IV1 and 4.4-IV2 among the supported releases; Kafka 4.3.1 stops at 4.4-IV0")),
        ("upgrade-release-dry-run", Difference::defect(&[Layer::Stdout], "krabka omits `kraft.version can be upgraded to 1.`, and prefixes the report with krabka's `DRY RUN: no change was made.` line")),
        ("downgrade-dry-run", Difference::intended(&[Layer::Stdout], "krabka prefixes a dry-run report with `DRY RUN: no change was made.`")),
        ("upgrade-unknown-feature", Difference::intended(&[Layer::Stderr], "krabka refuses an unknown feature before it sends UpdateFeatures, with kafka-storage's message; kafka-features sends it and prints the controller's refusal")),
        ("no-subcommand", CLAP_MISSING),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// get-offsets
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const GET_OFFSETS: Matrix = Matrix {
    script: "kafka-get-offsets.sh",
    command: "get-offsets",
    prefix: "go",
    rows: &[
        row("latest", &["--topic", "fx-alpha"], ACCEPTED),
        row("earliest", &["--topic", "fx-alpha", "--time", "-2"], ACCEPTED),
        row("earliest-word", &["--topic", "fx-bravo", "--time", "earliest"], ACCEPTED),
        row("max-timestamp", &["--topic", "fx-bravo", "--time", "-3"], ACCEPTED),
        row("regex", &["--topic", "fx-.*"], ACCEPTED),
        row("topic-partitions", &["--topic-partitions", "fx-alpha:0"], ACCEPTED),
        row("partitions", &["--topic", "fx-alpha", "--partitions", "1"], ACCEPTED),
        row("missing-topic", &["--topic", "missing"], REJECTED),
        row("bad-time", &["--time", "bogus"], REJECTED),
        row("unknown-flag", &["--bogus"], REJECTED),
    ],
    differences: &[
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// leader-election
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const LEADER_ELECTION: Matrix = Matrix {
    script: "kafka-leader-election.sh",
    command: "leader-election",
    prefix: "le",
    rows: &[
        row("preferred-all", &["--election-type", "preferred", "--all-topic-partitions"], ACCEPTED),
        row("preferred-one", &["--election-type", "preferred", "--topic", "fx-alpha", "--partition", "0"], ACCEPTED),
        row("unclean-one", &["--election-type", "unclean", "--topic", "fx-alpha", "--partition", "1"], ACCEPTED),
        Row { file: r#"{"partitions":[{"topic":"fx-alpha","partition":0}]}"#,
            ..row("path-to-json-file", &["--election-type", "preferred", "--path-to-json-file", "{f}"], ACCEPTED) },
        row("no-election-type", &["--all-topic-partitions"], REJECTED),
        row("bad-election-type", &["--election-type", "bogus", "--all-topic-partitions"], REJECTED),
        row("no-partitions", &["--election-type", "preferred"], REJECTED),
        row("unknown-flag", &["--bogus"], REJECTED),
    ],
    differences: &[
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// log-dirs
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const LOG_DIRS: Matrix = Matrix {
    script: "kafka-log-dirs.sh",
    command: "log-dirs",
    prefix: "ld",
    rows: &[
        row("describe-topic", &["--describe", "--topic-list", "fx-alpha"], ACCEPTED),
        row("describe-broker", &["--describe", "--broker-list", "1", "--topic-list", "fx-bravo"], ACCEPTED),
        row("describe-missing", &["--describe", "--broker-list", "1", "--topic-list", "missing"], ACCEPTED),
        row("no-action", &[], REJECTED),
        row("unknown-flag", &["--describe", "--bogus"], REJECTED),
    ],
    differences: &[
        ("no-action", CLAP_MISSING),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// metadata-quorum
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const METADATA_QUORUM: Matrix = Matrix {
    script: "kafka-metadata-quorum.sh",
    command: "metadata-quorum",
    prefix: "mq",
    rows: &[
        Row { scrub: &[Scrub::Field("HighWatermark:")], ..row("describe-status", &["describe", "--status"], ACCEPTED) },
        Row { scrub: &[Scrub::TabColumn(2), Scrub::TabColumn(4), Scrub::TabColumn(5)], ..row("describe-replication", &["describe", "--replication"], ACCEPTED) },
        Row { scrub: &[Scrub::TabColumn(2), Scrub::TabColumn(4), Scrub::TabColumn(5)], ..row("describe-replication-abbreviated", &["describe", "--re"], ACCEPTED) },
        row("describe-without-mode", &["describe"], REJECTED),
        row("no-subcommand", &[], REJECTED),
        row("unknown-flag", &["describe", "--bogus"], REJECTED),
    ],
    differences: &[
        ("describe-status", Difference::defect(&[Layer::Stdout], "krabka prints CurrentVoters without the voters' endpoints, `[{\"id\": 1}]`, where Kafka prints `[{\"id\": 1, \"endpoints\": [\"CONTROLLER://localhost:9093\"]}]`; the pinned client does not carry them yet")),
        ("describe-replication-abbreviated", Difference::intended(OUTCOME, "joptsimple accepts an unambiguous prefix of a long option; clap does not")),
        ("no-subcommand", CLAP_MISSING),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// reassign-partitions
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const REASSIGN_PARTITIONS: Matrix = Matrix {
    script: "kafka-reassign-partitions.sh",
    command: "reassign-partitions",
    prefix: "rp",
    rows: &[
        row("list", &["--list"], ACCEPTED),
        Row { file: r#"{"topics":[{"topic":"fx-alpha"}],"version":1}"#,
            ..row("generate", &["--generate", "--topics-to-move-json-file", "{f}", "--broker-list", "1"], ACCEPTED) },
        Row { file: r#"{"version":1,"partitions":[{"topic":"fx-bravo","partition":0,"replicas":[1],"log_dirs":["any"]}]}"#,
            ..row("verify", &["--verify", "--reassignment-json-file", "{f}"], ACCEPTED) },
        Row { env: YES, file: r#"{"version":1,"partitions":[{"topic":"fx-bravo","partition":0,"replicas":[1],"log_dirs":["any"]}]}"#,
            ..resolved(step("kafka-topics.sh", &["--describe", "--topic", "fx-bravo"]),
                row("execute-noop", &["--execute", "--reassignment-json-file", "{f}"], ACCEPTED)) },
        row("no-action", &[], REJECTED),
        row("generate-without-file", &["--generate", "--broker-list", "1"], REJECTED),
        row("unknown-flag", &["--list", "--bogus"], REJECTED),
    ],
    differences: &[
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};

// ---------------------------------------------------------------------------
// storage (every subcommand but `format`, which the format matrix covers)
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const STORAGE: Matrix = Matrix {
    script: "kafka-storage.sh",
    command: "storage",
    prefix: "st",
    rows: &[
        offline(row("version-mapping-default", &["version-mapping"], ACCEPTED)),
        offline(row("version-mapping-3.3", &["version-mapping", "--release-version", "3.3"], ACCEPTED)),
        offline(row("version-mapping-4.0", &["version-mapping", "--release-version", "4.0"], ACCEPTED)),
        offline(row("version-mapping-unknown", &["version-mapping", "--release-version", "9.9"], REJECTED)),
        offline(row("feature-dependencies", &["feature-dependencies", "--feature", "transaction.version=2"], ACCEPTED)),
        offline(row("feature-dependencies-unknown", &["feature-dependencies", "--feature", "bogus.version=1"], REJECTED)),
        Row { scrub: &[Scrub::Uuid], ..offline(row("random-uuid", &["random-uuid"], ACCEPTED)) },
        offline(row("no-subcommand", &[], REJECTED)),
    ],
    differences: &[
        ("version-mapping-default", Difference::defect(&[Layer::Stdout], "krabka prints kraft.version=0 where Kafka's release mapping gives kraft.version=1")),
        ("version-mapping-4.0", Difference::defect(&[Layer::Stdout], "krabka prints kraft.version=0 where Kafka's mapping for 4.0 gives kraft.version=1")),
        ("version-mapping-unknown", Difference::defect(&[Layer::Stderr], "krabka lists Kafka trunk's 4.4-IV1 and 4.4-IV2 among the supported releases; Kafka 4.3.1 stops at 4.4-IV0")),
        ("no-subcommand", CLAP_MISSING),
    ],
};

// ---------------------------------------------------------------------------
// transactions
// ---------------------------------------------------------------------------

#[rustfmt::skip]
pub const TRANSACTIONS: Matrix = Matrix {
    script: "kafka-transactions.sh",
    command: "transactions",
    prefix: "tx",
    rows: &[
        row("list", &["list"], ACCEPTED),
        row("find-hanging", &["find-hanging", "--broker-id", "1"], ACCEPTED),
        Row { scrub: &[Scrub::TabColumn(4)], ..row("describe-producers", &["describe-producers", "--topic", "fx-bravo", "--partition", "0"], ACCEPTED) },
        row("describe-missing", &["describe", "--transactional-id", "missing"], REJECTED),
        row("find-hanging-without-target", &["find-hanging"], REJECTED),
        row("no-subcommand", &[], REJECTED),
        row("unknown-flag", &["list", "--bogus"], REJECTED),
    ],
    differences: &[
        ("describe-missing", Difference::intended(&[Layer::Stderr], "kafka-transactions words the not-found error through the admin client's exception, `... failed because the ID could not be found.`; krabka prints the broker's error code and message")),
        ("no-subcommand", CLAP_MISSING),
        ("unknown-flag", CLAP_UNKNOWN),
    ],
};
