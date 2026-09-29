use assert2::check;
use clap::Parser;

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: ReassignPartitionsArgs,
}

fn parse(argv: &[&str]) -> ReassignPartitionsArgs {
    Command::try_parse_from(std::iter::once("reassign-partitions").chain(argv.iter().copied()))
        .expect("the command line parses")
        .args
}

fn validate(argv: &[&str]) -> Result<Action, String> {
    parse(argv).validate()
}

const SERVER: &str = "--bootstrap-server=h:9092";
const CONTROLLER: &str = "--bootstrap-controller=h:9093";

#[test]
fn actions_resolve_as_kafka_resolves_them() {
    let cases = [
        (vec![SERVER, "--list"], Action::List),
        (vec![CONTROLLER, "--list"], Action::List),
        (
            vec![
                SERVER,
                "--generate",
                "--topics-to-move-json-file",
                "t.json",
                "--broker-list",
                "1,2",
            ],
            Action::Generate {
                topics_file: "t.json".into(),
                broker_list: "1,2".into(),
                rack_aware: true,
            },
        ),
        (
            vec![
                SERVER,
                "--generate",
                "--topics-to-move-json-file",
                "t.json",
                "--broker-list",
                "1",
                "--disable-rack-aware",
            ],
            Action::Generate {
                topics_file: "t.json".into(),
                broker_list: "1".into(),
                rack_aware: false,
            },
        ),
        (
            vec![SERVER, "--execute", "--reassignment-json-file", "r.json"],
            Action::Execute {
                file: "r.json".into(),
                additional: false,
                throttle: -1,
                log_dir_throttle: -1,
                disallow_replication_factor_change: false,
            },
        ),
        (
            vec![
                SERVER,
                "--execute",
                "--reassignment-json-file",
                "r.json",
                "--additional",
                "--throttle",
                "50000000",
                "--replica-alter-log-dirs-throttle",
                "1000",
                "--timeout",
                "5000",
            ],
            Action::Execute {
                file: "r.json".into(),
                additional: true,
                throttle: 50_000_000,
                log_dir_throttle: 1000,
                disallow_replication_factor_change: false,
            },
        ),
        (
            vec![
                SERVER,
                "--verify",
                "--reassignment-json-file",
                "r.json",
                "--preserve-throttles",
            ],
            Action::Verify {
                file: "r.json".into(),
                preserve_throttles: true,
            },
        ),
        (
            vec![
                CONTROLLER,
                "--cancel",
                "--reassignment-json-file",
                "r.json",
                "--preserve-throttles",
            ],
            Action::Cancel {
                file: "r.json".into(),
                preserve_throttles: true,
            },
        ),
        (
            vec![
                SERVER,
                "--verify",
                "--topic",
                "orders",
                "--replication-factor",
                "2",
            ],
            Action::ReplicationFactor {
                topic: "orders".into(),
                replication_factor: 2,
                execute: false,
            },
        ),
        (
            vec![
                SERVER,
                "--execute",
                "--topic",
                "orders",
                "--replication-factor",
                "2",
                "--yes",
            ],
            Action::ReplicationFactor {
                topic: "orders".into(),
                replication_factor: 2,
                execute: true,
            },
        ),
    ];
    for (argv, expected) in cases {
        check!(validate(&argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn invalid_option_combinations_fail_with_kafkas_messages() {
    let one_action = "Command must include exactly one action: --generate, --execute, --verify, \
                      --cancel, --list";
    let cases = [
        (vec![SERVER], one_action),
        (vec![SERVER, "--list", "--generate"], one_action),
        (
            vec!["--list"],
            "Please specify either --bootstrap-server or --bootstrap-controller",
        ),
        (
            vec![SERVER, "--verify"],
            "Missing required argument \"[reassignment-json-file]\"",
        ),
        (
            vec![SERVER, "--generate", "--topics-to-move-json-file", "t.json"],
            "Missing required argument \"[broker-list]\"",
        ),
        (
            vec![
                SERVER,
                "--execute",
                "--reassignment-json-file",
                "r.json",
                "--preserve-throttles",
            ],
            "Option \"[preserve-throttles]\" can't be used with action \"[execute]\"",
        ),
        (
            vec![
                SERVER,
                "--cancel",
                "--reassignment-json-file",
                "r.json",
                "--throttle",
                "5",
            ],
            "Option \"[throttle]\" can't be used with action \"[cancel]\"",
        ),
        (
            vec![SERVER, "--list", "--preserve-throttles"],
            "Option \"[preserve-throttles]\" can't be used with action \"[list]\"",
        ),
        (
            vec![
                SERVER,
                "--generate",
                "--broker-list",
                "1",
                "--topics-to-move-json-file",
                "t.json",
                "--reassignment-json-file",
                "r.json",
            ],
            "Option \"[reassignment-json-file]\" can't be used with action \"[generate]\"",
        ),
        (
            vec![
                SERVER,
                "--verify",
                "--reassignment-json-file",
                "r.json",
                "--additional",
            ],
            "Option \"[additional]\" can't be used with action \"[verify]\"",
        ),
        (
            vec![
                CONTROLLER,
                "--execute",
                "--reassignment-json-file",
                "r.json",
            ],
            "Option \"[bootstrap-controller]\" can't be used with action \"[execute]\"",
        ),
        (
            vec![CONTROLLER, "--verify", "--reassignment-json-file", "r.json"],
            "Option \"[bootstrap-controller]\" can't be used with action \"[verify]\"",
        ),
        (
            vec![SERVER, "--list", "--dry-run"],
            "--dry-run and --yes are only valid with --execute or --cancel",
        ),
        (
            vec![SERVER, "--verify", "--topic", "orders"],
            "--topic and --replication-factor must be given together",
        ),
        (
            vec![
                SERVER,
                "--list",
                "--topic",
                "orders",
                "--replication-factor",
                "2",
            ],
            "--topic and --replication-factor take only --execute or --verify and the broker \
             connection flags",
        ),
    ];
    for (argv, message) in cases {
        check!(validate(&argv) == Err(message.to_owned()), "{argv:?}");
    }
}

#[test]
fn both_bootstrap_flags_are_refused_when_parsed() {
    let both = Command::try_parse_from(["reassign-partitions", SERVER, CONTROLLER, "--list"]);
    check!(both.is_err());
}

#[test]
fn an_unknown_broker_is_the_first_in_kafkas_hash_set_order() {
    let node = |id| ClusterNode {
        id,
        host: "h".into(),
        port: 9092,
        rack: None,
        is_fenced: false,
    };
    let nodes = [node(1), node(2)];
    let proposed = |replicas: Vec<i32>| BTreeMap::from([(TopicPartition::new("foo", 0), replicas)]);
    let cases = [
        (vec![2, 1], Ok(())),
        (vec![1, 18, 3], Err("Unknown broker id 18".to_owned())),
        (vec![17, 5], Err("Unknown broker id 17".to_owned())),
    ];
    for (replicas, expected) in cases {
        check!(unknown_broker(&proposed(replicas), &nodes) == expected);
    }
}

#[test]
fn timeouts_read_as_milliseconds_or_with_a_unit() {
    check!(timeout_ms("10000") == Ok(Time::from_millis(10_000)));
    check!(timeout_ms("30s") == Ok(Time::from_millis(30_000)));
    check!(timeout_ms("soon").is_err());
    check!(parse(&[SERVER, "--list"]).connection.timeout == Time::from_millis(10_000));
}

#[test]
fn missing_topics_and_partitions_fail_with_kafkas_messages() {
    let found = BTreeMap::from([(TopicPartition::new("foo", 0), vec![1])]);
    check!(
        missing_topic(
            &BTreeSet::from(["foo".to_owned(), "nope".to_owned()]),
            &found
        ) == Err(
            "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Topic nope \
                    not found."
                .into()
        )
    );
    check!(
        missing_partitions(
            &[
                TopicPartition::new("foo", 9),
                TopicPartition::new("foo", 8),
                TopicPartition::new("foo", 0)
            ],
            &found
        ) == Err(
            "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Unable to \
                  find partition: foo-8, foo-9"
                .into()
        )
    );
    check!(missing_partitions(&[TopicPartition::new("foo", 0)], &found) == Ok(()));
}
