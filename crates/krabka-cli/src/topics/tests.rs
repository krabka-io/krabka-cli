use assert2::{assert, check};
use clap::Parser;

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    topics: TopicsArgs,
}

const BOOTSTRAP: [&str; 2] = ["--bootstrap-server", "broker:9092"];

fn parse(args: &[&str]) -> TopicsArgs {
    let command_line = std::iter::once("krabka-topics").chain(args.iter().copied());
    Command::try_parse_from(command_line)
        .unwrap_or_else(|error| panic!("{args:?}: {error}"))
        .topics
}

fn plan(args: &[&str]) -> Result<Plan, String> {
    parse(args).plan()
}

fn selection(patterns: &[&str], exclude_internal: bool, require_exists: bool) -> Selection {
    Selection {
        patterns: patterns
            .iter()
            .map(|pattern| (*pattern).to_owned())
            .collect(),
        exclude_internal,
        require_exists,
    }
}

fn plain(action: Action) -> Plan {
    Plan {
        action,
        notices: Vec::new(),
        delete_config_notice: false,
    }
}

#[test]
fn every_kafka_topics_flag_parses_into_the_whole_args_struct() {
    let args = parse(&[
        "--bootstrap-server",
        "a:1,b:2",
        "--command-config",
        "admin.properties",
        "--create",
        "--topic",
        "orders",
        "--partitions",
        "3",
        "--replication-factor",
        "2",
        "--config",
        "retention.ms=1",
        "--config",
        "cleanup.policy=compact",
        "--if-not-exists",
    ]);
    let expected = TopicsArgs {
        connection: ConnectionArgs {
            bootstrap_server: vec!["a:1".into(), "b:2".into()],
            bootstrap_controller: Vec::new(),
            command_config: Some("admin.properties".into()),
            client_id: None,
            request_timeout_ms: None,
            timeout: args.connection.timeout,
        },
        list: 0,
        create: 1,
        delete: 0,
        alter: 0,
        describe: 0,
        topic: vec!["orders".into()],
        topic_id: Vec::new(),
        config: vec!["retention.ms=1".into(), "cleanup.policy=compact".into()],
        delete_config: Vec::new(),
        partitions: vec!["3".into()],
        replication_factor: vec!["2".into()],
        replica_assignment: Vec::new(),
        under_replicated_partitions: 0,
        unavailable_partitions: 0,
        under_min_isr_partitions: 0,
        at_min_isr_partitions: 0,
        topics_with_overrides: 0,
        if_exists: false,
        if_not_exists: true,
        exclude_internal: false,
        partition_size_limit_per_response: Vec::new(),
        confirm: ConfirmArgs::default(),
    };
    assert!(args == expected);
}

#[test]
fn accepted_invocations_plan_the_request_kafka_topics_sends() {
    let describe_limited = |target, selectors: &[Selector], partition_limit| Action::Describe {
        target,
        selectors: Selectors(selectors.iter().copied().collect()),
        partition_limit,
    };
    let describe = |target, selectors: &[Selector]| {
        describe_limited(target, selectors, DEFAULT_PARTITION_LIMIT)
    };
    let cases: Vec<(Vec<&str>, Plan)> = vec![
        (
            vec!["--list"],
            plain(Action::List(selection(&[], false, false))),
        ),
        (
            vec!["--list", "--list", "--exclude-internal", "--topic", "ord.*"],
            plain(Action::List(selection(&["ord.*"], true, false))),
        ),
        (
            vec!["--list", "--topic=orders"],
            plain(Action::List(selection(&["orders"], false, false))),
        ),
        (
            vec!["--create", "--topic", "orders"],
            plain(Action::Create(Create {
                names: vec!["orders".into()],
                partitions: None,
                replication_factor: None,
                configs: BTreeMap::new(),
                replica_assignment: BTreeMap::new(),
                if_not_exists: false,
            })),
        ),
        (
            vec![
                "--create",
                "--topic",
                "orders",
                "--partitions",
                "3",
                "--replication-factor",
                "2",
                "--config",
                " retention.ms = 5 ",
                "--if-not-exists",
            ],
            plain(Action::Create(Create {
                names: vec!["orders".into()],
                partitions: Some(3),
                replication_factor: Some(2),
                configs: BTreeMap::from([("retention.ms".into(), "5".into())]),
                replica_assignment: BTreeMap::new(),
                if_not_exists: true,
            })),
        ),
        (
            vec![
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "4",
                "--if-exists",
            ],
            plain(Action::Alter {
                selection: selection(&["orders"], false, false),
                partitions: 4,
                assignment: None,
            }),
        ),
        (
            vec!["--delete", "--topic", "orders"],
            plain(Action::Delete(selection(&["orders"], false, true))),
        ),
        (
            vec!["--describe"],
            plain(describe(Target::Names(selection(&[], false, true)), &[])),
        ),
        (
            vec![
                "--describe",
                "--topic",
                "orders",
                "--under-replicated-partitions",
                "--unavailable-partitions",
                "--under-min-isr-partitions",
                "--at-min-isr-partitions",
                "--partition-size-limit-per-response",
                "5",
            ],
            plain(describe_limited(
                Target::Names(selection(&["orders"], false, true)),
                &[
                    Selector::UnderReplicated,
                    Selector::Unavailable,
                    Selector::UnderMinIsr,
                    Selector::AtMinIsr,
                ],
                5,
            )),
        ),
        (
            vec![
                "--describe",
                "--topics-with-overrides",
                "--exclude-internal",
            ],
            plain(describe(
                Target::Names(selection(&[], true, true)),
                &[Selector::TopicsWithOverrides],
            )),
        ),
        // The zero topic ID falls back to the names.
        (
            vec!["--describe", "--topic-id", "AAAAAAAAAAAAAAAAAAAAAA"],
            plain(describe(Target::Names(selection(&[], false, true)), &[])),
        ),
        (
            vec![
                "--describe",
                "--topic-id",
                "4IgIMEgZQYS5IcnSzjHAqw",
                "--topic",
                "orders",
                "--if-exists",
            ],
            Plan {
                action: describe(
                    Target::Id {
                        id: [
                            0xe0, 0x88, 0x08, 0x30, 0x48, 0x19, 0x41, 0x84, 0xb9, 0x21, 0xc9, 0xd2,
                            0xce, 0x31, 0xc0, 0xab,
                        ],
                        exclude_internal: false,
                        require_exists: false,
                    },
                    &[],
                ),
                notices: vec![TOPIC_ID_NOTICE.into()],
                delete_config_notice: false,
            },
        ),
        (
            vec!["--list", "--delete-config", "retention.ms"],
            Plan {
                action: Action::List(selection(&[], false, false)),
                notices: Vec::new(),
                delete_config_notice: true,
            },
        ),
        // krabka accepts `--topic` more than once; `kafka-topics` refuses it.
        (
            vec!["--create", "--topic", "a", "--topic", "b"],
            plain(Action::Create(Create {
                names: vec!["a".into(), "b".into()],
                partitions: None,
                replication_factor: None,
                configs: BTreeMap::new(),
                replica_assignment: BTreeMap::new(),
                if_not_exists: false,
            })),
        ),
        (
            vec!["--create", "--topic", "orders", "--dry-run"],
            plain(Action::Create(Create {
                names: vec!["orders".into()],
                partitions: None,
                replication_factor: None,
                configs: BTreeMap::new(),
                replica_assignment: BTreeMap::new(),
                if_not_exists: false,
            })),
        ),
    ];
    check_plans(cases);
}

fn check_plans(cases: Vec<(Vec<&str>, Plan)>) {
    for (args, expected) in cases {
        let argv = BOOTSTRAP
            .iter()
            .copied()
            .chain(args.iter().copied())
            .collect::<Vec<_>>();
        check!(plan(&argv) == Ok(expected), "{args:?}");
    }
}

#[test]
fn replica_assignments_plan_the_assignment_kafka_topics_sends() {
    let cases: Vec<(Vec<&str>, Plan)> = vec![
        (
            vec![
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "3",
                "--replica-assignment",
                "1:2,2:3,3:1",
            ],
            plain(Action::Alter {
                selection: selection(&["orders"], false, true),
                partitions: 3,
                assignment: Some(vec![vec![1, 2], vec![2, 3], vec![3, 1]]),
            }),
        ),
        // An empty assignment is no assignment, as in Kafka.
        (
            vec![
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "3",
                "--replica-assignment",
                "",
            ],
            plain(Action::Alter {
                selection: selection(&["orders"], false, true),
                partitions: 3,
                assignment: None,
            }),
        ),
        (
            vec![
                "--create",
                "--topic",
                "orders",
                "--replica-assignment",
                "1:2, 2:3",
            ],
            plain(Action::Create(Create {
                names: vec!["orders".into()],
                partitions: None,
                replication_factor: None,
                configs: BTreeMap::new(),
                replica_assignment: BTreeMap::from([(0, vec![1, 2]), (1, vec![2, 3])]),
                if_not_exists: false,
            })),
        ),
    ];
    check_plans(cases);
}

#[test]
fn rejected_option_combinations_fail_with_the_kafka_topics_message() {
    let multiple = |option: &str| {
        format!("Found multiple arguments for option {option}, but you asked for only one")
    };
    let cannot = |a: &str, b: &str| format!("Option \"[{a}]\" can't be used with option \"[{b}]\"");
    let cases: Vec<(Vec<&str>, String)> = vec![
        (vec![], ACTIONS.into()),
        (vec!["--list", "--describe"], ACTIONS.into()),
        (
            vec!["--create"],
            "Missing required argument \"[topic]\"".into(),
        ),
        (
            vec!["--delete"],
            "Missing required argument \"[topic]\"".into(),
        ),
        (
            vec!["--alter", "--topic", "orders"],
            "Missing required argument \"[partitions]\"".into(),
        ),
        (
            vec![
                "--alter",
                "--topic",
                "orders",
                "--partitions",
                "5",
                "--config",
                "a=b",
            ],
            format!(
                "Option combination \"[[bootstrap-server], [config]]\" can't be used with option \
                 \"[alter]\"{ALTER_CONFIGS_HINT}"
            ),
        ),
        (
            vec!["--describe", "--if-exists"],
            "--topic or --topic-id is required to describe a topic".into(),
        ),
        (
            vec!["--describe", "--config", "a=b"],
            cannot("config", "describe"),
        ),
        (
            vec!["--describe", "--partitions", "3"],
            cannot("partitions", "describe"),
        ),
        (
            vec![
                "--alter",
                "--topic",
                "t",
                "--partitions",
                "3",
                "--replication-factor",
                "1",
            ],
            cannot("replication-factor", "alter"),
        ),
        (
            vec!["--list", "--replica-assignment", "1"],
            cannot("replica-assignment", "list"),
        ),
        (
            vec![
                "--create",
                "--topic",
                "t",
                "--partitions",
                "1",
                "--replica-assignment",
                "1",
            ],
            cannot("replica-assignment", "partitions"),
        ),
        (
            vec![
                "--create",
                "--topic",
                "t",
                "--replication-factor",
                "1",
                "--replica-assignment",
                "1",
            ],
            cannot("replica-assignment", "replication-factor"),
        ),
        (
            vec!["--list", "--under-replicated-partitions"],
            cannot("under-replicated-partitions", "list"),
        ),
        (
            vec![
                "--describe",
                "--unavailable-partitions",
                "--topics-with-overrides",
            ],
            cannot("unavailable-partitions", "topics-with-overrides"),
        ),
        (
            vec![
                "--describe",
                "--topics-with-overrides",
                "--at-min-isr-partitions",
            ],
            cannot("at-min-isr-partitions", "topics-with-overrides"),
        ),
        (
            vec!["--create", "--topic", "t", "--if-exists"],
            cannot("if-exists", "create"),
        ),
        (
            vec!["--list", "--if-not-exists"],
            cannot("if-not-exists", "list"),
        ),
        (
            vec!["--delete", "--topic", "t", "--exclude-internal"],
            cannot("exclude-internal", "delete"),
        ),
        (
            vec![
                "--alter",
                "--topic",
                "t",
                "--partitions",
                "5",
                "--partitions",
                "6",
            ],
            multiple("partitions"),
        ),
    ];
    for (args, expected) in cases {
        let argv = BOOTSTRAP
            .iter()
            .copied()
            .chain(args.iter().copied())
            .collect::<Vec<_>>();
        check!(plan(&argv) == Err(expected), "{args:?}");
    }
    check!(plan(&["--list"]) == Err("--bootstrap-server must be specified".into()));
}

#[test]
fn rejected_option_values_fail_with_the_kafka_topics_message() {
    let cases: Vec<(Vec<&str>, String)> = vec![
        (
            vec!["--create", "--topic", "t", "--partitions", "abc"],
            "Cannot parse argument 'abc' of option partitions".into(),
        ),
        (
            vec!["--describe", "--partition-size-limit-per-response", "x"],
            "Cannot parse argument 'x' of option partition-size-limit-per-response".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--partitions", "0"],
            "The partitions must be greater than 0".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--partitions", "-1"],
            "The partitions must be greater than 0".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--replication-factor", "40000"],
            "The replication factor must be between 1 and 32767 inclusive".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--replication-factor", "-1"],
            "The replication factor must be between 1 and 32767 inclusive".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--config", "retention.ms"],
            "requirement failed: Invalid topic config: all configs to be added must be in the \
             format \"key=val\"."
                .into(),
        ),
        (
            vec!["--create", "--topic", "t", "--config", "foo=bar"],
            "Unknown topic config name: foo".into(),
        ),
        (
            vec!["--create", "--topic", "t", "--replica-assignment", "1:1"],
            "Partition replica lists may not contain duplicate entries: 1".into(),
        ),
        (
            vec!["--describe", "--topic-id", "nonsense"],
            "Input string `nonsense` decoded as 6 bytes, which is not equal to the expected 16 \
             bytes of a base64-encoded UUID"
                .into(),
        ),
        (
            vec!["--list", "--topic", "["],
            "[ is an invalid regex.".into(),
        ),
        (
            vec!["--list", "--dry-run"],
            "--dry-run is only valid with --create or --delete".into(),
        ),
        (
            vec![
                "--alter",
                "--topic",
                "t",
                "--partitions",
                "3",
                "--replica-assignment",
                "1,2:3",
            ],
            "Partition 1 has different replication factor: [2, 3]".into(),
        ),
    ];
    for (args, expected) in cases {
        let argv = BOOTSTRAP
            .iter()
            .copied()
            .chain(args.iter().copied())
            .collect::<Vec<_>>();
        check!(plan(&argv) == Err(expected), "{args:?}");
    }
}

#[test]
fn a_flag_that_kafka_topics_does_not_know_is_a_parse_error() {
    let argv = ["krabka-topics", "--list", "--bogus"];
    assert!(Command::try_parse_from(argv).is_err());
}

#[test]
fn topic_patterns_resolve_as_kafka_topics_resolves_them() {
    let all = [
        "__consumer_offsets",
        "audit",
        "my.topic_x",
        "orders",
        "orders-dlq",
    ]
    .map(str::to_owned)
    .to_vec();
    let cases: Vec<(Selection, Result<Vec<&str>, String>)> = vec![
        (
            selection(&[], false, false),
            Ok(all.iter().map(String::as_str).collect()),
        ),
        (
            selection(&[], true, false),
            Ok(vec!["audit", "my.topic_x", "orders", "orders-dlq"]),
        ),
        // The pattern matches the whole name.
        (selection(&["orders"], false, true), Ok(vec!["orders"])),
        (
            selection(&["ord.*"], false, true),
            Ok(vec!["orders", "orders-dlq"]),
        ),
        // `,` reads as `|`, and spaces and surrounding quotes go.
        (
            selection(&["'audit, orders'"], false, true),
            Ok(vec!["audit", "orders"]),
        ),
        (selection(&["__.*"], true, false), Ok(vec![])),
        (
            selection(&["__.*"], true, true),
            Err("Topic '__.*' does not exist as expected".into()),
        ),
        (
            selection(&["missing"], false, true),
            Err("Topic 'missing' does not exist as expected".into()),
        ),
        (selection(&["missing"], false, false), Ok(vec![])),
        // An empty pattern matches nothing and is never an error.
        (selection(&[""], false, true), Ok(vec![])),
        (
            selection(&["audit", "orders"], false, true),
            Ok(vec!["audit", "orders"]),
        ),
    ];
    for (selection, expected) in cases {
        let expected = expected.map(|names| names.into_iter().map(str::to_owned).collect());
        check!(resolve(&all, &selection) == expected, "{selection:?}");
    }
}

#[test]
fn a_per_topic_error_prints_the_kafka_exception_message() {
    let error = |code, name, message: Option<&str>| KafkaError {
        code,
        name,
        message: message.map(str::to_owned),
    };
    let cases = [
        (
            error(
                36,
                "TOPIC_ALREADY_EXISTS",
                Some("Topic 'orders' already exists."),
            ),
            "Error while executing topic command : Topic 'orders' already exists.",
        ),
        (
            error(29, "TOPIC_AUTHORIZATION_FAILED", None),
            "Error while executing topic command : Topic authorization failed.",
        ),
        (
            error(37, "INVALID_PARTITIONS", Some("")),
            "Error while executing topic command : Number of partitions is below 1.",
        ),
        // `Errors.forCode` maps a code it does not know to
        // `UNKNOWN_SERVER_ERROR`.
        (
            error(999, "UNKNOWN", None),
            "Error while executing topic command : The server experienced an unexpected error \
             when processing the request.",
        ),
    ];
    for (error, expected) in cases {
        check!(failure_line(&error) == expected, "{error:?}");
    }
}
