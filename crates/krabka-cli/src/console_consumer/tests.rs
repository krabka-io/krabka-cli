use assert2::assert;
use clap::Parser as _;

use super::*;
use crate::{Cli, Command};

fn parse(command_line: &[&str]) -> ConsoleConsumerArgs {
    let cli = Cli::try_parse_from([&["krabka", "console-consumer"][..], command_line].concat())
        .expect("the command line parses");
    let Command::ConsoleConsumer(args) = cli.command else {
        panic!("expected console-consumer");
    };
    args
}

/// The arguments with nothing given but the defaults that clap fills in.
fn defaults() -> ConsoleConsumerArgs {
    ConsoleConsumerArgs {
        formatter: DEFAULT_FORMATTER.to_owned(),
        ..ConsoleConsumerArgs::default()
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn properties(pairs: &[(&str, &str)]) -> Properties {
    let mut properties = Properties::default();
    for (key, value) in pairs {
        properties.insert(*key, *value);
    }
    properties
}

#[test]
fn every_kafka_flag_parses_into_the_args() {
    let cases = [
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--from-beginning",
                "--max-messages",
                "1",
            ],
            ConsoleConsumerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                from_beginning: true,
                max_messages: Some(1),
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--partition",
                "0",
                "--offset",
                "earliest",
                "--timeout-ms",
                "500",
                "--isolation-level",
                "read_committed",
                "--skip-message-on-error",
                "--enable-systest-events",
            ],
            ConsoleConsumerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                partition: Some(0),
                offset: Some("earliest".into()),
                timeout_ms: Some(500),
                isolation_level: Some("read_committed".into()),
                skip_message_on_error: true,
                enable_systest_events: true,
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--include",
                "orders.*",
                "--group",
                "g",
                "--consumer-property",
                "a=1",
                "--consumer-property",
                "b=2",
                "--consumer.config",
                "/c.properties",
                "--property",
                "print.key=true",
                "--formatter",
                "NoOpMessageFormatter",
                "--key-deserializer",
                "K",
                "--value-deserializer",
                "V",
            ],
            ConsoleConsumerArgs {
                bootstrap_server: Some("h:9092".into()),
                include: Some("orders.*".into()),
                group: Some("g".into()),
                consumer_property: strings(&["a=1", "b=2"]),
                consumer_config: Some("/c.properties".into()),
                property: strings(&["print.key=true"]),
                formatter: "NoOpMessageFormatter".into(),
                key_deserializer: Some("K".into()),
                value_deserializer: Some("V".into()),
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--command-property",
                "x=y",
                "--command-config",
                "/p",
                "--formatter-property",
                "print.offset=true",
                "--formatter-config",
                "/f",
                "--partition",
                "-1",
                "--offset",
                "-5",
                "--max-messages",
                "-1",
                "--timeout-ms",
                "-1",
            ],
            ConsoleConsumerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                command_property: strings(&["x=y"]),
                command_config: Some("/p".into()),
                formatter_property: strings(&["print.offset=true"]),
                formatter_config: Some("/f".into()),
                partition: Some(-1),
                offset: Some("-5".into()),
                max_messages: Some(-1),
                timeout_ms: Some(-1),
                ..defaults()
            },
        ),
    ];
    for (argv, expected) in cases {
        assert!(parse(&argv) == expected, "{argv:?}");
    }
}

/// The refusals and their messages are the ones `kafka-console-consumer`
/// 4.3.1 printed for the same command lines.
#[test]
fn a_bad_command_line_is_refused_as_the_jvm_tool_refuses_it() {
    let usage = |message: &str| Err(Refusal::Usage(message.to_owned()));
    let cases = [
        (vec!["--topic", "t"], usage("Missing required argument \"[bootstrap-server]\"")),
        (
            vec!["--bootstrap-server", "h:1"],
            usage("Exactly one of the following arguments is required: [topic], [include]"),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--include", "x"],
            usage("Exactly one of the following arguments is required: [topic], [include]"),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--include", "x", "--partition", "0"],
            usage("The topic is required when partition is specified."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--offset", "3"],
            usage("The partition is required when offset is specified."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--partition", "0", "--offset", "3", "--from-beginning"],
            usage("Options from-beginning and offset cannot be specified together."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--partition", "0", "--offset", "-3"],
            usage("The provided offset value '-3' is incorrect. Valid values are 'earliest', 'latest', or a non-negative long."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--partition", "0", "--offset", "soon"],
            usage("The provided offset value 'soon' is incorrect. Valid values are 'earliest', 'latest', or a non-negative long."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--partition", "0", "--group", "g"],
            usage("Options group and partition cannot be specified together."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--group", "g", "--command-property", "group.id=h"],
            usage("The group ids provided in different places (directly using '--group', via '--consumer-property', or via '--consumer.config') do not match. Detected group ids: 'g', 'h'"),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--consumer-property", "a=1", "--command-property", "b=2"],
            usage("Options --consumer-property and --command-property cannot be specified together."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--consumer.config", "/a", "--command-config", "/b"],
            usage("Options --consumer.config and --command-config cannot be specified together."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--property", "a=1", "--formatter-property", "b=2"],
            usage("Options --property and --formatter-property cannot be specified together."),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--from-beginning", "--command-property", "auto.offset.reset=latest"],
            Err(Refusal::Failure("Can't simultaneously specify --from-beginning and 'auto.offset.reset=latest', please remove one option".into())),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--include", "("],
            Err(Refusal::Failure(java_pattern("(").unwrap_err())),
        ),
        (
            vec!["--bootstrap-server", "h:1", "--topic", "t", "--formatter", "com.example.Mine"],
            usage("com.example.Mine: no such formatter; krabka builds in DefaultMessageFormatter, LoggingMessageFormatter and NoOpMessageFormatter"),
        ),
    ];
    for (argv, expected) in cases {
        let actual = parse(&argv).plan().map(|_| ());
        assert!(actual == expected, "{argv:?}");
    }
}

#[test]
fn a_subscribed_run_resolves_the_properties_as_build_consumer_props_does() {
    let args = parse(&[
        "--bootstrap-server",
        "a:1,b:2",
        "--topic",
        "t",
        "--from-beginning",
        "--consumer-property",
        "client.id=mine",
        "--max-messages",
        "3",
        "--timeout-ms",
        "-4",
        "--property",
        "print.key=true",
    ]);
    let (formatter, _) =
        Formatter::new(DEFAULT_FORMATTER, &properties(&[("print.key", "true")])).unwrap();
    assert!(
        args.plan()
            == Ok(Plan {
                bootstrap: strings(&["a:1", "b:2"]),
                source: Source::Topic("t".into()),
                properties: properties(&[
                    ("auto.offset.reset", "earliest"),
                    ("client.id", "mine"),
                    ("enable.auto.commit", "false"),
                    ("isolation.level", "read_uncommitted"),
                ]),
                group: None,
                max_messages: 3,
                timeout_ms: None,
                skip_message_on_error: false,
                formatter,
                formatter_warnings: Vec::new(),
                systest_events: false,
                warnings: strings(&[
                    "Option --consumer-property is deprecated and will be removed in a future version. Use --command-property instead.",
                    "Option --property is deprecated and will be removed in a future version. Use --formatter-property instead.",
                ]),
            })
    );
}

#[test]
fn a_partition_run_starts_where_the_offset_flags_say() {
    let cases = [
        (vec!["--offset", "earliest"], StartOffset::Earliest),
        (vec!["--offset", "LATEST"], StartOffset::Latest),
        (vec!["--offset", "17"], StartOffset::At(17)),
        (vec!["--from-beginning"], StartOffset::Earliest),
        (vec![], StartOffset::Latest),
    ];
    for (flags, offset) in cases {
        let argv = [
            &[
                "--bootstrap-server",
                "h:1",
                "--topic",
                "t",
                "--partition",
                "2",
            ][..],
            &flags,
        ]
        .concat();
        let plan = parse(&argv).plan().expect("valid");
        assert!(
            plan.source
                == Source::Partition {
                    topic: "t".into(),
                    partition: 2,
                    offset,
                },
            "{flags:?}"
        );
    }
}

#[test]
fn a_named_group_is_kept_and_auto_commit_stays_on() {
    let plan = parse(&[
        "--bootstrap-server",
        "h:1",
        "--topic",
        "t",
        "--group",
        "g",
        "--command-property",
        "group.id=g",
    ])
    .plan()
    .expect("valid");
    assert!(
        (plan.group, plan.properties)
            == (
                Some("g".to_owned()),
                properties(&[
                    ("auto.offset.reset", "latest"),
                    ("client.id", "console-consumer"),
                    ("group.id", "g"),
                    ("isolation.level", "read_uncommitted"),
                ])
            )
    );
}

/// The JVM spells the isolation levels with underscores, and the client
/// with hyphens; the command takes the JVM spelling.
#[test]
fn isolation_levels_map_to_the_client_variants() {
    let settings = |level: &str| {
        let plan = parse(&[
            "--bootstrap-server",
            "h:1",
            "--topic",
            "t",
            "--isolation-level",
            level,
        ])
        .plan()
        .expect("valid");
        ClientSettings::from_properties(&plan.properties, "g".into())
            .map(|settings| settings.isolation_level)
    };
    assert!(settings("read_committed") == Ok(IsolationLevel::ReadCommitted));
    assert!(settings("read_uncommitted") == Ok(IsolationLevel::ReadUncommitted));
    assert!(
        settings("read-committed")
            == Err("Invalid value read-committed for configuration isolation.level: String must be one of: read_committed, read_uncommitted".into())
    );
}

#[test]
fn client_settings_read_the_consumer_properties() {
    let set = properties(&[
        ("auto.offset.reset", "none"),
        ("isolation.level", "read_committed"),
        ("enable.auto.commit", "FALSE"),
        ("auto.commit.interval.ms", "100"),
        ("session.timeout.ms", "10000"),
        ("heartbeat.interval.ms", "1000"),
        ("max.poll.interval.ms", "20000"),
        ("fetch.min.bytes", "2"),
        ("fetch.max.bytes", "4096"),
        ("max.partition.fetch.bytes", "1024"),
        ("fetch.max.wait.ms", "250"),
        ("group.instance.id", "i-1"),
        ("client.rack", "r1"),
        (
            "partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor, org.apache.kafka.clients.consumer.RangeAssignor",
        ),
        ("group.protocol", "CLASSIC"),
    ]);
    assert!(
        ClientSettings::from_properties(&set, "g".into())
            == Ok(ClientSettings {
                group_id: "g".into(),
                auto_offset_reset: OffsetReset::None,
                isolation_level: IsolationLevel::ReadCommitted,
                enable_auto_commit: false,
                auto_commit_interval_ms: 100,
                session_timeout_ms: 10_000,
                heartbeat_interval_ms: 1_000,
                rebalance_timeout_ms: 20_000,
                fetch_min_bytes: 2,
                fetch_max_bytes: 4_096,
                max_partition_fetch_bytes: 1_024,
                fetch_max_wait_ms: 250,
                group_instance_id: Some("i-1".into()),
                client_rack: Some("r1".into()),
                assignors: vec![Assignor::CooperativeSticky, Assignor::Range],
            })
    );
}

#[test]
fn unusable_consumer_properties_are_refused() {
    let cases = [
        (
            ("auto.offset.reset", "sometimes"),
            "Invalid value sometimes for configuration auto.offset.reset: Invalid value `sometimes` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.",
        ),
        (
            ("auto.offset.reset", "by_duration:PT1H"),
            "auto.offset.reset=by_duration:PT1H is not supported by this build: the by_duration strategy (KIP-1106) needs AutoOffsetReset::ByDuration from a newer krabka-client-consumer",
        ),
        (
            ("group.protocol", "consumer"),
            "group.protocol=consumer is not supported by this build: the KIP-848 consumer protocol needs a newer krabka-client-consumer",
        ),
        (
            ("group.protocol", "other"),
            "Invalid value other for configuration group.protocol: String must be one of (case insensitive): CLASSIC, CONSUMER",
        ),
        (
            ("session.timeout.ms", "soon"),
            "Invalid value soon for configuration session.timeout.ms: Not a number of type INT",
        ),
        (
            ("enable.auto.commit", "yes"),
            "Invalid value yes for configuration enable.auto.commit: Expected value to be either true or false",
        ),
        (
            ("partition.assignment.strategy", "com.example.Mine"),
            "partition.assignment.strategy=com.example.Mine is not supported; krabka implements RangeAssignor, RoundRobinAssignor, StickyAssignor and CooperativeStickyAssignor",
        ),
    ];
    for ((key, value), expected) in cases {
        assert!(
            ClientSettings::from_properties(&properties(&[(key, value)]), "g".into())
                == Err(expected.to_owned()),
            "{key}={value}"
        );
    }
}

#[test]
fn a_generated_group_is_console_consumer_and_a_number_below_100000() {
    let group = generated_group();
    let number = group
        .strip_prefix("console-consumer-")
        .and_then(|number| number.parse::<u32>().ok());
    assert!(number.is_some_and(|number| number < 100_000), "{group}");
}

#[test]
fn an_include_pattern_matches_whole_topic_names() {
    let pattern = java_pattern("orders|pay.*").unwrap();
    for (topic, expected) in [
        ("orders", true),
        ("payments", true),
        ("orders-v2", false),
        ("my-orders", false),
    ] {
        assert!(pattern.is_match(topic) == expected, "{topic}");
    }
}

#[test]
fn json_output_is_one_data_line_per_record() {
    let record = Record {
        topic: "t".into(),
        partition: 1,
        offset: 5,
        timestamp: 7,
        key: None,
        value: Some(b"v".to_vec()),
        headers: vec![("h".into(), Some(b"x".to_vec()))],
    };
    let (formatter, _) = Formatter::new(DEFAULT_FORMATTER, &Properties::default()).unwrap();
    let line = render(&formatter, &record, OutputFormat::Json).unwrap();
    let value: Value = serde_json::from_slice(&line).unwrap();
    assert!(
        value
            == json!({"data": {
                "topic": "t", "partition": 1, "offset": 5, "timestamp": 7,
                "key": null, "value": "v", "headers": [{"key": "h", "value": "x"}],
            }})
    );
    assert!(line.last() == Some(&b'\n'));
    assert!(render(&Formatter::NoOp, &record, OutputFormat::Json) == Ok(Vec::new()));
    assert!(render(&formatter, &record, OutputFormat::Human) == Ok(b"v\n".to_vec()));
}
