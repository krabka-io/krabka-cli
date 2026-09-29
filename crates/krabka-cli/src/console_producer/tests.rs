use assert2::assert;
use clap::Parser as _;

use super::*;
use crate::{Cli, Command};

fn parse(command_line: &[&str]) -> ConsoleProducerArgs {
    let cli = Cli::try_parse_from([&["krabka", "console-producer"][..], command_line].concat())
        .expect("the command line parses");
    let Command::ConsoleProducer(args) = cli.command else {
        panic!("expected console-producer");
    };
    args
}

fn defaults() -> ConsoleProducerArgs {
    ConsoleProducerArgs {
        line_reader: LINE_READER.to_owned(),
        ..ConsoleProducerArgs::default()
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
            vec!["--bootstrap-server", "h:9092", "--topic", "t"],
            ConsoleProducerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--sync",
                "--compression-codec",
                "--batch-size",
                "100",
                "--request-required-acks",
                "all",
                "--timeout",
                "5",
                "--max-block-ms",
                "7",
                "--request-timeout-ms",
                "9",
                "--message-send-max-retries",
                "2",
                "--retry-backoff-ms",
                "11",
                "--metadata-expiry-ms",
                "13",
                "--max-memory-bytes",
                "15",
                "--socket-buffer-size",
                "17",
                "--max-partition-memory-bytes",
                "19",
            ],
            ConsoleProducerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                sync: true,
                compression_codec: Some(String::new()),
                batch_size: Some(100),
                request_required_acks: Some("all".into()),
                timeout: Some(5),
                max_block_ms: Some(7),
                request_timeout_ms: Some(9),
                message_send_max_retries: Some(2),
                retry_backoff_ms: Some(11),
                metadata_expiry_ms: Some(13),
                max_memory_bytes: Some(15),
                socket_buffer_size: Some(17),
                max_partition_memory_bytes: Some(19),
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--compression-codec",
                "zstd",
                "--property",
                "parse.key=true",
                "--property",
                "key.separator=:",
                "--producer-property",
                "acks=1",
                "--producer.config",
                "/p",
                "--reader-config",
                "/r",
                "--line-reader",
                "org.apache.kafka.tools.LineMessageReader",
            ],
            ConsoleProducerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                compression_codec: Some("zstd".into()),
                property: strings(&["parse.key=true", "key.separator=:"]),
                producer_property: strings(&["acks=1"]),
                producer_config: Some("/p".into()),
                reader_config: Some("/r".into()),
                ..defaults()
            },
        ),
        (
            vec![
                "--bootstrap-server",
                "h:9092",
                "--topic",
                "t",
                "--reader-property",
                "a=b",
                "--command-property",
                "c=d",
                "--command-config",
                "/c",
                "--request-required-acks",
                "-1",
            ],
            ConsoleProducerArgs {
                bootstrap_server: Some("h:9092".into()),
                topic: Some("t".into()),
                reader_property: strings(&["a=b"]),
                command_property: strings(&["c=d"]),
                command_config: Some("/c".into()),
                request_required_acks: Some("-1".into()),
                ..defaults()
            },
        ),
    ];
    for (argv, expected) in cases {
        assert!(parse(&argv) == expected, "{argv:?}");
    }
}

#[test]
fn a_bad_command_line_is_refused_as_the_jvm_tool_refuses_it() {
    let usage = |message: &str| Err(Refusal::Usage(message.to_owned()));
    let cases = [
        (vec!["--bootstrap-server", "h:1"], usage("Missing required argument \"[topic]\"")),
        (vec!["--topic", "t"], usage("Error while validating the bootstrap address")),
        (
            vec!["--topic", "t", "--bootstrap-server", "h"],
            usage("Please provide valid host:port like host1:9091,host2:9092"),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1,nope"],
            usage("Please provide valid host:port like host1:9091,host2:9092"),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1", "--command-config", "/a", "--producer.config", "/b"],
            usage("Options --command-config and --producer.config cannot be specified together."),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1", "--command-property", "a=1", "--producer-property", "b=2"],
            usage("Options --command-property and --producer-property cannot be specified together."),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1", "--reader-property", "a=1", "--property", "b=2"],
            usage("Options --reader-property and --property cannot be specified together."),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1", "--line-reader", "com.example.Reader"],
            Err(Refusal::Failure("com.example.Reader: no such reader; krabka builds in org.apache.kafka.tools.LineMessageReader".into())),
        ),
        (
            vec!["--topic", "t", "--bootstrap-server", "h:1", "--reader-property", "null.marker=:"],
            Err(Refusal::Failure("null.marker and headers.key.separator may not be equal".into())),
        ),
    ];
    for (argv, expected) in cases {
        assert!(parse(&argv).plan().map(|_| ()) == expected, "{argv:?}");
    }
}

#[test]
fn bootstrap_entries_are_checked_as_utils_get_port_checks_them() {
    let cases = [
        ("h:1", true),
        ("PLAINTEXT://h.example:9092", true),
        ("[::1]:9092", true),
        ("10.0.0.1:1", true),
        ("h", false),
        ("h:", false),
        ("h:x", false),
        ("h_b@c:1", false),
    ];
    for (address, expected) in cases {
        assert!(has_port(address) == expected, "{address}");
    }
}

#[test]
fn the_producer_properties_merge_as_producer_props_does() {
    let args = parse(&[
        "--bootstrap-server",
        "h:1",
        "--topic",
        "t",
        "--producer-property",
        "linger.ms=9",
        "--producer-property",
        "acks=1",
        "--request-required-acks",
        "0",
        "--compression-codec",
    ]);
    let plan = args.plan().expect("valid");
    assert!(
        (plan.properties, plan.warnings)
            == (
                properties(&[
                    ("acks", "0"),
                    ("batch.size", "16384"),
                    ("bootstrap.servers", "h:1"),
                    ("buffer.memory", "33554432"),
                    ("client.id", "console-producer"),
                    ("compression.type", "gzip"),
                    ("linger.ms", "9"),
                    ("max.block.ms", "60000"),
                    ("metadata.max.age.ms", "300000"),
                    ("request.timeout.ms", "1500"),
                    ("retries", "3"),
                    ("retry.backoff.ms", "100"),
                    ("send.buffer.bytes", "102400"),
                ]),
                strings(&[
                    "Warning: --producer-property is deprecated and will be removed in a future version. Use --command-property instead."
                ])
            )
    );
}

#[test]
fn the_reader_takes_the_reader_properties_over_the_topic() {
    let plan = parse(&[
        "--bootstrap-server",
        "h:1",
        "--topic",
        "t",
        "--property",
        "parse.key=true",
        "--property",
        "key.separator=:",
    ])
    .plan()
    .expect("valid");
    let expected = LineReader::configure(
        &properties(&[
            ("topic", "t"),
            ("parse.key", "true"),
            ("key.separator", ":"),
        ]),
        "t",
    )
    .unwrap();
    assert!(plan.reader == expected);
}

#[test]
fn settings_follow_the_console_producer_defaults() {
    let plan = parse(&["--bootstrap-server", "h:1", "--topic", "t"])
        .plan()
        .expect("valid");
    assert!(
        Settings::from_properties(&plan.properties)
            == Ok(Settings {
                compression: Compression::None,
                acks: Acks::All,
                enable_idempotence: true,
                linger_ms: 1_000,
                batch_size: 16_384,
                request_timeout_ms: 1_500,
                retries: 3,
                retry_backoff_ms: 100,
                retry_backoff_max_ms: 1_000,
                max_block_ms: 60_000,
                delivery_timeout_ms: 120_000,
                buffer_memory: 33_554_432,
                max_request_size: 1_048_576,
                max_in_flight: 5,
                metadata_max_age_ms: 300_000,
                metadata_max_idle_ms: 300_000,
                enable_metrics_push: true,
                send_buffer: Some(102_400),
                receive_buffer: Some(32_768),
            })
    );
}

/// The memory, metadata and timing flags reach the producer settings, as
/// `producerProps` merges them into `buffer.memory`, `metadata.max.age.ms`
/// and `max.block.ms`.
#[test]
fn memory_and_metadata_flags_reach_the_producer_settings() {
    let argv = [
        "--bootstrap-server",
        "h:1",
        "--topic",
        "t",
        "--max-memory-bytes",
        "1048576",
        "--metadata-expiry-ms",
        "9000",
        "--max-block-ms",
        "250",
        "--command-property",
        "max.request.size=4096",
        "--command-property",
        "delivery.timeout.ms=5000",
        "--command-property",
        "retry.backoff.max.ms=300",
    ];
    let settings = Settings::from_properties(&parse(&argv).plan().unwrap().properties).unwrap();
    assert!(
        (
            settings.buffer_memory,
            settings.metadata_max_age_ms,
            settings.max_block_ms,
            settings.max_request_size,
            settings.delivery_timeout_ms,
            settings.retry_backoff_max_ms,
        ) == (1_048_576, 9_000, 250, 4_096, 5_000, 300)
    );
}

#[test]
fn acks_and_compression_map_to_the_client_values() {
    let cases = [
        (
            vec!["--request-required-acks", "1"],
            Acks::One,
            false,
            Compression::None,
        ),
        (
            vec!["--request-required-acks", "0"],
            Acks::Zero,
            false,
            Compression::None,
        ),
        (
            vec!["--request-required-acks", "all"],
            Acks::All,
            true,
            Compression::None,
        ),
        (
            vec!["--compression-codec", "lz4"],
            Acks::All,
            true,
            Compression::Lz4,
        ),
        (
            vec!["--compression-codec", "snappy"],
            Acks::All,
            true,
            Compression::Snappy,
        ),
        (
            vec!["--compression-codec"],
            Acks::All,
            true,
            Compression::Gzip,
        ),
    ];
    for (flags, acks, idempotent, compression) in cases {
        let argv = [&["--bootstrap-server", "h:1", "--topic", "t"][..], &flags].concat();
        let settings = Settings::from_properties(&parse(&argv).plan().unwrap().properties).unwrap();
        assert!(
            (
                settings.acks,
                settings.enable_idempotence,
                settings.compression
            ) == (acks, idempotent, compression),
            "{flags:?}"
        );
    }
}

#[test]
fn unusable_producer_properties_are_refused() {
    let cases: [(&[(&str, &str)], &str); 12] = [
        (
            &[("acks", "2")],
            "Invalid value 2 for configuration acks: String must be one of: all, -1, 0, 1",
        ),
        (
            &[("compression.type", "brotli")],
            "Invalid value brotli for configuration compression.type: String must be one of: none, gzip, snappy, lz4, zstd",
        ),
        (
            &[("linger.ms", "-1")],
            "Invalid value -1 for configuration linger.ms: Value must be at least 0",
        ),
        (
            &[("batch.size", "big")],
            "Invalid value big for configuration batch.size: Not a number of type INT",
        ),
        (
            &[("acks", "1"), ("enable.idempotence", "true")],
            "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.",
        ),
        (
            &[("retries", "0"), ("enable.idempotence", "true")],
            "Must set retries to non-zero when using the idempotent producer.",
        ),
        (
            &[("transactional.id", "tx")],
            "Cannot perform a 'send' before completing a call to initTransactions when transactions are enabled.",
        ),
        (
            &[("buffer.memory", "-1")],
            "Invalid value -1 for configuration buffer.memory: Value must be at least 0",
        ),
        (
            &[("metadata.max.idle.ms", "10")],
            "Invalid value 10 for configuration metadata.max.idle.ms: Value must be at least 5000",
        ),
        (
            &[("max.in.flight.requests.per.connection", "6")],
            "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6.",
        ),
        (
            &[("send.buffer.bytes", "-2")],
            "Invalid value -2 for configuration send.buffer.bytes: Value must be at least -1",
        ),
        (
            &[("receive.buffer.bytes", "small")],
            "Invalid value small for configuration receive.buffer.bytes: Not a number of type INT",
        ),
    ];
    for (pairs, expected) in cases {
        assert!(
            Settings::from_properties(&properties(pairs)) == Err(expected.to_owned()),
            "{pairs:?}"
        );
    }
}

/// The send and receive socket buffer sizes, `None` for -1.
type SocketBuffers = (Option<u64>, Option<u64>);

/// `--socket-buffer-size` and the `send.buffer.bytes` and
/// `receive.buffer.bytes` properties reach the producer's socket buffers,
/// with -1 keeping the operating system's buffer as Kafka's `-1` does.
#[test]
fn socket_buffer_sizes_reach_the_producer_settings() {
    let cases: [(&str, &[&str], SocketBuffers); 5] = [
        (
            "console-producer defaults",
            &[],
            (Some(102_400), Some(32_768)),
        ),
        (
            "--socket-buffer-size",
            &["--socket-buffer-size", "65536"],
            (Some(65_536), Some(32_768)),
        ),
        (
            "--socket-buffer-size -1",
            &["--socket-buffer-size", "-1"],
            (None, Some(32_768)),
        ),
        (
            "producer properties",
            &[
                "--command-property",
                "send.buffer.bytes=262144",
                "--command-property",
                "receive.buffer.bytes=8192",
            ],
            (Some(262_144), Some(8_192)),
        ),
        (
            "both -1",
            &[
                "--command-property",
                "send.buffer.bytes=-1",
                "--command-property",
                "receive.buffer.bytes=-1",
            ],
            (None, None),
        ),
    ];
    for (case, flags, expected) in cases {
        let argv = ["--bootstrap-server", "h:1", "--topic", "t"]
            .iter()
            .chain(flags)
            .copied()
            .collect::<Vec<_>>();
        let settings = Settings::from_properties(&parse(&argv).plan().unwrap().properties).unwrap();
        assert!(
            (settings.send_buffer, settings.receive_buffer) == expected,
            "{case}"
        );
    }
}

#[test]
fn retries_of_zero_turn_idempotence_off_unless_it_was_asked_for() {
    let settings = Settings::from_properties(&properties(&[("retries", "0")])).unwrap();
    assert!((settings.enable_idempotence, settings.retries) == (false, 0));
}

#[test]
fn a_failed_send_is_described_by_size_as_error_logging_callback_does() {
    let record = ProducerRecord {
        topic: "t".into(),
        key: None,
        value: Some(b"abc".to_vec().into()),
        ..ProducerRecord::default()
    };
    assert!(describe(&record) == "topic t with key: null, value: 3 bytes");
    assert!(producer_error(&ProducerError::Server(3)) == "UNKNOWN_TOPIC_OR_PARTITION (3)");
    assert!(producer_error(&ProducerError::Server(999)) == "broker error code 999");
}
