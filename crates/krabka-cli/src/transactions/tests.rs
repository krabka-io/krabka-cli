use std::collections::{BTreeMap, BTreeSet};

use assert2::{assert, check};
use clap::Parser;
use krabka_units::Time;

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: TransactionsArgs,
}

fn parse(argv: &[&str]) -> Result<TransactionsArgs, clap::Error> {
    Command::try_parse_from(std::iter::once("transactions").chain(argv.iter().copied()))
        .map(|command| command.args)
}

fn description(state: &str, start_time_ms: Option<i64>) -> TransactionDescription {
    TransactionDescription {
        transactional_id: "payments".into(),
        coordinator_id: 3,
        state: state.into(),
        timeout: Time::from_millis(60_000),
        start_time_ms,
        producer_id: 4242,
        producer_epoch: 7,
        topic_partitions: BTreeSet::from([("orders".to_owned(), 0), ("orders".to_owned(), 1)]),
    }
}

#[test]
fn pretty_table_pads_every_cell_and_ends_each_line_with_a_tab() {
    let lines = pretty_table(
        &["A", "Long"],
        &[
            vec!["wide cell".into(), "x".into()],
            vec!["é".into(), String::new()],
        ],
    );
    assert!(
        lines
            == [
                "A        \tLong\t",
                "wide cell\tx   \t",
                "é        \t    \t"
            ]
    );
}

#[test]
fn describe_renders_kafkas_nine_columns_in_order() {
    let result = described(
        &[
            description("Ongoing", Some(1_700_000_000_000)),
            TransactionDescription {
                transactional_id: "idle".into(),
                topic_partitions: BTreeSet::new(),
                ..description("Empty", None)
            },
        ],
        1_700_000_000_500,
    );
    let expected = [
        "CoordinatorId\tTransactionalId\tProducerId\tProducerEpoch\tTransactionState\t\
         TransactionTimeoutMs\tCurrentTransactionStartTimeMs\tTransactionDurationMs\t\
         TopicPartitions  \t",
        "3            \tpayments       \t4242      \t7            \tOngoing         \t\
         60000               \t1700000000000                \t500                  \t\
         orders-0,orders-1\t",
        "3            \tidle           \t4242      \t7            \tEmpty           \t\
         60000               \tNone                         \tNone                 \t\
         \x20                \t",
    ];
    check!(result.human == expected);
    check!(
        result.data
            == json!([
                {
                    "coordinator_id": 3,
                    "transactional_id": "payments",
                    "producer_id": 4242,
                    "producer_epoch": 7,
                    "transaction_state": "Ongoing",
                    "transaction_timeout_ms": 60000,
                    "current_transaction_start_time_ms": 1_700_000_000_000_i64,
                    "transaction_duration_ms": 500,
                    "topic_partitions": ["orders-0", "orders-1"],
                },
                {
                    "coordinator_id": 3,
                    "transactional_id": "idle",
                    "producer_id": 4242,
                    "producer_epoch": 7,
                    "transaction_state": "Empty",
                    "transaction_timeout_ms": 60000,
                    "current_transaction_start_time_ms": null,
                    "transaction_duration_ms": null,
                    "topic_partitions": [],
                },
            ])
    );
    check!(!result.failed);
}

#[test]
fn a_state_that_kafka_does_not_name_prints_as_unknown() {
    for (state, expected) in [
        ("Ongoing", "Ongoing"),
        ("PrepareEpochFence", "PrepareEpochFence"),
        ("CompleteCommit", "CompleteCommit"),
        ("Dead", "Unknown"),
        ("", "Unknown"),
    ] {
        check!(transaction_state(state) == expected, "{state}");
    }
}

#[test]
fn describe_takes_one_or_more_transactional_ids() {
    for (argv, expected) in [
        (&["--transactional-id", "a"][..], vec!["a"]),
        (
            &["--transactional-id", "a", "--transactional-id", "b"][..],
            vec!["a", "b"],
        ),
    ] {
        let command_line = [&["--bootstrap-server", "h:1", "describe"][..], argv].concat();
        let TransactionsCommand::Describe(args) = parse(&command_line).unwrap().command else {
            panic!("expected describe");
        };
        check!(args.transactional_id == expected);
    }
}

#[test]
fn force_terminate_takes_kafkas_name_and_the_krabka_spelling() {
    let yes = ConfirmArgs {
        dry_run: false,
        yes: true,
    };
    for (argv, expected) in [
        (
            &["forceTerminateTransaction", "--transactionalId", "t"][..],
            ("t", ConfirmArgs::default()),
        ),
        (
            &["force-terminate", "--transactional-id", "t", "--yes"][..],
            ("t", yes),
        ),
    ] {
        let command_line = [&["--bootstrap-server", "h:1"][..], argv].concat();
        let TransactionsCommand::ForceTerminateTransaction(args) =
            parse(&command_line).unwrap().command
        else {
            panic!("expected forceTerminateTransaction");
        };
        check!((args.transactional_id.as_str(), args.confirm) == expected);
    }
}

#[test]
fn find_hanging_defaults_the_timeout_to_fifteen_minutes() {
    let command_line = [
        "--bootstrap-server",
        "h:1",
        "find-hanging",
        "--broker-id",
        "3",
    ];
    let TransactionsCommand::FindHanging(args) = parse(&command_line).unwrap().command else {
        panic!("expected find-hanging");
    };
    check!(
        (
            args.broker_id,
            args.max_transaction_timeout,
            args.topic,
            args.partition
        ) == (Some(3), 15, None, None)
    );
}

#[test]
fn required_flags_are_enforced_by_the_parser() {
    for argv in [
        &["--bootstrap-server", "h:1", "describe"][..],
        &[
            "--bootstrap-server",
            "h:1",
            "describe-producers",
            "--topic",
            "t",
        ][..],
        &["--bootstrap-server", "h:1", "abort", "--partition", "0"][..],
        &["--bootstrap-server", "h:1", "forceTerminateTransaction"][..],
        &["--bootstrap-server", "h:1"][..],
    ] {
        assert!(parse(argv).is_err(), "{argv:?}");
    }
}

#[test]
fn abort_identifies_the_transaction_as_kafka_does() {
    let target = |extra: &[&str]| {
        let command_line = [
            &[
                "--bootstrap-server",
                "h:1",
                "abort",
                "--topic",
                "t",
                "--partition",
                "0",
            ][..],
            extra,
        ]
        .concat();
        let TransactionsCommand::Abort(args) = parse(&command_line).unwrap().command else {
            panic!("expected abort");
        };
        args.target()
    };
    let cases: [(&[&str], Result<AbortTarget, String>); 6] = [
        (&["--start-offset", "42"], Ok(AbortTarget::StartOffset(42))),
        (
            &["--start-offset", "42", "--producer-id", "5"],
            Ok(AbortTarget::StartOffset(42)),
        ),
        (
            &[],
            Err(
                "The transaction to abort must be identified either with --start-offset (for \
                 brokers on 3.0 or above) or with --producer-id, --producer-epoch, and \
                 --coordinator-epoch (for older brokers)"
                    .into(),
            ),
        ),
        (
            &["--producer-id", "5"],
            Err("Missing required argument --producer-epoch".into()),
        ),
        (
            &["--producer-id", "5", "--producer-epoch", "1"],
            Err("Missing required argument --coordinator-epoch".into()),
        ),
        (
            &[
                "--producer-id",
                "5",
                "--producer-epoch",
                "1",
                "--coordinator-epoch",
                "-1",
            ],
            Ok(AbortTarget::Producer {
                producer_id: 5,
                producer_epoch: 1,
                coordinator_epoch: 0,
            }),
        ),
    ];
    for (extra, expected) in cases {
        check!(target(extra) == expected, "{extra:?}");
    }
}

#[test]
fn find_hanging_needs_a_topic_or_a_broker() {
    let args = FindHangingArgs {
        broker_id: None,
        max_transaction_timeout: 15,
        topic: None,
        partition: None,
    };
    check!(
        args.check()
            == Err(
                "The `find-hanging` command requires either --topic or --broker-id to limit the \
                 scope of the search"
                    .into()
            )
    );
}

fn producer(producer_id: i64, start: Option<i64>, last_timestamp_ms: i64) -> ProducerStateInfo {
    ProducerStateInfo {
        producer_id,
        producer_epoch: 3,
        last_sequence: 9,
        last_timestamp_ms,
        coordinator_epoch: 2,
        current_txn_start_offset: start,
    }
}

#[test]
fn list_sends_kafkas_filters() {
    let request = |extra: &[&str]| {
        let command_line = [&["--bootstrap-server", "h:1", "list"][..], extra].concat();
        let TransactionsCommand::List(args) = parse(&command_line).unwrap().command else {
            panic!("expected list");
        };
        args.request()
    };
    let cases: [(&[&str], ListTransactionsRequest); 3] = [
        (
            &[],
            ListTransactionsRequest {
                duration_filter: -1,
                ..Default::default()
            },
        ),
        (
            &[
                "--duration-filter",
                "60000",
                "--transactional-id-pattern",
                "pay.*",
            ],
            ListTransactionsRequest {
                duration_filter: 60_000,
                transactional_id_pattern: Some("pay.*".into()),
                ..Default::default()
            },
        ),
        // Kafka sends no pattern for an empty one.
        (
            &["--transactional-id-pattern", ""],
            ListTransactionsRequest {
                duration_filter: -1,
                ..Default::default()
            },
        ),
    ];
    for (extra, expected) in cases {
        check!(request(extra) == expected, "{extra:?}");
    }
}

#[test]
fn list_prints_the_coordinator_of_each_transaction_in_hash_map_order() {
    let listing = |id: &str, producer_id, state: &str| TransactionListing {
        transactional_id: id.into(),
        producer_id,
        state: state.into(),
    };
    let result = listed(vec![
        (2, vec![listing("b", 7, "Ongoing")]),
        (
            1,
            vec![listing("a", 5, "CompleteCommit"), listing("c", 6, "Dead")],
        ),
    ]);
    check!(
        result.human
            == [
                "TransactionalId\tCoordinator\tProducerId\tTransactionState\t",
                "a              \t1          \t5         \tCompleteCommit  \t",
                "c              \t1          \t6         \tUnknown         \t",
                "b              \t2          \t7         \tOngoing         \t",
            ]
    );
    check!(
        result.data[2]
            == json!({"transactional_id": "b", "coordinator": 2, "producer_id": 7, "transaction_state": "Ongoing"})
    );
}

#[test]
fn describe_producers_prints_kafkas_table() {
    let result = producers_table(&[
        producer(12, Some(40), 1_700_000_000_000),
        ProducerStateInfo {
            coordinator_epoch: -1,
            ..producer(13, None, 5)
        },
    ]);
    check!(
        result.human
            == [
                "ProducerId\tProducerEpoch\tLatestCoordinatorEpoch\tLastSequence\tLastTimestamp\t\
                 CurrentTransactionStartOffset\t",
                "12        \t3            \t2                     \t9           \t1700000000000\t\
                 40                           \t",
                "13        \t3            \t-1                    \t9           \t5            \t\
                 None                         \t",
            ]
    );
}

#[test]
fn a_producer_state_reads_negative_values_as_kafka_does() {
    let cases = [
        (
            ProducerStateInfo {
                coordinator_epoch: -7,
                current_txn_start_offset: Some(-3),
                ..producer(1, None, 0)
            },
            ProducerStateInfo {
                coordinator_epoch: -1,
                ..producer(1, None, 0)
            },
        ),
        (producer(1, Some(0), 0), producer(1, Some(0), 0)),
    ];
    for (state, expected) in cases {
        check!(normalized(&state) == expected, "{state:?}");
    }
}

#[test]
fn an_abort_spec_prints_as_kafkas_to_string() {
    let spec = AbortTransactionSpec {
        topic: "t".into(),
        partition: 1,
        producer_id: 5,
        producer_epoch: 2,
        coordinator_epoch: 3,
    };
    check!(
        spec_string(&spec)
            == "AbortTransactionSpec(topicPartition=t-1, producerId=5, producerEpoch=2, \
                coordinatorEpoch=3)"
    );
}

#[test]
fn java_casts_and_hashes_match_the_jvm() {
    check!([java_short(7), java_short(65_535), java_short(32_768)] == [7, -1, -32_768]);
    check!(
        [
            long_hash(0),
            long_hash(42),
            long_hash(-1),
            long_hash(1 << 32)
        ] == [0, 42, 0, 1]
    );
    check!(
        [java_optional(Some(3)), java_optional(None)]
            == ["Optional[3]".to_owned(), "Optional.empty".to_owned()]
    );
}

#[test]
fn find_hanging_keeps_only_open_transactions_older_than_the_timeout() {
    let now = 10_000_000;
    let states = vec![
        (
            ("t".to_owned(), 0),
            vec![
                producer(1, Some(5), now - 900_001),
                producer(2, None, 0),
                producer(3, Some(9), now - 900_000),
            ],
        ),
        (("t".to_owned(), 1), vec![producer(4, Some(1), 0)]),
    ];
    let candidates = open_candidates(states, 2, now, 900_000);
    check!(
        candidates
            // `new HashMap<>(2)` of t-0 (hash 1077) and t-1 (hash 1108)
            // yields t-1 first.
            == [
                OpenTransaction {
                    partition: ("t".into(), 1),
                    state: producer(4, Some(1), 0),
                },
                OpenTransaction {
                    partition: ("t".into(), 0),
                    state: producer(1, Some(5), now - 900_001),
                },
            ]
    );
}

#[test]
fn find_hanging_filters_as_kafka_does() {
    let open = |producer_id, partition| OpenTransaction {
        partition: ("t".into(), partition),
        state: producer(producer_id, Some(1), 0),
    };
    let by_producer = group_by_producer(vec![
        open(1, 0),
        open(2, 0),
        open(3, 0),
        open(3, 1),
        open(4, 0),
    ]);
    check!(by_producer.iter().map(|(id, _)| *id).collect::<Vec<_>>() == [1, 2, 3, 4]);
    let transactional_ids = BTreeMap::from([
        (2, "gone".to_owned()),
        (3, "live".to_owned()),
        (4, "done".to_owned()),
    ]);
    let descriptions = BTreeMap::from([
        ("gone".to_owned(), None),
        (
            "live".to_owned(),
            Some(TransactionDescription {
                topic_partitions: BTreeSet::from([("t".to_owned(), 1)]),
                ..description("Ongoing", Some(0))
            }),
        ),
        (
            "done".to_owned(),
            Some(TransactionDescription {
                topic_partitions: BTreeSet::from([("t".to_owned(), 0)]),
                ..description("PrepareCommit", Some(0))
            }),
        ),
    ]);
    check!(
        hanging_transactions(by_producer, &transactional_ids, &descriptions)
            == [open(1, 0), open(2, 0), open(3, 0)]
    );
}

#[test]
fn find_hanging_prints_kafkas_table() {
    let hanging = [OpenTransaction {
        partition: ("orders".into(), 2),
        state: ProducerStateInfo {
            coordinator_epoch: -1,
            ..producer(77, Some(120), 1_000)
        },
    }];
    let result = hanging_table(&hanging, 1_000 + 20 * 60_000 + 59_999);
    check!(
        result.human
            == [
                "Topic \tPartition\tProducerId\tProducerEpoch\tCoordinatorEpoch\tStartOffset\t\
                 LastTimestamp\tDuration(min)\t",
                "orders\t2        \t77        \t3            \t-1              \t120        \t\
                 1000         \t20           \t",
            ]
    );
    check!(hanging_table(&[], 0).human.len() == 1);
}
