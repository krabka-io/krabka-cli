use std::collections::BTreeSet;

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

#[tokio::test]
async fn subcommands_the_pinned_client_cannot_serve_fail_before_connecting() {
    let cases: [(&[&str], &str); 8] = [
        (
            &["list"],
            "Failed to list transactions: not supported by this build; it needs \
             AdminClient::list_transactions from a newer krabka-client-rs",
        ),
        (
            &[
                "list",
                "--duration-filter",
                "-1",
                "--transactional-id-pattern",
                "pay.*",
            ],
            "Failed to list transactions with --duration-filter -1 --transactional-id-pattern \
             pay.*: not supported by this build; it needs AdminClient::list_transactions from a \
             newer krabka-client-rs",
        ),
        (
            &[
                "describe-producers",
                "--topic",
                "t",
                "--partition",
                "0",
                "--broker-id",
                "2",
            ],
            "Failed to describe producers for partition t-0 on broker 2: not supported by this \
             build; it needs AdminClient::describe_producers from a newer krabka-client-rs",
        ),
        (
            &[
                "abort",
                "--topic",
                "t",
                "--partition",
                "1",
                "--start-offset",
                "9",
            ],
            "Failed to validate producer state for partition t-1: not supported by this build; \
             it needs AdminClient::describe_producers and AdminClient::abort_transaction from a \
             newer krabka-client-rs",
        ),
        (
            &[
                "abort",
                "--topic",
                "t",
                "--partition",
                "1",
                "--producer-id",
                "5",
                "--producer-epoch",
                "2",
                "--coordinator-epoch",
                "3",
            ],
            "Failed to abort transaction AbortTransactionSpec(topicPartition=t-1, producerId=5, \
             producerEpoch=2, coordinatorEpoch=3): not supported by this build; it needs \
             AdminClient::abort_transaction from a newer krabka-client-rs",
        ),
        (
            &[
                "abort",
                "--topic",
                "t",
                "--partition",
                "1",
                "--producer-id",
                "5",
            ],
            "Missing required argument --producer-epoch",
        ),
        (
            &["find-hanging", "--topic", "t"],
            "Failed to find hanging transactions older than 15 minutes: not supported by this \
             build; it needs AdminClient::describe_topics and AdminClient::describe_producers and \
             AdminClient::list_transactions from a newer krabka-client-rs",
        ),
        (
            &["find-hanging", "--partition", "0", "--broker-id", "1"],
            "The --partition argument requires --topic to be provided",
        ),
    ];
    for (argv, expected) in cases {
        // An address that cannot resolve: any attempt to connect fails with a
        // different message.
        let argv = [&["--bootstrap-server", "unreachable.invalid:1"][..], argv].concat();
        let error = parse(&argv).unwrap().run().await.unwrap_err();
        check!(error.to_string() == expected, "{argv:?}");
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
