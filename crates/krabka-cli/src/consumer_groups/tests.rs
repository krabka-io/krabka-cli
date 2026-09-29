use assert2::{assert, check};
use clap::{Parser, error::ErrorKind};
use krabka_units::{Time, convert::TimeExt as _};

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: ConsumerGroupsArgs,
}

fn parse(argv: &[&str]) -> Result<ConsumerGroupsArgs, ErrorKind> {
    Command::try_parse_from(std::iter::once("consumer-groups").chain(argv.iter().copied()))
        .map(|command| command.args)
        .map_err(|error| error.kind())
}

fn base() -> ConsumerGroupsArgs {
    ConsumerGroupsArgs {
        connection: ConnectionArgs {
            bootstrap_server: vec!["h:9092".into()],
            bootstrap_controller: Vec::new(),
            command_config: None,
            client_id: None,
            request_timeout_ms: None,
            timeout: Time::from_millis(30_000),
        },
        list: None,
        describe: None,
        delete: None,
        reset_offsets: None,
        delete_offsets: None,
        validate_regex: None,
        group: Vec::new(),
        all_groups: None,
        topic: Vec::new(),
        all_topics: None,
        execute: None,
        export: None,
        to_offset: None,
        to_earliest: None,
        to_latest: None,
        to_current: None,
        shift_by: None,
        to_datetime: None,
        by_duration: None,
        from_file: None,
        members: None,
        offsets: None,
        state: None,
        group_type: None,
        confirm: ConfirmArgs::default(),
    }
}

fn groups(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn each_action_parses_to_the_whole_expected_struct() {
    let cases = [
        (
            vec!["--list", "--bootstrap-server", "h:9092"],
            ConsumerGroupsArgs {
                list: Some(true),
                ..base()
            },
        ),
        (
            vec![
                "--list",
                "--state",
                "--type",
                "classic",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                list: Some(true),
                state: Some(String::new()),
                group_type: Some("classic".into()),
                ..base()
            },
        ),
        (
            vec![
                "--describe",
                "--group",
                "g1",
                "--group",
                "g2",
                "--members",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                describe: Some(true),
                group: groups(&["g1", "g2"]),
                members: Some(true),
                ..base()
            },
        ),
        (
            vec![
                "--describe",
                "--all-groups",
                "--state",
                "--timeout",
                "5000",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                describe: Some(true),
                all_groups: Some(true),
                state: Some(String::new()),
                connection: ConnectionArgs {
                    timeout: Time::from_millis(5000),
                    ..base().connection
                },
                ..base()
            },
        ),
        (
            vec![
                "--delete",
                "--group",
                "g1",
                "--yes",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                delete: Some(true),
                group: groups(&["g1"]),
                confirm: ConfirmArgs {
                    dry_run: false,
                    yes: true,
                },
                ..base()
            },
        ),
        (
            vec![
                "--delete-offsets",
                "--group",
                "g1",
                "--topic",
                "orders:0,1",
                "--dry-run",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                delete_offsets: Some(true),
                group: groups(&["g1"]),
                topic: groups(&["orders:0,1"]),
                confirm: ConfirmArgs {
                    dry_run: true,
                    yes: false,
                },
                ..base()
            },
        ),
        (
            vec![
                "--reset-offsets",
                "--group",
                "g1",
                "--topic",
                "orders",
                "--shift-by",
                "-5",
                "--execute",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                reset_offsets: Some(true),
                group: groups(&["g1"]),
                topic: groups(&["orders"]),
                shift_by: Some(-5),
                execute: Some(true),
                ..base()
            },
        ),
        (
            vec![
                "--reset-offsets",
                "--all-groups",
                "--all-topics",
                "--to-earliest",
                "--dry-run",
                "--export",
                "--bootstrap-server",
                "h:9092",
            ],
            ConsumerGroupsArgs {
                reset_offsets: Some(true),
                all_groups: Some(true),
                all_topics: Some(true),
                to_earliest: Some(true),
                export: Some(true),
                confirm: ConfirmArgs {
                    dry_run: true,
                    yes: false,
                },
                ..base()
            },
        ),
        (
            vec!["--validate-regex", "ord.*"],
            ConsumerGroupsArgs {
                validate_regex: Some("ord.*".into()),
                connection: ConnectionArgs {
                    bootstrap_server: Vec::new(),
                    ..base().connection
                },
                ..base()
            },
        ),
    ];
    for (argv, expected) in cases {
        check!(parse(&argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn clap_refuses_what_the_jvm_tool_refuses_before_any_request() {
    let bootstrap = ["--bootstrap-server", "h:9092"];
    let cases: [(&[&str], ErrorKind); 11] = [
        (
            &[
                "--reset-offsets",
                "--group",
                "g",
                "--topic",
                "t",
                "--to-earliest",
            ],
            ErrorKind::MissingRequiredArgument,
        ),
        (
            &[
                "--reset-offsets",
                "--group",
                "g",
                "--topic",
                "t",
                "--dry-run",
            ],
            ErrorKind::MissingRequiredArgument,
        ),
        (
            &[
                "--reset-offsets",
                "--group",
                "g",
                "--topic",
                "t",
                "--to-earliest",
                "--to-latest",
                "--dry-run",
            ],
            ErrorKind::ArgumentConflict,
        ),
        (
            &[
                "--reset-offsets",
                "--group",
                "g",
                "--topic",
                "t",
                "--to-offset",
                "1",
                "--shift-by",
                "1",
                "--execute",
            ],
            ErrorKind::ArgumentConflict,
        ),
        (
            &[
                "--reset-offsets",
                "--group",
                "g",
                "--topic",
                "t",
                "--to-earliest",
                "--dry-run",
                "--execute",
            ],
            ErrorKind::ArgumentConflict,
        ),
        (&["--list", "--describe"], ErrorKind::ArgumentConflict),
        (&[], ErrorKind::MissingRequiredArgument),
        (&["--list", "--members"], ErrorKind::MissingRequiredArgument),
        (
            &["--describe", "--group", "g", "--type"],
            ErrorKind::MissingRequiredArgument,
        ),
        (
            &["--delete", "--group", "g", "--to-earliest"],
            ErrorKind::MissingRequiredArgument,
        ),
        (
            &["--delete", "--group", "g", "--state"],
            ErrorKind::MissingRequiredArgument,
        ),
    ];
    for (argv, kind) in cases {
        let argv = argv.iter().chain(&bootstrap).copied().collect::<Vec<_>>();
        check!(parse(&argv).map(|_| ()) == Err(kind), "{argv:?}");
    }
}

#[test]
fn argument_rules_carry_the_jvm_tool_messages() {
    let cases: [(&[&str], bool, &str); 12] = [
        (
            &["--describe"],
            false,
            "Option [describe] takes one of these options: [all-groups], [group]",
        ),
        (
            &["--describe", "--group", "g", "--members", "--state"],
            false,
            "Option [describe] takes at most one of these options: [members], [offsets], [state]",
        ),
        (
            &["--describe", "--group", "g", "--state", "x"],
            false,
            "Option [describe] does not take a value for [state]",
        ),
        (
            &["--delete"],
            false,
            "Option [delete] takes one of these options: [all-groups], [group]",
        ),
        (
            &["--delete", "--group", "g", "--topic", "t"],
            false,
            "The consumer does not support topic-specific offset deletion from a consumer group.",
        ),
        (
            &["--delete-offsets", "--group", "g"],
            false,
            "Option [delete-offsets] takes the following options: [topic], [group]",
        ),
        (
            &["--reset-offsets", "--to-earliest", "--dry-run"],
            false,
            "Option [reset-offsets] takes one of these options: [all-groups], [group]",
        ),
        (
            &["--describe", "--group", "g", "--all-groups"],
            false,
            "Option \"[group]\" can't be used with option \"[all-groups]\"",
        ),
        (
            &["--list", "--group", "g"],
            false,
            "Option \"[group]\" can't be used with option \"[list]\"",
        ),
        (
            &["--describe", "--group", "g", "--topic", "t"],
            false,
            "Option \"[topic]\" can't be used with option \"[describe]\"",
        ),
        (
            &["--list"],
            true,
            "Option(s) [verbose] are unavailable given other options on the command line",
        ),
        (
            &["--describe", "--group", "g", "--dry-run"],
            false,
            "--dry-run is only valid with --reset-offsets, --delete or --delete-offsets",
        ),
    ];
    for (argv, verbose, message) in cases {
        let args = parse(argv).unwrap();
        check!(
            args.check_args(verbose).unwrap_err().to_string() == message,
            "{argv:?}"
        );
    }
    assert!(
        parse(&["--describe", "--group", "g"])
            .unwrap()
            .check_args(true)
            .is_ok()
    );
}

#[test]
fn list_filters_are_validated_as_kafka_validates_them() {
    let cases = [
        (group_states("stable,EMPTY").map_err(|error| error.to_string()), Ok(vec!["Stable", "Empty"])),
        (
            group_states("bogus").map_err(|error| error.to_string()),
            Err("Invalid state list 'bogus'. Valid states are: Dead, CompletingRebalance, Empty, Stable, Assigning, Reconciling, PreparingRebalance".to_owned()),
        ),
        (group_types("Classic, consumer").map_err(|error| error.to_string()), Ok(vec!["Classic", "Consumer"])),
        (
            group_types("share").map_err(|error| error.to_string()),
            Err("Invalid types list 'share'. Valid types are: Consumer, Classic".to_owned()),
        ),
    ];
    for (actual, expected) in cases {
        check!(actual == expected);
    }
}

#[test]
fn validate_regex_reports_as_the_jvm_tool_does() {
    check!(validate_regex("ord.*").human == ["The regular expression `ord.*` is valid."]);
    let invalid = validate_regex("a(b");
    check!(invalid.human == ["The regular expression `a(b` is invalid: unclosed group."]);
    check!(!invalid.failed);
}

fn member(id: &str, assignment: &[(&str, i32)]) -> Member {
    Member {
        consumer_id: id.into(),
        group_instance_id: None,
        client_id: format!("client-{id}"),
        host: "/10.0.0.1".into(),
        assignment: assignment
            .iter()
            .map(|(topic, partition)| ((*topic).to_owned(), *partition))
            .collect(),
        target_assignment: None,
        epoch: Some(3),
        upgraded: None,
    }
}

fn p(topic: &str, partition: i32) -> Partition {
    (topic.to_owned(), partition)
}

#[test]
fn offset_rows_order_members_by_size_then_unassigned_and_compute_lag() {
    let members = [
        member("small", &[("orders", 1)]),
        member("idle", &[]),
        member("big", &[("orders", 2), ("events", 0)]),
    ];
    let committed = BTreeMap::from([
        (p("orders", 1), 5),
        (p("events", 0), 9),
        (p("audit", 0), 3),
        (p("orders", 2), 12),
    ]);
    let log_end = BTreeMap::from([
        (p("orders", 1), 7),
        (p("events", 0), 9),
        (p("orders", 2), 10),
    ]);
    let rows = offset_rows("g", &members, &committed, &log_end)
        .into_iter()
        .map(|row| {
            (
                format!("{}-{}", row.topic.unwrap(), row.partition.unwrap()),
                row.offset,
                row.log_end_offset,
                row.lag,
                row.consumer_id.unwrap(),
            )
        })
        .collect::<Vec<_>>();
    check!(
        rows == vec![
            (
                "events-0".to_owned(),
                Some(9),
                Some(9),
                Some(0),
                "big".to_owned()
            ),
            (
                "orders-2".to_owned(),
                Some(12),
                Some(10),
                Some(-2),
                "big".to_owned()
            ),
            (
                "orders-1".to_owned(),
                Some(5),
                Some(7),
                Some(2),
                "small".to_owned()
            ),
            ("audit-0".to_owned(), Some(3), None, None, "-".to_owned()),
        ]
    );
}

fn offset_row(
    group: &str,
    partition: Partition,
    offset: Option<i64>,
    end: Option<i64>,
) -> OffsetRow {
    OffsetRow {
        group: group.into(),
        topic: Some(partition.0),
        partition: Some(partition.1),
        leader_epoch: None,
        offset,
        log_end_offset: end,
        lag: render::lag(offset, end),
        consumer_id: Some("-".into()),
        host: Some("-".into()),
        client_id: Some("-".into()),
    }
}

#[test]
fn the_offsets_report_follows_each_group_state_and_fails_for_a_failed_one() {
    let answers = vec![
        (
            "alpha".to_owned(),
            Err(CommandError::Other(
                "org.apache.kafka.common.errors.GroupAuthorizationException: denied".into(),
            )),
        ),
        ("dead".to_owned(), Ok(("Dead".to_owned(), Vec::new()))),
        ("empty".to_owned(), Ok(("Empty".to_owned(), Vec::new()))),
        (
            "zeta".to_owned(),
            Ok((
                "Empty".to_owned(),
                vec![offset_row("zeta", p("orders", 0), Some(4), Some(10))],
            )),
        ),
    ];
    let report = offsets_report(answers, false);
    let expected = CommandResult::rows(
        vec![
            String::new(),
            "Error: Consumer group 'dead' does not exist.".to_owned(),
            String::new(),
            "GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID".to_owned(),
            "zeta            orders          0          4               10              6               -               -               -".to_owned(),
        ],
        report.data.clone(),
        true,
    )
    .with_notices(vec![
        "Error: Executing consumer group command failed for group 'alpha' due to org.apache.kafka.common.errors.GroupAuthorizationException: denied".to_owned(),
        String::new(),
        "Consumer group 'empty' has no active members.".to_owned(),
        String::new(),
        "Consumer group 'zeta' has no active members.".to_owned(),
    ]);
    check!(report == expected);
}

/// A group described as the admin client describes it.
fn description(state: &str, members: Vec<Member>) -> GroupDescription {
    GroupDescription {
        state: state.into(),
        coordinator: ("localhost".into(), 9092, 1),
        partition_assignor: "range".into(),
        members,
        group_epoch: None,
        target_assignment_epoch: None,
    }
}

#[test]
fn members_and_state_reports_print_each_described_group() {
    let described = || {
        vec![
            (
                "active1".to_owned(),
                Ok(description(
                    "Stable",
                    vec![member("m1", &[("events", 1), ("events", 0)])],
                )),
            ),
            ("g1".to_owned(), Ok(description("Empty", vec![]))),
            (
                "nope".to_owned(),
                Err(CommandError::Other(
                    "org.apache.kafka.common.errors.GroupIdNotFoundException: Group nope not found."
                        .into(),
                )),
            ),
        ]
    };
    let notices = |empty: &str| {
        vec![
            String::new(),
            empty.to_owned(),
            "Error: Executing consumer group command failed for group 'nope' due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group nope not found.".to_owned(),
        ]
    };
    let cases = [
        (
            true,
            vec![
                "",
                "GROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     ",
                "active1         m1              /10.0.0.1       client-m1       2               ",
                "",
                "GROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     ",
            ],
        ),
        (
            false,
            vec![
                "",
                "GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS",
                "active1         localhost:9092  (1)       range                Stable               1",
                "",
                "GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS",
                "g1              localhost:9092  (1)       range                Empty                0",
            ],
        ),
    ];
    for (members, human) in cases {
        let report = members_or_state_report(described(), members, false);
        let expected = CommandResult::rows(
            human.into_iter().map(ToOwned::to_owned).collect(),
            report.data.clone(),
            true,
        )
        .with_notices(notices("Consumer group 'g1' has no active members."));
        check!(report == expected, "members: {members}");
    }
}

fn error(code: i16) -> KafkaError {
    KafkaError {
        code,
        name: KafkaException::for_code(code).name(),
        message: None,
    }
}

#[test]
fn the_delete_report_matches_kafka() {
    let cases = [
        (
            vec![("g1".to_owned(), None)],
            vec!["Deletion of requested consumer groups ('g1') was successful."],
            false,
        ),
        (
            vec![
                ("nope".to_owned(), Some(error(69))),
                ("g1".to_owned(), None),
                ("busy".to_owned(), Some(error(68))),
            ],
            vec![
                "",
                "Error: Deletion of some consumer groups failed:",
                "* Group 'nope' could not be deleted due to: org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.",
                "* Group 'busy' could not be deleted due to: org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.",
                "",
                "These consumer groups were deleted successfully: 'g1'",
            ],
            true,
        ),
    ];
    for (results, human, failed) in cases {
        let report = delete_report(&results);
        check!(
            (report.human.clone(), report.failed)
                == (human.into_iter().map(ToOwned::to_owned).collect(), failed)
        );
    }
}

#[test]
fn the_delete_offsets_report_picks_kafkas_verdict() {
    let rows: Vec<DeleteOffsetRow> = vec![(("orders".to_owned(), Some(0)), None)];
    let failed_rows: Vec<DeleteOffsetRow> = vec![(
        ("orders".to_owned(), Some(0)),
        Some(
            "org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist."
                .to_owned(),
        ),
    )];
    let cases = [
        (
            None,
            &rows,
            vec!["Request succeeded for deleting offsets from group g."],
            false,
        ),
        (
            None,
            &failed_rows,
            vec![
                "",
                "Error: Encountered some partition-level error, see the follow-up details.",
            ],
            true,
        ),
        (
            Some(69),
            &failed_rows,
            vec!["", "Error: The group id does not exist."],
            true,
        ),
        (
            Some(86),
            &rows,
            vec![
                "",
                "Error: Encountered some partition-level error, see the follow-up details.",
            ],
            true,
        ),
        (
            Some(7),
            &rows,
            vec![
                "",
                "Error: Encountered some unknown error: REQUEST_TIMED_OUT",
            ],
            true,
        ),
    ];
    for (top_level, rows, head, failed) in cases {
        let report = delete_offsets_report("g", top_level, rows);
        check!(report.human[..head.len()] == head[..], "{top_level:?}");
        check!(report.failed == failed);
    }
}

#[test]
fn offset_deletions_mark_each_partition_and_name_the_first_failure() {
    use krabka_client_admin::ConsumerGroupOffsetOutcome;
    let outcome = |partition: i32, code: Option<i16>| ConsumerGroupOffsetOutcome {
        topic: "orders".into(),
        partition,
        error: code.map(error),
    };
    let partitions = [p("orders", 0), p("orders", 1), p("orders", 2)];
    let fresh = || -> Vec<DeleteOffsetRow> {
        vec![
            (("nosuch".to_owned(), None), Some("gone".to_owned())),
            (("orders".to_owned(), Some(0)), None),
            (("orders".to_owned(), Some(1)), None),
            (("orders".to_owned(), Some(2)), None),
        ]
    };
    let subscribed = "org.apache.kafka.common.errors.GroupSubscribedToTopicException: Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.";
    let cases = [
        (
            vec![outcome(0, None), outcome(1, None), outcome(2, None)],
            None,
            vec![None, None, None],
        ),
        (
            vec![outcome(0, None), outcome(1, Some(86)), outcome(2, None)],
            Some(86),
            vec![None, Some(subscribed.to_owned()), None],
        ),
        (
            vec![outcome(0, None), outcome(1, None)],
            Some(-1),
            vec![
                None,
                None,
                Some("java.lang.IllegalArgumentException: Offset deletion result for partition \"orders-2\" was not included in the response".to_owned()),
            ],
        ),
    ];
    for (outcomes, top_level, statuses) in cases {
        let mut rows = fresh();
        let actual = apply_offset_deletions(&mut rows, &partitions, &outcomes);
        let mut expected = fresh();
        for (row, status) in expected[1..].iter_mut().zip(statuses) {
            row.1 = status;
        }
        check!((actual, rows) == (top_level, expected));
    }
}

#[test]
fn the_reset_report_orders_groups_as_kafka_and_exports() {
    let plans = vec![
        ("g2".to_owned(), vec![(p("orders", 0), 0)]),
        (
            "g1".to_owned(),
            vec![(p("orders", 0), 0), (p("orders", 1), 0)],
        ),
    ];
    let table = reset_report(plans.clone(), None, vec![], false, false);
    check!(
        table.human
            == [
                "",
                "GROUP           TOPIC           PARTITION  NEW-OFFSET",
                "g1              orders          0          0",
                "g1              orders          1          0",
                "g2              orders          0          0",
            ]
    );
    let export = reset_report(plans, None, vec![], true, false);
    check!(export.human == ["g1,orders,0,0", "g1,orders,1,0", "g2,orders,0,0", ""]);
    let failure = KafkaError {
        code: 25,
        name: "UNKNOWN_MEMBER_ID",
        message: None,
    };
    let failed = reset_report(
        vec![("g".to_owned(), vec![(p("orders", 0), 3)])],
        Some(&failure),
        vec![],
        false,
        true,
    );
    check!(failed.failed);
    check!(
        failed.human
            == [
                "",
                "Error: Executing consumer group command failed due to The coordinator is not aware of this member.",
            ]
    );
}

#[test]
fn a_state_no_consumer_group_has_fails_the_group() {
    check!(
        member_state("g", "Unknown", 1).unwrap_err().to_string()
            == "org.apache.kafka.common.KafkaException: Expected a valid consumer group state, but found 'Unknown'."
    );
}
