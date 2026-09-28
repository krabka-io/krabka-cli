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

#[test]
fn the_offsets_report_prints_every_group_and_fails_for_a_failed_one() {
    let answers = vec![
        ("zeta".to_owned(), Ok(BTreeMap::from([(p("orders", 0), 4)]))),
        (
            "alpha".to_owned(),
            Err(CommandError::Other(
                "OffsetFetch failed: GROUP_AUTHORIZATION_FAILED (30)".into(),
            )),
        ),
        ("empty".to_owned(), Ok(BTreeMap::new())),
    ];
    let report = offsets_report(answers, false);
    check!(
        report.human
            == [
                "",
                "GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID",
                "zeta            orders          0          4               -               -               -               -               -",
            ]
    );
    check!(report.failed);
    check!(
        report.notices[..2]
            == [
                "Error: Executing consumer group command failed for group 'alpha' due to OffsetFetch failed: GROUP_AUTHORIZATION_FAILED (30)",
                "Consumer group 'empty' has no committed offsets.",
            ]
    );
    check!(report.notices[2].contains("AdminClient::describe_consumer_groups"));
    let clean = offsets_report(
        vec![("g".to_owned(), Ok(BTreeMap::from([(p("t", 0), 1)])))],
        true,
    );
    check!(!clean.failed);
}

/// A describer that knows fixed groups.
struct Known(BTreeMap<String, GroupDescription>);

impl Groups for Known {
    async fn list(
        &self,
        states: &[&str],
        _types: &[&str],
    ) -> Result<Vec<ListedGroup>, CommandError> {
        Ok(self
            .0
            .iter()
            .filter(|(_, description)| {
                states.is_empty() || states.contains(&description.state.as_str())
            })
            .map(|(group, description)| ListedGroup {
                group_id: group.clone(),
                group_type: "Classic".into(),
                state: description.state.clone(),
            })
            .collect())
    }

    async fn describe(&self, group: &str) -> Result<GroupDescription, CommandError> {
        self.0.get(group).cloned().ok_or_else(|| {
            format!(
                "org.apache.kafka.common.errors.GroupIdNotFoundException: Group {group} not found."
            )
            .into()
        })
    }

    fn can_delete(&self, _what: &str) -> Result<(), CommandError> {
        Ok(())
    }

    async fn delete(&self, group: &str) -> Result<Option<i16>, CommandError> {
        Ok((!self.0.contains_key(group)).then_some(GROUP_ID_NOT_FOUND))
    }

    async fn delete_offsets(
        &self,
        _group: &str,
        partitions: &[Partition],
    ) -> Result<(Option<i16>, BTreeMap<Partition, Option<i16>>), CommandError> {
        Ok((
            None,
            partitions
                .iter()
                .map(|partition| (partition.clone(), None))
                .collect(),
        ))
    }
}

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

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn members_and_state_describe_through_the_seam() {
    let known = Known(BTreeMap::from([
        (
            "active1".to_owned(),
            description(
                "Stable",
                vec![member("m1", &[("events", 1), ("events", 0)])],
            ),
        ),
        ("g1".to_owned(), description("Empty", vec![])),
    ]));
    let both = groups(&["g1", "active1"]);
    let members = block_on(describe_members_or_state(&both, true, false, &known)).unwrap();
    check!(
        members.human
            == [
                "",
                "GROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     ",
                "active1         m1              /10.0.0.1       client-m1       2               ",
                "",
                "GROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     ",
            ]
    );
    check!(members.notices == ["Consumer group 'g1' has no active members."]);
    let state = block_on(describe_members_or_state(&both, false, false, &known)).unwrap();
    check!(
        state.human
            == [
                "",
                "GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS",
                "active1         localhost:9092  (1)       range                Stable               1",
                "",
                "GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS",
                "g1              localhost:9092  (1)       range                Empty                0",
            ]
    );
    let missing = block_on(describe_members_or_state(
        &groups(&["nope"]),
        false,
        false,
        &known,
    ));
    check!(
        missing.unwrap_err().to_string()
            == "org.apache.kafka.common.errors.GroupIdNotFoundException: Group nope not found."
    );
    let unsupported = block_on(describe_members_or_state(
        &groups(&["g1"]),
        true,
        false,
        &Unavailable,
    ));
    check!(
        unsupported.unwrap_err().to_string()
            == "--describe --members is not supported by this build: it needs AdminClient::describe_consumer_groups, which the pinned krabka-client-rs revision does not have"
    );
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
                ("nope".to_owned(), Some(69)),
                ("g1".to_owned(), None),
                ("nope2".to_owned(), Some(69)),
            ],
            vec![
                "",
                "Error: Deletion of some consumer groups failed:",
                "* Group 'nope' could not be deleted due to: org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.",
                "* Group 'nope2' could not be deleted due to: org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.",
                "",
                "These consumer groups were deleted successfully: 'g1'",
            ],
            true,
        ),
    ];
    for (results, human, failed) in cases {
        let report = delete_report(&results);
        check!(report.human == human);
        check!(report.failed == failed);
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
            "Request succeeded for deleting offsets from group g.",
            false,
        ),
        (None, &failed_rows, "", true),
        (Some(69), &failed_rows, "", true),
        (Some(86), &rows, "", true),
        (Some(7), &rows, "", true),
    ];
    let verdicts = [
        "Request succeeded for deleting offsets from group g.",
        "Error: Encountered some partition-level error, see the follow-up details.",
        "Error: The group id does not exist.",
        "Error: Encountered some partition-level error, see the follow-up details.",
        "Error: Encountered some unknown error: REQUEST_TIMED_OUT",
    ];
    for ((top_level, rows, first, failed), verdict) in cases.into_iter().zip(verdicts) {
        let report = delete_offsets_report("g", top_level, rows);
        let head = if first.is_empty() {
            vec!["", verdict]
        } else {
            vec![first]
        };
        check!(report.human[..head.len()] == head[..], "{top_level:?}");
        check!(report.failed == failed);
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
    let table = reset_report(plans.clone(), &[], vec![], false, false);
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
    let export = reset_report(plans, &[], vec![], true, false);
    check!(export.human == ["g1,orders,0,0", "g1,orders,1,0", "g2,orders,0,0", ""]);
    let failure = krabka_client_admin::KafkaError {
        code: 25,
        name: "UNKNOWN_MEMBER_ID",
        message: None,
    };
    let failed = reset_report(
        vec![("g".to_owned(), vec![(p("orders", 0), 3)])],
        &[("g".to_owned(), p("orders", 0), failure)],
        vec![],
        false,
        true,
    );
    check!(failed.failed);
    check!(
        failed.human
            == [
                "",
                "Error: Executing consumer group command failed due to org.apache.kafka.common.errors.UnknownMemberIdException: The coordinator is not aware of this member.",
            ]
    );
}

#[test]
fn destructive_actions_without_support_refuse_before_confirming() {
    // No broker is listening on this address: the command must refuse on the
    // missing AdminClient call before it connects, prompts or deletes.
    let connection = ConnectionArgs {
        bootstrap_server: vec!["127.0.0.1:1".into()],
        request_timeout_ms: Some(200),
        ..base().connection
    };
    let delete = ConsumerGroupsArgs {
        delete: Some(true),
        group: groups(&["g1"]),
        connection: connection.clone(),
        ..base()
    };
    check!(
        block_on(delete.run(false)).unwrap_err().to_string()
            == "--delete is not supported by this build: it needs AdminClient::delete_consumer_groups, which the pinned krabka-client-rs revision does not have"
    );
    let list_state = ConsumerGroupsArgs {
        list: Some(true),
        state: Some("stable".into()),
        connection,
        ..base()
    };
    let error = block_on(list_state.run(false)).unwrap_err();
    check!(matches!(error, CommandError::Unsupported(_)));
    check!(error.exit() == crate::exit::Exit::Failure);
}

#[test]
fn delete_with_support_asks_for_confirmation_first() {
    let known = Known(BTreeMap::new());
    let delete = ConsumerGroupsArgs {
        delete: Some(true),
        group: groups(&["g1"]),
        ..base()
    };
    // stdin is not a terminal under the test runner and --yes is absent.
    let error = block_on(delete.delete_groups(&known)).unwrap_err();
    check!(matches!(
        error,
        CommandError::Refused(crate::safety::Refusal::NonInteractive)
    ));
    let yes = ConsumerGroupsArgs {
        delete: Some(true),
        group: groups(&["g1"]),
        confirm: ConfirmArgs {
            dry_run: false,
            yes: true,
        },
        ..base()
    };
    let report = block_on(yes.delete_groups(&known)).unwrap();
    check!(report.failed);
    check!(report.human[1] == "Error: Deletion of some consumer groups failed:");
}

#[test]
fn list_with_state_renders_the_listing_of_the_seam() {
    let known = Known(BTreeMap::from([
        ("active1".to_owned(), description("Stable", vec![])),
        ("g1".to_owned(), description("Empty", vec![])),
    ]));
    let list = ConsumerGroupsArgs {
        list: Some(true),
        state: Some("empty".into()),
        ..base()
    };
    let report = block_on(list.list_groups(&known)).unwrap();
    check!(
        report.human
            == [
                "GROUP                     STATE               ",
                "g1                        Empty               ",
            ]
    );
    let bad = ConsumerGroupsArgs {
        list: Some(true),
        state: Some("bogus".into()),
        ..base()
    };
    check!(
        block_on(bad.list_groups(&known))
            .unwrap_err()
            .to_string()
            .starts_with("Invalid state list 'bogus'.")
    );
}
