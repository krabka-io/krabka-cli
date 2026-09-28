use assert2::{assert, check};
use clap::Parser;
use krabka_client_admin::{AclOperation as WireOperation, PatternType as WirePattern};

use super::*;
use crate::{exit::Exit, safety::confirm_with};

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    acls: AclsArgs,
}

fn parse(argv: &[&str]) -> AclsArgs {
    let argv = ["acls", "--bootstrap-server", "127.0.0.1:1"]
        .iter()
        .chain(argv)
        .copied();
    Cli::try_parse_from(argv)
        .unwrap_or_else(|error| panic!("{error}"))
        .acls
}

fn plan(argv: &[&str]) -> Result<Plan, CommandError> {
    parse(argv).plan()
}

fn resource(kind: ResourceType, name: &str, pattern_type: PatternType) -> Resource {
    Resource {
        kind,
        name: name.into(),
        pattern_type,
    }
}

fn topic(name: &str) -> Resource {
    resource(ResourceType::Topic, name, PatternType::Literal)
}

fn entry(principal: &str, host: &str, operation: Operation, permission: Permission) -> Entry {
    Entry {
        principal: principal.into(),
        host: host.into(),
        operation,
        permission,
    }
}

fn allow(principal: &str, operation: Operation) -> Entry {
    entry(principal, "*", operation, Permission::Allow)
}

fn acls<const N: usize>(rows: [(Resource, Vec<Entry>); N]) -> Acls {
    rows.into_iter()
        .map(|(resource, entries)| (resource, entries.into_iter().collect()))
        .collect()
}

#[test]
fn every_operation_parses_from_its_kafka_names_and_no_other() {
    for operation in Operation::ALL {
        let pascal = pascal_case(operation.name());
        check!(Operation::parse(&pascal) == operation, "{pascal}");
        check!(Operation::parse(&pascal.to_uppercase()) == operation);
        check!(Operation::parse(&pascal.to_lowercase()) == operation);
        // Kafka looks the upper-case value up against the names without
        // their underscores, so `DESCRIBE_CONFIGS` names nothing.
        if operation.name().contains('_') {
            check!(Operation::parse(operation.name()) == Operation::Unknown);
        }
    }
    check!(Operation::parse("Foo") == Operation::Unknown);
}

#[test]
fn every_concrete_operation_round_trips_through_the_wire_enum() {
    let supported = Operation::ALL
        .into_iter()
        .filter_map(|operation| {
            let wire = operation.to_wire().ok()?;
            Some((operation, wire, Operation::from_wire(wire)))
        })
        .collect::<Vec<_>>();
    check!(
        supported
            == [
                (Operation::All, WireOperation::All, Operation::All),
                (Operation::Read, WireOperation::Read, Operation::Read),
                (Operation::Write, WireOperation::Write, Operation::Write),
                (Operation::Create, WireOperation::Create, Operation::Create),
                (Operation::Delete, WireOperation::Delete, Operation::Delete),
                (Operation::Alter, WireOperation::Alter, Operation::Alter),
                (
                    Operation::Describe,
                    WireOperation::Describe,
                    Operation::Describe
                ),
                (
                    Operation::ClusterAction,
                    WireOperation::ClusterAction,
                    Operation::ClusterAction
                ),
                (
                    Operation::DescribeConfigs,
                    WireOperation::DescribeConfigs,
                    Operation::DescribeConfigs
                ),
                (
                    Operation::AlterConfigs,
                    WireOperation::AlterConfigs,
                    Operation::AlterConfigs
                ),
                (
                    Operation::IdempotentWrite,
                    WireOperation::IdempotentWrite,
                    Operation::IdempotentWrite
                ),
                (
                    Operation::CreateTokens,
                    WireOperation::CreateTokens,
                    Operation::CreateTokens
                ),
                (
                    Operation::DescribeTokens,
                    WireOperation::DescribeTokens,
                    Operation::DescribeTokens
                ),
                (
                    Operation::TwoPhaseCommit,
                    WireOperation::TwoPhaseCommit,
                    Operation::TwoPhaseCommit
                ),
            ]
    );
    let refused = [Operation::Unknown, Operation::Any]
        .map(|operation| operation.to_wire().unwrap_err().to_string());
    check!(
        refused
            == [
                "operation UNKNOWN does not name a concrete operation",
                "operation ANY does not name a concrete operation",
            ]
    );
}

#[test]
fn every_resource_type_round_trips_through_the_wire_enum() {
    let round_trips = ResourceType::ALL.map(|kind| {
        let wire = kind.to_wire();
        (kind, wire, ResourceType::from_wire(wire))
    });
    check!(
        round_trips
            == [
                (
                    ResourceType::Topic,
                    admin::ResourceType::Topic,
                    ResourceType::Topic
                ),
                (
                    ResourceType::Group,
                    admin::ResourceType::Group,
                    ResourceType::Group
                ),
                (
                    ResourceType::Cluster,
                    admin::ResourceType::Cluster,
                    ResourceType::Cluster
                ),
                (
                    ResourceType::TransactionalId,
                    admin::ResourceType::TransactionalId,
                    ResourceType::TransactionalId
                ),
                (
                    ResourceType::DelegationToken,
                    admin::ResourceType::DelegationToken,
                    ResourceType::DelegationToken
                ),
                (
                    ResourceType::User,
                    admin::ResourceType::User,
                    ResourceType::User
                ),
            ]
    );
}

#[test]
fn every_pattern_type_parses_in_any_case_and_maps_to_its_filter() {
    for pattern in PatternType::ALL {
        for spelling in [
            pattern.name().to_owned(),
            pattern.name().to_lowercase(),
            pascal_case(pattern.name()),
        ] {
            check!(PatternType::parse(&spelling).ok() == Some(pattern));
        }
    }
    check!(
        PatternType::ALL.map(PatternType::to_wire_filter)
            == [
                None,
                Some(WirePattern::Match),
                Some(WirePattern::Literal),
                Some(WirePattern::Prefixed),
            ]
    );
    check!(PatternType::from_wire(WirePattern::Match) == PatternType::Match);
    for wire in [WirePattern::Literal, WirePattern::Prefixed] {
        check!(PatternType::from_wire(wire).to_wire().ok() == Some(wire));
    }
    let error = PatternType::parse("foo").unwrap_err();
    check!(
        (error.exit(), error.to_string())
            == (
                Exit::Usage,
                "Cannot parse argument 'foo' of option resource-pattern-type".into()
            )
    );
}

#[test]
fn every_permission_round_trips_through_the_wire_enum() {
    for permission in Permission::ALL {
        check!(Permission::from_wire(permission.to_wire()) == permission);
    }
    check!(Permission::ALL.map(Permission::name) == ["ALLOW", "DENY"]);
}

#[test]
fn resources_and_entries_print_as_kafka_prints_them() {
    let printed = [
        topic("orders").to_string(),
        Resource::cluster().to_string(),
        resource(ResourceType::TransactionalId, "tx", PatternType::Any).to_string(),
        entry(
            "User:bob",
            "10.0.0.1",
            Operation::IdempotentWrite,
            Permission::Deny,
        )
        .to_string(),
    ];
    check!(
        printed
            == [
                "ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)",
                "ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=LITERAL)",
                "ResourcePattern(resourceType=TRANSACTIONAL_ID, name=tx, patternType=ANY)",
                "(principal=User:bob, host=10.0.0.1, operation=IDEMPOTENT_WRITE, \
                 permissionType=DENY)",
            ]
    );
}

fn check_plans(cases: Vec<(&[&str], Plan)>) {
    for (argv, expected) in cases {
        check!(plan(argv).ok() == Some(expected), "{argv:?}");
    }
}

#[test]
fn list_flags_resolve_to_the_filters_that_kafka_resolves() {
    use PatternType::Any;
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &["--list", "--topic", "orders"],
            Plan::List {
                filters: [topic("orders")].into(),
                principals: None,
            },
        ),
        (
            &["--list"],
            Plan::List {
                filters: BTreeSet::new(),
                principals: None,
            },
        ),
        (
            &[
                "--list",
                "--principal",
                "User:alice",
                "--principal",
                " User:c ",
                "--principal",
                "User:alice",
            ],
            Plan::List {
                filters: BTreeSet::new(),
                principals: Some(vec!["User:alice".into(), "User:c".into()]),
            },
        ),
        (
            &["--list", "--principal"],
            Plan::List {
                filters: BTreeSet::new(),
                principals: Some(Vec::new()),
            },
        ),
        (
            &[
                "--list",
                "--topic",
                "orders",
                "--resource-pattern-type",
                "any",
            ],
            Plan::List {
                filters: [resource(ResourceType::Topic, "orders", Any)].into(),
                principals: None,
            },
        ),
        // Kafka adds the cluster resource only under a literal pattern type.
        (
            &["--list", "--cluster", "--resource-pattern-type", "prefixed"],
            Plan::List {
                filters: BTreeSet::new(),
                principals: None,
            },
        ),
    ];
    check_plans(cases);
}

#[test]
fn add_flags_resolve_to_the_entries_that_kafka_resolves() {
    use Operation::{All, Alter, Create, Describe, IdempotentWrite, Read, Write};
    use PatternType::{Literal, Prefixed};
    use ResourceType::{Cluster, Group, TransactionalId};
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &[
                "--add",
                "--allow-principal",
                "User:alice",
                "--allow-principal",
                "User:bob",
                "--operation",
                "Read",
                "--operation",
                "write",
                "--topic",
                "orders",
            ],
            Plan::Add(acls([(
                topic("orders"),
                vec![
                    allow("User:alice", Read),
                    allow("User:alice", Write),
                    allow("User:bob", Read),
                    allow("User:bob", Write),
                ],
            )])),
        ),
        (
            &["--add", "--allow-principal", "User:a", "--topic", "t"],
            Plan::Add(acls([(topic("t"), vec![allow("User:a", All)])])),
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:alice",
                "--producer",
                "--topic",
                "t",
            ],
            Plan::Add(acls([(
                topic("t"),
                vec![
                    allow("User:alice", Write),
                    allow("User:alice", Describe),
                    allow("User:alice", Create),
                ],
            )])),
        ),
        (
            &[
                "--add",
                "--producer",
                "--idempotent",
                "--allow-principal",
                "User:p",
                "--topic",
                "orders",
                "--transactional-id",
                "tx1",
            ],
            Plan::Add(acls([
                (
                    topic("orders"),
                    vec![
                        allow("User:p", Write),
                        allow("User:p", Describe),
                        allow("User:p", Create),
                    ],
                ),
                (Resource::cluster(), vec![allow("User:p", IdempotentWrite)]),
                (
                    resource(TransactionalId, "tx1", Literal),
                    vec![allow("User:p", Write), allow("User:p", Describe)],
                ),
            ])),
        ),
        (
            &[
                "--add",
                "--consumer",
                "--allow-principal",
                "User:c",
                "--topic",
                "orders",
                "--group",
                "g1",
                "--resource-pattern-type",
                "prefixed",
            ],
            Plan::Add(acls([
                (
                    resource(ResourceType::Topic, "orders", Prefixed),
                    vec![allow("User:c", Read), allow("User:c", Describe)],
                ),
                (resource(Group, "g1", Prefixed), vec![allow("User:c", Read)]),
            ])),
        ),
        (
            &[
                "--add",
                "--producer",
                "--consumer",
                "--allow-principal",
                "User:pc",
                "--topic",
                "t",
                "--group",
                "g",
            ],
            Plan::Add(acls([
                (
                    topic("t"),
                    vec![
                        allow("User:pc", Write),
                        allow("User:pc", Describe),
                        allow("User:pc", Create),
                        allow("User:pc", Read),
                    ],
                ),
                (resource(Group, "g", Literal), vec![allow("User:pc", Read)]),
            ])),
        ),
        (
            &[
                "--add",
                "--deny-principal",
                "User:eve",
                "--deny-host",
                "10.0.0.1",
                "--deny-host",
                "10.0.0.2",
                "--operation",
                "Read",
                "--group",
                "g",
            ],
            Plan::Add(acls([(
                resource(Group, "g", Literal),
                vec![
                    entry("User:eve", "10.0.0.1", Read, Permission::Deny),
                    entry("User:eve", "10.0.0.2", Read, Permission::Deny),
                ],
            )])),
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--allow-host",
                "h1",
                "--deny-principal",
                "User:b",
                "--cluster",
                "--operation",
                "Alter",
            ],
            Plan::Add(acls([(
                resource(Cluster, "kafka-cluster", Literal),
                vec![
                    entry("User:a", "h1", Alter, Permission::Allow),
                    entry("User:b", "*", Alter, Permission::Deny),
                ],
            )])),
        ),
    ];
    check_plans(cases);
}

#[test]
fn remove_flags_resolve_to_the_entries_that_kafka_resolves() {
    use Operation::Read;
    use PatternType::Any;
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &["--remove", "--topic", "t"],
            Plan::Remove(acls([(topic("t"), Vec::new())])),
        ),
        (
            &[
                "--remove",
                "--topic",
                "t",
                "--allow-principal",
                "User:a",
                "--operation",
                "Read",
                "--resource-pattern-type",
                "ANY",
            ],
            Plan::Remove(acls([(
                resource(ResourceType::Topic, "t", Any),
                vec![allow("User:a", Read)],
            )])),
        ),
    ];
    check_plans(cases);
}

#[test]
fn a_command_line_that_kafka_refuses_is_refused_with_kafka_s_message() {
    let cases: [(&[&str], &str); 20] = [
        (
            &[],
            "Command must include exactly one action: --list, --add, --remove. ",
        ),
        (
            &["--list", "--add"],
            "Command must include exactly one action: --list, --add, --remove. ",
        ),
        (
            &["--list", "--producer"],
            "Option \"[list]\" can't be used with option \"[producer]\"",
        ),
        (
            &["--list", "--allow-host", "h"],
            "Option \"[list]\" can't be used with option \"[allow-host]\"",
        ),
        (
            &[
                "--add",
                "--producer",
                "--operation",
                "Read",
                "--topic",
                "t",
                "--allow-principal",
                "User:a",
            ],
            "Option \"[producer]\" can't be used with option \"[operation]\"",
        ),
        (
            &[
                "--add",
                "--consumer",
                "--deny-principal",
                "User:a",
                "--topic",
                "t",
                "--group",
                "g",
            ],
            "Option \"[consumer]\" can't be used with option \"[deny-principal]\"",
        ),
        (
            &["--add", "--principal", "User:a", "--topic", "t"],
            "The --principal option is only available if --list is set",
        ),
        (
            &[
                "--add",
                "--idempotent",
                "--topic",
                "t",
                "--allow-principal",
                "User:a",
            ],
            "The --idempotent option is only available if --producer is set",
        ),
        (
            &[
                "--add",
                "--consumer",
                "--topic",
                "t",
                "--allow-principal",
                "User:a",
            ],
            "With --consumer you must specify a --topic and a --group and no --cluster or \
             --transactional-id option should be specified.",
        ),
        (
            &[
                "--add",
                "--consumer",
                "--topic",
                "t",
                "--group",
                "g",
                "--cluster",
                "--allow-principal",
                "User:a",
            ],
            "With --consumer you must specify a --topic and a --group and no --cluster or \
             --transactional-id option should be specified.",
        ),
        (
            &["--add", "--producer", "--allow-principal", "User:a"],
            "With --producer you must specify a --topic",
        ),
        (
            &["--add", "--allow-principal", "User:a"],
            "You must provide at least one resource: --topic <topic> or --cluster or --group \
             <group> or --delegation-token <Delegation Token ID>",
        ),
        (
            &["--add", "--topic", "x"],
            "You must specify one of: --allow-principal, --deny-principal when trying to add \
             ACLs.",
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--topic",
                "x",
                "--resource-pattern-type",
                "match",
            ],
            "A '--resource-pattern-type' value of 'MATCH' is not valid when adding acls.",
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--group",
                "x",
                "--operation",
                "Write",
            ],
            "ResourceType GROUP only supports operations [READ, DESCRIBE, DELETE, \
             DESCRIBE_CONFIGS, ALTER_CONFIGS, ALL]",
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--operation",
                "Foo",
                "--topic",
                "x",
            ],
            "ResourceType TOPIC only supports operations [READ, WRITE, CREATE, DESCRIBE, DELETE, \
             ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS, ALL]",
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--delegation-token",
                "token-1",
                "--operation",
                "CreateTokens",
            ],
            "ResourceType DELEGATION_TOKEN only supports operations [DESCRIBE, ALL]",
        ),
        (
            &["--add", "--allow-principal", "a", "--topic", "x"],
            "expected a string in format principalType:principalName but got a",
        ),
        (
            &[
                "--add",
                "--allow-principal",
                "User:a",
                "--operation",
                "Any",
                "--topic",
                "x",
            ],
            "operation must not be ANY",
        ),
        (
            &["--list", "--dry-run"],
            "--dry-run is only valid with --add or --remove",
        ),
    ];
    for (argv, message) in cases {
        let error = plan(argv).unwrap_err();
        check!(
            (error.exit(), error.to_string()) == (Exit::Usage, message.to_owned()),
            "{argv:?}"
        );
    }
}

#[test]
fn a_missing_bootstrap_flag_is_refused_with_kafka_s_message() {
    let args = Cli::try_parse_from(["acls", "--list"]).unwrap().acls;
    let error = args.plan().unwrap_err();
    check!(
        (error.exit(), error.to_string())
            == (
                Exit::Usage,
                "One of --bootstrap-server or --bootstrap-controller must be specified".into()
            )
    );
}

fn wire(principal: &str, operation: WireOperation, resource: &str) -> AclEntry {
    AclEntry {
        resource_type: admin::ResourceType::Topic,
        resource_name: resource.into(),
        pattern_type: WirePattern::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: admin::PermissionType::Allow,
    }
}

fn topic_filter(name: &str, pattern_type: Option<WirePattern>) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(admin::ResourceType::Topic),
        resource_name: Some(name.into()),
        pattern_type,
        ..AclEntryFilter::default()
    }
}

fn add_requests(argv: &[&str]) -> Vec<(AclEntryFilter, Vec<AclEntry>)> {
    let Ok(Plan::Add(acls)) = plan(argv) else {
        panic!("{argv:?} is not an add");
    };
    add_steps(acls)
        .unwrap()
        .into_iter()
        .map(|step| {
            let creations = step.creations.into_iter().map(|(_, wire)| wire).collect();
            (step.existing, creations)
        })
        .collect()
}

#[test]
fn the_producer_expansion_creates_one_entry_per_operation_after_one_describe() {
    check!(
        add_requests(&[
            "--add",
            "--allow-principal",
            "User:alice",
            "--producer",
            "--topic",
            "t",
        ]) == [(
            topic_filter("t", Some(WirePattern::Literal)),
            vec![
                wire("User:alice", WireOperation::Write, "t"),
                wire("User:alice", WireOperation::Create, "t"),
                wire("User:alice", WireOperation::Describe, "t"),
            ],
        )]
    );
}

#[test]
fn the_consumer_expansion_creates_its_entries_on_each_resource() {
    let group = |operation| AclEntry {
        resource_type: admin::ResourceType::Group,
        ..wire("User:c", operation, "g")
    };
    check!(
        add_requests(&[
            "--add",
            "--consumer",
            "--allow-principal",
            "User:c",
            "--topic",
            "t",
            "--group",
            "g",
        ]) == [
            (
                topic_filter("t", Some(WirePattern::Literal)),
                vec![
                    wire("User:c", WireOperation::Read, "t"),
                    wire("User:c", WireOperation::Describe, "t"),
                ],
            ),
            (
                AclEntryFilter {
                    resource_type: Some(admin::ResourceType::Group),
                    ..topic_filter("g", Some(WirePattern::Literal))
                },
                vec![group(WireOperation::Read)],
            ),
        ]
    );
}

#[test]
fn remove_and_list_send_the_filters_that_kafka_sends() {
    let remove = |argv: &[&str]| {
        let Ok(Plan::Remove(acls)) = plan(argv) else {
            panic!("{argv:?} is not a remove");
        };
        remove_steps(acls)
            .unwrap()
            .into_iter()
            .map(|step| step.filters)
            .collect::<Vec<_>>()
    };
    let list = |argv: &[&str]| {
        let Ok(Plan::List { filters, .. }) = plan(argv) else {
            panic!("{argv:?} is not a list");
        };
        list_requests(&filters)
    };
    check!(
        remove(&["--remove", "--topic", "t"])
            == [vec![topic_filter("t", Some(WirePattern::Literal))]]
    );
    check!(
        remove(&[
            "--remove",
            "--topic",
            "t",
            "--allow-principal",
            "User:a",
            "--operation",
            "Read",
            "--operation",
            "Write",
            "--resource-pattern-type",
            "any",
        ]) == [[WireOperation::Read, WireOperation::Write]
            .map(|operation| AclEntryFilter {
                principal: Some("User:a".into()),
                host: Some("*".into()),
                operation: Some(operation),
                permission_type: Some(admin::PermissionType::Allow),
                ..topic_filter("t", None)
            })
            .to_vec()]
    );
    check!(list(&["--list"]) == [AclEntryFilter::default()]);
    check!(
        list(&["--list", "--topic", "t", "--resource-pattern-type", "match"])
            == [topic_filter("t", Some(WirePattern::Match))]
    );
    check!(
        list(&[
            "--list",
            "--topic",
            "t",
            "--resource-pattern-type",
            "prefixed"
        ]) == [topic_filter("t", Some(WirePattern::Prefixed))]
    );
}

#[test]
fn listing_prints_kafka_s_grouped_shape() {
    let current = acls([
        (
            topic("orders"),
            vec![
                allow("User:alice", Operation::Read),
                allow("User:bob", Operation::Write),
            ],
        ),
        (
            resource(ResourceType::Group, "g1", PatternType::Prefixed),
            vec![allow("User:c", Operation::Read)],
        ),
    ]);
    let all = listed(&current, None);
    check!(
        all.human.join("\n")
            == "Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, \
                patternType=LITERAL)`:\n\
                \t(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)\n\
                \t(principal=User:bob, host=*, operation=WRITE, permissionType=ALLOW)\n\
                \n\
                Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=g1, \
                patternType=PREFIXED)`:\n\
                \t(principal=User:c, host=*, operation=READ, permissionType=ALLOW)\n"
    );
    let one = listed(&current, Some(&["User:c".into(), "User:nobody".into()]));
    check!(
        one.human.join("\n")
            == "ACLs for principal `User:c`\n\
                Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=g1, \
                patternType=PREFIXED)`:\n\
                \t(principal=User:c, host=*, operation=READ, permissionType=ALLOW)\n\
                \n\
                ACLs for principal `User:nobody`"
    );
    check!(
        one.data
            == json!([
                {"principal": "User:c", "acls": [{
                    "resource": {"resource_type": "GROUP", "name": "g1", "pattern_type": "PREFIXED"},
                    "acls": [{"principal": "User:c", "host": "*", "operation": "READ", "permission_type": "ALLOW"}],
                }]},
                {"principal": "User:nobody", "acls": []},
            ])
    );
}

async fn execute_declining(argv: &[&str]) -> (Result<CommandResult, CommandError>, Vec<Impact>) {
    let mut asked = Vec::new();
    let outcome = parse(argv)
        .execute(|impact| {
            let mut prompt = Vec::new();
            let answer = confirm_with(
                false,
                true,
                "krabka acls",
                &impact,
                &mut "n\n".as_bytes(),
                &mut prompt,
            );
            asked.push(impact);
            std::future::ready(answer)
        })
        .await;
    (outcome, asked)
}

#[tokio::test]
async fn a_declined_removal_stops_before_it_connects() {
    // 127.0.0.1:1 has no listener, so a command that got as far as
    // connecting would fail with a connection error rather than a refusal.
    let (outcome, asked) = execute_declining(&[
        "--remove",
        "--topic",
        "t",
        "--allow-principal",
        "User:a",
        "--operation",
        "Read",
        "--group",
        "g",
    ])
    .await;
    assert!(matches!(
        outcome,
        Err(CommandError::Refused(Refusal::Declined))
    ));
    check!(
        asked
            == [Impact {
                summary: "remove ACLs from 2 resource filter(s)".into(),
                resources: vec![
                    "(principal=User:a, host=*, operation=READ, permissionType=ALLOW) from \
                     resource filter `ResourcePattern(resourceType=TOPIC, name=t, \
                     patternType=LITERAL)`"
                        .into(),
                    "(principal=User:a, host=*, operation=READ, permissionType=ALLOW) from \
                     resource filter `ResourcePattern(resourceType=GROUP, name=g, \
                     patternType=LITERAL)`"
                        .into(),
                ],
            }]
    );
    let (_, asked) = execute_declining(&["--remove", "--cluster"]).await;
    check!(
        asked
            == [Impact {
                summary: "remove ACLs from 1 resource filter(s)".into(),
                resources: vec![
                    "all ACLs for resource filter `ResourcePattern(resourceType=CLUSTER, \
                     name=kafka-cluster, patternType=LITERAL)`"
                        .into()
                ],
            }]
    );
}

#[tokio::test]
async fn a_dry_run_or_a_read_does_not_ask() {
    for argv in [
        &["--remove", "--topic", "t", "--dry-run"][..],
        &["--list", "--topic", "t"],
        &["--add", "--allow-principal", "User:a", "--topic", "t"],
    ] {
        let (outcome, asked) = execute_declining(argv).await;
        check!(asked.is_empty(), "{argv:?}");
        // Nothing listens on the bootstrap address, so each fails to connect.
        check!(matches!(outcome, Err(CommandError::Other(_))), "{argv:?}");
    }
}

#[test]
fn force_and_yes_both_parse_and_repeat_as_kafka_allows() {
    let args = parse(&["--remove", "--remove", "--force", "--yes", "--topic", "t"]);
    check!((args.force, args.confirm.yes, args.action.remove) == (true, true, true));
}
