use assert2::check;
use krabka_client_admin::KafkaError;

use super::*;

fn tp(topic: &str, partition: i32) -> TopicPartition {
    TopicPartition::new(topic, partition)
}

fn active(
    topic: &str,
    partition: i32,
    replicas: &[i32],
    adding: &[i32],
    removing: &[i32],
) -> (TopicPartition, PartitionAssignment) {
    (
        tp(topic, partition),
        PartitionAssignment {
            topic: topic.into(),
            partition,
            replicas: replicas.to_vec(),
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
        },
    )
}

#[test]
fn list_renders_as_kafka_does() {
    check!(list_lines(&BTreeMap::new()) == vec!["No partition reassignments found."]);
    let reassignments = BTreeMap::from([
        active("foo", 1, &[1, 2, 3, 4], &[4], &[1]),
        active("bar", 0, &[2, 3], &[3], &[]),
        active("foo", 0, &[1, 2], &[], &[]),
    ]);
    check!(
        list_lines(&reassignments)
            == vec![
                "Current partition reassignments:",
                "bar-0: replicas: 2,3. adding: 3.",
                "foo-0: replicas: 1,2.",
                "foo-1: replicas: 1,2,3,4. adding: 4. removing: 1.",
            ]
    );
}

#[test]
fn verify_reports_each_partition_as_kafka_does() {
    let targets = vec![
        (tp("foo", 0), vec![1, 2]),
        (tp("foo", 1), vec![2, 3]),
        (tp("bar", 0), vec![1]),
        (tp("gone", 0), vec![1]),
    ];
    let reassigning = BTreeMap::from([active("foo", 1, &[1, 2, 3], &[3], &[1])]);
    let described = BTreeMap::from([(tp("foo", 0), vec![1, 2]), (tp("bar", 0), vec![2])]);
    let states = partition_states(&targets, &reassigning, &described);
    check!(
        states
            == BTreeMap::from([
                (
                    tp("bar", 0),
                    PartitionState {
                        current: vec![2],
                        target: vec![1],
                        done: true
                    }
                ),
                (
                    tp("foo", 0),
                    PartitionState {
                        current: vec![1, 2],
                        target: vec![1, 2],
                        done: true
                    }
                ),
                (
                    tp("foo", 1),
                    PartitionState {
                        current: vec![1, 2, 3],
                        target: vec![2, 3],
                        done: false
                    }
                ),
                (
                    tp("gone", 0),
                    PartitionState {
                        current: vec![],
                        target: vec![1],
                        done: true
                    }
                ),
            ])
    );
    check!(
        status_lines(&states)
            == vec![
                "Status of partition reassignment:",
                "There is no active reassignment of partition bar-0, but replica set is 2 rather \
                 than 1.",
                "Reassignment of partition foo-0 is completed.",
                "Reassignment of partition foo-1 is still in progress.",
                "There is no active reassignment of partition gone-0, but replica set is  rather \
                 than 1.",
            ]
    );
    check!(
        states_json(&states)
            .iter()
            .map(|state| state["status"].clone())
            .collect::<Vec<_>>()
            == vec![
                json!("mismatch"),
                json!("completed"),
                json!("in_progress"),
                json!("mismatch")
            ]
    );
}

#[test]
fn throttles_are_computed_as_kafka_computes_them() {
    let reassigning = BTreeMap::from([active("foo", 1, &[1, 2, 3], &[3], &[1])]);
    let proposed = BTreeMap::from([
        (tp("foo", 0), vec![2, 3]),
        (tp("foo", 1), vec![3, 4]),
        (tp("bar", 10), vec![5]),
    ]);
    let current = BTreeMap::from([
        (tp("foo", 0), vec![1, 2]),
        (tp("foo", 1), vec![1, 2]),
        (tp("bar", 10), vec![4]),
    ]);
    let moves = MoveMap::proposed(&reassigning, &proposed, &current).unwrap();
    check!(
        moves.leader_throttles()
            == BTreeMap::from([
                ("bar".to_owned(), "10:4".to_owned()),
                ("foo".to_owned(), "0:1,0:2,1:1,1:2".to_owned()),
            ])
    );
    check!(
        moves.follower_throttles()
            == BTreeMap::from([
                ("bar".to_owned(), "10:5".to_owned()),
                ("foo".to_owned(), "0:3,1:3,1:4".to_owned()),
            ])
    );
    check!(moves.brokers() == BTreeSet::from([1, 2, 3, 4, 5]));
    let log_dir_moves = BTreeMap::from([(
        Replica {
            broker: 5,
            topic: "bar".into(),
            partition: 10,
        },
        "/data/b".to_owned(),
    )]);
    let throttles = Throttles::new(&moves, &log_dir_moves, 50_000_000, 1_000);
    let rate = |extra: Option<&str>| {
        let mut configs = BTreeMap::from([
            (LEADER_RATE, "50000000".to_owned()),
            (FOLLOWER_RATE, "50000000".to_owned()),
        ]);
        if let Some(value) = extra {
            configs.insert(LOG_DIR_RATE, value.to_owned());
        }
        configs
    };
    check!(
        throttles.brokers
            == BTreeMap::from([
                (1, rate(None)),
                (2, rate(None)),
                (3, rate(None)),
                (4, rate(None)),
                (5, rate(Some("1000"))),
            ])
    );
    check!(
        throttles.topics
            == BTreeMap::from([
                (
                    "bar".to_owned(),
                    BTreeMap::from([
                        (LEADER_REPLICAS, "10:4".to_owned()),
                        (FOLLOWER_REPLICAS, "10:5".to_owned()),
                    ])
                ),
                (
                    "foo".to_owned(),
                    BTreeMap::from([
                        (LEADER_REPLICAS, "0:1,0:2,1:1,1:2".to_owned()),
                        (FOLLOWER_REPLICAS, "0:3,1:3,1:4".to_owned()),
                    ])
                ),
            ])
    );
    check!(
        throttles.lines()
            == vec![
                "Warning: You must run --verify periodically, until the reassignment completes, \
                 to ensure the throttle is removed.",
                "The inter-broker throttle limit was set to 50000000 B/s",
                "The replica-alter-dir throttle limit was set to 1000 B/s",
            ]
    );
    let off = Throttles::new(&moves, &log_dir_moves, -1, -1);
    check!(
        (
            off.topics.is_empty(),
            off.brokers.is_empty(),
            off.lines().is_empty()
        ) == (true, true, true)
    );
}

#[test]
fn a_proposal_without_current_replicas_is_refused() {
    check!(
        MoveMap::proposed(
            &BTreeMap::new(),
            &BTreeMap::from([(tp("foo", 0), vec![1])]),
            &BTreeMap::new()
        ) == Err("Trying to reassign a topic partition foo-0 with 0 replicas".into())
    );
}

#[test]
fn execute_and_cancel_lines_match_kafka() {
    check!(
        rollback_lines("{}")
            == vec![
                "Current partition replica assignment",
                "",
                "{}",
                "",
                "Save this to use as the --reassignment-json-file option during rollback",
            ]
    );
    check!(
        started_line(&[tp("bar", 0), tp("foo", 0)])
            == "Successfully started partition reassignments for bar-0,foo-0"
    );
    check!(
        started_line(&[tp("foo", 0)]) == "Successfully started partition reassignment for foo-0"
    );
    check!(
        cancelled_line(&BTreeSet::from([tp("foo", 1)]))
            == "Successfully cancelled partition reassignment for: foo-1"
    );
    check!(
        cancelled_line(&BTreeSet::from([tp("foo", 1), tp("bar", 0)]))
            == "Successfully cancelled partition reassignments for: bar-0,foo-1"
    );
    let moves = BTreeMap::from([(
        Replica {
            broker: 2,
            topic: "foo".into(),
            partition: 0,
        },
        "/data/a".to_owned(),
    )]);
    check!(
        move_lines(&moves)
            == vec![
                "Successfully started moving log directory to /data/a for replica foo-0 with \
                     broker 2 "
            ]
    );
}

#[test]
fn partition_errors_are_sorted_with_their_messages() {
    let outcome = |topic: &str, partition, error: Option<KafkaError>| PartitionAssignmentOutcome {
        topic: topic.into(),
        partition,
        error,
    };
    let outcomes = [
        outcome("foo", 1, None),
        outcome(
            "foo",
            0,
            Some(KafkaError {
                code: 85,
                name: "NO_REASSIGNMENT_IN_PROGRESS",
                message: None,
            }),
        ),
        outcome(
            "bar",
            3,
            Some(KafkaError {
                code: 38,
                name: "INVALID_REPLICATION_FACTOR",
                message: Some("Replica 7 is not alive".into()),
            }),
        ),
    ];
    check!(
        partition_errors(&outcomes)
            == vec![
                "bar-3: Replica 7 is not alive",
                "foo-0: NO_REASSIGNMENT_IN_PROGRESS (85)",
            ]
    );
}

#[test]
fn usable_brokers_honour_rack_awareness() {
    let node = |id, rack: Option<&str>| Node {
        id,
        host: "h".into(),
        port: 9092,
        rack: rack.map(Into::into),
        fenced: false,
    };
    let nodes = [node(1, Some("a")), node(2, None), node(3, Some("b"))];
    check!(
        usable_brokers(&nodes, &[1, 2], true)
            == Err(
                "Not all brokers have rack information. Add --disable-rack-aware in command \
                    line to make replica assignment without rack information."
                    .into()
            )
    );
    check!(
        usable_brokers(&nodes, &[1, 3, 9], true)
            == Ok(vec![
                UsableBroker {
                    id: 1,
                    rack: Some("a".into()),
                    fenced: false
                },
                UsableBroker {
                    id: 3,
                    rack: Some("b".into()),
                    fenced: false
                },
            ])
    );
    check!(
        usable_brokers(&nodes, &[1, 2], false)
            == Ok(vec![
                UsableBroker {
                    id: 1,
                    rack: None,
                    fenced: false
                },
                UsableBroker {
                    id: 2,
                    rack: None,
                    fenced: false
                },
            ])
    );
}
