use assert2::check;

use super::*;

fn target(topic: &str, partition: i32, replicas: &[i32], log_dirs: &[&str]) -> PartitionTarget {
    PartitionTarget {
        partition: TopicPartition::new(topic, partition),
        replicas: replicas.to_vec(),
        log_dirs: log_dirs.iter().map(|dir| (*dir).to_owned()).collect(),
    }
}

/// `kafka-reassign-partitions --generate` output from apache/kafka:4.3.1,
/// verbatim: the current assignment, then the proposal.
const JVM_CURRENT: &str = r#"{"version":1,"partitions":[{"topic":"bar","partition":0,"replicas":[1],"log_dirs":["/tmp/kraft-combined-logs"]},{"topic":"foo","partition":0,"replicas":[1],"log_dirs":["/tmp/kraft-combined-logs"]},{"topic":"foo","partition":1,"replicas":[1],"log_dirs":["/tmp/kraft-combined-logs"]},{"topic":"foo","partition":2,"replicas":[1],"log_dirs":["/tmp/kraft-combined-logs"]}]}"#;
const JVM_PROPOSED: &str = r#"{"version":1,"partitions":[{"topic":"bar","partition":0,"replicas":[1],"log_dirs":["any"]},{"topic":"foo","partition":0,"replicas":[1],"log_dirs":["any"]},{"topic":"foo","partition":1,"replicas":[1],"log_dirs":["any"]},{"topic":"foo","partition":2,"replicas":[1],"log_dirs":["any"]}]}"#;

#[test]
fn a_jvm_generated_file_parses_and_formats_back_byte_for_byte() {
    let dir = "/tmp/kraft-combined-logs";
    let current = parse_reassignment(JVM_CURRENT).unwrap();
    check!(
        current
            == ReassignmentFile {
                partitions: vec![
                    target("bar", 0, &[1], &[dir]),
                    target("foo", 0, &[1], &[dir]),
                    target("foo", 1, &[1], &[dir]),
                    target("foo", 2, &[1], &[dir]),
                ],
            }
    );
    let assignment = current.targets().into_iter().collect::<BTreeMap<_, _>>();
    check!(format_reassignment(&assignment, &current.log_dir_moves()) == JVM_CURRENT);
    check!(format_reassignment(&assignment, &BTreeMap::new()) == JVM_PROPOSED);
    let proposed = parse_reassignment(JVM_PROPOSED).unwrap();
    check!(proposed.log_dir_moves().is_empty());
    check!(proposed.targets() == current.targets());
}

#[test]
fn replica_order_is_kept_and_output_is_sorted_by_topic_then_partition() {
    let assignment = BTreeMap::from([
        (TopicPartition::new("foo", 10), vec![3, 1, 2]),
        (TopicPartition::new("foo", 9), vec![2, 3, 1]),
        (TopicPartition::new("bar", 0), vec![1]),
    ]);
    let log_dirs = BTreeMap::from([(
        Replica {
            broker: 1,
            topic: "foo".into(),
            partition: 10,
        },
        "/data/a".to_owned(),
    )]);
    let text = format_reassignment(&assignment, &log_dirs);
    check!(
        text == r#"{"version":1,"partitions":[{"topic":"bar","partition":0,"replicas":[1],"log_dirs":["any"]},{"topic":"foo","partition":9,"replicas":[2,3,1],"log_dirs":["any","any","any"]},{"topic":"foo","partition":10,"replicas":[3,1,2],"log_dirs":["any","/data/a","any"]}]}"#
    );
    let parsed = parse_reassignment(&text).unwrap();
    check!(parsed.targets().into_iter().collect::<BTreeMap<_, _>>() == assignment);
    check!(parsed.log_dir_moves() == log_dirs);
}

#[test]
fn optional_fields_default_as_kafka_defaults_them() {
    check!(
        parse_reassignment(r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"#)
            == Ok(ReassignmentFile {
                partitions: vec![target("foo", 0, &[1, 2], &["any", "any"])],
            })
    );
    check!(parse_reassignment(r#"{"version":1}"#) == Ok(ReassignmentFile::default()));
}

#[test]
fn malformed_reassignment_files_fail_with_kafkas_messages() {
    let cases = [
        ("", "The input string shouldn't be empty"),
        ("  ", "Expected JSON object, received "),
        ("[]", "Expected JSON object, received []"),
        (r#"{"version":2}"#, "Not supported version field value 2"),
        (
            r#"{"version":"1"}"#,
            "Expected `Integer` value, received \"1\"",
        ),
        (r#"{"partitions":{}}"#, "Expected JSON array, received {}"),
        (
            r#"{"partitions":[{"topic":"foo","partition":0}]}"#,
            "No such field exists: `replicas`",
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[1,2],"log_dirs":["any"]}]}"#,
            "Size of replicas list [1, 2] is different from size of log dirs list [any] for \
             partition foo-0",
        ),
    ];
    for (text, message) in cases {
        check!(
            parse_reassignment(text) == Err(message.to_owned()),
            "{text}"
        );
    }
    check!(parse_reassignment("{").is_err());
}

#[test]
fn execute_refuses_what_kafka_refuses() {
    let file = |text: &str| parse_reassignment(text).unwrap();
    let cases = [
        (
            r#"{"partitions":[]}"#,
            Err("Partition reassignment list cannot be empty"),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[]}]}"#,
            Err("Partition replica list cannot be empty"),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[1]},{"topic":"foo","partition":0,"replicas":[1]},{"topic":"bar","partition":1,"replicas":[1]},{"topic":"bar","partition":1,"replicas":[1]}]}"#,
            Err("Partition reassignment contains duplicate topic partitions: bar-1,foo-0"),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[1,1,2,2]},{"topic":"bar","partition":0,"replicas":[3,3]}]}"#,
            Err(
                "Partition replica lists may not contain duplicate entries: foo-0 contains \
                 multiple entries for 1,2. bar-0 contains multiple entries for 3",
            ),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"#,
            Ok(()),
        ),
    ];
    for (text, expected) in cases {
        check!(
            check_execute(&file(text)) == expected.map_err(ToOwned::to_owned),
            "{text}"
        );
    }
}

#[test]
fn generate_arguments_parse_or_fail_with_kafkas_messages() {
    let topics = r#"{"topics":[{"topic":"foo"},{"topic":"bar"}],"version":1}"#;
    check!(
        parse_generate(topics, "1,2,3")
            == Ok((vec![1, 2, 3], vec!["foo".to_owned(), "bar".to_owned()]))
    );
    check!(parse_generate(r#"{"version":1}"#, "1") == Ok((vec![1], Vec::new())));
    let cases = [
        (
            topics,
            "1,2,1,2",
            "Broker list contains duplicate entries: [1, 2]",
        ),
        (topics, "1,x", "For input string: \"x\""),
        (topics, "", "For input string: \"\""),
        (
            r#"{"topics":[{"topic":"foo"},{"topic":"foo"}]}"#,
            "1",
            "List of topics to reassign contains duplicate entries: [foo]",
        ),
        ("{", "1", "The input string is not a valid JSON"),
        (
            r#"{"version":2}"#,
            "1",
            "Not supported version field value 2",
        ),
        (r#"{"topics":[{}]}"#, "1", "No such field exists: `topic`"),
    ];
    for (topics, brokers, message) in cases {
        check!(
            parse_generate(topics, brokers) == Err(message.to_owned()),
            "{topics} {brokers}"
        );
    }
}

#[test]
fn duplicates_follow_hash_set_order() {
    // Kafka prints the duplicate partitions of this file as bar-1,foo-0: the
    // `HashSet` order, not the file order.
    let partitions = [
        TopicPartition::new("foo", 0),
        TopicPartition::new("foo", 0),
        TopicPartition::new("bar", 1),
        TopicPartition::new("bar", 1),
    ];
    check!(
        duplicates(&partitions, TopicPartition::java_hash)
            == vec![TopicPartition::new("bar", 1), TopicPartition::new("foo", 0)]
    );
    check!(duplicates(&[3, 1, 3, 1, 2], |id| *id) == vec![1, 3]);
}
