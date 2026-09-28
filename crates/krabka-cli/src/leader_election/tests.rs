use assert2::check;
use clap::Parser;

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: LeaderElectionArgs,
}

fn validate(argv: &[&str]) -> Result<Election, String> {
    let argv = std::iter::once("leader-election").chain(argv.iter().copied());
    Command::try_parse_from(argv)
        .map_err(|error| error.to_string())?
        .args
        .validate()
}

fn partitions(entries: &[(&str, i32)]) -> BTreeSet<TopicPartition> {
    entries
        .iter()
        .map(|(topic, partition)| TopicPartition::new(*topic, *partition))
        .collect()
}

const SERVER: [&str; 2] = ["--bootstrap-server", "h:9092"];

#[test]
fn selection_forms_are_validated_as_kafka_does() {
    let with_server = |rest: &[&str]| {
        let argv = SERVER.iter().chain(rest).copied().collect::<Vec<_>>();
        validate(&argv)
    };
    let election = |kind, target| {
        Ok(Election {
            kind,
            target,
            notices: Vec::new(),
        })
    };
    let one_of = Err(
        "One and only one of the following options is required: topic, \
                      all-topic-partitions, path-to-json-file"
            .to_owned(),
    );
    let cases = [
        (
            vec!["--election-type", "preferred", "--all-topic-partitions"],
            election(ElectionType::Preferred, Target::All),
        ),
        (
            vec![
                "--election-type",
                "UNCLEAN",
                "--topic",
                "foo",
                "--partition",
                "0",
            ],
            election(
                ElectionType::Unclean,
                Target::Partitions(partitions(&[("foo", 0)])),
            ),
        ),
        (
            vec![
                "--election-type",
                "Preferred",
                "--path-to-json-file",
                "e.json",
            ],
            election(ElectionType::Preferred, Target::File("e.json".into())),
        ),
        (
            vec!["--all-topic-partitions"],
            Err("Missing required option(s): election-type".to_owned()),
        ),
        (
            vec!["--election-type", "preferred", "--topic", "foo"],
            Err("Missing required option(s): partition".to_owned()),
        ),
        (
            vec![
                "--election-type",
                "preferred",
                "--all-topic-partitions",
                "--partition",
                "1",
            ],
            Err("Option partition is only allowed if topic is used".to_owned()),
        ),
        (
            vec![
                "--election-type",
                "preferred",
                "--all-topic-partitions",
                "--topic",
                "foo",
            ],
            one_of.clone(),
        ),
        (
            vec![
                "--election-type",
                "preferred",
                "--topic",
                "foo",
                "--partition",
                "0",
                "--path-to-json-file",
                "e.json",
            ],
            one_of.clone(),
        ),
        (
            vec![
                "--election-type",
                "preferred",
                "--all-topic-partitions",
                "--path-to-json-file",
                "e.json",
            ],
            one_of.clone(),
        ),
        (vec!["--election-type", "preferred"], one_of),
        (
            vec!["--election-type", "bogus", "--all-topic-partitions"],
            Err("Cannot parse argument 'bogus' of option election-type".to_owned()),
        ),
    ];
    for (argv, expected) in cases {
        check!(with_server(&argv) == expected, "{argv:?}");
    }
    check!(
        validate(&["--all-topic-partitions"])
            == Err("Missing required option(s): bootstrap-server, election-type".to_owned())
    );
}

#[test]
fn the_deprecated_admin_config_is_noted_and_conflicts_with_command_config() {
    let argv = |extra: &[&'static str]| {
        let mut argv = vec![
            "--bootstrap-server",
            "h:9092",
            "--election-type",
            "preferred",
            "--all-topic-partitions",
            "--admin.config",
            "a.properties",
        ];
        argv.extend_from_slice(extra);
        argv
    };
    check!(
        validate(&argv(&[])).map(|election| election.notices)
            == Ok(vec![
                "Option --admin.config has been deprecated and will be removed in a future \
                 version. Use --command-config instead."
                    .to_owned()
            ])
    );
    check!(
        validate(&argv(&["--command-config", "c.properties"]))
            == Err(
                "Option \"[admin.config]\" can't be used with option \"[command-config]\""
                    .to_owned()
            )
    );
}

#[test]
fn all_partitions_and_no_partitions_are_distinct_requests() {
    check!(Selection::All.request_partitions() == None);
    check!(Selection::Partitions(BTreeSet::new()).request_partitions() == Some(Vec::new()));
    check!(
        Selection::Partitions(partitions(&[("foo", 1), ("bar", 0)])).request_partitions()
            == Some(vec![("bar".to_owned(), 0), ("foo".to_owned(), 1)])
    );
}

#[test]
fn election_files_parse_or_fail_with_kafkas_messages() {
    let cases = [
        (
            r#"{"partitions": [{"topic": "foo", "partition": 1}, {"topic": "bar", "partition": 0}]}"#,
            Ok(partitions(&[("foo", 1), ("bar", 0)])),
        ),
        (r#"{"partitions": []}"#, Ok(BTreeSet::new())),
        ("", Err("Replica election data is empty")),
        ("{", Err("Replica election data is empty")),
        ("\n", Err("Expected JSON object, received ")),
        ("[]", Err("Expected JSON object, received []")),
        (
            "{}",
            Err("Replica election data is missing \"partitions\" field"),
        ),
        (
            r#"{"partitions": {}}"#,
            Err("Expected JSON array, received {}"),
        ),
        (
            r#"{"partitions": [{"topic": "foo"}]}"#,
            Err("No such field exists: `partition`"),
        ),
        (
            r#"{"partitions": [{"topic": "foo", "partition": "1"}]}"#,
            Err("Expected `Integer` value, received \"1\""),
        ),
        (
            r#"{"partitions": [{"topic": "foo", "partition": 1}, {"topic": "foo", "partition": 1}]}"#,
            Err("Replica election data contains duplicate partitions: [foo-1]"),
        ),
    ];
    for (text, expected) in cases {
        check!(
            parse_election_data(text) == expected.map_err(ToOwned::to_owned),
            "{text}"
        );
    }
}

fn error(code: i16, name: &'static str, message: &str) -> KafkaError {
    KafkaError {
        code,
        name,
        message: Some(message.into()),
    }
}

#[test]
fn results_render_as_kafka_leader_election_prints_them() {
    let results = ElectionResults::from([
        (TopicPartition::new("foo", 0), None),
        (TopicPartition::new("bar", 1), None),
        (
            TopicPartition::new("foo", 1),
            Some(error(
                84,
                "ELECTION_NOT_NEEDED",
                "Leader election not needed",
            )),
        ),
        (
            TopicPartition::new("nope", 0),
            Some(error(
                3,
                "UNKNOWN_TOPIC_OR_PARTITION",
                "No such topic as nope",
            )),
        ),
    ]);
    let report = election_report(ElectionType::Unclean, &results);
    check!(
        report.human
            == vec![
                "Successfully completed leader election (UNCLEAN) for partitions bar-1, foo-0",
                "Valid replica already elected for partitions foo-1",
                "Error completing leader election (UNCLEAN) for partition: nope-0: \
                 UNKNOWN_TOPIC_OR_PARTITION (3): No such topic as nope",
            ]
    );
    check!(report.failed);
    check!(
        report.data[3]
            == json!({
                "topic": "nope",
                "partition": 0,
                "election_type": "UNCLEAN",
                "error": {"code": 3, "name": "UNKNOWN_TOPIC_OR_PARTITION", "message": "No such topic as nope"},
            })
    );
}

#[test]
fn election_not_needed_is_not_a_failure() {
    let results = ElectionResults::from([(
        TopicPartition::new("foo", 0),
        Some(error(84, "ELECTION_NOT_NEEDED", "")),
    )]);
    let report = election_report(ElectionType::Preferred, &results);
    check!(
        (report.human, report.failed)
            == (
                vec!["Valid replica already elected for partitions foo-0".to_owned()],
                false
            )
    );
    let empty = election_report(ElectionType::Preferred, &ElectionResults::new());
    check!((empty.human, empty.failed) == (Vec::<String>::new(), false));
}
