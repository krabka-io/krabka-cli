use assert2::check;

use super::*;

fn op(topic: &str, partition: i32, offset: i64) -> DeleteRecordsOp {
    DeleteRecordsOp {
        topic: topic.into(),
        partition,
        offset,
    }
}

#[test]
fn the_offset_json_file_parses_as_kafkas_parser_reads_it() {
    let cases: [(&str, Result<Vec<DeleteRecordsOp>, String>); 14] = [
        (
            r#"{"partitions":[{"topic":"foo","partition":1,"offset":10},{"topic":"bar","partition":0,"offset":0}],"version":1}"#,
            Ok(vec![op("foo", 1, 10), op("bar", 0, 0)]),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"offset":-1}]}"#,
            Ok(vec![op("foo", 0, -1)]),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"offset":-5}]}"#,
            Ok(vec![op("foo", 0, -5)]),
        ),
        (r#"{"partitions":[]}"#, Ok(Vec::new())),
        (r#"{"version":1}"#, Err("Missing partitions field".into())),
        (
            "{not json",
            Err("The input string is not a valid JSON".into()),
        ),
        (
            r#"{"partitions":[],"version":2}"#,
            Err("Not supported version field value 2".into()),
        ),
        ("[1]", Err("Expected JSON object, received [1]".into())),
        (
            r#"{"partitions":{}}"#,
            Err("Expected JSON array, received {}".into()),
        ),
        (
            r#"{"partitions":[{"topic":"foo","offset":1}]}"#,
            Err("No such field exists: `partition`".into()),
        ),
        (
            r#"{"partitions":[{"topic":7,"partition":0,"offset":1}]}"#,
            Err("Expected `String` value, received 7".into()),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":4294967296,"offset":1}]}"#,
            Err("Expected `Integer` value, received 4294967296".into()),
        ),
        (
            r#"{"partitions":[{"topic":"foo","partition":0,"offset":"1"}]}"#,
            Err(r#"Expected `Long` value, received "1""#.into()),
        ),
        (
            r#"{"partitions":[{"topic":"a","partition":0,"offset":1},{"topic":"b","partition":1,"offset":1},{"topic":"a","partition":0,"offset":2},{"topic":"b","partition":1,"offset":3},{"topic":"a","partition":0,"offset":4}]}"#,
            Err("Offset json file contains duplicate topic partitions: a-0,b-1".into()),
        ),
    ];
    for (text, expected) in cases {
        check!(parse_offset_json(text) == expected, "{text}");
    }
}

#[test]
fn the_report_prints_kafkas_lines_in_file_order() {
    let ops = [op("t", 1, -1), op("t", 0, 1), op("gone", 0, 1)];
    let outcomes = [
        DeleteRecordsOutcome {
            topic: "t".into(),
            partition: 0,
            error_code: 1,
            low_watermark: -1,
        },
        DeleteRecordsOutcome {
            topic: "t".into(),
            partition: 1,
            error_code: 0,
            low_watermark: 11,
        },
    ];
    let result = deleted(&ops, &outcomes);
    check!(
        result
            == CommandResult::rows(
                vec![
                    "Executing records delete operation".into(),
                    "Records delete operation completed:".into(),
                    "partition: t-1\tlow_watermark: 11".into(),
                    "partition: t-0\terror: org.apache.kafka.common.errors.OffsetOutOfRangeException: \
                     The requested offset is not within the range of offsets maintained by the server."
                        .into(),
                    "partition: gone-0\terror: org.apache.kafka.common.errors.ApiException: The \
                     response did not contain a result for topic partition gone-0"
                        .into(),
                ],
                json!([
                    {"topic": "t", "partition": 1, "offset": -1, "low_watermark": 11, "error": null},
                    {
                        "topic": "t",
                        "partition": 0,
                        "offset": 1,
                        "low_watermark": null,
                        "error": {
                            "code": 1,
                            "name": "OFFSET_OUT_OF_RANGE",
                            "message": "The requested offset is not within the range of offsets \
                                        maintained by the server.",
                        },
                    },
                    {
                        "topic": "gone",
                        "partition": 0,
                        "offset": 1,
                        "low_watermark": null,
                        "error": {
                            "code": -1,
                            "name": "UNKNOWN_SERVER_ERROR",
                            "message": "The server experienced an unexpected error when processing \
                                        the request.",
                        },
                    },
                ]),
                true,
            )
    );
}

#[test]
fn a_dry_run_plans_each_partition_from_the_metadata() {
    let ops = [op("t", 0, -1), op("t", 1, 5), op("t", 2, 5), op("x", 0, 5)];
    let counts = BTreeMap::from([("t".to_owned(), Ok(2)), ("x".to_owned(), Err(29))]);
    let result = planned(&ops, &counts);
    check!(
        result.human
            == [
                "Executing records delete operation",
                "Records delete operation completed:",
                "partition: t-0\tdelete_before: high_watermark",
                "partition: t-1\tdelete_before: 5",
                "partition: t-2\terror: org.apache.kafka.common.errors.\
                 UnknownTopicOrPartitionException: This server does not host this topic-partition.",
                "partition: x-0\terror: org.apache.kafka.common.errors.TopicAuthorizationException: \
                 Topic authorization failed.",
            ]
    );
    check!(result.failed);
    check!(
        result.data[0]
            == json!({"topic": "t", "partition": 0, "offset": -1, "low_watermark": null, "error": null})
    );
    check!(result.data[3]["error"]["name"] == "TOPIC_AUTHORIZATION_FAILED");
}

#[test]
fn the_confirmation_names_each_partition_and_its_bound() {
    check!(describe_op(&op("t", 0, -1)) == "t-0 before the high watermark");
    check!(describe_op(&op("t", 3, 42)) == "t-3 before offset 42");
}
