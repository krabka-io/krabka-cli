use assert2::{assert, check};
use krabka_client_core::ClientError;
use krabka_units::{Time, convert::TimeExt as _};

use super::*;

fn human(value: &impl Emit) -> String {
    let mut out = Vec::new();
    render_success(value, OutputFormat::Human, &mut out).unwrap();
    String::from_utf8(out).unwrap()
}

fn json_line(value: &impl Emit) -> Value {
    let mut out = Vec::new();
    render_success(value, OutputFormat::Json, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(text.ends_with('\n') && text.matches('\n').count() == 1);
    serde_json::from_str(&text).unwrap()
}

#[test]
fn a_success_renders_the_human_lines_and_the_data_envelope() {
    let result = CommandResult::success(
        vec!["Created topic orders.".into()],
        json!([{"topic": "orders"}]),
    );
    check!(human(&result) == "Created topic orders.\n");
    check!(json_line(&result) == json!({"data": [{"topic": "orders"}]}));
}

#[test]
fn a_dry_run_differs_from_the_real_report_only_by_its_marker() {
    let real = CommandResult::success(
        vec!["Deleted topic orders.".into()],
        json!([{"topic": "orders"}]),
    );
    let dry = real.clone().into_dry_run();
    check!(human(&dry) == format!("DRY RUN: no change was made.\n{}", human(&real)));
    let mut envelope = json_line(&dry);
    check!(envelope["dry_run"] == json!(true));
    envelope.as_object_mut().unwrap().remove("dry_run");
    check!(envelope == json_line(&real));
}

#[test]
fn every_row_renders_and_a_failed_row_sets_the_failure_flag() {
    let rows = [("a", None), ("b", Some(36)), ("c", None)];
    let failed = rows.iter().any(|(_, error)| error.is_some());
    let result = CommandResult::rows(
        rows.iter()
            .map(|(topic, error)| match error {
                Some(code) => format!("{topic}\tERROR\tTOPIC_ALREADY_EXISTS ({code})"),
                None => format!("Created topic {topic}."),
            })
            .collect(),
        rows.iter()
            .map(|(topic, error)| json!({"topic": topic, "error": error}))
            .collect::<Vec<_>>(),
        failed,
    );
    check!(
        human(&result)
            == "Created topic a.\nb\tERROR\tTOPIC_ALREADY_EXISTS (36)\nCreated topic c.\n"
    );
    check!(result.failed);
    check!(Exit::from_failed(result.failed) == Exit::Failure);
}

#[test]
fn admin_errors_render_one_line_with_the_kafka_error_name() {
    let cases = [
        (
            AdminError::Broker {
                api: "CreateTopics",
                code: 36,
                name: "TOPIC_ALREADY_EXISTS",
                message: Some("topic 'orders' already exists".into()),
            },
            "CreateTopics failed: TOPIC_ALREADY_EXISTS (36): topic 'orders' already exists",
        ),
        (
            AdminError::Broker {
                api: "DescribeCluster",
                code: 31,
                name: "CLUSTER_AUTHORIZATION_FAILED",
                message: None,
            },
            "DescribeCluster failed: CLUSTER_AUTHORIZATION_FAILED (31)",
        ),
        (
            AdminError::Broker {
                api: "DeleteTopics",
                code: 3,
                name: "UNKNOWN_TOPIC_OR_PARTITION",
                message: Some(String::new()),
            },
            "DeleteTopics failed: UNKNOWN_TOPIC_OR_PARTITION (3)",
        ),
        (
            AdminError::Connect {
                tried: 2,
                source: None,
            },
            "no bootstrap address connected: tried 2",
        ),
        (
            AdminError::Connect {
                tried: 1,
                source: Some(Box::new(AdminError::Protocol("bad frame".into()))),
            },
            "no bootstrap address connected: tried 1; last error: protocol: bad frame",
        ),
        (
            AdminError::Transport(ClientError::Timeout(Time::from_millis(300))),
            &format!(
                "client-core: {}",
                ClientError::Timeout(Time::from_millis(300))
            ),
        ),
        (
            AdminError::Protocol("bad frame".into()),
            "protocol: bad frame",
        ),
    ];
    for (error, expected) in cases {
        let message = CommandError::from(error).to_string();
        check!(message == expected);
        let mut out = Vec::new();
        render_error(
            "krabka topics",
            &message,
            Exit::Failure,
            OutputFormat::Human,
            &mut out,
        )
        .unwrap();
        check!(String::from_utf8(out).unwrap() == format!("krabka topics: {expected}\n"));
    }
}

#[test]
fn a_json_failure_carries_the_exit_code() {
    for exit in [Exit::Failure, Exit::Usage, Exit::Cancelled] {
        let mut out = Vec::new();
        render_error("krabka topics", "boom", exit, OutputFormat::Json, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        check!(text.matches('\n').count() == 1);
        check!(
            serde_json::from_str::<Value>(&text).unwrap()
                == json!({"error": {"code": exit.code(), "message": "boom"}})
        );
    }
}

#[test]
fn per_row_kafka_errors_render_as_json() {
    let cases = [
        (None, Value::Null),
        (
            Some(KafkaError {
                code: 36,
                name: "TOPIC_ALREADY_EXISTS",
                message: Some("exists".into()),
            }),
            json!({"code": 36, "name": "TOPIC_ALREADY_EXISTS", "message": "exists"}),
        ),
        (
            Some(KafkaError {
                code: 29,
                name: "TOPIC_AUTHORIZATION_FAILED",
                message: None,
            }),
            json!({"code": 29, "name": "TOPIC_AUTHORIZATION_FAILED", "message": null}),
        ),
    ];
    for (error, expected) in cases {
        check!(kafka_error(error.as_ref()) == expected);
    }
}
