//! Argument parsers and renderings that more than one admin command uses.

use serde_json::{Value, json};

/// Parses a `key=value` argument. The value keeps every `=` after the first.
pub fn key_value(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .ok_or_else(|| "expected key=value".into())
}

/// Renders a per-row Kafka error as JSON, or `null` for a row that succeeded.
pub fn kafka_error(error: Option<&krabka_client_admin::KafkaError>) -> Value {
    error.map_or(
        Value::Null,
        |error| json!({"code": error.code, "name": error.name, "message": error.message}),
    )
}

/// Flattens an error to the message that the output layer prints.
pub fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn key_value_parser_preserves_equals_in_value() {
        assert!(key_value("a=b=c").unwrap() == ("a".into(), "b=c".into()));
        assert!(key_value("missing").is_err());
    }
}
