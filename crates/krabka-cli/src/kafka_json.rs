//! Kafka's `DecodeJson` reading of tool input files, with its messages.
//!
//! `kafka-leader-election --path-to-json-file` and the reassignment files of
//! `kafka-reassign-partitions` go through Kafka's `Json` and `DecodeJson`
//! helpers. These functions refuse the same values with the same text, so an
//! operator who fixes a file after one tool's complaint fixes it for both.

use serde_json::{Map, Value};

/// A parsed document: a value, or Jackson's `MissingNode` for a text of only
/// whitespace.
pub type Document = Option<Value>;

/// `Json.tryParseFull`: the document of a non-empty text.
///
/// Jackson reads a text of only whitespace as a `MissingNode`, which is
/// `None` here, so the caller fails later as Kafka does.
///
/// # Errors
/// Returns `The input string shouldn't be empty` for an empty text, and the
/// parser's message for a text that is not JSON.
pub fn try_parse_full(text: &str) -> Result<Document, String> {
    if text.is_empty() {
        return Err("The input string shouldn't be empty".into());
    }
    if text.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(text)
        .map(Some)
        .map_err(|error| error.to_string())
}

/// `Json.parseFull`: the document, or `None` when the text is empty or is not
/// JSON.
#[must_use]
pub fn parse_full(text: &str) -> Option<Document> {
    try_parse_full(text).ok()
}

/// `JsonValue.asJsonObject` of a document.
///
/// # Errors
/// Returns `Expected JSON object, received <node>` for any other value. A
/// `MissingNode` renders as nothing, as Jackson renders it.
pub fn document_object(document: Option<&Value>) -> Result<&Map<String, Value>, String> {
    document.map_or_else(|| Err("Expected JSON object, received ".into()), object)
}

/// `JsonValue.asJsonObject`.
///
/// # Errors
/// Returns `Expected JSON object, received <node>` for any other value.
pub fn object(value: &Value) -> Result<&Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("Expected JSON object, received {value}"))
}

/// `JsonValue.asJsonArray`.
///
/// # Errors
/// Returns `Expected JSON array, received <node>` for any other value.
pub fn array(value: &Value) -> Result<&Vec<Value>, String> {
    value
        .as_array()
        .ok_or_else(|| format!("Expected JSON array, received {value}"))
}

/// `JsonObject.apply`: the field, which must be present.
///
/// # Errors
/// Returns ``No such field exists: `<name>` `` when the field is absent.
pub fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value, String> {
    object
        .get(name)
        .ok_or_else(|| format!("No such field exists: `{name}`"))
}

fn mismatch(expected: &str, value: &Value) -> String {
    format!("Expected `{expected}` value, received {value}")
}

/// `DecodeJson.DecodeInteger`: a JSON integer that fits an `int`.
///
/// # Errors
/// Returns ``Expected `Integer` value, received <node>`` for anything else,
/// a fraction included.
pub fn int(value: &Value) -> Result<i32, String> {
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .ok_or_else(|| mismatch("Integer", value))
}

/// `DecodeJson.DecodeString`.
///
/// # Errors
/// Returns ``Expected `String` value, received <node>`` for anything else.
pub fn string(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| mismatch("String", value))
}

/// `DecodeJson.decodeList(INT)`.
///
/// # Errors
/// Returns the message of the first value that does not decode.
pub fn int_list(value: &Value) -> Result<Vec<i32>, String> {
    value
        .as_array()
        .ok_or_else(|| mismatch("JSON array", value))?
        .iter()
        .map(int)
        .collect()
}

/// `DecodeJson.decodeList(STRING)`.
///
/// # Errors
/// Returns the message of the first value that does not decode.
pub fn string_list(value: &Value) -> Result<Vec<String>, String> {
    value
        .as_array()
        .ok_or_else(|| mismatch("JSON array", value))?
        .iter()
        .map(string)
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use serde_json::json;

    use super::*;

    #[test]
    fn values_decode_or_fail_with_kafkas_messages() {
        let cases: [(Result<String, String>, Result<String, String>); 10] = [
            (int(&json!(7)).map(|v| v.to_string()), Ok("7".into())),
            (
                int(&json!(1.0)).map(|v| v.to_string()),
                Err("Expected `Integer` value, received 1.0".into()),
            ),
            (
                int(&json!(4_294_967_296_i64)).map(|v| v.to_string()),
                Err("Expected `Integer` value, received 4294967296".into()),
            ),
            (
                int(&json!("7")).map(|v| v.to_string()),
                Err("Expected `Integer` value, received \"7\"".into()),
            ),
            (string(&json!("foo")), Ok("foo".into())),
            (
                string(&json!(1)),
                Err("Expected `String` value, received 1".into()),
            ),
            (
                int_list(&json!([1, 2])).map(|v| format!("{v:?}")),
                Ok("[1, 2]".into()),
            ),
            (
                int_list(&json!(null)).map(|v| format!("{v:?}")),
                Err("Expected `JSON array` value, received null".into()),
            ),
            (
                string_list(&json!(["a", 1])).map(|v| format!("{v:?}")),
                Err("Expected `String` value, received 1".into()),
            ),
            (
                object(&json!([1])).map(|_| String::new()),
                Err("Expected JSON object, received [1]".into()),
            ),
        ];
        for (actual, expected) in cases {
            check!(actual == expected);
        }
        check!(field(&Map::new(), "topic") == Err("No such field exists: `topic`".into()));
        check!(array(&json!({})) == Err("Expected JSON array, received {}".into()));
        check!(parse_full("") == None);
        check!(parse_full("{") == None);
        check!(parse_full(" \n") == Some(None));
        check!(parse_full("{\"a\":1}") == Some(Some(json!({"a": 1}))));
        check!(try_parse_full("") == Err("The input string shouldn't be empty".into()));
        check!(document_object(None).map(|_| ()) == Err("Expected JSON object, received ".into()));
    }
}
