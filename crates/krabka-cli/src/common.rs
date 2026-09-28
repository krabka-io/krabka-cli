//! Argument parsers that more than one admin command uses.

/// Parses a `key=value` argument. The value keeps every `=` after the first.
pub fn key_value(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .ok_or_else(|| "expected key=value".into())
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
