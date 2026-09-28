//! Argument parsers and failures that more than one admin command uses.

use crate::output::CommandError;

/// The failure of a sub-feature that needs an `AdminClient` call that the
/// pinned `krabka-client-admin` does not provide.
///
/// The command exits [`crate::exit::Exit::Failure`] and names the call, so
/// the operator knows that the build, not the command line, is the problem.
#[must_use]
pub fn unsupported(feature: &str, call: &str) -> CommandError {
    CommandError::Other(format!(
        "{feature} is not supported by this build: it needs {call}, which the pinned \
         krabka-client-admin does not provide"
    ))
}

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

    #[test]
    fn an_unsupported_feature_names_the_missing_call_and_exits_1() {
        let error = unsupported("cluster-id", "AdminClient::describe_cluster");
        assert!(error.exit() == crate::exit::Exit::Failure);
        assert!(
            error.to_string()
                == "cluster-id is not supported by this build: it needs \
                    AdminClient::describe_cluster, which the pinned krabka-client-admin does \
                    not provide"
        );
    }
}
