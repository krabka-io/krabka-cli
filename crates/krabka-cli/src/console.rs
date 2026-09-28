//! What `console-consumer` and `console-producer` share: Kafka's `key=value`
//! argument parser, client property files, the `ConfigException` wording, and
//! the Ctrl-C token that stops a streaming command.
//!
//! Both commands take their client settings the way the JVM tools do, as a
//! properties file and repeated `key=value` flags, rather than through
//! [`ConnectionArgs`](crate::connection::ConnectionArgs): the JVM tools spell
//! `--command-config` and `--timeout` with other meanings, and a runbook's
//! `kafka-console-*` line must run unchanged.

use std::path::Path;

use tokio_util::sync::CancellationToken;

use crate::{
    connection::Properties,
    exit::Exit,
    output::{OutputFormat, emit_error},
};

/// Parses `key=value` arguments as `CommandLineUtils.parseKeyValueArgs` does:
/// the value keeps every `=` after the first, a missing value is the empty
/// string, and a later key replaces an earlier one.
pub(crate) fn key_value_args(args: &[String]) -> Properties {
    let mut properties = Properties::default();
    for arg in args {
        let (key, value) = arg.split_once('=').unwrap_or((arg, ""));
        properties.insert(key, value);
    }
    properties
}

/// Reads a properties file as `Utils.loadProps` does.
pub(crate) fn load_properties(path: &Path) -> Result<Properties, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Properties::parse(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

/// `base` with every key of `overrides` set over it, as `putAll` does.
pub(crate) fn overlay(mut base: Properties, overrides: &Properties) -> Properties {
    for (key, value) in overrides.iter() {
        base.insert(key, value);
    }
    base
}

/// The message of a Kafka `ConfigException` for `name`.
pub(crate) fn config_error(name: &str, value: &str, reason: &str) -> String {
    format!("Invalid value {value} for configuration {name}: {reason}")
}

/// A numeric client property of Kafka type `kind`, such as `INT` or `LONG`,
/// or `default` when it is not set.
pub(crate) fn number_property(
    properties: &Properties,
    name: &str,
    kind: &str,
    default: i64,
) -> Result<i64, String> {
    properties.get(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| config_error(name, value, &format!("Not a number of type {kind}")))
    })
}

/// A property as the JVM tool reads it for a formatter or a line reader:
/// untrimmed, unlike a client property.
pub(crate) fn raw<'a>(properties: &'a Properties, key: &str) -> Option<&'a str> {
    properties
        .iter()
        .find_map(|(name, value)| (name == key).then_some(value))
}

/// A `BOOLEAN` client property, or `default` when it is not set.
pub(crate) fn bool_property(
    properties: &Properties,
    name: &str,
    default: bool,
) -> Result<bool, String> {
    match properties.get(name) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => Err(config_error(
            name,
            value,
            "Expected value to be either true or false",
        )),
    }
}

/// Whether a formatter or reader property reads as `true`, as
/// `value.trim().equalsIgnoreCase("true")` does.
pub(crate) fn is_true(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("true")
}

/// A token that fires on Ctrl-C, and the task that watches for it.
///
/// A streaming command owns its own shutdown: Ctrl-C is how an operator stops
/// `console-consumer`, and the command still flushes, commits and reports
/// before it exits.
pub(crate) fn cancel_on_ctrl_c() -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let on_signal = cancel.clone();
    let watcher = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            on_signal.cancel();
        }
    });
    (cancel, watcher)
}

/// Reports a failure of `command` through the output layer and returns
/// [`Exit::Failure`].
pub(crate) fn fail(command: &str, message: &str, format: OutputFormat) -> Exit {
    let _ = emit_error(command, message, Exit::Failure, format);
    Exit::Failure
}

/// A warning that the JVM tool prints, such as a deprecated flag. The JVM tool
/// prints some of these on stdout; krabka keeps stdout for records and prints
/// them on stderr.
pub(crate) fn warn(message: &str) {
    eprintln!("{message}");
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn properties(pairs: &[(&str, &str)]) -> Properties {
        let mut properties = Properties::default();
        for (key, value) in pairs {
            properties.insert(*key, *value);
        }
        properties
    }

    #[test]
    fn key_value_args_split_on_the_first_equals_and_default_to_empty() {
        type Case<'a> = (&'a [&'a str], &'a [(&'a str, &'a str)]);
        let cases: [Case<'_>; 5] = [
            (&["a=b"], &[("a", "b")]),
            (&["a=b=c"], &[("a", "b=c")]),
            (&["a"], &[("a", "")]),
            (&["a="], &[("a", "")]),
            (&["a=1", "a=2", "=x"], &[("a", "2"), ("", "x")]),
        ];
        for (args, expected) in cases {
            let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
            assert!(key_value_args(&args) == properties(expected), "{args:?}");
        }
    }

    #[test]
    fn overlay_replaces_keys_and_keeps_the_rest() {
        let base = properties(&[("a", "1"), ("b", "2")]);
        let result = overlay(base, &properties(&[("b", "3"), ("c", "4")]));
        assert!(result == properties(&[("a", "1"), ("b", "3"), ("c", "4")]));
    }

    #[test]
    fn typed_properties_use_the_kafka_config_exception_wording() {
        let set = properties(&[("n", " 42 "), ("bad", "x"), ("t", "TRUE"), ("f", "yes")]);
        assert!(number_property(&set, "n", "INT", 1) == Ok(42));
        assert!(number_property(&set, "missing", "INT", 7) == Ok(7));
        assert!(
            number_property(&set, "bad", "LONG", 1)
                == Err("Invalid value x for configuration bad: Not a number of type LONG".into())
        );
        assert!(bool_property(&set, "t", false) == Ok(true));
        assert!(bool_property(&set, "missing", true) == Ok(true));
        assert!(
            bool_property(&set, "f", false)
                == Err(
                    "Invalid value yes for configuration f: Expected value to be either true or false"
                        .into()
                )
        );
    }

    #[test]
    fn is_true_trims_and_ignores_case() {
        for (value, expected) in [("true", true), (" TRUE ", true), ("1", false), ("", false)] {
            assert!(is_true(value) == expected, "{value:?}");
        }
    }
}
