//! The value grammars of `kafka-configs`: `--add-config`, the SCRAM
//! credential value, config keys, and topic names.

use super::java;
use crate::connection::Secret;

/// The message of the `require` that `ConfigCommand.parseConfigsToBeAdded`
/// fails, including its double space.
pub const INVALID_ENTITY_CONFIG: &str = "requirement failed: Invalid entity config: all configs to be added must be in the format \"key=val\" or  \"key=[val1,val2]\" to group values which contain commas.";

/// The iteration count that `kafka-configs` uses for a SCRAM credential that
/// names none, `ConfigCommand.DefaultScramIterations`.
pub const DEFAULT_SCRAM_ITERATIONS: i32 = 4096;

/// A SCRAM mechanism, as `ScramMechanism` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    Sha256,
    Sha512,
}

impl Mechanism {
    /// The mechanism that a config key names, or `None` for a key that is
    /// not a SCRAM mechanism.
    pub fn from_config_key(key: &str) -> Option<Self> {
        match key {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }

    /// The mechanism name, which is also its config key.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The Java enum constant, which `kafka-configs` prints in one message.
    const fn constant(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM_SHA_256",
            Self::Sha512 => "SCRAM_SHA_512",
        }
    }

    /// `ScramMechanism.minIterations`.
    const fn min_iterations(self) -> i32 {
        match self {
            Self::Sha256 | Self::Sha512 => 4096,
        }
    }
}

/// One SCRAM credential to set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: Mechanism,
    pub iterations: i32,
    pub password: Secret<String>,
}

/// Parses the `--add-config` value as `ConfigCommand.parseConfigsToBeAdded`
/// does. A comma or an `=` inside square brackets does not split, every
/// square bracket is removed from a value, and keys and values are trimmed.
/// A later key replaces an earlier one.
///
/// # Errors
/// Returns [`INVALID_ENTITY_CONFIG`] when an entry does not split into
/// exactly one key and one value.
pub fn add_config(value: &str) -> Result<Vec<(String, String)>, String> {
    let entries = split(value, ',', false)
        .into_iter()
        .map(|entry| split(entry, '=', true))
        .collect::<Vec<_>>();
    if entries.iter().any(|pair| pair.len() != 2) {
        return Err(INVALID_ENTITY_CONFIG.into());
    }
    Ok(entries
        .into_iter()
        .map(|pair| {
            let value = pair[1].replace(['[', ']'], "");
            (
                java::trim(pair[0]).to_owned(),
                java::trim(&value).to_owned(),
            )
        })
        .collect())
}

/// Whether a separator followed by `rest` lies outside square brackets: the
/// lookahead `(?=[^\]]*(?:\[|$))` of `parseConfigsToBeAdded`.
fn outside_brackets(rest: &str) -> bool {
    rest.find(']')
        .is_none_or(|close| rest[..close].contains('['))
}

/// `\s` in a Java regular expression.
const fn is_java_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `String.split` on `separator` outside square brackets. With
/// `around_space` the separator takes the whitespace on both sides, as
/// `\s*=\s*` does, and every piece is kept, as the limit `-1` keeps them.
/// Without it, trailing empty pieces are dropped, as the limit `0` drops
/// them.
fn split(value: &str, separator: char, around_space: bool) -> Vec<&str> {
    let bytes = value.as_bytes();
    let mut pieces = Vec::new();
    let mut start = 0;
    for (at, c) in value.char_indices() {
        if c != separator || at < start || !outside_brackets(&value[at + 1..]) {
            continue;
        }
        let mut end = at;
        let mut next = at + 1;
        if around_space {
            while end > start && is_java_space(bytes[end - 1]) {
                end -= 1;
            }
            while next < bytes.len() && is_java_space(bytes[next]) {
                next += 1;
            }
        }
        pieces.push(&value[start..end]);
        start = next;
    }
    if pieces.is_empty() {
        return vec![value];
    }
    pieces.push(&value[start..]);
    if !around_space {
        while pieces.last().is_some_and(|piece| piece.is_empty()) {
            pieces.pop();
        }
    }
    pieces
}

/// Checks a config key as `ConfigCommand.validatePropsKey` does.
///
/// # Errors
/// Returns Kafka's message for a key with a character outside
/// `[$a-zA-Z0-9._-]`.
pub fn config_key(key: &str) -> Result<(), String> {
    if key
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'$' | b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(format!("Invalid character found for config key: {key}"))
    }
}

/// Parses a SCRAM credential value, `[iterations=<n>,]password=<password>`,
/// as `ConfigCommand.alterUserScramCredentialConfigs` does. An iteration
/// count that is absent or `-1` is [`DEFAULT_SCRAM_ITERATIONS`].
///
/// Everything after `password=` is the password, commas included, so
/// `password=s,iterations=8192` is the password `s,iterations=8192` with the
/// default iteration count, as it is for `kafka-configs`.
///
/// # Errors
/// Returns Kafka's message for a value that does not match, an iteration
/// count that is not an integer, and one below the mechanism's minimum. The
/// message withholds the value, which may hold the password.
pub fn scram_credential(mechanism: Mechanism, value: &str) -> Result<ScramCredential, String> {
    let invalid = || {
        format!(
            "Invalid credential property {}=[redacted]",
            mechanism.constant()
        )
    };
    let (iterations, password) = if let Some(password) = value.strip_prefix("password=") {
        (None, password)
    } else {
        let rest = value.strip_prefix("iterations=").ok_or_else(invalid)?;
        let digits = rest.strip_prefix('-').unwrap_or(rest);
        let digits_len = digits.bytes().take_while(u8::is_ascii_digit).count();
        let count_len = rest.len() - digits.len() + digits_len;
        let password = rest[count_len..]
            .strip_prefix(",password=")
            .ok_or_else(invalid)?;
        (Some(&rest[..count_len]), password)
    };
    // `.` in a Java regular expression does not match a line terminator.
    if password.contains(['\n', '\r', '\u{85}', '\u{2028}', '\u{2029}']) {
        return Err(invalid());
    }
    let iterations = match iterations {
        Some(count) if count != "-1" => java::parse_int(count)?,
        _ => DEFAULT_SCRAM_ITERATIONS,
    };
    if iterations < mechanism.min_iterations() {
        return Err(format!(
            "Iterations {iterations} is less than the minimum {} required for {}",
            mechanism.min_iterations(),
            mechanism.name()
        ));
    }
    Ok(ScramCredential {
        mechanism,
        iterations,
        password: Secret::new(password.to_owned()),
    })
}

/// Checks a topic name as `Topic.validate` does.
///
/// # Errors
/// Returns Kafka's `Topic name is invalid: ...` message.
pub fn topic_name(name: &str) -> Result<(), String> {
    const MAX_NAME_LENGTH: usize = 249;
    let reason = if name.is_empty() {
        "the empty string is not allowed".to_owned()
    } else if name == "." {
        "'.' is not allowed".to_owned()
    } else if name == ".." {
        "'..' is not allowed".to_owned()
    } else if name.encode_utf16().count() > MAX_NAME_LENGTH {
        format!("the length of '{name}' is longer than the max allowed length {MAX_NAME_LENGTH}")
    } else if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        format!(
            "'{name}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        )
    } else {
        return Ok(());
    };
    Err(format!("Topic name is invalid: {reason}"))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    type Parsed = Result<Vec<(String, String)>, String>;

    #[test]
    fn add_config_splits_as_kafka_does() {
        let cases: [(&str, Parsed); 14] = [
            ("retention.ms=1000", Ok(pairs(&[("retention.ms", "1000")]))),
            (
                "retention.ms=1000,cleanup.policy=[compact,delete]",
                Ok(pairs(&[
                    ("retention.ms", "1000"),
                    ("cleanup.policy", "compact,delete"),
                ])),
            ),
            (
                " retention.ms = 5 , cleanup.policy = [ compact , delete ] ",
                Ok(pairs(&[
                    ("retention.ms", "5"),
                    ("cleanup.policy", "compact , delete"),
                ])),
            ),
            (
                "SCRAM-SHA-512=[password=s,iterations=8192]",
                Ok(pairs(&[("SCRAM-SHA-512", "password=s,iterations=8192")])),
            ),
            (
                "SCRAM-SHA-256=[iterations=8192,password=a=b]",
                Ok(pairs(&[("SCRAM-SHA-256", "iterations=8192,password=a=b")])),
            ),
            (
                "ssl.endpoint.identification.algorithm=",
                Ok(pairs(&[("ssl.endpoint.identification.algorithm", "")])),
            ),
            ("a=1,", Ok(pairs(&[("a", "1")]))),
            ("a=1,,", Ok(pairs(&[("a", "1")]))),
            (",", Ok(Vec::new())),
            ("a=b=c", Err(INVALID_ENTITY_CONFIG.into())),
            ("novalue", Err(INVALID_ENTITY_CONFIG.into())),
            ("a=1,,b=2", Err(INVALID_ENTITY_CONFIG.into())),
            ("", Err(INVALID_ENTITY_CONFIG.into())),
            ("a= =b", Err(INVALID_ENTITY_CONFIG.into())),
        ];
        for (input, expected) in cases {
            check!(add_config(input) == expected, "{input:?}");
        }
    }

    #[test]
    fn config_keys_allow_only_kafka_characters() {
        let cases = [
            ("retention.ms", true),
            ("org.apache.kafka.Foo$Bar", true),
            ("consumer_byte_rate", true),
            ("SCRAM-SHA-512", true),
            ("", true),
            ("bad key", false),
            ("a/b", false),
        ];
        for (key, valid) in cases {
            check!(config_key(key).is_ok() == valid, "{key:?}");
        }
        check!(
            config_key("bad key") == Err("Invalid character found for config key: bad key".into())
        );
    }

    #[test]
    fn scram_credentials_parse_as_kafka_parses_them() {
        let credential = |mechanism, iterations, password: &str| {
            Ok(ScramCredential {
                mechanism,
                iterations,
                password: Secret::new(password.to_owned()),
            })
        };
        let cases = [
            (
                Mechanism::Sha512,
                "password=s,iterations=8192",
                credential(Mechanism::Sha512, 4096, "s,iterations=8192"),
            ),
            (
                Mechanism::Sha256,
                "iterations=8192,password=s",
                credential(Mechanism::Sha256, 8192, "s"),
            ),
            (
                Mechanism::Sha256,
                "iterations=-1,password=p=q",
                credential(Mechanism::Sha256, 4096, "p=q"),
            ),
            (
                Mechanism::Sha512,
                "password=",
                credential(Mechanism::Sha512, 4096, ""),
            ),
            (
                Mechanism::Sha256,
                "iterations=100,password=x",
                Err(
                    "Iterations 100 is less than the minimum 4096 required for SCRAM-SHA-256"
                        .into(),
                ),
            ),
            (
                Mechanism::Sha256,
                "iterations=-5,password=x",
                Err(
                    "Iterations -5 is less than the minimum 4096 required for SCRAM-SHA-256".into(),
                ),
            ),
            (
                Mechanism::Sha256,
                "iterations=,password=x",
                Err("For input string: \"\"".into()),
            ),
            (
                Mechanism::Sha256,
                "bogus",
                Err("Invalid credential property SCRAM_SHA_256=[redacted]".into()),
            ),
            (
                Mechanism::Sha512,
                "iterations=x,password=secret",
                Err("Invalid credential property SCRAM_SHA_512=[redacted]".into()),
            ),
            (
                Mechanism::Sha512,
                "password=a\nb",
                Err("Invalid credential property SCRAM_SHA_512=[redacted]".into()),
            ),
        ];
        for (mechanism, value, expected) in cases {
            check!(scram_credential(mechanism, value) == expected, "{value:?}");
        }
    }

    #[test]
    fn topic_names_are_checked_as_kafka_checks_them() {
        let long = "a".repeat(250);
        let cases = [
            ("orders", Ok(())),
            ("__consumer_offsets", Ok(())),
            ("", Err("Topic name is invalid: the empty string is not allowed".to_owned())),
            (".", Err("Topic name is invalid: '.' is not allowed".to_owned())),
            ("..", Err("Topic name is invalid: '..' is not allowed".to_owned())),
            (
                "bad name",
                Err("Topic name is invalid: 'bad name' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'".to_owned()),
            ),
            (
                long.as_str(),
                Err(format!("Topic name is invalid: the length of '{long}' is longer than the max allowed length 249")),
            ),
        ];
        for (name, expected) in cases {
            check!(topic_name(name) == expected, "{name:?}");
        }
    }
}
