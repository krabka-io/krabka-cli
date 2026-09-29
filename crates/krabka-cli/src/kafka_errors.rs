//! Kafka's names, exception classes and messages for the errors that
//! `transactions`, `delegation-tokens` and `delete-records` report, from the
//! one `Errors` table in [`crate::compat`].

use krabka_client_admin::{AdminError, KafkaError};

use crate::{compat::KafkaException, output::CommandError};

/// The per-row error of `code`, with Kafka's name and message.
#[must_use]
pub fn row_error(code: i16) -> KafkaError {
    let exception = KafkaException::for_code(code);
    KafkaError {
        code,
        name: exception.name(),
        message: Some(exception.message().to_owned()),
    }
}

/// The text of the exception that a Kafka admin future fails with for
/// `code`: `org.apache.kafka.common.errors.<Class>: <message>`.
#[must_use]
pub fn exception_text(code: i16) -> String {
    KafkaException::for_code(code).to_java_string()
}

/// `error` as a [`CommandError`], with Kafka's name for a code that the
/// client calls `UNKNOWN` and Kafka's message where the broker sent none.
#[must_use]
pub fn admin_error(error: AdminError) -> CommandError {
    match error {
        AdminError::Broker {
            api,
            code,
            name,
            message,
        } => {
            let known = KafkaException::is_known(code);
            let exception = KafkaException::for_code(code);
            CommandError::Broker {
                api,
                code,
                name: if known { exception.name() } else { name },
                message: message
                    .filter(|message| !message.is_empty())
                    .or_else(|| known.then(|| exception.message().to_owned())),
            }
        }
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn exception_text_is_the_class_and_message_of_kafkas_exception() {
        check!(
            exception_text(1)
                == "org.apache.kafka.common.errors.OffsetOutOfRangeException: The requested \
                    offset is not within the range of offsets maintained by the server."
        );
        check!(
            exception_text(12_345)
                == "org.apache.kafka.common.errors.UnknownServerException: The server \
                    experienced an unexpected error when processing the request."
        );
    }

    #[test]
    fn admin_error_names_codes_the_client_does_not_and_keeps_a_broker_message() {
        let broker = |code, name, message: Option<&str>| AdminError::Broker {
            api: "CreateDelegationToken",
            code,
            name,
            message: message.map(str::to_owned),
        };
        let cases = [
            (
                broker(64, "UNKNOWN", None),
                "CreateDelegationToken failed: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64): \
                 Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on \
                 delegation token authenticated channels.",
            ),
            (
                broker(66, "DELEGATION_TOKEN_EXPIRED", Some("expired at noon")),
                "CreateDelegationToken failed: DELEGATION_TOKEN_EXPIRED (66): expired at noon",
            ),
            (
                broker(9999, "UNKNOWN", None),
                "CreateDelegationToken failed: UNKNOWN (9999)",
            ),
            (
                AdminError::Protocol("bad frame".into()),
                "protocol: bad frame",
            ),
        ];
        for (error, expected) in cases {
            check!(admin_error(error).to_string() == expected);
        }
    }
}
