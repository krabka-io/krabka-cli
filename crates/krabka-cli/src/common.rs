//! Failures that more than one admin command reports.

use krabka_client_admin::KafkaError;

use crate::compat::KafkaException;

/// `Throwable.toString()` of the exception that Kafka's admin client builds
/// for a per-resource error: `Errors.forCode(code).exception(message)`, whose
/// message is the broker's, or the error's default when the broker sent none.
///
/// `kafka-leader-election` and `kafka-reassign-partitions` print it for each
/// partition or resource that failed.
#[must_use]
pub fn java_exception(error: &KafkaError) -> String {
    format!(
        "{}: {}",
        KafkaException::for_code(error.code).class(),
        java_message(error)
    )
}

/// `getMessage()` of the same exception: the broker's message, or the
/// error's default.
#[must_use]
pub fn java_message(error: &KafkaError) -> String {
    error
        .message
        .clone()
        .unwrap_or_else(|| KafkaException::for_code(error.code).message().to_owned())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn exceptions_render_with_the_broker_message_or_kafkas_default() {
        let error = |code, message: Option<&str>| KafkaError {
            code,
            name: "",
            message: message.map(ToOwned::to_owned),
        };
        let cases = [
            (
                error(3, Some("No such topic as nope")),
                "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: No such topic \
                 as nope",
                "No such topic as nope",
            ),
            (
                error(3, None),
                "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server \
                 does not host this topic-partition.",
                "This server does not host this topic-partition.",
            ),
            (
                error(40, Some("")),
                "org.apache.kafka.common.errors.InvalidConfigurationException: ",
                "",
            ),
        ];
        for (error, exception, message) in cases {
            check!(
                (java_exception(&error), java_message(&error))
                    == (exception.to_owned(), message.to_owned())
            );
        }
    }
}
