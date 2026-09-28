//! The Kafka error codes that `transactions`, `delegation-tokens` and
//! `delete-records` meet, with the names, exception classes and messages of
//! Kafka 4.3.1's `Errors` enum.
//!
//! The pinned krabka-client-rs names only some of these codes and attaches no
//! message to a broker error, so these commands complete both from here. A
//! code that is not in the table is `UNKNOWN_SERVER_ERROR`, as
//! `Errors.forCode` maps it.

use krabka_client_admin::{AdminError, KafkaError};

use crate::output::CommandError;

/// One row of Kafka's `Errors` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorInfo {
    pub code: i16,
    pub name: &'static str,
    /// The simple name of the exception class in
    /// `org.apache.kafka.common.errors`.
    pub exception: &'static str,
    pub message: &'static str,
}

const UNKNOWN_SERVER_ERROR: ErrorInfo = ErrorInfo {
    code: -1,
    name: "UNKNOWN_SERVER_ERROR",
    exception: "UnknownServerException",
    message: "The server experienced an unexpected error when processing the request.",
};

const ERRORS: [ErrorInfo; 32] = [
    UNKNOWN_SERVER_ERROR,
    ErrorInfo {
        code: 1,
        name: "OFFSET_OUT_OF_RANGE",
        exception: "OffsetOutOfRangeException",
        message: "The requested offset is not within the range of offsets maintained by the server.",
    },
    ErrorInfo {
        code: 3,
        name: "UNKNOWN_TOPIC_OR_PARTITION",
        exception: "UnknownTopicOrPartitionException",
        message: "This server does not host this topic-partition.",
    },
    ErrorInfo {
        code: 5,
        name: "LEADER_NOT_AVAILABLE",
        exception: "LeaderNotAvailableException",
        message: "There is no leader for this topic-partition as we are in the middle of a \
                  leadership election.",
    },
    ErrorInfo {
        code: 6,
        name: "NOT_LEADER_OR_FOLLOWER",
        exception: "NotLeaderOrFollowerException",
        message: "For requests intended only for the leader, this error indicates that the broker \
                  is not the current leader. For requests intended for any replica, this error \
                  indicates that the broker is not a replica of the topic partition.",
    },
    ErrorInfo {
        code: 7,
        name: "REQUEST_TIMED_OUT",
        exception: "TimeoutException",
        message: "The request timed out.",
    },
    ErrorInfo {
        code: 14,
        name: "COORDINATOR_LOAD_IN_PROGRESS",
        exception: "CoordinatorLoadInProgressException",
        message: "The coordinator is loading and hence can't process requests.",
    },
    ErrorInfo {
        code: 15,
        name: "COORDINATOR_NOT_AVAILABLE",
        exception: "CoordinatorNotAvailableException",
        message: "The coordinator is not available.",
    },
    ErrorInfo {
        code: 16,
        name: "NOT_COORDINATOR",
        exception: "NotCoordinatorException",
        message: "This is not the correct coordinator.",
    },
    ErrorInfo {
        code: 17,
        name: "INVALID_TOPIC_EXCEPTION",
        exception: "InvalidTopicException",
        message: "The request attempted to perform an operation on an invalid topic.",
    },
    ErrorInfo {
        code: 19,
        name: "NOT_ENOUGH_REPLICAS",
        exception: "NotEnoughReplicasException",
        message: "Messages are rejected since there are fewer in-sync replicas than required.",
    },
    ErrorInfo {
        code: 29,
        name: "TOPIC_AUTHORIZATION_FAILED",
        exception: "TopicAuthorizationException",
        message: "Topic authorization failed.",
    },
    ErrorInfo {
        code: 31,
        name: "CLUSTER_AUTHORIZATION_FAILED",
        exception: "ClusterAuthorizationException",
        message: "Cluster authorization failed.",
    },
    ErrorInfo {
        code: 35,
        name: "UNSUPPORTED_VERSION",
        exception: "UnsupportedVersionException",
        message: "The version of API is not supported.",
    },
    ErrorInfo {
        code: 42,
        name: "INVALID_REQUEST",
        exception: "InvalidRequestException",
        message: "This most likely occurs because of a request being malformed by the client \
                  library or the message was sent to an incompatible broker. See the broker logs \
                  for more details.",
    },
    ErrorInfo {
        code: 44,
        name: "POLICY_VIOLATION",
        exception: "PolicyViolationException",
        message: "Request parameters do not satisfy the configured policy.",
    },
    ErrorInfo {
        code: 47,
        name: "INVALID_PRODUCER_EPOCH",
        exception: "InvalidProducerEpochException",
        message: "Producer attempted to produce with an old epoch.",
    },
    ErrorInfo {
        code: 48,
        name: "INVALID_TXN_STATE",
        exception: "InvalidTxnStateException",
        message: "The producer attempted a transactional operation in an invalid state.",
    },
    ErrorInfo {
        code: 49,
        name: "INVALID_PRODUCER_ID_MAPPING",
        exception: "InvalidPidMappingException",
        message: "The producer attempted to use a producer id which is not currently assigned to \
                  its transactional id.",
    },
    ErrorInfo {
        code: 51,
        name: "CONCURRENT_TRANSACTIONS",
        exception: "ConcurrentTransactionsException",
        message: "The producer attempted to update a transaction while another concurrent \
                  operation on the same transaction was ongoing.",
    },
    ErrorInfo {
        code: 53,
        name: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        exception: "TransactionalIdAuthorizationException",
        message: "Transactional Id authorization failed.",
    },
    ErrorInfo {
        code: 56,
        name: "KAFKA_STORAGE_ERROR",
        exception: "KafkaStorageException",
        message: "Disk error when trying to access log file on the disk.",
    },
    ErrorInfo {
        code: 61,
        name: "DELEGATION_TOKEN_AUTH_DISABLED",
        exception: "DelegationTokenDisabledException",
        message: "Delegation Token feature is not enabled.",
    },
    ErrorInfo {
        code: 62,
        name: "DELEGATION_TOKEN_NOT_FOUND",
        exception: "DelegationTokenNotFoundException",
        message: "Delegation Token is not found on server.",
    },
    ErrorInfo {
        code: 63,
        name: "DELEGATION_TOKEN_OWNER_MISMATCH",
        exception: "DelegationTokenOwnerMismatchException",
        message: "Specified Principal is not valid Owner/Renewer.",
    },
    ErrorInfo {
        code: 64,
        name: "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        exception: "UnsupportedByAuthenticationException",
        message: "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and \
                  on delegation token authenticated channels.",
    },
    ErrorInfo {
        code: 65,
        name: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        exception: "DelegationTokenAuthorizationException",
        message: "Delegation Token authorization failed.",
    },
    ErrorInfo {
        code: 66,
        name: "DELEGATION_TOKEN_EXPIRED",
        exception: "DelegationTokenExpiredException",
        message: "Delegation Token is expired.",
    },
    ErrorInfo {
        code: 67,
        name: "INVALID_PRINCIPAL_TYPE",
        exception: "InvalidPrincipalTypeException",
        message: "Supplied principalType is not supported.",
    },
    ErrorInfo {
        code: 90,
        name: "PRODUCER_FENCED",
        exception: "ProducerFencedException",
        message: "There is a newer producer with the same transactionalId which fences the \
                  current one.",
    },
    ErrorInfo {
        code: 100,
        name: "UNKNOWN_TOPIC_ID",
        exception: "UnknownTopicIdException",
        message: "This server does not host this topic ID.",
    },
    ErrorInfo {
        code: 105,
        name: "TRANSACTIONAL_ID_NOT_FOUND",
        exception: "TransactionalIdNotFoundException",
        message: "The transactionalId could not be found.",
    },
];

/// The row of `code`, or `UNKNOWN_SERVER_ERROR` for a code that is not in the
/// table.
#[must_use]
pub fn lookup(code: i16) -> ErrorInfo {
    ERRORS
        .iter()
        .find(|info| info.code == code)
        .copied()
        .unwrap_or(UNKNOWN_SERVER_ERROR)
}

/// The per-row error of `code`, with Kafka's name and message.
#[must_use]
pub fn row_error(code: i16) -> KafkaError {
    let info = lookup(code);
    KafkaError {
        code,
        name: info.name,
        message: Some(info.message.to_owned()),
    }
}

/// The text of the exception that a Kafka admin future fails with for
/// `code`: `org.apache.kafka.common.errors.<Class>: <message>`.
#[must_use]
pub fn exception_text(code: i16) -> String {
    let info = lookup(code);
    format!(
        "org.apache.kafka.common.errors.{}: {}",
        info.exception, info.message
    )
}

/// `error` as a [`CommandError`], with Kafka's name for a code that the
/// pinned client calls `UNKNOWN` and Kafka's message where the broker sent
/// none.
#[must_use]
pub fn admin_error(error: AdminError) -> CommandError {
    match error {
        AdminError::Broker {
            api,
            code,
            name,
            message,
        } => {
            let info = lookup(code);
            let known = info.code == code;
            CommandError::Broker {
                api,
                code,
                name: if known { info.name } else { name },
                message: message
                    .filter(|message| !message.is_empty())
                    .or_else(|| known.then(|| info.message.to_owned())),
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
    fn lookup_falls_back_to_unknown_server_error() {
        for (code, expected) in [
            (1, "OFFSET_OUT_OF_RANGE"),
            (64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED"),
            (-1, "UNKNOWN_SERVER_ERROR"),
            (9999, "UNKNOWN_SERVER_ERROR"),
        ] {
            check!(lookup(code).name == expected, "{code}");
        }
    }

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
