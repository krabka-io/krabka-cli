//! Kafka's `Errors` table: the name, exception class and message of every
//! Kafka error code, as the JVM tools print an error with
//! `Throwable.toString()`. Operators grep those lines, so krabka prints the
//! same text rather than renaming.

/// Every Kafka error code with its `Errors` name, the fully qualified class
/// name of its exception and its default message, as `org.apache.kafka.common.protocol.Errors` has
/// them at Kafka 4.3.1. `NONE` is not an error and is absent.
const ERRORS: &[(i16, &str, &str, &str)] = &[
    (
        -1,
        "UNKNOWN_SERVER_ERROR",
        "org.apache.kafka.common.errors.UnknownServerException",
        "The server experienced an unexpected error when processing the request.",
    ),
    (
        1,
        "OFFSET_OUT_OF_RANGE",
        "org.apache.kafka.common.errors.OffsetOutOfRangeException",
        "The requested offset is not within the range of offsets maintained by the server.",
    ),
    (
        2,
        "CORRUPT_MESSAGE",
        "org.apache.kafka.common.errors.CorruptRecordException",
        "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
    ),
    (
        3,
        "UNKNOWN_TOPIC_OR_PARTITION",
        "org.apache.kafka.common.errors.UnknownTopicOrPartitionException",
        "This server does not host this topic-partition.",
    ),
    (
        4,
        "INVALID_FETCH_SIZE",
        "org.apache.kafka.common.errors.InvalidFetchSizeException",
        "The requested fetch size is invalid.",
    ),
    (
        5,
        "LEADER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.LeaderNotAvailableException",
        "There is no leader for this topic-partition as we are in the middle of a leadership election.",
    ),
    (
        6,
        "NOT_LEADER_OR_FOLLOWER",
        "org.apache.kafka.common.errors.NotLeaderOrFollowerException",
        "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
    ),
    (
        7,
        "REQUEST_TIMED_OUT",
        "org.apache.kafka.common.errors.TimeoutException",
        "The request timed out.",
    ),
    (
        8,
        "BROKER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.BrokerNotAvailableException",
        "The broker is not available.",
    ),
    (
        9,
        "REPLICA_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.ReplicaNotAvailableException",
        "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
    ),
    (
        10,
        "MESSAGE_TOO_LARGE",
        "org.apache.kafka.common.errors.RecordTooLargeException",
        "The request included a message larger than the max message size the server will accept.",
    ),
    (
        11,
        "STALE_CONTROLLER_EPOCH",
        "org.apache.kafka.common.errors.ControllerMovedException",
        "The controller moved to another broker.",
    ),
    (
        12,
        "OFFSET_METADATA_TOO_LARGE",
        "org.apache.kafka.common.errors.OffsetMetadataTooLarge",
        "The metadata field of the offset request was too large.",
    ),
    (
        13,
        "NETWORK_EXCEPTION",
        "org.apache.kafka.common.errors.NetworkException",
        "The server disconnected before a response was received.",
    ),
    (
        14,
        "COORDINATOR_LOAD_IN_PROGRESS",
        "org.apache.kafka.common.errors.CoordinatorLoadInProgressException",
        "The coordinator is loading and hence can't process requests.",
    ),
    (
        15,
        "COORDINATOR_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.CoordinatorNotAvailableException",
        "The coordinator is not available.",
    ),
    (
        16,
        "NOT_COORDINATOR",
        "org.apache.kafka.common.errors.NotCoordinatorException",
        "This is not the correct coordinator.",
    ),
    (
        17,
        "INVALID_TOPIC_EXCEPTION",
        "org.apache.kafka.common.errors.InvalidTopicException",
        "The request attempted to perform an operation on an invalid topic.",
    ),
    (
        18,
        "RECORD_LIST_TOO_LARGE",
        "org.apache.kafka.common.errors.RecordBatchTooLargeException",
        "The request included message batch larger than the configured segment size on the server.",
    ),
    (
        19,
        "NOT_ENOUGH_REPLICAS",
        "org.apache.kafka.common.errors.NotEnoughReplicasException",
        "Messages are rejected since there are fewer in-sync replicas than required.",
    ),
    (
        20,
        "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        "org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException",
        "Messages are written to the log, but to fewer in-sync replicas than required.",
    ),
    (
        21,
        "INVALID_REQUIRED_ACKS",
        "org.apache.kafka.common.errors.InvalidRequiredAcksException",
        "Produce request specified an invalid value for required acks.",
    ),
    (
        22,
        "ILLEGAL_GENERATION",
        "org.apache.kafka.common.errors.IllegalGenerationException",
        "Specified group generation id is not valid.",
    ),
    (
        23,
        "INCONSISTENT_GROUP_PROTOCOL",
        "org.apache.kafka.common.errors.InconsistentGroupProtocolException",
        "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
    ),
    (
        24,
        "INVALID_GROUP_ID",
        "org.apache.kafka.common.errors.InvalidGroupIdException",
        "The group id is invalid.",
    ),
    (
        25,
        "UNKNOWN_MEMBER_ID",
        "org.apache.kafka.common.errors.UnknownMemberIdException",
        "The coordinator is not aware of this member.",
    ),
    (
        26,
        "INVALID_SESSION_TIMEOUT",
        "org.apache.kafka.common.errors.InvalidSessionTimeoutException",
        "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
    ),
    (
        27,
        "REBALANCE_IN_PROGRESS",
        "org.apache.kafka.common.errors.RebalanceInProgressException",
        "The group is rebalancing, so a rejoin is needed.",
    ),
    (
        28,
        "INVALID_COMMIT_OFFSET_SIZE",
        "org.apache.kafka.common.errors.InvalidCommitOffsetSizeException",
        "The committing offset data size is not valid.",
    ),
    (
        29,
        "TOPIC_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.TopicAuthorizationException",
        "Topic authorization failed.",
    ),
    (
        30,
        "GROUP_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.GroupAuthorizationException",
        "Group authorization failed.",
    ),
    (
        31,
        "CLUSTER_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.ClusterAuthorizationException",
        "Cluster authorization failed.",
    ),
    (
        32,
        "INVALID_TIMESTAMP",
        "org.apache.kafka.common.errors.InvalidTimestampException",
        "The timestamp of the message is out of acceptable range.",
    ),
    (
        33,
        "UNSUPPORTED_SASL_MECHANISM",
        "org.apache.kafka.common.errors.UnsupportedSaslMechanismException",
        "The broker does not support the requested SASL mechanism.",
    ),
    (
        34,
        "ILLEGAL_SASL_STATE",
        "org.apache.kafka.common.errors.IllegalSaslStateException",
        "Request is not valid given the current SASL state.",
    ),
    (
        35,
        "UNSUPPORTED_VERSION",
        "org.apache.kafka.common.errors.UnsupportedVersionException",
        "The version of API is not supported.",
    ),
    (
        36,
        "TOPIC_ALREADY_EXISTS",
        "org.apache.kafka.common.errors.TopicExistsException",
        "Topic with this name already exists.",
    ),
    (
        37,
        "INVALID_PARTITIONS",
        "org.apache.kafka.common.errors.InvalidPartitionsException",
        "Number of partitions is below 1.",
    ),
    (
        38,
        "INVALID_REPLICATION_FACTOR",
        "org.apache.kafka.common.errors.InvalidReplicationFactorException",
        "Replication factor is below 1 or larger than the number of available brokers.",
    ),
    (
        39,
        "INVALID_REPLICA_ASSIGNMENT",
        "org.apache.kafka.common.errors.InvalidReplicaAssignmentException",
        "Replica assignment is invalid.",
    ),
    (
        40,
        "INVALID_CONFIG",
        "org.apache.kafka.common.errors.InvalidConfigurationException",
        "Configuration is invalid.",
    ),
    (
        41,
        "NOT_CONTROLLER",
        "org.apache.kafka.common.errors.NotControllerException",
        "This is not the correct controller for this cluster.",
    ),
    (
        42,
        "INVALID_REQUEST",
        "org.apache.kafka.common.errors.InvalidRequestException",
        "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
    ),
    (
        43,
        "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        "org.apache.kafka.common.errors.UnsupportedForMessageFormatException",
        "The message format version on the broker does not support the request.",
    ),
    (
        44,
        "POLICY_VIOLATION",
        "org.apache.kafka.common.errors.PolicyViolationException",
        "Request parameters do not satisfy the configured policy.",
    ),
    (
        45,
        "OUT_OF_ORDER_SEQUENCE_NUMBER",
        "org.apache.kafka.common.errors.OutOfOrderSequenceException",
        "The broker received an out of order sequence number.",
    ),
    (
        46,
        "DUPLICATE_SEQUENCE_NUMBER",
        "org.apache.kafka.common.errors.DuplicateSequenceException",
        "The broker received a duplicate sequence number.",
    ),
    (
        47,
        "INVALID_PRODUCER_EPOCH",
        "org.apache.kafka.common.errors.InvalidProducerEpochException",
        "Producer attempted to produce with an old epoch.",
    ),
    (
        48,
        "INVALID_TXN_STATE",
        "org.apache.kafka.common.errors.InvalidTxnStateException",
        "The producer attempted a transactional operation in an invalid state.",
    ),
    (
        49,
        "INVALID_PRODUCER_ID_MAPPING",
        "org.apache.kafka.common.errors.InvalidPidMappingException",
        "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
    ),
    (
        50,
        "INVALID_TRANSACTION_TIMEOUT",
        "org.apache.kafka.common.errors.InvalidTxnTimeoutException",
        "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
    ),
    (
        51,
        "CONCURRENT_TRANSACTIONS",
        "org.apache.kafka.common.errors.ConcurrentTransactionsException",
        "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
    ),
    (
        52,
        "TRANSACTION_COORDINATOR_FENCED",
        "org.apache.kafka.common.errors.TransactionCoordinatorFencedException",
        "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
    ),
    (
        53,
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.TransactionalIdAuthorizationException",
        "Transactional Id authorization failed.",
    ),
    (
        54,
        "SECURITY_DISABLED",
        "org.apache.kafka.common.errors.SecurityDisabledException",
        "Security features are disabled.",
    ),
    (
        55,
        "OPERATION_NOT_ATTEMPTED",
        "org.apache.kafka.common.errors.OperationNotAttemptedException",
        "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
    ),
    (
        56,
        "KAFKA_STORAGE_ERROR",
        "org.apache.kafka.common.errors.KafkaStorageException",
        "Disk error when trying to access log file on the disk.",
    ),
    (
        57,
        "LOG_DIR_NOT_FOUND",
        "org.apache.kafka.common.errors.LogDirNotFoundException",
        "The user-specified log directory is not found in the broker config.",
    ),
    (
        58,
        "SASL_AUTHENTICATION_FAILED",
        "org.apache.kafka.common.errors.SaslAuthenticationException",
        "SASL Authentication failed.",
    ),
    (
        59,
        "UNKNOWN_PRODUCER_ID",
        "org.apache.kafka.common.errors.UnknownProducerIdException",
        "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
    ),
    (
        60,
        "REASSIGNMENT_IN_PROGRESS",
        "org.apache.kafka.common.errors.ReassignmentInProgressException",
        "A partition reassignment is in progress.",
    ),
    (
        61,
        "DELEGATION_TOKEN_AUTH_DISABLED",
        "org.apache.kafka.common.errors.DelegationTokenDisabledException",
        "Delegation Token feature is not enabled.",
    ),
    (
        62,
        "DELEGATION_TOKEN_NOT_FOUND",
        "org.apache.kafka.common.errors.DelegationTokenNotFoundException",
        "Delegation Token is not found on server.",
    ),
    (
        63,
        "DELEGATION_TOKEN_OWNER_MISMATCH",
        "org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException",
        "Specified Principal is not valid Owner/Renewer.",
    ),
    (
        64,
        "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        "org.apache.kafka.common.errors.UnsupportedByAuthenticationException",
        "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
    ),
    (
        65,
        "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.DelegationTokenAuthorizationException",
        "Delegation Token authorization failed.",
    ),
    (
        66,
        "DELEGATION_TOKEN_EXPIRED",
        "org.apache.kafka.common.errors.DelegationTokenExpiredException",
        "Delegation Token is expired.",
    ),
    (
        67,
        "INVALID_PRINCIPAL_TYPE",
        "org.apache.kafka.common.errors.InvalidPrincipalTypeException",
        "Supplied principalType is not supported.",
    ),
    (
        68,
        "NON_EMPTY_GROUP",
        "org.apache.kafka.common.errors.GroupNotEmptyException",
        "The group is not empty.",
    ),
    (
        69,
        "GROUP_ID_NOT_FOUND",
        "org.apache.kafka.common.errors.GroupIdNotFoundException",
        "The group id does not exist.",
    ),
    (
        70,
        "FETCH_SESSION_ID_NOT_FOUND",
        "org.apache.kafka.common.errors.FetchSessionIdNotFoundException",
        "The fetch session ID was not found.",
    ),
    (
        71,
        "INVALID_FETCH_SESSION_EPOCH",
        "org.apache.kafka.common.errors.InvalidFetchSessionEpochException",
        "The fetch session epoch is invalid.",
    ),
    (
        72,
        "LISTENER_NOT_FOUND",
        "org.apache.kafka.common.errors.ListenerNotFoundException",
        "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
    ),
    (
        73,
        "TOPIC_DELETION_DISABLED",
        "org.apache.kafka.common.errors.TopicDeletionDisabledException",
        "Topic deletion is disabled.",
    ),
    (
        74,
        "FENCED_LEADER_EPOCH",
        "org.apache.kafka.common.errors.FencedLeaderEpochException",
        "The leader epoch in the request is older than the epoch on the broker.",
    ),
    (
        75,
        "UNKNOWN_LEADER_EPOCH",
        "org.apache.kafka.common.errors.UnknownLeaderEpochException",
        "The leader epoch in the request is newer than the epoch on the broker.",
    ),
    (
        76,
        "UNSUPPORTED_COMPRESSION_TYPE",
        "org.apache.kafka.common.errors.UnsupportedCompressionTypeException",
        "The requesting client does not support the compression type of given partition.",
    ),
    (
        77,
        "STALE_BROKER_EPOCH",
        "org.apache.kafka.common.errors.StaleBrokerEpochException",
        "Broker epoch has changed.",
    ),
    (
        78,
        "OFFSET_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.OffsetNotAvailableException",
        "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
    ),
    (
        79,
        "MEMBER_ID_REQUIRED",
        "org.apache.kafka.common.errors.MemberIdRequiredException",
        "The group member needs to have a valid member id before actually entering a consumer group.",
    ),
    (
        80,
        "PREFERRED_LEADER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException",
        "The preferred leader was not available.",
    ),
    (
        81,
        "GROUP_MAX_SIZE_REACHED",
        "org.apache.kafka.common.errors.GroupMaxSizeReachedException",
        "The group has reached its maximum size.",
    ),
    (
        82,
        "FENCED_INSTANCE_ID",
        "org.apache.kafka.common.errors.FencedInstanceIdException",
        "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
    ),
    (
        83,
        "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.EligibleLeadersNotAvailableException",
        "Eligible topic partition leaders are not available.",
    ),
    (
        84,
        "ELECTION_NOT_NEEDED",
        "org.apache.kafka.common.errors.ElectionNotNeededException",
        "Leader election not needed for topic partition.",
    ),
    (
        85,
        "NO_REASSIGNMENT_IN_PROGRESS",
        "org.apache.kafka.common.errors.NoReassignmentInProgressException",
        "No partition reassignment is in progress.",
    ),
    (
        86,
        "GROUP_SUBSCRIBED_TO_TOPIC",
        "org.apache.kafka.common.errors.GroupSubscribedToTopicException",
        "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
    ),
    (
        87,
        "INVALID_RECORD",
        "org.apache.kafka.common.InvalidRecordException",
        "This record has failed the validation on broker and hence will be rejected.",
    ),
    (
        88,
        "UNSTABLE_OFFSET_COMMIT",
        "org.apache.kafka.common.errors.UnstableOffsetCommitException",
        "There are unstable offsets that need to be cleared.",
    ),
    (
        89,
        "THROTTLING_QUOTA_EXCEEDED",
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException",
        "The throttling quota has been exceeded.",
    ),
    (
        90,
        "PRODUCER_FENCED",
        "org.apache.kafka.common.errors.ProducerFencedException",
        "There is a newer producer with the same transactionalId which fences the current one.",
    ),
    (
        91,
        "RESOURCE_NOT_FOUND",
        "org.apache.kafka.common.errors.ResourceNotFoundException",
        "A request illegally referred to a resource that does not exist.",
    ),
    (
        92,
        "DUPLICATE_RESOURCE",
        "org.apache.kafka.common.errors.DuplicateResourceException",
        "A request illegally referred to the same resource twice.",
    ),
    (
        93,
        "UNACCEPTABLE_CREDENTIAL",
        "org.apache.kafka.common.errors.UnacceptableCredentialException",
        "Requested credential would not meet criteria for acceptability.",
    ),
    (
        94,
        "INCONSISTENT_VOTER_SET",
        "org.apache.kafka.common.errors.InconsistentVoterSetException",
        "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.",
    ),
    (
        95,
        "INVALID_UPDATE_VERSION",
        "org.apache.kafka.common.errors.InvalidUpdateVersionException",
        "The given update version was invalid.",
    ),
    (
        96,
        "FEATURE_UPDATE_FAILED",
        "org.apache.kafka.common.errors.FeatureUpdateFailedException",
        "Unable to update finalized features due to an unexpected server error.",
    ),
    (
        97,
        "PRINCIPAL_DESERIALIZATION_FAILURE",
        "org.apache.kafka.common.errors.PrincipalDeserializationException",
        "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.",
    ),
    (
        98,
        "SNAPSHOT_NOT_FOUND",
        "org.apache.kafka.common.errors.SnapshotNotFoundException",
        "Requested snapshot was not found.",
    ),
    (
        99,
        "POSITION_OUT_OF_RANGE",
        "org.apache.kafka.common.errors.PositionOutOfRangeException",
        "Requested position is not greater than or equal to zero, and less than the size of the snapshot.",
    ),
    (
        100,
        "UNKNOWN_TOPIC_ID",
        "org.apache.kafka.common.errors.UnknownTopicIdException",
        "This server does not host this topic ID.",
    ),
    (
        101,
        "DUPLICATE_BROKER_REGISTRATION",
        "org.apache.kafka.common.errors.DuplicateBrokerRegistrationException",
        "This broker ID is already in use.",
    ),
    (
        102,
        "BROKER_ID_NOT_REGISTERED",
        "org.apache.kafka.common.errors.BrokerIdNotRegisteredException",
        "The given broker ID was not registered.",
    ),
    (
        103,
        "INCONSISTENT_TOPIC_ID",
        "org.apache.kafka.common.errors.InconsistentTopicIdException",
        "The log's topic ID did not match the topic ID in the request.",
    ),
    (
        104,
        "INCONSISTENT_CLUSTER_ID",
        "org.apache.kafka.common.errors.InconsistentClusterIdException",
        "The clusterId in the request does not match that found on the server.",
    ),
    (
        105,
        "TRANSACTIONAL_ID_NOT_FOUND",
        "org.apache.kafka.common.errors.TransactionalIdNotFoundException",
        "The transactionalId could not be found.",
    ),
    (
        106,
        "FETCH_SESSION_TOPIC_ID_ERROR",
        "org.apache.kafka.common.errors.FetchSessionTopicIdException",
        "The fetch session encountered inconsistent topic ID usage.",
    ),
    (
        107,
        "INELIGIBLE_REPLICA",
        "org.apache.kafka.common.errors.IneligibleReplicaException",
        "The new ISR contains at least one ineligible replica.",
    ),
    (
        108,
        "NEW_LEADER_ELECTED",
        "org.apache.kafka.common.errors.NewLeaderElectedException",
        "The AlterPartition request successfully updated the partition state but the leader has changed.",
    ),
    (
        109,
        "OFFSET_MOVED_TO_TIERED_STORAGE",
        "org.apache.kafka.common.errors.OffsetMovedToTieredStorageException",
        "The requested offset is moved to tiered storage.",
    ),
    (
        110,
        "FENCED_MEMBER_EPOCH",
        "org.apache.kafka.common.errors.FencedMemberEpochException",
        "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.",
    ),
    (
        111,
        "UNRELEASED_INSTANCE_ID",
        "org.apache.kafka.common.errors.UnreleasedInstanceIdException",
        "The instance ID is still used by another member in the consumer group. That member must leave first.",
    ),
    (
        112,
        "UNSUPPORTED_ASSIGNOR",
        "org.apache.kafka.common.errors.UnsupportedAssignorException",
        "The assignor or its version range is not supported by the consumer group.",
    ),
    (
        113,
        "STALE_MEMBER_EPOCH",
        "org.apache.kafka.common.errors.StaleMemberEpochException",
        "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.",
    ),
    (
        114,
        "MISMATCHED_ENDPOINT_TYPE",
        "org.apache.kafka.common.errors.MismatchedEndpointTypeException",
        "The request was sent to an endpoint of the wrong type.",
    ),
    (
        115,
        "UNSUPPORTED_ENDPOINT_TYPE",
        "org.apache.kafka.common.errors.UnsupportedEndpointTypeException",
        "This endpoint type is not supported yet.",
    ),
    (
        116,
        "UNKNOWN_CONTROLLER_ID",
        "org.apache.kafka.common.errors.UnknownControllerIdException",
        "This controller ID is not known.",
    ),
    (
        117,
        "UNKNOWN_SUBSCRIPTION_ID",
        "org.apache.kafka.common.errors.UnknownSubscriptionIdException",
        "Client sent a push telemetry request with an invalid or outdated subscription ID.",
    ),
    (
        118,
        "TELEMETRY_TOO_LARGE",
        "org.apache.kafka.common.errors.TelemetryTooLargeException",
        "Client sent a push telemetry request larger than the maximum size the broker will accept.",
    ),
    (
        119,
        "INVALID_REGISTRATION",
        "org.apache.kafka.common.errors.InvalidRegistrationException",
        "The controller has considered the broker registration to be invalid.",
    ),
    (
        120,
        "TRANSACTION_ABORTABLE",
        "org.apache.kafka.common.errors.TransactionAbortableException",
        "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.",
    ),
    (
        121,
        "INVALID_RECORD_STATE",
        "org.apache.kafka.common.errors.InvalidRecordStateException",
        "The record state is invalid. The acknowledgement of delivery could not be completed.",
    ),
    (
        122,
        "SHARE_SESSION_NOT_FOUND",
        "org.apache.kafka.common.errors.ShareSessionNotFoundException",
        "The share session was not found.",
    ),
    (
        123,
        "INVALID_SHARE_SESSION_EPOCH",
        "org.apache.kafka.common.errors.InvalidShareSessionEpochException",
        "The share session epoch is invalid.",
    ),
    (
        124,
        "FENCED_STATE_EPOCH",
        "org.apache.kafka.common.errors.FencedStateEpochException",
        "The share coordinator rejected the request because the share-group state epoch did not match.",
    ),
    (
        125,
        "INVALID_VOTER_KEY",
        "org.apache.kafka.common.errors.InvalidVoterKeyException",
        "The voter key doesn't match the receiving replica's key.",
    ),
    (
        126,
        "DUPLICATE_VOTER",
        "org.apache.kafka.common.errors.DuplicateVoterException",
        "The voter is already part of the set of voters.",
    ),
    (
        127,
        "VOTER_NOT_FOUND",
        "org.apache.kafka.common.errors.VoterNotFoundException",
        "The voter is not part of the set of voters.",
    ),
    (
        128,
        "INVALID_REGULAR_EXPRESSION",
        "org.apache.kafka.common.errors.InvalidRegularExpression",
        "The regular expression is not valid.",
    ),
    (
        129,
        "REBOOTSTRAP_REQUIRED",
        "org.apache.kafka.common.errors.RebootstrapRequiredException",
        "Client metadata is stale. The client should rebootstrap to obtain new metadata.",
    ),
    (
        130,
        "STREAMS_INVALID_TOPOLOGY",
        "org.apache.kafka.common.errors.StreamsInvalidTopologyException",
        "The supplied topology is invalid.",
    ),
    (
        131,
        "STREAMS_INVALID_TOPOLOGY_EPOCH",
        "org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException",
        "The supplied topology epoch is invalid.",
    ),
    (
        132,
        "STREAMS_TOPOLOGY_FENCED",
        "org.apache.kafka.common.errors.StreamsTopologyFencedException",
        "The supplied topology epoch is outdated.",
    ),
    (
        133,
        "SHARE_SESSION_LIMIT_REACHED",
        "org.apache.kafka.common.errors.ShareSessionLimitReachedException",
        "The limit of share sessions has been reached.",
    ),
];

/// The exception of a Kafka error code, as `Errors.forCode(code).exception()`
/// builds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KafkaException {
    /// The name of the `Errors` constant, such as `GROUP_ID_NOT_FOUND`.
    name: &'static str,
    /// The fully qualified Java class name.
    class: &'static str,
    /// The default message of the error.
    message: &'static str,
}

impl KafkaException {
    /// The exception of `code`. An unknown code maps to
    /// `UnknownServerException`, as `Errors.forCode` maps it.
    #[must_use]
    pub fn for_code(code: i16) -> Self {
        let (_, name, class, message) = ERRORS
            .iter()
            .copied()
            .find(|(known, ..)| *known == code)
            .unwrap_or(ERRORS[0]);
        Self {
            name,
            class,
            message,
        }
    }

    /// Whether Kafka's `Errors` defines `code`.
    #[must_use]
    pub fn is_known(code: i16) -> bool {
        ERRORS.iter().any(|(known, ..)| *known == code)
    }

    /// The name of the `Errors` constant, as `Errors.toString()` returns it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// The fully qualified class name, as `getClass().getName()` returns it.
    #[must_use]
    pub const fn class(self) -> &'static str {
        self.class
    }

    /// The default message, as `getMessage()` returns it.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.message
    }

    /// `Throwable.toString()`: the class name, `": "`, and the message.
    #[must_use]
    pub fn to_java_string(self) -> String {
        format!("{}: {}", self.class, self.message)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn exceptions_carry_the_kafka_class_and_message() {
        let cases = [
            (
                69,
                "org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.",
            ),
            (
                87,
                "org.apache.kafka.common.InvalidRecordException: This record has failed the validation on broker and hence will be rejected.",
            ),
            (
                -1,
                "org.apache.kafka.common.errors.UnknownServerException: The server experienced an unexpected error when processing the request.",
            ),
            (
                9999,
                "org.apache.kafka.common.errors.UnknownServerException: The server experienced an unexpected error when processing the request.",
            ),
        ];
        for (code, expected) in cases {
            check!(KafkaException::for_code(code).to_java_string() == expected);
        }
        check!(
            (
                KafkaException::for_code(56).name(),
                KafkaException::for_code(56).class()
            ) == (
                "KAFKA_STORAGE_ERROR",
                "org.apache.kafka.common.errors.KafkaStorageException"
            )
        );
    }
}
