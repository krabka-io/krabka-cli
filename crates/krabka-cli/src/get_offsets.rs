//! `krabka get-offsets`, the counterpart of `kafka-get-offsets`
//! (`GetOffsetShell`).
//!
//! The command selects partitions with `--topic` and `--partitions`, or with
//! `--topic-partitions`, reads the topics from the cluster metadata, and asks
//! each partition's leader for the offset that `--time` names. It prints one
//! `topic:partition:offset` line per partition, ordered as the JVM tool orders
//! them, and leaves out a partition with no such offset.
//!
//! The offset lookup is `ListOffsets`, behind [`OffsetLookup`]. The pinned
//! `krabka-client-rs` has no `AdminClient::list_offsets`, so the lookup is
//! [`Unavailable`] and, after the selection resolves, the command fails with
//! a "not supported by this build" error. Parsing, selection and rendering
//! are complete.

use std::{cmp::Ordering, collections::BTreeMap};

use clap::Args;
use regex::Regex;
use serde_json::json;

use crate::{
    compat::{KafkaException, not_supported},
    connection::ConnectionArgs,
    output::{CommandError, CommandResult},
};

/// The topics that Kafka's `Topic.isInternal` names.
const INTERNAL_TOPICS: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

#[derive(Debug, Args)]
pub struct GetOffsetsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Comma separated list of topic-partition patterns to get the offsets
    /// for, such as `topic1:1,topic2:0-3,topic3,topic4:5-,topic5:-3`.
    #[arg(long)]
    topic_partitions: Option<String>,
    /// The topic to get the offsets for. It also accepts a regular
    /// expression. If not present, all authorized topics are queried.
    #[arg(long)]
    topic: Option<String>,
    /// Comma separated list of partition ids to get the offsets for.
    #[arg(long)]
    partitions: Option<String>,
    /// The timestamp of the offsets before that: `-1` or `latest`, `-2` or
    /// `earliest`, `-3` or `max-timestamp`, `-4` or `earliest-local`, `-5` or
    /// `latest-tiered`, `-6` or `earliest-pending-upload`, or a timestamp in
    /// milliseconds.
    #[arg(long, default_value = "latest", allow_hyphen_values = true)]
    time: String,
    /// By default, internal topics are included. If specified, internal
    /// topics are excluded.
    #[arg(long)]
    exclude_internal_topics: bool,
}

/// Which offset of a partition to look up, as Kafka's `OffsetSpec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    Earliest,
    Latest,
    MaxTimestamp,
    EarliestLocal,
    LatestTiered,
    EarliestPendingUpload,
    Timestamp(i64),
}

impl OffsetSpec {
    /// The `timestamp` that `ListOffsets` carries for this spec.
    #[must_use]
    pub const fn wire_timestamp(self) -> i64 {
        match self {
            Self::Latest => -1,
            Self::Earliest => -2,
            Self::MaxTimestamp => -3,
            Self::EarliestLocal => -4,
            Self::LatestTiered => -5,
            Self::EarliestPendingUpload => -6,
            Self::Timestamp(timestamp) => timestamp,
        }
    }
}

/// `GetOffsetShell.parseOffsetSpec`.
pub fn parse_offset_spec(value: &str) -> Result<OffsetSpec, String> {
    Ok(match value {
        "earliest" => OffsetSpec::Earliest,
        "latest" => OffsetSpec::Latest,
        "max-timestamp" => OffsetSpec::MaxTimestamp,
        "earliest-local" => OffsetSpec::EarliestLocal,
        "latest-tiered" => OffsetSpec::LatestTiered,
        "earliest-pending-upload" => OffsetSpec::EarliestPendingUpload,
        _ => match value.parse::<i64>() {
            Ok(-1) => OffsetSpec::Latest,
            Ok(-2) => OffsetSpec::Earliest,
            Ok(-3) => OffsetSpec::MaxTimestamp,
            Ok(-4) => OffsetSpec::EarliestLocal,
            Ok(-5) => OffsetSpec::LatestTiered,
            Ok(-6) => OffsetSpec::EarliestPendingUpload,
            Ok(timestamp) => OffsetSpec::Timestamp(timestamp),
            Err(_) => {
                return Err(format!(
                    "Malformed time argument {value}. Please use -1 or latest / -2 or earliest / -3 or max-timestamp / -4 or earliest-local / -5 or latest-tiered / -6 or earliest-pending-upload, or a specified long format timestamp"
                ));
            }
        },
    })
}

/// `String.split(",")` as Java does it: trailing empty strings are dropped,
/// and an input with no comma is one element, even when it is empty.
fn java_split(value: &str) -> Vec<&str> {
    let mut parts = value.split(',').collect::<Vec<_>>();
    if parts.len() > 1 {
        while parts.last() == Some(&"") {
            parts.pop();
        }
    }
    parts
}

/// Kafka's `TopicFilter.IncludeList`: the pattern with commas as
/// alternatives, spaces and surrounding quotes removed, matched against the
/// whole topic name.
#[derive(Debug, Clone)]
struct TopicPattern(Regex);

impl TopicPattern {
    fn new(raw: &str) -> Result<Self, String> {
        let cleaned = raw.trim().replace(',', "|").replace(' ', "");
        let cleaned = cleaned
            .trim_start_matches(['"', '\''])
            .trim_end_matches(['"', '\'']);
        Regex::new(&format!("^(?:{cleaned})$"))
            .map(Self)
            .map_err(|_| format!("{cleaned} is an invalid regex."))
    }

    fn allows(&self, topic: &str) -> bool {
        self.0.is_match(topic)
    }
}

/// Which partitions of an allowed topic a rule keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PartitionFilter {
    /// `--partitions`: the listed ids, or every partition when empty.
    Set(Vec<i32>),
    /// `topic:N`.
    Unique(i32),
    /// `topic:L-U`, lower inclusive and upper exclusive.
    Range(i32, i32),
}

impl PartitionFilter {
    fn allows(&self, partition: i32) -> bool {
        match self {
            Self::Set(ids) => ids.is_empty() || ids.contains(&partition),
            Self::Unique(id) => *id == partition,
            Self::Range(lower, upper) => (*lower..*upper).contains(&partition),
        }
    }
}

/// One rule: a topic pattern and a partition filter.
#[derive(Debug, Clone)]
struct Rule {
    topic: TopicPattern,
    partitions: PartitionFilter,
}

/// Every rule of a selection. A partition is selected when any rule keeps it.
#[derive(Debug, Clone)]
pub struct Selection(Vec<Rule>);

impl Selection {
    fn allows_topic(&self, topic: &str) -> bool {
        self.0.iter().any(|rule| rule.topic.allows(topic))
    }

    fn allows(&self, topic: &str, partition: i32) -> bool {
        self.0
            .iter()
            .any(|rule| rule.topic.allows(topic) && rule.partitions.allows(partition))
    }
}

fn parse_int(digits: &str) -> Result<i32, String> {
    digits
        .parse::<i32>()
        .map_err(|_| format!("For input string: \"{digits}\""))
}

/// One `--topic-partitions` rule, as `GetOffsetShell.parseRuleSpec` reads it
/// against `([^:,]*)(?::(?:([0-9]*)|(?:([0-9]*)-([0-9]*))))?`.
fn parse_rule(spec: &str) -> Result<Rule, String> {
    let invalid = || format!("Invalid rule specification: {spec}");
    let (topic, partitions) = match spec.split_once(':') {
        None => (spec, None),
        Some((topic, partitions)) => (topic, Some(partitions)),
    };
    let digits = |value: &str| value.bytes().all(|byte| byte.is_ascii_digit());
    let partitions = match partitions {
        None | Some("") => PartitionFilter::Range(0, i32::MAX),
        Some(single) if digits(single) => PartitionFilter::Unique(parse_int(single)?),
        Some(range) => {
            let (lower, upper) = range.split_once('-').ok_or_else(invalid)?;
            if !digits(lower) || !digits(upper) {
                return Err(invalid());
            }
            PartitionFilter::Range(
                if lower.is_empty() {
                    0
                } else {
                    parse_int(lower)?
                },
                if upper.is_empty() {
                    i32::MAX
                } else {
                    parse_int(upper)?
                },
            )
        }
    };
    Ok(Rule {
        topic: TopicPattern::new(if topic.is_empty() { ".*" } else { topic })?,
        partitions,
    })
}

/// The selection of `--topic-partitions`, or of `--topic` and `--partitions`.
pub fn selection(
    topic_partitions: Option<&str>,
    topic: Option<&str>,
    partitions: Option<&str>,
) -> Result<Selection, String> {
    if topic_partitions.is_some() && (topic.is_some() || partitions.is_some()) {
        return Err("--topic-partitions cannot be used with --topic or --partitions".into());
    }
    if let Some(spec) = topic_partitions {
        return java_split(spec)
            .into_iter()
            .map(parse_rule)
            .collect::<Result<_, _>>()
            .map(Selection);
    }
    let ids = match partitions {
        None | Some("") => Vec::new(),
        Some(list) => java_split(list)
            .into_iter()
            .map(str::parse::<i32>)
            .collect::<Result<_, _>>()
            .map_err(|_| {
                format!(
                    "--partitions expects a comma separated list of numeric partition ids, but received: {list}"
                )
            })?,
    };
    Ok(Selection(vec![Rule {
        topic: TopicPattern::new(topic.unwrap_or(".*"))?,
        partitions: PartitionFilter::Set(ids),
    }]))
}

/// The partitions of `cluster` that `selection` keeps, internal topics left
/// out under `exclude_internal`.
pub fn select_partitions(
    cluster: &[(String, i32)],
    selection: &Selection,
    exclude_internal: bool,
) -> Vec<(String, i32)> {
    cluster
        .iter()
        .filter(|(topic, partition)| {
            !(exclude_internal && INTERNAL_TOPICS.contains(&topic.as_str()))
                && selection.allows_topic(topic)
                && selection.allows(topic, *partition)
        })
        .cloned()
        .collect()
}

/// What the lookup found for one partition: the offset, `-1` when the
/// partition has none, or the Kafka error code of the partition.
pub type PartitionOffset = Result<i64, i16>;

/// Where log offsets come from: `ListOffsets`, as Kafka's `Admin.listOffsets`
/// sends it to each partition's leader.
///
/// This is the seam for `AdminClient::list_offsets`, which the pinned
/// `krabka-client-rs` revision does not have. [`Unavailable`] stands in for
/// it until the pin moves.
pub trait OffsetLookup {
    /// The offset that `spec` names for each of `partitions`. A partition
    /// the answer leaves out has an unknown offset.
    async fn offsets(
        &self,
        partitions: &[(String, i32)],
        spec: OffsetSpec,
    ) -> Result<BTreeMap<(String, i32), PartitionOffset>, CommandError>;
}

/// The lookup of the pinned `krabka-client-rs`, which has no `list_offsets`:
/// every lookup fails with [`CommandError::Unsupported`].
pub struct Unavailable;

impl OffsetLookup for Unavailable {
    async fn offsets(
        &self,
        _partitions: &[(String, i32)],
        spec: OffsetSpec,
    ) -> Result<BTreeMap<(String, i32), PartitionOffset>, CommandError> {
        Err(not_supported(
            &format!(
                "reading log offsets (ListOffsets timestamp {})",
                spec.wire_timestamp()
            ),
            "list_offsets",
        ))
    }
}

/// `TopicPartition.toString()` order, which `GetOffsetShell` sorts by.
fn by_name(left: &(String, i32), right: &(String, i32)) -> Ordering {
    format!("{}-{}", left.0, left.1).cmp(&format!("{}-{}", right.0, right.1))
}

/// The report of the lookup: a `topic:partition:offset` line for each
/// partition with an offset, a notice on stderr for each partition that
/// failed, and a failure when any did.
#[must_use]
pub fn offsets_result(mut answers: Vec<((String, i32), PartitionOffset)>) -> CommandResult {
    answers.sort_by(|(left, _), (right, _)| by_name(left, right));
    let mut human = Vec::new();
    let mut notices = Vec::new();
    let mut data = Vec::new();
    for ((topic, partition), answer) in &answers {
        match answer {
            Ok(-1) => {
                data.push(
                    json!({"topic": topic, "partition": partition, "offset": null, "error": null}),
                );
            }
            Ok(offset) => {
                human.push(format!("{topic}:{partition}:{offset}"));
                data.push(json!({"topic": topic, "partition": partition, "offset": offset, "error": null}));
            }
            Err(code) => {
                let exception = KafkaException::for_code(*code);
                notices.push(format!(
                    "Skip getting offsets for topic-partition {topic}-{partition} due to error: {}",
                    exception.to_java_string()
                ));
                data.push(json!({
                    "topic": topic,
                    "partition": partition,
                    "offset": null,
                    "error": {"code": code, "message": exception.message()},
                }));
            }
        }
    }
    let failed = !notices.is_empty();
    CommandResult::rows(human, data, failed).with_notices(notices)
}

impl GetOffsetsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let selection = selection(
            self.topic_partitions.as_deref(),
            self.topic.as_deref(),
            self.partitions.as_deref(),
        )?;
        let spec = parse_offset_spec(&self.time)?;
        let mut client = self.connection.connect("get-offsets").await?;
        let cluster = client
            .describe_partition_assignments(&[])
            .await?
            .into_iter()
            .map(|assignment| (assignment.topic, assignment.partition))
            .collect::<Vec<_>>();
        let partitions = select_partitions(&cluster, &selection, self.exclude_internal_topics);
        if partitions.is_empty() {
            return Err("Could not match any topic-partitions with the specified filters".into());
        }
        let answers = Unavailable.offsets(&partitions, spec).await?;
        Ok(offsets_result(answers.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn time_maps_to_the_list_offsets_timestamp_kafka_sends() {
        let malformed = |value: &str| {
            Err(format!(
                "Malformed time argument {value}. Please use -1 or latest / -2 or earliest / -3 or max-timestamp / -4 or earliest-local / -5 or latest-tiered / -6 or earliest-pending-upload, or a specified long format timestamp"
            ))
        };
        let cases = [
            ("-1", Ok(-1)),
            ("latest", Ok(-1)),
            ("-2", Ok(-2)),
            ("earliest", Ok(-2)),
            ("-3", Ok(-3)),
            ("max-timestamp", Ok(-3)),
            ("-4", Ok(-4)),
            ("earliest-local", Ok(-4)),
            ("-5", Ok(-5)),
            ("latest-tiered", Ok(-5)),
            ("-6", Ok(-6)),
            ("earliest-pending-upload", Ok(-6)),
            ("1700000000000", Ok(1_700_000_000_000)),
            ("0", Ok(0)),
            ("-7", Ok(-7)),
            ("9223372036854775808", malformed("9223372036854775808")),
            ("bogus", malformed("bogus")),
            ("Latest", malformed("Latest")),
        ];
        for (value, expected) in cases {
            check!(
                parse_offset_spec(value).map(OffsetSpec::wire_timestamp) == expected,
                "{value}"
            );
        }
    }

    fn cluster() -> Vec<(String, i32)> {
        [
            ("orders", 2),
            ("events", 12),
            ("__consumer_offsets", 2),
            ("audit", 1),
        ]
        .into_iter()
        .flat_map(|(topic, count)| (0..count).map(move |partition| (topic.to_owned(), partition)))
        .collect()
    }

    fn names(partitions: &[(String, i32)]) -> Vec<String> {
        partitions
            .iter()
            .map(|(topic, partition)| format!("{topic}:{partition}"))
            .collect()
    }

    #[test]
    fn both_selection_forms_keep_what_get_offset_shell_keeps() {
        struct Case {
            topic_partitions: Option<&'static str>,
            topic: Option<&'static str>,
            partitions: Option<&'static str>,
            exclude_internal: bool,
            expected: Result<Vec<&'static str>, &'static str>,
        }
        let case = |topic_partitions, topic, partitions, expected| Case {
            topic_partitions,
            topic,
            partitions,
            exclude_internal: false,
            expected,
        };
        let cases = [
            case(None, Some("orders"), None, Ok(vec!["orders:0", "orders:1"])),
            case(None, Some("orders"), Some("1"), Ok(vec!["orders:1"])),
            case(
                None,
                Some("ord.*,audit"),
                Some("0,"),
                Ok(vec!["orders:0", "audit:0"]),
            ),
            case(None, Some("ord"), None, Ok(vec![])),
            case(
                None,
                Some("orders"),
                Some("x"),
                Err(
                    "--partitions expects a comma separated list of numeric partition ids, but received: x",
                ),
            ),
            case(
                None,
                Some("orders"),
                Some("1,,0"),
                Err(
                    "--partitions expects a comma separated list of numeric partition ids, but received: 1,,0",
                ),
            ),
            case(None, Some("("), None, Err("( is an invalid regex.")),
            case(
                Some("orders"),
                Some("orders"),
                None,
                Err("--topic-partitions cannot be used with --topic or --partitions"),
            ),
            case(
                Some("events:8-11,orders:1"),
                None,
                None,
                Ok(vec!["orders:1", "events:8", "events:9", "events:10"]),
            ),
            case(
                Some("events:10-"),
                None,
                None,
                Ok(vec!["events:10", "events:11"]),
            ),
            case(
                Some("events:-2,audit"),
                None,
                None,
                Ok(vec!["events:0", "events:1", "audit:0"]),
            ),
            case(
                Some("events:"),
                None,
                None,
                Ok((0..12)
                    .map(|p| {
                        [
                            "events:0",
                            "events:1",
                            "events:2",
                            "events:3",
                            "events:4",
                            "events:5",
                            "events:6",
                            "events:7",
                            "events:8",
                            "events:9",
                            "events:10",
                            "events:11",
                        ][p]
                    })
                    .collect()),
            ),
            case(
                Some(":1"),
                None,
                None,
                Ok(vec!["orders:1", "events:1", "__consumer_offsets:1"]),
            ),
            case(
                Some("orders:x"),
                None,
                None,
                Err("Invalid rule specification: orders:x"),
            ),
            case(
                Some("orders:1-2-3"),
                None,
                None,
                Err("Invalid rule specification: orders:1-2-3"),
            ),
            case(
                Some("orders:99999999999"),
                None,
                None,
                Err("For input string: \"99999999999\""),
            ),
            Case {
                exclude_internal: true,
                ..case(Some(":1"), None, None, Ok(vec!["orders:1", "events:1"]))
            },
        ];
        for case in cases {
            let actual =
                selection(case.topic_partitions, case.topic, case.partitions).map(|selection| {
                    names(&select_partitions(
                        &cluster(),
                        &selection,
                        case.exclude_internal,
                    ))
                });
            let expected = case
                .expected
                .map(|names| names.into_iter().map(ToOwned::to_owned).collect::<Vec<_>>())
                .map_err(ToOwned::to_owned);
            check!(
                actual == expected,
                "{:?} {:?} {:?}",
                case.topic_partitions,
                case.topic,
                case.partitions
            );
        }
    }

    #[test]
    fn offsets_render_in_topic_partition_string_order_and_errors_fail() {
        let answers = vec![
            (("events".to_owned(), 10), Ok(4)),
            (("events".to_owned(), 8), Ok(0)),
            (("orders".to_owned(), 1), Err(6)),
            (("orders".to_owned(), 0), Ok(-1)),
            (("events".to_owned(), 9), Ok(12)),
        ];
        let result = offsets_result(answers);
        check!(result.human == ["events:10:4", "events:8:0", "events:9:12"]);
        check!(
            result.notices
                == [
                    "Skip getting offsets for topic-partition orders-1 due to error: org.apache.kafka.common.errors.NotLeaderOrFollowerException: For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."
                ]
        );
        check!(result.failed);
        let clean = offsets_result(vec![(("orders".to_owned(), 0), Ok(10))]);
        check!((clean.human, clean.failed) == (vec!["orders:0:10".to_owned()], false));
    }
}
