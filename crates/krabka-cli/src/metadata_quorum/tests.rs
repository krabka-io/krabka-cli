use assert2::check;
use clap::Parser;

use super::*;

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: MetadataQuorumArgs,
}

fn directory(last: u8) -> uuid::Uuid {
    let mut bytes = [0; 16];
    bytes[15] = last;
    uuid::Uuid::from_bytes(bytes)
}

fn replica(
    node_id: i32,
    last: u8,
    log_end_offset: i64,
    fetch: i64,
    caught_up: i64,
) -> QuorumReplica {
    QuorumReplica {
        node_id,
        directory_id: directory(last),
        log_end_offset,
        last_fetch_timestamp: fetch,
        last_caught_up_timestamp: caught_up,
    }
}

/// A leader, two followers and one observer.
fn quorum() -> MetadataQuorum {
    MetadataQuorum {
        leader_id: 2,
        leader_epoch: 7,
        high_watermark: 120,
        voters: vec![
            replica(1, 1, 110, 1_790_000_000_900, 1_790_000_000_800),
            replica(2, 2, 125, 1_790_000_001_000, 1_790_000_001_000),
            replica(3, 0, 125, 1_790_000_000_950, 1_790_000_000_950),
        ],
        observers: vec![replica(4, 4, 100, -1, -1)],
        nodes: Vec::new(),
    }
}

#[test]
fn describe_replication_renders_kafkas_table() {
    let rows = replication_rows(&quorum(), None).unwrap();
    check!(
        pretty_table(&REPLICATION_HEADERS, &rows)
            == vec![
                "NodeId\tDirectoryId           \tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus  \t",
                "2     \tAAAAAAAAAAAAAAAAAAAAAg\t125         \t0  \t1790000001000     \t1790000001000        \tLeader  \t",
                "1     \tAAAAAAAAAAAAAAAAAAAAAQ\t110         \t15 \t1790000000900     \t1790000000800        \tFollower\t",
                "3     \tAAAAAAAAAAAAAAAAAAAAAA\t125         \t0  \t1790000000950     \t1790000000950        \tFollower\t",
                "4     \tAAAAAAAAAAAAAAAAAAAABA\t100         \t25 \t-1                \t-1                   \tObserver\t",
            ]
    );
}

#[test]
fn human_readable_timestamps_are_relative() {
    let rows = replication_rows(&quorum(), Some(1_790_000_001_010)).unwrap();
    let times = rows
        .iter()
        .map(|row| (row[4].clone(), row[5].clone(), row[6].clone()))
        .collect::<Vec<_>>();
    check!(
        times
            == vec![
                ("10 ms ago".into(), "10 ms ago".into(), "Leader".into()),
                ("110 ms ago".into(), "210 ms ago".into(), "Follower".into()),
                ("60 ms ago".into(), "60 ms ago".into(), "Follower".into()),
                ("-1".into(), "-1".into(), "Observer".into()),
            ]
    );
    check!(
        replication_rows(&quorum(), Some(1_790_000_000_000))
            == Err(
                "Error while computing relative time, possible drift in system clock.\n\
                    Current timestamp is 1790000000000, last fetch timestamp is 1790000001000"
                    .into()
            )
    );
}

#[test]
fn a_quorum_without_its_leader_among_the_voters_fails_as_kafka_does() {
    let mut quorum = quorum();
    quorum.leader_id = 9;
    check!(replication_rows(&quorum, None) == Err("No value present".into()));
}

#[test]
fn describe_status_renders_kafkas_labels() {
    let quorum = MetadataQuorum {
        nodes: vec![QuorumNode {
            node_id: 2,
            endpoints: vec![
                RaftVoterEndpoint::new("CONTROLLER", "controller-2", 9093).unwrap(),
                RaftVoterEndpoint::new("CONTROLLER_V6", "::1", 9094).unwrap(),
            ],
        }],
        ..quorum()
    };
    check!(
        status_lines("5L6g3nShT-eMCtK--X86sw", &quorum)
            == Ok(vec![
                "ClusterId:              5L6g3nShT-eMCtK--X86sw".to_owned(),
                "LeaderId:               2".to_owned(),
                "LeaderEpoch:            7".to_owned(),
                "HighWatermark:          120".to_owned(),
                "MaxFollowerLag:         15".to_owned(),
                "MaxFollowerLagTimeMs:   200".to_owned(),
                "CurrentVoters:          [{\"id\": 1, \"directoryId\": \"AAAAAAAAAAAAAAAAAAAAAQ\"}, \
                 {\"id\": 2, \"directoryId\": \"AAAAAAAAAAAAAAAAAAAAAg\", \"endpoints\": \
                 [\"CONTROLLER://controller-2:9093\", \"CONTROLLER_V6://[::1]:9094\"]}, {\"id\": 3}]"
                    .to_owned(),
                "CurrentObservers:       [{\"id\": 4, \"directoryId\": \"AAAAAAAAAAAAAAAAAAAABA\"}]"
                    .to_owned(),
            ])
    );
}

#[test]
fn max_follower_lag_time_follows_kafkas_three_cases() {
    let single = MetadataQuorum {
        leader_id: 1,
        leader_epoch: 1,
        high_watermark: 42,
        voters: vec![replica(1, 0, 45, 5, 5)],
        observers: Vec::new(),
        nodes: Vec::new(),
    };
    let unknown = MetadataQuorum {
        voters: vec![replica(1, 0, 45, 5, 5), replica(2, 0, 40, -1, -1)],
        ..single.clone()
    };
    let cases = [
        (
            single,
            "MaxFollowerLag:         0",
            "MaxFollowerLagTimeMs:   0",
        ),
        (
            unknown,
            "MaxFollowerLag:         5",
            "MaxFollowerLagTimeMs:   -1",
        ),
    ];
    for (quorum, lag, lag_time) in cases {
        let lines = status_lines("id", &quorum).unwrap();
        check!((lines[4].as_str(), lines[5].as_str()) == (lag, lag_time));
    }
}

#[test]
fn describe_flags_resolve_or_fail_with_kafkas_messages() {
    let cases = [
        (true, false, false, Ok(Report::Status)),
        (
            false,
            true,
            false,
            Ok(Report::Replication {
                human_readable: false,
            }),
        ),
        (
            false,
            true,
            true,
            Ok(Report::Replication {
                human_readable: true,
            }),
        ),
        (
            true,
            true,
            false,
            Err(
                "Only one of --status or --replication should be specified with describe \
                 sub-command",
            ),
        ),
        (
            true,
            false,
            true,
            Err("The option --human-readable is only supported along with --replication"),
        ),
        (
            false,
            false,
            false,
            Err("One of --status or --replication must be specified with describe sub-command"),
        ),
        (
            false,
            false,
            true,
            Err("One of --status or --replication must be specified with describe sub-command"),
        ),
    ];
    for (status, replication, human_readable, expected) in cases {
        let args = DescribeArgs {
            status,
            replication,
            human_readable,
        };
        check!(report(&args) == expected.map_err(ToOwned::to_owned));
    }
}

#[test]
fn remove_controller_checks_match_kafka() {
    let one = KafkaUuid::ONE;
    check!(removal(7, "AAAAAAAAAAAAAAAAAAAAAQ") == Ok((7, one)));
    check!(
        removal(-1, "AAAAAAAAAAAAAAAAAAAAAQ") == Err("Invalid negative --controller-id: -1".into())
    );
    check!(
        removal(7, "bogus")
            == Err(
                "Failed to parse --controller-directory-id: Last unit does not have enough \
                    valid bits"
                    .into()
            )
    );
    check!(
        removed_line(7, one, true)
            == "DRY RUN of removing  KRaft controller 7 with directory id AAAAAAAAAAAAAAAAAAAAAQ"
    );
    check!(
        removed_line(7, one, false)
            == "Removed  KRaft controller 7 with directory id AAAAAAAAAAAAAAAAAAAAAQ"
    );
}

#[test]
fn kafka_metadata_quorum_command_lines_parse() {
    let ok = [
        vec!["q", "--bootstrap-server", "h:9092", "describe", "--status"],
        vec![
            "q",
            "--bootstrap-controller",
            "h:9093",
            "describe",
            "--replication",
            "--human-readable",
        ],
        vec![
            "q",
            "--bootstrap-server",
            "h:9092",
            "remove-controller",
            "-i",
            "3",
            "-d",
            "AAAAAAAAAAAAAAAAAAAAAQ",
        ],
        vec![
            "q",
            "--bootstrap-server",
            "h:9092",
            "remove-controller",
            "--controller-id",
            "3",
            "--controller-directory-id",
            "-AAAAAAAAAAAAAAAAAAAAAQ",
            "--dry-run",
        ],
        vec![
            "q",
            "--bootstrap-server",
            "h:9092",
            "--command-config",
            "c.properties",
            "add-controller",
            "--dry-run",
        ],
    ];
    for argv in ok {
        check!(Command::try_parse_from(&argv).is_ok(), "{argv:?}");
    }
    let refused = [
        vec![
            "q",
            "--bootstrap-server",
            "h:9092",
            "remove-controller",
            "-i",
            "3",
        ],
        vec![
            "q",
            "--bootstrap-server",
            "h:9092",
            "remove-controller",
            "-d",
            "AAAAAAAAAAAAAAAAAAAAAQ",
        ],
        vec!["q", "--bootstrap-server", "h:9092"],
    ];
    for argv in refused {
        check!(Command::try_parse_from(&argv).is_err(), "{argv:?}");
    }
}

fn properties(text: &str) -> Properties {
    Properties::parse(text.as_bytes()).unwrap()
}

const CONTROLLER: &str = "node.id=3\nprocess.roles=controller\nmetadata.log.dir=/data/meta\n\
    controller.listener.names=CONTROLLER\nlisteners=CONTROLLER://:9093\n\
    advertised.listeners=CONTROLLER://controller-3:9093\n";

fn meta(name: &'static str, text: &'static str) -> impl Fn(&Path) -> Option<(String, String)> {
    move |directory| {
        (directory == Path::new("/data/meta")).then(|| (name.to_owned(), text.to_owned()))
    }
}

#[test]
fn add_controller_reads_the_controllers_identity_as_kafka_does() {
    let kafka_meta = meta(
        "meta.properties",
        "version=1\nnode.id=3\ndirectory.id=AAAAAAAAAAAAAAAAAAAAAQ\n",
    );
    let krabka_meta = meta(
        "meta.properties.json",
        r#"{"cluster_id": "e4bea0dd-7b3a-4fe3-8c0a-d2be3ff5f3ab", "directory_id": "00000000-0000-0000-0000-000000000001", "version": 1}"#,
    );
    let expected = NewController {
        id: 3,
        directory_id: KafkaUuid::ONE,
        endpoints: vec![VoterEndpoint {
            listener: "CONTROLLER".into(),
            host: "controller-3".into(),
            port: 9093,
        }],
    };
    check!(new_controller(&properties(CONTROLLER), &kafka_meta) == Ok(expected.clone()));
    check!(new_controller(&properties(CONTROLLER), &krabka_meta) == Ok(expected.clone()));
    check!(
        added_line(&expected, true)
            == "DRY RUN of adding controller 3 with directory id AAAAAAAAAAAAAAAAAAAAAQ and \
                endpoints: CONTROLLER://controller-3:9093"
    );
}

#[test]
fn add_controller_refuses_an_invalid_configuration_with_kafkas_messages() {
    let good_meta = meta("meta.properties", "directory.id=AAAAAAAAAAAAAAAAAAAAAQ\n");
    let cases = [
        (
            CONTROLLER.replace("node.id=3\n", ""),
            "node.id not found in configuration file. Is this a valid controller configuration \
             file?",
        ),
        (
            CONTROLLER.replace("node.id=3", "node.id=-1"),
            "node.id was negative in configuration file. Is this a valid controller \
             configuration file?",
        ),
        (
            CONTROLLER.replace("process.roles=controller", "process.roles=broker"),
            "process.roles did not contain 'controller' in configuration file. Is this a valid \
             controller configuration file?",
        ),
        (
            CONTROLLER.replace("metadata.log.dir=/data/meta\n", ""),
            "Neither metadata.log.dir nor log.dirs were found. Is this a valid controller \
             configuration file?",
        ),
        (
            CONTROLLER.replace(
                "metadata.log.dir=/data/meta",
                "log.dirs=/data/other,/data/meta",
            ),
            "Unable to read meta.properties from /data/other",
        ),
        (
            CONTROLLER.replace("controller.listener.names=CONTROLLER\n", ""),
            "controller.listener.names was not found. Is this a valid controller configuration \
             file?",
        ),
        (
            CONTROLLER.replace(
                "controller.listener.names=CONTROLLER",
                "controller.listener.names=ctl",
            ),
            "Cannot find information about controller listener name: CTL",
        ),
        (
            CONTROLLER.replace("listeners=CONTROLLER://:9093", "listeners=CONTROLLER:9093"),
            "Unable to parse CONTROLLER:9093 to a broker endpoint",
        ),
    ];
    for (config, message) in cases {
        check!(
            new_controller(&properties(&config), &good_meta) == Err(message.to_owned()),
            "{config}"
        );
    }
    let no_id = meta("meta.properties", "version=1\n");
    check!(
        new_controller(&properties(CONTROLLER), &no_id)
            == Err("No directory id found in /data/meta".into())
    );
}

#[test]
fn listeners_fall_back_to_localhost_and_keep_ipv6_hosts() {
    check!(
        listener_entries("controller://:9093, SSL://[::1]:9094").map(|endpoints| endpoints
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>())
            == Ok(vec![
                "CONTROLLER://localhost:9093".to_owned(),
                "SSL://[::1]:9094".to_owned(),
            ])
    );
}
