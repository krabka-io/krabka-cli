//! Kafka's `StripedReplicaPlacerTest`, with its `MockRandom`, and the
//! `java.util.Random` and `HashMap` behaviour that the port relies on.

use assert2::check;

use super::*;

/// Kafka's `MockRandom`: a fixed-seed generator for placer tests.
struct MockRandom(u64);

impl MockRandom {
    const fn new() -> Self {
        Self(17)
    }
}

impl JavaRandom for MockRandom {
    fn next(&mut self, bits: u32) -> i32 {
        self.0 = self
            .0
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        i32::from_ne_bytes(
            u32::try_from(self.0 >> (64 - bits))
                .unwrap_or(0)
                .to_ne_bytes(),
        )
    }
}

impl<R> RackList<'_, R> {
    fn rack_names(&self) -> Vec<Option<String>> {
        self.rack_names
            .iter()
            .map(|&index| self.racks[index].0.clone())
            .collect()
    }
}

fn broker(id: i32, rack: Option<&str>, fenced: bool) -> UsableBroker {
    UsableBroker {
        id,
        rack: rack.map(Into::into),
        fenced,
    }
}

#[test]
fn lcg48_matches_java_util_random() {
    // `new Random(42)`: nextInt() twice, nextInt(10) three times, and
    // nextInt(16), as the JVM returns them.
    let mut random = Lcg48::new(42);
    check!(random.next(32) == -1_170_105_035);
    check!(random.next(32) == 234_785_527);
    check!(
        [
            random.next_int(10),
            random.next_int(10),
            random.next_int(10)
        ] == [8, 4, 0]
    );
    check!(random.next_int(16) == 15);
}

#[test]
fn avoid_fenced_replica_if_possible_on_single_rack() {
    let mut random = MockRandom::new();
    let brokers = [
        broker(3, None, false),
        broker(1, None, true),
        broker(0, None, false),
        broker(4, None, false),
        broker(2, None, false),
    ];
    let mut racks = RackList::new(&mut random, &brokers);
    check!((racks.total, racks.unfenced) == (5, 4));
    check!(racks.rack_names() == vec![None]);
    check!(racks.place(0).is_err());
    check!(racks.place(-1).is_err());
    let placed = (0..5).map(|_| racks.place(4).unwrap()).collect::<Vec<_>>();
    check!(
        placed
            == vec![
                vec![3, 4, 0, 2],
                vec![4, 0, 2, 3],
                vec![0, 2, 3, 4],
                vec![2, 3, 4, 0],
                vec![0, 4, 3, 2],
            ]
    );
}

#[test]
fn multi_partition_topic_placement_on_single_unfenced_broker() {
    let mut random = MockRandom::new();
    check!(
        place(
            &mut random,
            3,
            1,
            &[broker(0, None, false), broker(1, None, true)]
        ) == Ok(vec![vec![0], vec![0], vec![0]])
    );
}

#[test]
fn placement_on_fenced_replica_on_single_rack() {
    let mut random = MockRandom::new();
    let brokers = [
        broker(3, None, false),
        broker(1, None, true),
        broker(2, None, false),
    ];
    let mut racks = RackList::new(&mut random, &brokers);
    check!((racks.total, racks.unfenced) == (3, 2));
    let placed = (0..4).map(|_| racks.place(3).unwrap()).collect::<Vec<_>>();
    check!(placed == vec![vec![3, 2, 1], vec![2, 3, 1], vec![3, 2, 1], vec![2, 3, 1]]);
}

#[test]
fn rack_list_with_multiple_racks() {
    let mut random = MockRandom::new();
    let brokers = [
        broker(11, Some("1"), false),
        broker(10, Some("1"), false),
        broker(30, Some("3"), false),
        broker(31, Some("3"), false),
        broker(21, Some("2"), false),
        broker(20, Some("2"), true),
    ];
    let mut racks = RackList::new(&mut random, &brokers);
    check!((racks.total, racks.unfenced) == (6, 5));
    check!(racks.rack_names() == vec![Some("1".into()), Some("2".into()), Some("3".into())]);
    let placed = (0..3).map(|_| racks.place(4).unwrap()).collect::<Vec<_>>();
    check!(
        placed
            == vec![
                vec![11, 21, 31, 10],
                vec![21, 30, 10, 20],
                vec![31, 11, 21, 30]
            ]
    );
}

#[test]
fn rack_list_with_invalid_racks() {
    let mut random = MockRandom::new();
    let brokers = [
        broker(11, Some("1"), false),
        broker(10, Some("1"), false),
        broker(30, Some("3"), true),
        broker(31, Some("3"), true),
        broker(20, Some("2"), true),
        broker(21, Some("2"), true),
        broker(41, Some("4"), false),
        broker(40, Some("4"), true),
    ];
    let mut racks = RackList::new(&mut random, &brokers);
    check!((racks.total, racks.unfenced) == (8, 3));
    let placed = (0..3).map(|_| racks.place(4).unwrap()).collect::<Vec<_>>();
    check!(
        placed
            == vec![
                vec![41, 11, 21, 30],
                vec![10, 20, 31, 41],
                vec![41, 21, 30, 11]
            ]
    );
}

#[test]
fn invalid_placements_fail_with_kafkas_messages() {
    let two = [broker(11, Some("1"), false), broker(10, Some("1"), false)];
    let fenced = [broker(11, Some("1"), true), broker(10, Some("1"), true)];
    let cases = [
        (
            1,
            &fenced[..],
            "All brokers are currently fenced, or have all their log directories cordoned.",
        ),
        (
            3,
            &two[..],
            "The target replication factor of 3 cannot be reached because only 2 broker(s) are \
             registered or some brokers have all their log directories cordoned.",
        ),
        (
            0,
            &two[..],
            "Invalid replication factor 0: the replication factor must be positive.",
        ),
    ];
    for (replication_factor, brokers, message) in cases {
        let mut random = MockRandom::new();
        check!(place(&mut random, 1, replication_factor, brokers) == Err(message.to_owned()));
    }
    let mut random = MockRandom::new();
    let all_fenced = [
        broker(0, None, true),
        broker(1, None, true),
        broker(2, None, true),
    ];
    check!(
        RackList::new(&mut random, &all_fenced).place(3)
            == Err(
                "All brokers are currently fenced, or have all their log directories cordoned."
                    .to_owned()
            )
    );
    let mut random = MockRandom::new();
    check!(
        RackList::new(&mut random, &two).place(-1)
            == Err(
                "Invalid replication factor -1: the replication factor must be positive.".into()
            )
    );
}

#[test]
fn successful_placement() {
    let mut random = MockRandom::new();
    let brokers = [
        broker(0, None, false),
        broker(3, None, false),
        broker(2, None, false),
        broker(1, None, false),
    ];
    check!(
        place(&mut random, 5, 3, &brokers)
            == Ok(vec![
                vec![2, 3, 0],
                vec![3, 0, 1],
                vec![0, 1, 2],
                vec![1, 2, 3],
                vec![1, 0, 2],
            ])
    );
}

#[test]
fn even_distribution() {
    let mut random = MockRandom::new();
    let brokers = (0..4).map(|id| broker(id, None, false)).collect::<Vec<_>>();
    let placed = place(&mut random, 200, 2, &brokers).unwrap();
    let mut counts = BTreeMap::<Vec<i32>, i32>::new();
    for replicas in placed {
        *counts.entry(replicas).or_default() += 1;
    }
    check!(
        counts
            == BTreeMap::from([
                (vec![0, 1], 14),
                (vec![0, 2], 22),
                (vec![0, 3], 14),
                (vec![1, 0], 17),
                (vec![1, 2], 17),
                (vec![1, 3], 16),
                (vec![2, 0], 13),
                (vec![2, 1], 17),
                (vec![2, 3], 20),
                (vec![3, 0], 20),
                (vec![3, 1], 19),
                (vec![3, 2], 11),
            ])
    );
}

#[test]
fn a_proposal_keeps_each_topics_shape() {
    let mut random = MockRandom::new();
    let current = BTreeMap::from([
        (("bar".to_owned(), 0), vec![1, 2]),
        (("foo".to_owned(), 0), vec![1]),
        (("foo".to_owned(), 1), vec![2]),
    ]);
    let brokers = (1..=3)
        .map(|id| broker(id, None, false))
        .collect::<Vec<_>>();
    let proposed = propose(&mut random, &current, &brokers).unwrap();
    let shape = proposed
        .iter()
        .map(|((topic, partition), replicas)| (topic.clone(), *partition, replicas.len()))
        .collect::<Vec<_>>();
    check!(
        shape
            == vec![
                ("bar".to_owned(), 0, 2),
                ("foo".to_owned(), 0, 1),
                ("foo".to_owned(), 1, 1),
            ]
    );
    check!(proposed.values().flatten().all(|id| (1..=3).contains(id)));
}
