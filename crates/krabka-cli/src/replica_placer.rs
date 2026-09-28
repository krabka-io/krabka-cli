//! Kafka's `StripedReplicaPlacer`, which `kafka-reassign-partitions
//! --generate` uses to propose an assignment.
//!
//! The port keeps the placer's use of `java.util.Random` and the iteration
//! order of its `HashMap`s, so a seeded run proposes what Kafka proposes for
//! the same seed. Kafka's own placer tests are the tests of this module.

use std::collections::BTreeMap;

use crate::jvm::{Table, hash_order, string_hash};

/// The pseudo-random source of `java.util.Random`: `next(bits)` and the
/// methods that Kafka's placer derives from it.
pub trait JavaRandom {
    /// `Random.next(bits)`.
    fn next(&mut self, bits: u32) -> i32;

    /// `Random.nextInt(bound)` for a positive `bound`.
    fn next_int(&mut self, bound: i32) -> i32 {
        let mut r = self.next(31);
        let m = bound - 1;
        if bound & m == 0 {
            let scaled = (i64::from(bound) * i64::from(r)) >> 31;
            return i32::try_from(scaled).unwrap_or(0);
        }
        let mut u = r;
        loop {
            r = u % bound;
            if u.wrapping_sub(r).wrapping_add(m) >= 0 {
                return r;
            }
            u = self.next(31);
        }
    }

    /// `Collections.shuffle(list, random)`.
    fn shuffle<T>(&mut self, list: &mut [T])
    where
        Self: Sized,
    {
        for i in (2..=list.len()).rev() {
            let bound = i32::try_from(i).unwrap_or(i32::MAX);
            let j = usize::try_from(self.next_int(bound)).unwrap_or(0);
            list.swap(i - 1, j);
        }
    }
}

/// `java.util.Random`: the 48-bit linear congruential generator.
#[derive(Debug, Clone)]
pub struct Lcg48 {
    seed: i64,
}

impl Lcg48 {
    const MULTIPLIER: i64 = 0x5_DEEC_E66D;
    const MASK: i64 = (1 << 48) - 1;

    /// `new Random(seed)`.
    #[must_use]
    pub const fn new(seed: i64) -> Self {
        Self {
            seed: (seed ^ Self::MULTIPLIER) & Self::MASK,
        }
    }

    /// A generator seeded from the clock, as `new Random()` is.
    #[must_use]
    pub fn from_time() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        Self::new(i64::from_ne_bytes(
            nanos.to_ne_bytes()[..8].try_into().unwrap_or([0; 8]),
        ))
    }
}

impl JavaRandom for Lcg48 {
    fn next(&mut self, bits: u32) -> i32 {
        self.seed = self.seed.wrapping_mul(Self::MULTIPLIER).wrapping_add(0xB) & Self::MASK;
        i32::from_ne_bytes(
            u32::try_from(self.seed >> (48 - bits))
                .unwrap_or(0)
                .to_ne_bytes(),
        )
    }
}

/// A broker that the placer may use, as Kafka's `UsableBroker`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsableBroker {
    pub id: i32,
    pub rack: Option<String>,
    pub fenced: bool,
}

/// `StripedReplicaPlacer.BrokerList`.
#[derive(Debug, Default)]
struct BrokerList {
    brokers: Vec<i32>,
    index: usize,
    offset: usize,
    epoch: usize,
}

impl BrokerList {
    fn initialize(&mut self, random: &mut impl JavaRandom) {
        if !self.brokers.is_empty() {
            self.brokers.sort_unstable();
            self.offset = random_index(random, self.brokers.len());
        }
    }

    fn next(&mut self, epoch: usize) -> Option<i32> {
        if self.brokers.is_empty() {
            return None;
        }
        if self.epoch != epoch {
            self.epoch = epoch;
            self.index = 0;
            self.offset = (self.offset + 1) % self.brokers.len();
        }
        if self.index >= self.brokers.len() {
            return None;
        }
        let broker = self.brokers[(self.index + self.offset) % self.brokers.len()];
        self.index += 1;
        Some(broker)
    }
}

fn random_index(random: &mut impl JavaRandom, len: usize) -> usize {
    usize::try_from(random.next_int(i32::try_from(len).unwrap_or(i32::MAX))).unwrap_or(0)
}

/// `StripedReplicaPlacer.Rack`.
#[derive(Debug, Default)]
struct Rack {
    fenced: BrokerList,
    unfenced: BrokerList,
}

impl Rack {
    fn next(&mut self, epoch: usize) -> Option<i32> {
        self.unfenced
            .next(epoch)
            .or_else(|| self.fenced.next(epoch))
    }
}

/// `StripedReplicaPlacer.RackList`.
pub struct RackList<'r, R> {
    random: &'r mut R,
    /// The racks in `HashMap` order, by rack name.
    racks: Vec<(Option<String>, Rack)>,
    /// The rack names sorted with the no-rack name first; indexes into
    /// `racks`.
    rack_names: Vec<usize>,
    total: usize,
    unfenced: usize,
    epoch: usize,
    offset: usize,
}

impl<'r, R: JavaRandom> RackList<'r, R> {
    /// Loads the brokers into racks and randomizes the start offsets.
    pub fn new(random: &'r mut R, brokers: &[UsableBroker]) -> Self {
        let mut inserted = Vec::<(Option<String>, Rack)>::new();
        let (mut total, mut unfenced) = (0, 0);
        for broker in brokers {
            let position = inserted
                .iter()
                .position(|(name, _)| *name == broker.rack)
                .unwrap_or_else(|| {
                    inserted.push((broker.rack.clone(), Rack::default()));
                    inserted.len() - 1
                });
            let rack = &mut inserted[position].1;
            if broker.fenced {
                rack.fenced.brokers.push(broker.id);
            } else {
                unfenced += 1;
                rack.unfenced.brokers.push(broker.id);
            }
            total += 1;
        }
        // `Optional.hashCode` is the value's hash, or 0 when empty.
        let racks = hash_order(inserted, Table::Default, |(name, _)| {
            name.as_deref().map_or(0, string_hash)
        });
        let mut list = Self {
            random,
            racks,
            rack_names: Vec::new(),
            total,
            unfenced,
            epoch: 0,
            offset: 0,
        };
        for (_, rack) in &mut list.racks {
            rack.fenced.initialize(list.random);
            rack.unfenced.initialize(list.random);
        }
        let mut names = (0..list.racks.len()).collect::<Vec<_>>();
        names.sort_by(|&a, &b| list.racks[a].0.cmp(&list.racks[b].0));
        list.rack_names = names;
        list.offset = if list.rack_names.is_empty() {
            0
        } else {
            random_index(list.random, list.rack_names.len())
        };
        list
    }

    fn shuffle(&mut self) {
        self.random.shuffle(&mut self.rack_names);
        for (_, rack) in &mut self.racks {
            self.random.shuffle(&mut rack.fenced.brokers);
            self.random.shuffle(&mut rack.unfenced.brokers);
        }
    }

    /// The replicas of the next partition.
    ///
    /// # Errors
    /// Returns Kafka's `InvalidReplicationFactorException` message.
    pub fn place(&mut self, replication_factor: i32) -> Result<Vec<i32>, String> {
        check_replication_factor(replication_factor, self.total, self.unfenced)?;
        if self.epoch == self.unfenced && self.unfenced > 1 {
            self.shuffle();
            self.epoch = 0;
        }
        if self.offset == self.rack_names.len() {
            self.offset = 0;
        }
        let mut brokers = Vec::new();
        let mut first_rack = self.offset;
        loop {
            let rack = self.rack_names[first_rack];
            if let Some(broker) = self.racks[rack].1.unfenced.next(self.epoch) {
                brokers.push(broker);
                break;
            }
            first_rack = (first_rack + 1) % self.rack_names.len();
        }
        let mut first_rack = Some(first_rack);
        let mut rack_index = self.offset;
        for _ in 1..replication_factor {
            let broker = loop {
                let mut result = None;
                if first_rack == Some(rack_index) {
                    first_rack = None;
                } else {
                    let rack = self.rack_names[rack_index];
                    result = self.racks[rack].1.next(self.epoch);
                }
                rack_index = (rack_index + 1) % self.rack_names.len();
                if let Some(broker) = result {
                    break broker;
                }
            };
            brokers.push(broker);
        }
        self.epoch += 1;
        self.offset += 1;
        Ok(brokers)
    }
}

fn check_positive(replication_factor: i32) -> Result<(), String> {
    if replication_factor <= 0 {
        return Err(format!(
            "Invalid replication factor {replication_factor}: the replication factor must be \
             positive."
        ));
    }
    Ok(())
}

fn check_enough_brokers(replication_factor: i32, total: usize) -> Result<(), String> {
    if usize::try_from(replication_factor).unwrap_or(usize::MAX) > total {
        return Err(format!(
            "The target replication factor of {replication_factor} cannot be reached because \
             only {total} broker(s) are registered or some brokers have all their log \
             directories cordoned."
        ));
    }
    Ok(())
}

fn check_unfenced(unfenced: usize) -> Result<(), String> {
    if unfenced == 0 {
        return Err(
            "All brokers are currently fenced, or have all their log directories cordoned.".into(),
        );
    }
    Ok(())
}

/// The checks of `RackList.place`, in its order.
fn check_replication_factor(
    replication_factor: i32,
    total: usize,
    unfenced: usize,
) -> Result<(), String> {
    check_positive(replication_factor)?;
    check_enough_brokers(replication_factor, total)?;
    check_unfenced(unfenced)
}

/// `StripedReplicaPlacer.place`: the replicas of `partitions` partitions.
///
/// # Errors
/// Returns Kafka's `InvalidReplicationFactorException` message.
pub fn place(
    random: &mut impl JavaRandom,
    partitions: usize,
    replication_factor: i32,
    brokers: &[UsableBroker],
) -> Result<Vec<Vec<i32>>, String> {
    let mut racks = RackList::new(random, brokers);
    check_positive(replication_factor)?;
    check_unfenced(racks.unfenced)?;
    check_enough_brokers(replication_factor, racks.total)?;
    (0..partitions)
        .map(|_| racks.place(replication_factor))
        .collect()
}

/// The proposal of `kafka-reassign-partitions --generate`: each topic keeps
/// its partition count and replication factor, and the placer spreads it
/// over `brokers`. Topics are placed in `HashMap` order, as Kafka places
/// them, because they share the one random source.
///
/// # Errors
/// Returns Kafka's `InvalidReplicationFactorException` message.
pub fn propose(
    random: &mut impl JavaRandom,
    current: &BTreeMap<(String, i32), Vec<i32>>,
    brokers: &[UsableBroker],
) -> Result<BTreeMap<(String, i32), Vec<i32>>, String> {
    let mut topics = BTreeMap::<&str, (usize, i32)>::new();
    for ((topic, _), replicas) in current {
        let entry = topics.entry(topic.as_str()).or_insert((0, 0));
        if entry.0 == 0 {
            entry.1 = i32::try_from(replicas.len()).unwrap_or(i32::MAX);
        }
        entry.0 += 1;
    }
    let names = topics.keys().copied().collect::<Vec<_>>();
    let mut proposed = BTreeMap::new();
    for topic in hash_order(names, Table::Default, |name| string_hash(name)) {
        let (partitions, replication_factor) = topics[topic];
        for (partition, replicas) in place(random, partitions, replication_factor, brokers)?
            .into_iter()
            .enumerate()
        {
            proposed.insert(
                (
                    topic.to_owned(),
                    i32::try_from(partition).unwrap_or(i32::MAX),
                ),
                replicas,
            );
        }
    }
    Ok(proposed)
}

#[cfg(test)]
mod tests;
