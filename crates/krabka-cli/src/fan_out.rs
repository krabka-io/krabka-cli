//! Runs a batch of requests with a bounded number in flight.
//!
//! A command that asks every broker, or every group, must not open one
//! connection per target at once. [`bounded`] starts at most `limit` of the
//! futures, starts the next as each finishes, and returns the outputs in the
//! order of the inputs. The futures run on the calling task, so they need not
//! be `Send` or `'static` and may borrow the command's arguments.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

/// Runs `start(item)` for every item with at most `limit` futures in flight,
/// and returns the outputs in the order of `items`. A `limit` of 0 is treated
/// as 1.
pub async fn bounded<T, F>(
    items: Vec<T>,
    limit: usize,
    mut start: impl FnMut(T) -> F,
) -> Vec<F::Output>
where
    F: Future,
{
    let limit = limit.max(1);
    let mut outputs = std::iter::repeat_with(|| None)
        .take(items.len())
        .collect::<Vec<Option<F::Output>>>();
    let mut pending = items.into_iter().enumerate();
    let mut running: Vec<(usize, Pin<Box<F>>)> = Vec::new();
    loop {
        while running.len() < limit {
            let Some((index, item)) = pending.next() else {
                break;
            };
            running.push((index, Box::pin(start(item))));
        }
        if running.is_empty() {
            break;
        }
        let (slot, output) = poll_fn(|cx| {
            for (slot, (_, future)) in running.iter_mut().enumerate() {
                if let Poll::Ready(output) = future.as_mut().poll(cx) {
                    return Poll::Ready((slot, output));
                }
            }
            Poll::Pending
        })
        .await;
        let (index, _) = running.swap_remove(slot);
        outputs[index] = Some(output);
    }
    outputs.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use assert2::check;

    use super::*;

    #[tokio::test]
    async fn at_most_limit_futures_run_and_outputs_keep_input_order() {
        for (items, limit, expected_peak) in
            [(10, 3, 3), (2, 8, 2), (5, 1, 1), (4, 0, 1), (0, 4, 0)]
        {
            let in_flight = Cell::new(0_usize);
            let peak = Cell::new(0_usize);
            let outputs = bounded((0..items).collect(), limit, |item: u64| {
                let (in_flight, peak) = (&in_flight, &peak);
                async move {
                    in_flight.set(in_flight.get() + 1);
                    peak.set(peak.get().max(in_flight.get()));
                    // Later items finish first, so completion order differs
                    // from input order.
                    tokio::time::sleep(Duration::from_millis(items - item)).await;
                    in_flight.set(in_flight.get() - 1);
                    item * 10
                }
            })
            .await;
            check!(outputs == (0..items).map(|item| item * 10).collect::<Vec<_>>());
            check!(peak.get() == expected_peak, "items={items} limit={limit}");
        }
    }
}
