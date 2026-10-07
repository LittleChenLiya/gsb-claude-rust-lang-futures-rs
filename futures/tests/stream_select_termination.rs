//! Termination masking and per-side state queries for the two-way select
//! streams (`select` / `select_with_strategy`).
//!
//! All fixtures are manually driven, poll-counting in-memory streams: every
//! poll pops one scripted response and bumps a shared counter. No timers and
//! no real time are involved.

use std::{
    cell::Cell,
    collections::VecDeque,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use futures::{
    executor::block_on,
    stream::{self, FusedStream, SelectState, Stream, StreamExt},
};
use futures_test::task::noop_context;

/// A manually controlled stream. Each `poll_next` call pops exactly one
/// scripted response (defaulting to `Pending` once the script is exhausted)
/// and increments a shared poll counter.
struct ManualStream {
    script: VecDeque<Poll<Option<u32>>>,
    polls: Rc<Cell<usize>>,
}

impl ManualStream {
    fn new(script: Vec<Poll<Option<u32>>>) -> (Self, Rc<Cell<usize>>) {
        let polls = Rc::new(Cell::new(0));
        (Self { script: script.into(), polls: polls.clone() }, polls)
    }
}

impl Stream for ManualStream {
    type Item = u32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u32>> {
        self.polls.set(self.polls.get() + 1);
        self.script.pop_front().unwrap_or(Poll::Pending)
    }
}

fn ready(item: u32) -> Poll<Option<u32>> {
    Poll::Ready(Some(item))
}

fn done() -> Poll<Option<u32>> {
    Poll::Ready(None)
}

/// One side terminates early: it is never polled again, the other side keeps
/// producing, and once both are done repeated polls stably return `None`.
#[test]
fn left_ends_early_right_continues_then_stable_none() {
    let mut cx = noop_context();

    // Left yields one item, then terminates. Right yields three items, then
    // terminates.
    let (left, left_polls) = ManualStream::new(vec![ready(1), done()]);
    let (right, right_polls) = ManualStream::new(vec![ready(2), ready(4), ready(6), done()]);

    let mut sel = stream::select(left, right);
    assert_eq!(sel.state(), SelectState::Running);
    assert!(!sel.is_terminated());

    // Round-robin starts on the left.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(1)));
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(2)));
    assert_eq!(sel.state(), SelectState::Running);

    // This poll observes `None` from the left and falls through to the right
    // within the same poll, so the left termination is masked.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(4)));
    assert_eq!(sel.state(), SelectState::LeftFinished);
    assert!(sel.is_left_terminated());
    assert!(!sel.is_right_terminated());
    assert!(!sel.is_terminated());
    let left_polls_at_end = left_polls.get();

    // The right side keeps producing; the terminated left side is not polled.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(6)));
    assert_eq!(left_polls.get(), left_polls_at_end, "terminated left side polled again");

    // Right terminates now.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(None));
    assert_eq!(sel.state(), SelectState::BothFinished);
    assert!(sel.is_left_terminated());
    assert!(sel.is_right_terminated());
    assert!(sel.is_terminated());
    let (left_total, right_total) = (left_polls.get(), right_polls.get());

    // Repeated polls stably return `None` without touching either side.
    for _ in 0..3 {
        assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(None));
    }
    assert_eq!(left_polls.get(), left_total, "left side polled after BothFinished");
    assert_eq!(right_polls.get(), right_total, "right side polled after BothFinished");
    assert_eq!(sel.state(), SelectState::BothFinished);
}

/// Mirror image: the right side terminates early and is masked, the left
/// side keeps producing. Uses `select_with_strategy` to show the state query
/// is available there as well.
#[test]
fn right_ends_early_left_continues_then_stable_none() {
    let mut cx = noop_context();

    let (left, left_polls) =
        ManualStream::new(vec![ready(1), Poll::Pending, ready(3), Poll::Pending, ready(5), done()]);
    let (right, right_polls) = ManualStream::new(vec![ready(2), done()]);

    fn prio_left(_: &mut ()) -> stream::PollNext {
        stream::PollNext::Left
    }
    let mut sel = stream::select_with_strategy(left, right, prio_left);
    assert_eq!(sel.state(), SelectState::Running);

    // prio_left: left is polled first every time.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(1)));
    // Left is pending on its second poll, so the right side is polled and
    // yields its only item.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(2)));
    assert_eq!(sel.state(), SelectState::Running);

    // Left yields again; right stays untouched while left produces.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(3)));
    // Left pending -> right polled -> right terminates (masked); the poll
    // reports Pending because the left side was pending.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Pending);
    assert_eq!(sel.state(), SelectState::RightFinished);
    assert!(sel.is_right_terminated());
    assert!(!sel.is_left_terminated());
    let right_polls_at_end = right_polls.get();

    // The left side keeps producing; the terminated right side is not polled.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(Some(5)));
    assert_eq!(right_polls.get(), right_polls_at_end, "terminated right side polled again");

    // Left terminates; both are done now.
    assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(None));
    assert_eq!(sel.state(), SelectState::BothFinished);
    assert!(sel.is_terminated());
    let (left_total, right_total) = (left_polls.get(), right_polls.get());

    // Repeated polls stably return `None` without touching either side.
    for _ in 0..3 {
        assert_eq!(Pin::new(&mut sel).poll_next(&mut cx), Poll::Ready(None));
    }
    assert_eq!(left_polls.get(), left_total, "left side polled after BothFinished");
    assert_eq!(right_polls.get(), right_total, "right side polled after BothFinished");
}

/// While both sides are live the fair round-robin selection is unchanged,
/// and the state query reports `Running` throughout.
#[test]
fn fair_selection_unchanged_while_both_sides_live() {
    let mut cx = noop_context();

    let (left, _) = ManualStream::new(vec![ready(1), ready(3), ready(5), done()]);
    let (right, _) = ManualStream::new(vec![ready(2), ready(4), ready(6), done()]);

    let mut sel = stream::select(left, right);
    let mut out = Vec::new();
    while let Poll::Ready(Some(item)) = Pin::new(&mut sel).poll_next(&mut cx) {
        out.push(item);
        if out.len() < 6 {
            assert_eq!(sel.state(), SelectState::Running);
        }
    }
    assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(sel.state(), SelectState::BothFinished);
}

/// The original interface keeps working unchanged when the new state query
/// is never used.
#[test]
fn original_interface_usable_without_state_query() {
    block_on(async {
        let a = stream::iter(vec![1, 2, 3]);
        let b = stream::iter(vec![4, 5, 6]);
        let vec = stream::select(a, b).collect::<Vec<_>>().await;
        assert_eq!(vec, vec![1, 4, 2, 5, 3, 6]);

        let a = stream::iter(vec![1, 2, 3]);
        let b = stream::iter(vec![4, 5]);
        let vec = stream::select(a, b).collect::<Vec<_>>().await;
        assert_eq!(vec, vec![1, 4, 2, 5, 3]);

        // `select_next_some` / `FusedStream` behave as before.
        let mut sel = stream::select(stream::iter(vec![1, 2]), stream::iter(vec![3]));
        assert_eq!(sel.select_next_some().await, 1);
        assert_eq!(sel.select_next_some().await, 3);
        assert_eq!(sel.select_next_some().await, 2);
        assert_eq!(sel.next().await, None);
        assert!(sel.is_terminated());
    });
}
