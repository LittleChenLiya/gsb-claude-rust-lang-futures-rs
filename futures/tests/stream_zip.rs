//! Tests for the explicit EOF-policy receipt of `StreamExt::zip`
//! (`Zip::into_remainder`).
//!
//! Run locally with:
//!   cargo test -p futures --test stream_zip
//!   cargo test -p futures-util --doc stream::stream::Zip
//!
//! All streams here are fully controllable fixtures: every `poll_next`
//! returns a scripted response and counts how often it was polled, so the
//! tests can assert exactly which side was polled and that a completed side
//! is never polled again.

use std::cell::Cell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt, Zip, ZipSideState};
use futures::task::noop_waker_ref;

/// A controllable stream: each poll pops the next scripted response and
/// bumps a shared poll counter.
struct Scripted<T> {
    script: VecDeque<Poll<Option<T>>>,
    polls: Rc<Cell<usize>>,
}

impl<T> Scripted<T> {
    fn new(script: Vec<Poll<Option<T>>>) -> (Self, Rc<Cell<usize>>) {
        let polls = Rc::new(Cell::new(0));
        (Self { script: script.into(), polls: Rc::clone(&polls) }, polls)
    }
}

impl<T> Unpin for Scripted<T> {}

impl<T> Stream for Scripted<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.polls.set(self.polls.get() + 1);
        self.script.pop_front().expect("scripted stream polled past its script")
    }
}

fn poll_zip<St1: Stream + Unpin, St2: Stream + Unpin>(
    zip: &mut Zip<St1, St2>,
) -> Poll<Option<(St1::Item, St2::Item)>> {
    zip.poll_next_unpin(&mut Context::from_waker(noop_waker_ref()))
}

/// Left: 2 items then EOF. Right: 3 items. Two pairs are produced; the
/// right side's third item was already prefetched when the left side ended
/// and must be recoverable from the receipt instead of being silently
/// dropped.
#[test]
fn short_stream_eof_receipt_recovers_prefetched_item() {
    let (left, left_polls) = Scripted::new(vec![
        Poll::Ready(Some(1)),
        Poll::Ready(Some(2)),
        Poll::Ready(None),
        Poll::Ready(None),
    ]);
    let (right, right_polls) = Scripted::new(vec![
        Poll::Ready(Some('a')),
        Poll::Ready(Some('b')),
        Poll::Ready(Some('c')),
        Poll::Ready(None),
    ]);

    let mut zip = left.zip(right);

    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((1, 'a'))));
    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((2, 'b'))));
    // Left side hits EOF; the right side's third item is prefetched in the
    // same poll and buffered inside the `Zip`.
    assert_eq!(poll_zip(&mut zip), Poll::Ready(None));
    assert_eq!(left_polls.get(), 3);
    assert_eq!(right_polls.get(), 3);

    // Polling the terminated zip again (e.g. while cancelling a select!)
    // must not poll the completed left side again, and must not re-poll the
    // right side while its prefetched item is still buffered.
    assert_eq!(poll_zip(&mut zip), Poll::Ready(None));
    assert_eq!(left_polls.get(), 3);
    assert_eq!(right_polls.get(), 3);

    // The receipt itself never polls either stream.
    let receipt = zip.into_remainder();
    assert_eq!(left_polls.get(), 3);
    assert_eq!(right_polls.get(), 3);

    // The leftover third item of the right stream is handed back exactly
    // once (it is not re-fetched by polling the right stream again).
    assert_eq!(receipt.queued1, None);
    assert_eq!(receipt.queued2, Some('c'));
    assert_eq!(receipt.state1, ZipSideState::Done);
    assert_eq!(receipt.state2, ZipSideState::Pending);
    assert!(!receipt.is_fully_drained());

    // The remaining handles are still usable.
    let (mut left_rest, mut right_rest) = receipt.into_inner();
    let mut cx = Context::from_waker(noop_waker_ref());
    assert_eq!(left_rest.poll_next_unpin(&mut cx), Poll::Ready(None));
    assert_eq!(right_rest.poll_next_unpin(&mut cx), Poll::Ready(None));
    assert_eq!(left_polls.get(), 4);
    assert_eq!(right_polls.get(), 4);
}

/// A side that returned `Poll::Pending` is live, not exhausted: the receipt
/// must report it as `ZipSideState::Pending` and must not misjudge it as
/// EOF.
#[test]
fn pending_side_is_not_misjudged_as_eof() {
    use futures::stream::FusedStream;

    let (left, left_polls) = Scripted::new(vec![
        Poll::Ready(Some(1)),
        Poll::Pending,
        Poll::Ready(Some(2)),
        Poll::Ready(None),
    ]);
    let (right, right_polls) =
        Scripted::new(vec![Poll::Ready(Some('a')), Poll::Ready(Some('b')), Poll::Ready(None)]);

    let mut zip = left.zip(right);

    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((1, 'a'))));
    // Left side pends; the right side's item is buffered. The zipped stream
    // is pending, not finished.
    assert_eq!(poll_zip(&mut zip), Poll::Pending);
    assert!(!zip.is_terminated());

    // Cancelling here yields a receipt without polling either side again.
    let receipt = zip.into_remainder();
    assert_eq!(left_polls.get(), 2);
    assert_eq!(right_polls.get(), 2);

    // The pending left side is reported as live, not as EOF.
    assert_eq!(receipt.state1, ZipSideState::Pending);
    assert_eq!(receipt.state2, ZipSideState::Pending);
    assert_eq!(receipt.queued1, None);
    assert_eq!(receipt.queued2, Some('b'));

    // The left handle still has its remaining items.
    let (mut left_rest, _right_rest) = receipt.into_inner();
    let mut cx = Context::from_waker(noop_waker_ref());
    assert_eq!(left_rest.poll_next_unpin(&mut cx), Poll::Ready(Some(2)));
    assert_eq!(left_rest.poll_next_unpin(&mut cx), Poll::Ready(None));
}

/// Error items are not termination signals: an `Err` prefetched from the
/// right side stays attributed to the right side of the receipt.
#[test]
fn error_item_stays_on_producing_side() {
    let (left, _) =
        Scripted::new(vec![Poll::Ready(Some(Ok::<_, &'static str>(1))), Poll::Ready(None)]);
    let (right, _) = Scripted::new(vec![
        Poll::Ready(Some(Ok::<_, &'static str>(2))),
        Poll::Ready(Some(Err("boom"))),
        Poll::Ready(None),
    ]);

    let mut zip = left.zip(right);

    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((Ok(1), Ok(2)))));
    // Left side ends while the right side's error item is prefetched.
    assert_eq!(poll_zip(&mut zip), Poll::Ready(None));

    let receipt = zip.into_remainder();
    // The error stays on the right side that produced it; the left side
    // holds nothing and is properly reported as done.
    assert_eq!(receipt.queued1, None);
    assert_eq!(receipt.queued2, Some(Err("boom")));
    assert_eq!(receipt.state1, ZipSideState::Done);
    assert_eq!(receipt.state2, ZipSideState::Pending);
}

/// The default policy is unchanged: `zip` still truncates on the shorter
/// stream, and equal-length streams drain fully.
#[test]
fn default_policy_and_fully_drained_receipt() {
    let (left, _) =
        Scripted::new(vec![Poll::Ready(Some(1)), Poll::Ready(Some(2)), Poll::Ready(None)]);
    let (right, _) =
        Scripted::new(vec![Poll::Ready(Some('a')), Poll::Ready(Some('b')), Poll::Ready(None)]);

    let mut zip = left.zip(right);
    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((1, 'a'))));
    assert_eq!(poll_zip(&mut zip), Poll::Ready(Some((2, 'b'))));
    assert_eq!(poll_zip(&mut zip), Poll::Ready(None));

    let receipt = zip.into_remainder();
    assert_eq!(receipt.state1, ZipSideState::Done);
    assert_eq!(receipt.state2, ZipSideState::Done);
    assert_eq!(receipt.queued1, None);
    assert_eq!(receipt.queued2, None);
    assert!(receipt.is_fully_drained());
}
