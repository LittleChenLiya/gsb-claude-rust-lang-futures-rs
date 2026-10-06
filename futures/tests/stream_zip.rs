//! Tests for the explicit EOF receipt of `StreamExt::zip`
//! (`Zip::into_remainder`, `ZipRemainder`, `ZipSide`).
//!
//! Run locally with:
//!
//! ```sh
//! cargo test -p futures --test stream_zip
//! cargo test -p futures-util --doc zip
//! ```

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::stream::{self, Stream, StreamExt, ZipSide};

/// A fully controlled stream: every poll pops the next scripted step, so
/// `Pending` and EOF points are deterministic. It panics if it is polled
/// again after reporting EOF, which makes any re-poll of a completed
/// stream fail loudly.
struct Controlled<Item> {
    steps: VecDeque<Poll<Option<Item>>>,
    polls: Arc<Mutex<usize>>,
    done: bool,
}

impl<Item> Controlled<Item> {
    fn new(steps: Vec<Poll<Option<Item>>>) -> (Self, Arc<Mutex<usize>>) {
        let polls = Arc::new(Mutex::new(0));
        (Self { steps: steps.into(), polls: Arc::clone(&polls), done: false }, polls)
    }
}

impl<Item> Unpin for Controlled<Item> {}

impl<Item> Stream for Controlled<Item> {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(!self.done, "stream polled again after it reported EOF");
        *self.polls.lock().unwrap() += 1;
        let step = self.steps.pop_front().expect("stream polled more times than scripted");
        match &step {
            Poll::Ready(None) => self.done = true,
            Poll::Pending => cx.waker().wake_by_ref(), // immediately re-pollable
            Poll::Ready(Some(_)) => {}
        }
        step
    }
}

fn polls(counter: &Arc<Mutex<usize>>) -> usize {
    *counter.lock().unwrap()
}

/// Left stream: 2 items. Right stream: 3 items. The zip yields 2 pairs and
/// ends; the prefetched third item of the right stream is recoverable from
/// the receipt, and neither stream is polled again after completion.
#[test]
fn zip_remainder_recovers_prefetched_item() {
    let (left, left_polls) =
        Controlled::new(vec![Poll::Ready(Some(1)), Poll::Ready(Some(2)), Poll::Ready(None)]);
    let (right, right_polls) = Controlled::new(vec![
        Poll::Ready(Some('a')),
        Poll::Ready(Some('b')),
        Poll::Ready(Some('c')),
        Poll::Ready(None),
    ]);

    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((1, 'a')));
    assert_eq!(block_on(zipped.next()), Some((2, 'b')));
    // Default behavior is unchanged: the zip ends when the shorter side ends.
    assert_eq!(block_on(zipped.next()), None);

    let receipt = zipped.into_remainder();
    assert!(!receipt.both_eof());
    assert!(receipt.left.is_eof());
    assert_eq!(receipt.left.buffered(), None);
    assert!(receipt.right.is_open());
    // The third right item was prefetched but never paired; the receipt
    // hands it back instead of silently dropping it.
    assert_eq!(receipt.right.buffered(), Some(&'c'));

    let (stream, buffered) = receipt.right.into_parts();
    assert_eq!(buffered, Some('c'));
    // The remaining handle still yields the rest of the right stream.
    let mut stream = stream.expect("right stream is still open");
    assert_eq!(block_on(stream.next()), None);

    // Left was polled exactly 3 times (2 items + EOF) and never again after
    // completing; right exactly 4 times (3 items + EOF).
    assert_eq!(polls(&left_polls), 3);
    assert_eq!(polls(&right_polls), 4);
}

/// A side that returns `Poll::Pending` is still open; the zip must wait for
/// it instead of declaring EOF, and the receipt must report it as `Open`.
#[test]
fn zip_pending_side_is_not_eof() {
    let (left, left_polls) = Controlled::new(vec![
        Poll::Ready(Some(1)),
        Poll::Pending, // left stalls here; not EOF
        Poll::Ready(Some(2)),
        Poll::Ready(None),
    ]);
    let (right, _) =
        Controlled::new(vec![Poll::Ready(Some('a')), Poll::Ready(Some('b')), Poll::Ready(None)]);

    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((1, 'a')));
    // Left pends before its second item: the zip pends too, then pairs
    // the item once left wakes up -- no false EOF in between.
    assert_eq!(block_on(zipped.next()), Some((2, 'b')));
    assert_eq!(block_on(zipped.next()), None);

    let receipt = zipped.into_remainder();
    assert!(receipt.both_eof());
    assert_eq!(polls(&left_polls), 4);
}

/// When the right stream ends while the left stream is merely pending, the
/// zip ends (right is genuinely at EOF), but the receipt keeps the left
/// side `Open` and hands back a still-usable stream handle.
#[test]
fn zip_receipt_keeps_pending_side_open_when_other_side_ends() {
    let (left, left_polls) = Controlled::new(vec![
        Poll::Ready(Some(1)),
        Poll::Pending,
        Poll::Ready(Some(2)),
        Poll::Ready(None),
    ]);
    let (right, _) = Controlled::new(vec![Poll::Ready(Some('a')), Poll::Ready(None)]);

    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((1, 'a')));
    assert_eq!(block_on(zipped.next()), None); // right reached EOF

    let receipt = zipped.into_remainder();
    assert!(receipt.right.is_eof());
    // Left only ever pended; it must not be reported as EOF and must not
    // hold a phantom buffered item.
    assert!(receipt.left.is_open());
    assert_eq!(receipt.left.buffered(), None);

    let (stream, buffered) = receipt.left.into_parts();
    assert_eq!(buffered, None);
    let mut stream = stream.expect("left stream is still open");
    assert_eq!(block_on(stream.next()), Some(2));
    assert_eq!(block_on(stream.next()), None);
    assert_eq!(polls(&left_polls), 4);
}

/// When zipping try-streams, a prefetched `Err` stays attributed to the
/// side it was polled from.
#[test]
fn zip_remainder_keeps_error_on_its_side() {
    // Error prefetched from the right stream stays on the right.
    let left = stream::iter(vec![Ok::<_, &'static str>(1)]);
    let right = stream::iter(vec![Ok('a'), Err("right-boom")]);
    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((Ok(1), Ok('a'))));
    assert_eq!(block_on(zipped.next()), None); // left ended

    let receipt = zipped.into_remainder();
    assert!(receipt.left.is_eof());
    assert_eq!(receipt.left.buffered(), None);
    match receipt.right {
        ZipSide::Open { buffered, .. } => assert_eq!(buffered, Some(Err("right-boom"))),
        ZipSide::Eof => panic!("buffered right-side error was silently dropped"),
    }

    // Mirror image: an error prefetched from the left stream stays left.
    let left = stream::iter(vec![Ok::<_, &'static str>(1), Err("left-boom")]);
    let right = stream::iter(vec![Ok::<_, &'static str>('a')]);
    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((Ok(1), Ok('a'))));
    assert_eq!(block_on(zipped.next()), None); // right ended

    let receipt = zipped.into_remainder();
    assert!(receipt.right.is_eof());
    match receipt.left {
        ZipSide::Open { buffered, .. } => assert_eq!(buffered, Some(Err("left-boom"))),
        ZipSide::Eof => panic!("buffered left-side error was silently dropped"),
    }
}

/// Cancelling the zip (via `into_remainder` or by dropping it) must never
/// poll a stream that has already completed. `Controlled` panics on any
/// poll after EOF, so a re-poll would fail the test loudly.
#[test]
fn zip_cancellation_never_repolls_completed_stream() {
    let (left, left_polls) = Controlled::new(vec![Poll::Ready(Some(1)), Poll::Ready(None)]);
    let (right, right_polls) =
        Controlled::new(vec![Poll::Ready(Some('a')), Poll::Ready(Some('b')), Poll::Ready(None)]);

    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((1, 'a')));
    assert_eq!(block_on(zipped.next()), None); // left reached EOF
    assert_eq!(polls(&left_polls), 2);
    assert_eq!(polls(&right_polls), 2);

    // Taking the receipt polls nothing...
    let receipt = zipped.into_remainder();
    assert_eq!(polls(&left_polls), 2);
    assert_eq!(polls(&right_polls), 2);
    assert!(receipt.left.is_eof());
    assert_eq!(receipt.right.buffered(), Some(&'b'));

    // ...and dropping the receipt (and the completed left stream with it)
    // polls nothing either.
    drop(receipt);
    assert_eq!(polls(&left_polls), 2);
    assert_eq!(polls(&right_polls), 2);

    // Dropping the zip outright is equally quiet.
    let (left, left_polls) = Controlled::new(vec![Poll::Ready(Some(1)), Poll::Ready(None)]);
    let (right, right_polls) = Controlled::new(vec![Poll::Ready(Some('a')), Poll::Ready(None)]);
    let mut zipped = left.zip(right);
    assert_eq!(block_on(zipped.next()), Some((1, 'a')));
    drop(zipped);
    assert_eq!(polls(&left_polls), 1);
    assert_eq!(polls(&right_polls), 1);
}
