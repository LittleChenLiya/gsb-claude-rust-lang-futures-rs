use core::{cmp, pin::Pin};

use futures_core::{
    stream::{FusedStream, Stream},
    task::{Context, Poll},
};
use pin_project_lite::pin_project;

use crate::stream::{Fuse, StreamExt};

pin_project! {
    /// Stream for the [`zip`](super::StreamExt::zip) method.
    ///
    /// # EOF policy
    ///
    /// `Zip` stops as soon as either side signals EOF (the default
    /// "truncate-on-shortest" policy). Because `Zip` polls both sides on every
    /// poll, the side that is still live may already have yielded an item that
    /// was prefetched but never paired. By default that item is silently
    /// dropped when the `Zip` is dropped.
    ///
    /// To make the EOF handling explicit, call [`Zip::into_remainder`] once
    /// the zipped stream has ended (or when cancelling it). It returns a
    /// [`ZipRemainder`] receipt that hands back both underlying streams, any
    /// prefetched-but-unpaired items, and the per-side EOF/pending state, so
    /// no item is discarded without the caller observing it.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct Zip<St1: Stream, St2: Stream> {
        #[pin]
        stream1: Fuse<St1>,
        #[pin]
        stream2: Fuse<St2>,
        queued1: Option<St1::Item>,
        queued2: Option<St2::Item>,
    }
}

/// Per-side state reported by [`ZipRemainder`].
///
/// The two variants deliberately keep "this side is exhausted" apart from
/// "this side is still live": a side that merely returned
/// [`Poll::Pending`](core::task::Poll::Pending) (or was never polled to
/// completion) is reported as [`Pending`](ZipSideState::Pending) and is never
/// misreported as EOF. If the side's items are `Result`s, an error item is
/// not a termination signal either; it stays attributed to the side that
/// produced it inside [`ZipRemainder::queued1`]/[`ZipRemainder::queued2`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ZipSideState {
    /// The side returned `Poll::Ready(None)`: it is exhausted. `Zip` (via
    /// [`Fuse`]) guarantees that an exhausted side is never polled again,
    /// and [`Zip::into_remainder`] does not poll it either.
    Done,
    /// The side has not signaled EOF. It may be temporarily pending or may
    /// still hold items; the returned handle can be polled further.
    Pending,
}

/// Explicit EOF-policy receipt for [`Zip`], returned by
/// [`Zip::into_remainder`].
///
/// It records, at the moment the zipped stream ended (or was cancelled):
///
/// * the remaining handles of both underlying streams
///   ([`stream1`](Self::stream1), [`stream2`](Self::stream2)),
/// * the items that were prefetched from one side but never paired
///   ([`queued1`](Self::queued1), [`queued2`](Self::queued2)) — these are the
///   items the default truncate policy would silently drop,
/// * the per-side EOF/pending state
///   ([`state1`](Self::state1), [`state2`](Self::state2)).
///
/// Building this receipt never polls either stream, so cancelling a `Zip`
/// this way does not poll an already-completed side again.
#[derive(Debug)]
#[must_use = "receipts do nothing unless inspected"]
pub struct ZipRemainder<St1: Stream, St2: Stream> {
    /// Remaining handle of the first (left) stream. If
    /// [`state1`](Self::state1) is [`ZipSideState::Pending`] it may still
    /// yield items; if it is [`ZipSideState::Done`] it is exhausted.
    pub stream1: St1,
    /// Remaining handle of the second (right) stream. See [`stream1`](Self::stream1).
    pub stream2: St2,
    /// Item prefetched from the first stream but never paired. If the item is
    /// a `Result`, an `Err` here is an error produced by the *left* side.
    pub queued1: Option<St1::Item>,
    /// Item prefetched from the second stream but never paired. If the item
    /// is a `Result`, an `Err` here is an error produced by the *right* side.
    pub queued2: Option<St2::Item>,
    /// EOF/pending state of the first stream at receipt time.
    pub state1: ZipSideState,
    /// EOF/pending state of the second stream at receipt time.
    pub state2: ZipSideState,
}

impl<St1: Stream, St2: Stream> ZipRemainder<St1, St2> {
    /// Consumes this receipt, returning only the two underlying streams and
    /// discarding any buffered items.
    pub fn into_inner(self) -> (St1, St2) {
        (self.stream1, self.stream2)
    }

    /// Returns `true` if both sides had signaled EOF, i.e. the zipped stream
    /// ended because it was fully drained rather than truncated early.
    pub fn is_fully_drained(&self) -> bool {
        self.state1 == ZipSideState::Done
            && self.state2 == ZipSideState::Done
            && self.queued1.is_none()
            && self.queued2.is_none()
    }
}

impl<St1: Stream, St2: Stream> Zip<St1, St2> {
    pub(super) fn new(stream1: St1, stream2: St2) -> Self {
        Self { stream1: stream1.fuse(), stream2: stream2.fuse(), queued1: None, queued2: None }
    }

    /// Acquires a reference to the underlying streams that this combinator is
    /// pulling from.
    pub fn get_ref(&self) -> (&St1, &St2) {
        (self.stream1.get_ref(), self.stream2.get_ref())
    }

    /// Acquires a mutable reference to the underlying streams that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// stream which may otherwise confuse this combinator.
    pub fn get_mut(&mut self) -> (&mut St1, &mut St2) {
        (self.stream1.get_mut(), self.stream2.get_mut())
    }

    /// Acquires a pinned mutable reference to the underlying streams that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// stream which may otherwise confuse this combinator.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> (Pin<&mut St1>, Pin<&mut St2>) {
        let this = self.project();
        (this.stream1.get_pin_mut(), this.stream2.get_pin_mut())
    }

    /// Consumes this combinator, returning the underlying streams.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    /// In particular any prefetched-but-unpaired item is silently dropped;
    /// use [`into_remainder`](Self::into_remainder) if that item must be
    /// recovered.
    pub fn into_inner(self) -> (St1, St2) {
        (self.stream1.into_inner(), self.stream2.into_inner())
    }

    /// Consumes this combinator, returning an explicit EOF-policy
    /// [`ZipRemainder`] receipt: the underlying streams, any
    /// prefetched-but-unpaired items, and the per-side EOF/pending state.
    ///
    /// This is the opt-in alternative to the default truncate-on-shortest
    /// behavior: instead of silently dropping an item that the longer side
    /// had already produced, the receipt hands it back together with the
    /// remaining stream handles.
    ///
    /// This function never polls either stream. In particular, cancelling a
    /// `Zip` by converting it into a receipt does not poll a side that has
    /// already completed, and a side that is merely pending is reported as
    /// [`ZipSideState::Pending`], not misjudged as EOF.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::stream::{self, StreamExt, ZipSideState};
    ///
    /// // Left stream: 2 items. Right stream: 3 items.
    /// let stream1 = stream::iter(1..=2);
    /// let stream2 = stream::iter(5..=7);
    ///
    /// let mut zip = stream1.zip(stream2);
    /// assert_eq!(zip.next().await, Some((1, 5)));
    /// assert_eq!(zip.next().await, Some((2, 6)));
    /// // Left side ended, so the zipped stream ends...
    /// assert_eq!(zip.next().await, None);
    ///
    /// // ...but the right side's third item was already prefetched. The
    /// // receipt hands it back instead of dropping it silently.
    /// let receipt = zip.into_remainder();
    /// assert_eq!(receipt.state1, ZipSideState::Done);
    /// assert_eq!(receipt.state2, ZipSideState::Pending);
    /// assert_eq!(receipt.queued1, None);
    /// assert_eq!(receipt.queued2, Some(7));
    /// # });
    /// ```
    pub fn into_remainder(self) -> ZipRemainder<St1, St2> {
        fn side_state<St: Stream>(stream: &Fuse<St>) -> ZipSideState {
            if stream.is_done() { ZipSideState::Done } else { ZipSideState::Pending }
        }
        ZipRemainder {
            state1: side_state(&self.stream1),
            state2: side_state(&self.stream2),
            stream1: self.stream1.into_inner(),
            stream2: self.stream2.into_inner(),
            queued1: self.queued1,
            queued2: self.queued2,
        }
    }
}

impl<St1, St2> FusedStream for Zip<St1, St2>
where
    St1: Stream,
    St2: Stream,
{
    fn is_terminated(&self) -> bool {
        self.stream1.is_terminated() && self.stream2.is_terminated()
    }
}

impl<St1, St2> Stream for Zip<St1, St2>
where
    St1: Stream,
    St2: Stream,
{
    type Item = (St1::Item, St2::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.queued1.is_none() {
            match this.stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item1)) => *this.queued1 = Some(item1),
                Poll::Ready(None) | Poll::Pending => {}
            }
        }
        if this.queued2.is_none() {
            match this.stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item2)) => *this.queued2 = Some(item2),
                Poll::Ready(None) | Poll::Pending => {}
            }
        }

        if this.queued1.is_some() && this.queued2.is_some() {
            let pair = (this.queued1.take().unwrap(), this.queued2.take().unwrap());
            Poll::Ready(Some(pair))
        } else if this.stream1.is_done() || this.stream2.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queued1_len = usize::from(self.queued1.is_some());
        let queued2_len = usize::from(self.queued2.is_some());
        let (stream1_lower, stream1_upper) = self.stream1.size_hint();
        let (stream2_lower, stream2_upper) = self.stream2.size_hint();

        let stream1_lower = stream1_lower.saturating_add(queued1_len);
        let stream2_lower = stream2_lower.saturating_add(queued2_len);

        let lower = cmp::min(stream1_lower, stream2_lower);

        let upper = match (stream1_upper, stream2_upper) {
            (Some(x), Some(y)) => {
                let x = x.saturating_add(queued1_len);
                let y = y.saturating_add(queued2_len);
                Some(cmp::min(x, y))
            }
            (Some(x), None) => x.checked_add(queued1_len),
            (None, Some(y)) => y.checked_add(queued2_len),
            (None, None) => None,
        };

        (lower, upper)
    }
}
