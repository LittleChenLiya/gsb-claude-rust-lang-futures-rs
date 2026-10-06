use core::{cmp, fmt, pin::Pin};

use futures_core::{
    stream::{FusedStream, Stream},
    task::{Context, Poll},
};
use pin_project_lite::pin_project;

use crate::stream::{Fuse, StreamExt};

pin_project! {
    /// Stream for the [`zip`](super::StreamExt::zip) method.
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
    /// In particular, an item that was already polled from one stream but
    /// not yet paired with an item from the other stream is silently
    /// discarded. Use [`into_remainder`](Self::into_remainder) if that
    /// buffered item and the exact end-of-stream state of each side matter.
    pub fn into_inner(self) -> (St1, St2) {
        (self.stream1.into_inner(), self.stream2.into_inner())
    }

    /// Takes this combinator apart, returning an explicit EOF receipt that
    /// describes the end-of-stream state of both sides.
    ///
    /// When the shorter stream ends, the zipped stream ends as well (this
    /// default behavior is unchanged), but the longer stream may already
    /// have been polled one extra time, producing an item that was never
    /// paired. Instead of silently dropping that item, this receipt gives
    /// it back together with the remaining stream handle, so ownership of
    /// the prefetched item is unambiguous.
    ///
    /// For each side the receipt reports whether the stream is still
    /// [`Open`](ZipSide::Open) (it never returned `None` and may yield more
    /// items, possibly starting with a `buffered` one) or already reached
    /// [`Eof`](ZipSide::Eof). A side that merely returned `Poll::Pending`
    /// is reported as `Open` with no buffered item; it is never mistaken
    /// for EOF. When zipping streams of `Result` items, a buffered `Err`
    /// stays attributed to the side it was polled from.
    ///
    /// This function never polls either stream: taking the receipt (or
    /// dropping the combinator) cancels the zip without polling a stream
    /// that has already completed again.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::stream::{self, StreamExt, ZipSide};
    ///
    /// let stream1 = stream::iter(1..=2); // left: two items
    /// let stream2 = stream::iter(5..=8); // right: four items
    ///
    /// let mut zipped = stream1.zip(stream2);
    /// assert_eq!(zipped.next().await, Some((1, 5)));
    /// assert_eq!(zipped.next().await, Some((2, 6)));
    /// // The left stream is exhausted, so the zipped stream ends...
    /// assert_eq!(zipped.next().await, None);
    ///
    /// // ...but the right stream had already been polled once more, and
    /// // the receipt hands the prefetched item back instead of dropping it.
    /// let receipt = zipped.into_remainder();
    /// assert!(receipt.left.is_eof());
    /// match receipt.right {
    ///     ZipSide::Open { stream, buffered } => {
    ///         assert_eq!(buffered, Some(7));
    ///         assert_eq!(stream.collect::<Vec<_>>().await, vec![8]);
    ///     }
    ///     ZipSide::Eof => unreachable!(),
    /// }
    /// # });
    /// ```
    pub fn into_remainder(self) -> ZipRemainder<St1, St2> {
        let Self { stream1, stream2, queued1, queued2 } = self;
        let left = if stream1.is_done() {
            // A fused stream that reported EOF was only ever polled while
            // its queue was empty, so there cannot be a leftover item.
            debug_assert!(queued1.is_none());
            ZipSide::Eof
        } else {
            ZipSide::Open { stream: stream1.into_inner(), buffered: queued1 }
        };
        let right = if stream2.is_done() {
            debug_assert!(queued2.is_none());
            ZipSide::Eof
        } else {
            ZipSide::Open { stream: stream2.into_inner(), buffered: queued2 }
        };
        ZipRemainder { left, right }
    }
}

/// Explicit EOF policy receipt returned by [`Zip::into_remainder`].
///
/// It records, for each side of the zip, whether the stream is still open
/// (together with any item that was prefetched but never paired) or has
/// already reported EOF, so that nothing is silently dropped while the
/// caller might assume every polled item was paired.
#[must_use = "the receipt owns the remaining stream handles and buffered items"]
pub struct ZipRemainder<St1: Stream, St2: Stream> {
    /// State of the first (left) stream passed to
    /// [`zip`](super::StreamExt::zip).
    pub left: ZipSide<St1>,
    /// State of the second (right) stream passed to
    /// [`zip`](super::StreamExt::zip).
    pub right: ZipSide<St2>,
}

impl<St1, St2> fmt::Debug for ZipRemainder<St1, St2>
where
    St1: Stream + fmt::Debug,
    St1::Item: fmt::Debug,
    St2: Stream + fmt::Debug,
    St2::Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZipRemainder")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<St1: Stream, St2: Stream> ZipRemainder<St1, St2> {
    /// Returns `true` if both sides already reported EOF, i.e. the two
    /// streams ended in lockstep and nothing remains to be recovered.
    pub fn both_eof(&self) -> bool {
        self.left.is_eof() && self.right.is_eof()
    }

    /// Consumes the receipt, returning the state of both sides.
    pub fn into_sides(self) -> (ZipSide<St1>, ZipSide<St2>) {
        (self.left, self.right)
    }
}

/// The state of one side of a [`Zip`] combinator, as reported by
/// [`Zip::into_remainder`].
pub enum ZipSide<St: Stream> {
    /// The stream has not returned `None` yet. It may have returned
    /// `Poll::Pending` any number of times, but pending is never reported
    /// as EOF: the stream is handed back and may still be polled for more
    /// items.
    Open {
        /// The remaining stream handle. It has never been polled again
        /// after reporting `None`, so polling it is safe.
        stream: St,
        /// An item that was already polled from `stream` but never paired
        /// with an item from the other side. If the zipped items are
        /// `Result`s, a buffered `Err` stays on this side.
        buffered: Option<St::Item>,
    },
    /// The stream already returned `None` (EOF). It is consumed by the zip
    /// and will never be polled again; no buffered item remains on this
    /// side.
    Eof,
}

impl<St> fmt::Debug for ZipSide<St>
where
    St: Stream + fmt::Debug,
    St::Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZipSide::Open { stream, buffered } => {
                f.debug_struct("Open").field("stream", stream).field("buffered", buffered).finish()
            }
            ZipSide::Eof => f.write_str("Eof"),
        }
    }
}

impl<St: Stream> ZipSide<St> {
    /// Returns `true` if this side already reported EOF.
    pub fn is_eof(&self) -> bool {
        matches!(self, ZipSide::Eof)
    }

    /// Returns `true` if this side is still open and may yield more items.
    pub fn is_open(&self) -> bool {
        matches!(self, ZipSide::Open { .. })
    }

    /// Returns a reference to the buffered, not-yet-paired item, if any.
    pub fn buffered(&self) -> Option<&St::Item> {
        match self {
            ZipSide::Open { buffered, .. } => buffered.as_ref(),
            ZipSide::Eof => None,
        }
    }

    /// Takes the buffered, not-yet-paired item out of this side, if any.
    pub fn take_buffered(&mut self) -> Option<St::Item> {
        match self {
            ZipSide::Open { buffered, .. } => buffered.take(),
            ZipSide::Eof => None,
        }
    }

    /// Consumes this side, returning the remaining stream handle (`None`
    /// if the side already reported EOF) and the buffered, not-yet-paired
    /// item, if any.
    pub fn into_parts(self) -> (Option<St>, Option<St::Item>) {
        match self {
            ZipSide::Open { stream, buffered } => (Some(stream), buffered),
            ZipSide::Eof => (None, None),
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
