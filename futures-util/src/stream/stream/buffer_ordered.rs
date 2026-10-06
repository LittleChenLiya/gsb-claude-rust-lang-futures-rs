use core::{fmt, num::NonZeroUsize, pin::Pin};

use futures_core::{
    future::Future,
    ready,
    stream::{FusedStream, Stream},
    task::{Context, Poll},
};
#[cfg(feature = "sink")]
use futures_sink::Sink;
use pin_project_lite::pin_project;

use crate::stream::{Fuse, FuturesOrdered, StreamExt};

/// Error yielded by the [`BufferOrdered`] stream when one of the managed
/// futures fails.
///
/// The error keeps the zero-based [`index`](IndexedError::index) of the input
/// future (in the order the futures were produced by the underlying stream)
/// that failed, alongside the original [`error`](IndexedError::error).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedError<E> {
    /// Zero-based index of the input future that produced this error.
    pub index: usize,
    /// The error produced by that future.
    pub error: E,
}

impl<E> IndexedError<E> {
    /// Creates a new [`IndexedError`] from an input index and an error.
    pub fn new(index: usize, error: E) -> Self {
        Self { index, error }
    }

    /// Consumes this error, returning the wrapped error value.
    pub fn into_inner(self) -> E {
        self.error
    }
}

impl<E: fmt::Display> fmt::Display for IndexedError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "future at input index {} failed: {}", self.index, self.error)
    }
}

#[cfg(feature = "std")]
impl<E> std::error::Error for IndexedError<E>
where
    E: fmt::Debug + fmt::Display,
{
}

pin_project! {
    /// Stream for the [`buffer_ordered`](super::StreamExt::buffer_ordered)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferOrdered<St>
    where
        St: Stream,
        St::Item: Future,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesOrdered<St::Item>,
        max: Option<NonZeroUsize>,
        // Number of items already yielded; also the input index of the next
        // item to be yielded, since outputs are produced in input order.
        next_output_index: usize,
    }
}

impl<St> fmt::Debug for BufferOrdered<St>
where
    St: Stream + fmt::Debug,
    St::Item: Future,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferOrdered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max", &self.max)
            .field("next_output_index", &self.next_output_index)
            .finish()
    }
}

impl<St> BufferOrdered<St>
where
    St: Stream,
    St::Item: Future,
{
    pub(super) fn new(stream: St, n: Option<usize>) -> Self {
        Self {
            stream: super::Fuse::new(stream),
            in_progress_queue: FuturesOrdered::new(),
            max: n.and_then(NonZeroUsize::new),
            next_output_index: 0,
        }
    }

    delegate_access_inner!(stream, St, (.));
}

impl<St, T, E> Stream for BufferOrdered<St>
where
    St: Stream,
    St::Item: Future<Output = Result<T, E>>,
{
    type Item = Result<T, IndexedError<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures. At most `max` futures are ever in flight.
        while this.max.map(|max| this.in_progress_queue.len() < max.get()).unwrap_or(true) {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => this.in_progress_queue.push_back(fut),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue.
        // `FuturesOrdered` yields outputs in the order the futures were
        // admitted, so the next output always corresponds to
        // `next_output_index`.
        let res = this.in_progress_queue.poll_next_unpin(cx);
        if let Some(output) = ready!(res) {
            let index = *this.next_output_index;
            *this.next_output_index += 1;
            return Poll::Ready(Some(output.map_err(|error| IndexedError { index, error })));
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() { Poll::Ready(None) } else { Poll::Pending }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

impl<St, T, E> FusedStream for BufferOrdered<St>
where
    St: Stream,
    St::Item: Future<Output = Result<T, E>>,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_done() && self.in_progress_queue.is_terminated()
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<S, Item> Sink<Item> for BufferOrdered<S>
where
    S: Stream + Sink<Item>,
    S::Item: Future,
{
    type Error = S::Error;

    delegate_sink!(stream, Item);
}
