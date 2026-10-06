use alloc::{boxed::Box, collections::VecDeque};
use core::{fmt, num::NonZeroUsize, pin::Pin};

use futures_core::{
    future::Future,
    stream::Stream,
    task::{Context, Poll},
};
#[cfg(feature = "sink")]
use futures_sink::Sink;
use pin_project_lite::pin_project;

use crate::stream::{Fuse, FusedStream};

/// Error returned by the [`buffered_ordered`](super::StreamExt::buffered_ordered)
/// stream.
///
/// It preserves the index (in the input stream of futures) of the future that
/// failed, together with the error that future returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexedError<E> {
    /// Index (in the input stream) of the future that failed.
    pub index: usize,
    /// The error returned by that future.
    pub error: E,
}

impl<E> IndexedError<E> {
    /// Creates a new `IndexedError` from an input index and an error.
    pub fn new(index: usize, error: E) -> Self {
        Self { index, error }
    }
}

impl<E: fmt::Display> fmt::Display for IndexedError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "future at input index {} failed: {}", self.index, self.error)
    }
}

#[cfg(feature = "std")]
impl<E> std::error::Error for IndexedError<E> where E: fmt::Debug + fmt::Display {}

pin_project! {
    /// Future wrapper that tags a future's output with its input index.
    #[derive(Debug)]
    struct TagIndex<F> {
        #[pin]
        future: F,
        index: usize,
    }
}

impl<F: Future> Future for TagIndex<F> {
    type Output = (usize, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let index = *this.index;
        this.future.poll(cx).map(|output| (index, output))
    }
}

/// Slot for an in-flight or completed future, keeping its position in the
/// input order until all earlier futures have been yielded.
#[derive(Debug)]
enum Slot<F: Future> {
    Running(Pin<Box<TagIndex<F>>>),
    Done((usize, F::Output)),
}

pin_project! {
    /// Stream for the [`buffered_ordered`](super::StreamExt::buffered_ordered)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferedOrdered<St>
    where
        St: Stream,
        St::Item: Future,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress: VecDeque<Slot<St::Item>>,
        next_index: usize,
        max: NonZeroUsize,
    }
}

impl<St> fmt::Debug for BufferedOrdered<St>
where
    St: Stream + fmt::Debug,
    St::Item: Future,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferedOrdered")
            .field("stream", &self.stream)
            .field("in_progress", &self.in_progress.len())
            .field("next_index", &self.next_index)
            .field("max", &self.max)
            .finish()
    }
}

impl<St> BufferedOrdered<St>
where
    St: Stream,
    St::Item: Future,
{
    pub(super) fn new(stream: St, n: usize) -> Self {
        assert!(n > 0, "buffered_ordered requires a non-zero limit");
        Self {
            stream: super::Fuse::new(stream),
            in_progress: VecDeque::new(),
            next_index: 0,
            // SAFETY: `n` is checked to be non-zero above.
            max: unsafe { NonZeroUsize::new_unchecked(n) },
        }
    }

    delegate_access_inner!(stream, St, (.));
}

impl<St, T, E> Stream for BufferedOrdered<St>
where
    St: Stream,
    St::Item: Future<Output = Result<T, E>>,
{
    type Item = Result<T, IndexedError<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures. Only futures that are still running count
        // against the limit; at most `max` futures are polled at a time.
        loop {
            let running =
                this.in_progress.iter().filter(|slot| matches!(slot, Slot::Running(_))).count();
            if running >= this.max.get() {
                break;
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    let index = *this.next_index;
                    *this.next_index += 1;
                    this.in_progress.push_back(Slot::Running(Box::pin(TagIndex {
                        future: fut,
                        index,
                    })));
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Poll every in-flight future so they all make progress; completed
        // ones keep their (index, output) pair in place until every earlier
        // future has been yielded.
        for slot in this.in_progress.iter_mut() {
            if let Slot::Running(fut) = slot {
                if let Poll::Ready(output) = fut.as_mut().poll(cx) {
                    *slot = Slot::Done(output);
                }
            }
        }

        // Yield the oldest future's output once it is ready, preserving the
        // original input order regardless of completion order.
        match this.in_progress.front() {
            Some(Slot::Done(_)) => {
                let (index, output) = match this.in_progress.pop_front() {
                    Some(Slot::Done(done)) => done,
                    _ => unreachable!(),
                };
                Poll::Ready(Some(output.map_err(|error| IndexedError { index, error })))
            }
            _ => {
                // If more values are still coming from the stream or in-flight
                // futures remain, we're not done yet.
                if this.stream.is_done() && this.in_progress.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

impl<St, T, E> FusedStream for BufferedOrdered<St>
where
    St: Stream,
    St::Item: Future<Output = Result<T, E>>,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_done() && self.in_progress.is_empty()
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<S, Item> Sink<Item> for BufferedOrdered<S>
where
    S: Stream + Sink<Item>,
    S::Item: Future,
{
    type Error = S::Error;

    delegate_sink!(stream, Item);
}
