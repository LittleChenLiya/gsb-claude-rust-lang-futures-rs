use core::{fmt, pin::Pin};

use futures_core::{
    stream::{FusedStream, Stream, TryStream},
    task::{Context, Poll},
};
#[cfg(feature = "sink")]
use futures_sink::Sink;
use pin_project_lite::pin_project;

pin_project! {
    /// Stream for the
    /// [`map_err_with_count`](super::TryStreamExt::map_err_with_count) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct MapErrWithCount<St, F> {
        #[pin]
        stream: St,
        f: F,
        ok_count: u64,
    }
}

impl<St, F> fmt::Debug for MapErrWithCount<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapErrWithCount")
            .field("stream", &self.stream)
            .field("ok_count", &self.ok_count)
            .finish()
    }
}

impl<St, F> MapErrWithCount<St, F>
where
    St: TryStream,
{
    pub(super) fn new(stream: St, f: F) -> Self {
        Self { stream, f, ok_count: 0 }
    }

    delegate_access_inner!(stream, St, ());
}

impl<St, E, F> Stream for MapErrWithCount<St, F>
where
    St: TryStream,
    F: FnMut(u64, St::Error) -> E,
{
    type Item = Result<St::Ok, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.try_poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                *this.ok_count += 1;
                Poll::Ready(Some(Ok(item)))
            }
            // Each incoming error is mapped exactly once, right where it
            // arrives. The mapper only converts the error: it cannot drop it
            // or inject extra items, and the stream state afterwards is
            // entirely up to the underlying stream.
            Poll::Ready(Some(Err(e))) => {
                let mapped = (this.f)(*this.ok_count, e);
                Poll::Ready(Some(Err(mapped)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<St, E, F> FusedStream for MapErrWithCount<St, F>
where
    St: TryStream + FusedStream,
    F: FnMut(u64, St::Error) -> E,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<S, F, Item> Sink<Item> for MapErrWithCount<S, F>
where
    S: Sink<Item>,
{
    type Error = S::Error;

    delegate_sink!(stream, Item);
}
