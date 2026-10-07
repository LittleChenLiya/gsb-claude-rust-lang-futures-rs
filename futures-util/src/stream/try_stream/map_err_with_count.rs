use core::{fmt, pin::Pin};

use futures_core::{
    ready,
    stream::{FusedStream, Stream, TryStream},
    task::{Context, Poll},
};
#[cfg(feature = "sink")]
use futures_sink::Sink;
use pin_project_lite::pin_project;

pin_project! {
    /// Stream for the [`map_err_with_count`](super::TryStreamExt::map_err_with_count)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct MapErrWithCount<St, F> {
        #[pin]
        stream: St,
        f: F,
        ok_count: usize,
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

impl<St, F> MapErrWithCount<St, F> {
    pub(super) fn new(stream: St, f: F) -> Self {
        Self { stream, f, ok_count: 0 }
    }

    delegate_access_inner!(stream, St, ());
}

impl<St, F, E> Stream for MapErrWithCount<St, F>
where
    St: TryStream,
    F: FnMut(usize, St::Error) -> E,
{
    type Item = Result<St::Ok, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.try_poll_next(cx)) {
            Some(Ok(ok)) => {
                *this.ok_count += 1;
                Poll::Ready(Some(Ok(ok)))
            }
            Some(Err(e)) => {
                // The mapper runs exactly once per incoming error and its
                // result is forwarded immediately, so it can neither swallow
                // the error nor inject additional `Ok` values.
                let mapped = (this.f)(*this.ok_count, e);
                Poll::Ready(Some(Err(mapped)))
            }
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<St, F, E> FusedStream for MapErrWithCount<St, F>
where
    St: TryStream + FusedStream,
    F: FnMut(usize, St::Error) -> E,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<St, F, Item> Sink<Item> for MapErrWithCount<St, F>
where
    St: Sink<Item>,
{
    type Error = St::Error;

    delegate_sink!(stream, Item);
}
