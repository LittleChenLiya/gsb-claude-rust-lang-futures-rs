use core::pin::Pin;
use std::convert::Infallible;

use futures::{
    Stream,
    stream::{self, Repeat, StreamExt, TryStreamExt, repeat},
    task::Poll,
};
use futures_executor::block_on;
use futures_task::Context;
use futures_test::task::noop_context;

#[test]
fn try_filter_map_after_err() {
    let cx = &mut noop_context();
    let mut s = stream::iter(1..=3)
        .map(Ok)
        .try_filter_map(|v| async move { Err::<Option<()>, _>(v) })
        .filter_map(|r| async move { r.ok() })
        .boxed();
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
}

#[test]
fn try_skip_while_after_err() {
    let cx = &mut noop_context();
    let mut s = stream::iter(1..=3)
        .map(Ok)
        .try_skip_while(|_| async move { Err::<_, ()>(()) })
        .filter_map(|r| async move { r.ok() })
        .boxed();
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
}

#[test]
fn try_take_while_after_err() {
    let cx = &mut noop_context();
    let mut s = stream::iter(1..=3)
        .map(Ok)
        .try_take_while(|_| async move { Err::<_, ()>(()) })
        .filter_map(|r| async move { r.ok() })
        .boxed();
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
}

#[test]
fn try_flatten_unordered() {
    let test_st = stream::iter(1..7)
        .map(|val: u32| {
            if val % 2 == 0 {
                Ok(stream::unfold((val, 1), |(val, pow)| async move {
                    Some((val.pow(pow), (val, pow + 1)))
                })
                .take(3)
                .map(move |val| if val % 16 != 0 { Ok(val) } else { Err(val) }))
            } else {
                Err(val)
            }
        })
        .map_ok(Box::pin)
        .try_flatten_unordered(None);

    block_on(async move {
        assert_eq!(
            // All numbers can be divided by 16 and odds must be `Err`
            // For all basic evens we must have powers from 1 to 3
            vec![
                Err(1),
                Err(3),
                Err(5),
                Ok(2),
                Ok(4),
                Ok(6),
                Ok(4),
                Err(16),
                Ok(36),
                Ok(8),
                Err(64),
                Ok(216)
            ],
            test_st.collect::<Vec<_>>().await
        )
    });

    #[derive(Clone, Debug)]
    struct ErrorStream {
        error_after: usize,
        polled: usize,
    }

    impl Stream for ErrorStream {
        type Item = Result<Repeat<Result<(), ()>>, ()>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.polled > self.error_after {
                panic!("Polled after error");
            } else {
                let out =
                    if self.polled == self.error_after { Err(()) } else { Ok(repeat(Ok(()))) };
                self.polled += 1;
                Poll::Ready(Some(out))
            }
        }
    }

    block_on(async move {
        let mut st = ErrorStream { error_after: 3, polled: 0 }.try_flatten_unordered(None);
        let mut ctr = 0;
        while (st.try_next().await).is_ok() {
            ctr += 1;
        }
        assert_eq!(ctr, 0);

        assert_eq!(
            ErrorStream { error_after: 10, polled: 0 }
                .try_flatten_unordered(None)
                .inspect_ok(|_| panic!("Unexpected `Ok`"))
                .try_collect::<Vec<_>>()
                .await,
            Err(())
        );

        let mut taken = 0;
        assert_eq!(
            ErrorStream { error_after: 10, polled: 0 }
                .map_ok(|st| st.take(3))
                .try_flatten_unordered(1)
                .inspect(|_| taken += 1)
                .try_fold((), |(), res| async move { res })
                .await,
            Err(())
        );
        assert_eq!(taken, 31);
    })
}

async fn is_even(number: u8) -> bool {
    number % 2 == 0
}

#[test]
fn try_all() {
    block_on(async {
        let empty: [Result<u8, Infallible>; 0] = [];
        let st = stream::iter(empty);
        let all = st.try_all(is_even).await;
        assert_eq!(Ok(true), all);

        let st = stream::iter([Ok::<_, Infallible>(2), Ok(4), Ok(6), Ok(8)]);
        let all = st.try_all(is_even).await;
        assert_eq!(Ok(true), all);

        let st = stream::iter([Ok::<_, Infallible>(2), Ok(3), Ok(4)]);
        let all = st.try_all(is_even).await;
        assert_eq!(Ok(false), all);

        let st = stream::iter([Ok(2), Ok(4), Err("err"), Ok(8)]);
        let all = st.try_all(is_even).await;
        assert_eq!(Err("err"), all);
    });
}

#[test]
fn try_any() {
    block_on(async {
        let empty: [Result<u8, Infallible>; 0] = [];
        let st = stream::iter(empty);
        let any = st.try_any(is_even).await;
        assert_eq!(Ok(false), any);

        let st = stream::iter([Ok::<_, Infallible>(1), Ok(2), Ok(3)]);
        let any = st.try_any(is_even).await;
        assert_eq!(Ok(true), any);

        let st = stream::iter([Ok::<_, Infallible>(1), Ok(3), Ok(5)]);
        let any = st.try_any(is_even).await;
        assert_eq!(Ok(false), any);

        let st = stream::iter([Ok(1), Ok(3), Err("err"), Ok(8)]);
        let any = st.try_any(is_even).await;
        assert_eq!(Err("err"), any);
    });
}

/// A small in-memory, hand-written stream fixture: yields the queued items
/// one poll at a time and terminates once the queue is empty.
struct ManualStream {
    items: std::collections::VecDeque<Result<i32, &'static str>>,
}

impl ManualStream {
    fn new(items: impl IntoIterator<Item = Result<i32, &'static str>>) -> Self {
        Self { items: items.into_iter().collect() }
    }
}

impl Stream for ManualStream {
    type Item = Result<i32, &'static str>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

#[test]
fn map_err_with_count_maps_error_once_with_ok_count() {
    let cx = &mut noop_context();
    // Two successful items, then an error, then the stream terminates.
    let stream = ManualStream::new([Ok(1), Ok(2), Err("boom")]);

    let mut calls = Vec::new();
    let mut s = stream.map_err_with_count(|ok_count, e| {
        calls.push((ok_count, e));
        format!("{e} after {ok_count} ok(s)")
    });

    assert_eq!(Poll::Ready(Some(Ok(1))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Ok(2))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Err("boom after 2 ok(s)".to_string()))), s.poll_next_unpin(cx));
    // The input stream is terminated, so the adapted stream is too.
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
    drop(s);
    // The error was mapped exactly once, with the count of successful items,
    // and polling after the error / termination triggered no extra calls.
    assert_eq!(calls, vec![(2usize, "boom")]);
}

#[test]
fn map_err_with_count_follows_input_stream_after_error() {
    let cx = &mut noop_context();

    // Case 1: the input stream continues after the error.
    let continuing = ManualStream::new([Ok(1), Ok(2), Err("boom"), Ok(3), Err("bang"), Ok(4)]);
    let mut calls = Vec::new();
    let mut out = Vec::new();
    {
        let mut s = continuing.map_err_with_count(|ok_count, e| {
            calls.push((ok_count, e));
            (e, ok_count)
        });
        while let Poll::Ready(Some(item)) = s.poll_next_unpin(cx) {
            out.push(item);
        }
    }
    assert_eq!(out, vec![Ok(1), Ok(2), Err(("boom", 2)), Ok(3), Err(("bang", 3)), Ok(4)]);
    // Each error was mapped exactly once, with the running count of
    // successful items; the errors did not terminate the stream.
    assert_eq!(calls, vec![(2usize, "boom"), (3usize, "bang")]);

    // Case 2: the input stream terminates right after the error.
    let terminating = ManualStream::new([Ok(1), Ok(2), Err("boom")]);
    let mut s = terminating.map_err_with_count(|ok_count, e| (e, ok_count));
    assert_eq!(Poll::Ready(Some(Ok(1))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Ok(2))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Err(("boom", 2)))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
}

#[test]
fn map_err_regression_matches_map_err_with_count() {
    // Short regression example for the original `map_err` interface: over the
    // same input, `map_err` (ignoring the count) and `map_err_with_count`
    // must produce identical item sequences.
    let items = || vec![Ok(1), Ok(2), Err("boom"), Ok(3)];

    let via_map_err: Vec<Result<i32, String>> =
        block_on(stream::iter(items()).map_err(|e| format!("mapped: {e}")).collect());
    let via_map_err_with_count: Vec<Result<i32, String>> = block_on(
        stream::iter(items()).map_err_with_count(|_ok_count, e| format!("mapped: {e}")).collect(),
    );

    assert_eq!(via_map_err, vec![Ok(1), Ok(2), Err("mapped: boom".to_string()), Ok(3)]);
    assert_eq!(via_map_err, via_map_err_with_count);
}
