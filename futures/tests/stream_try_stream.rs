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

/// Hand-written in-memory stream fixture: replays a fixed script of items and
/// then terminates (fused), so tests can observe post-error and
/// post-termination states deterministically.
struct ManualStream {
    items: std::vec::IntoIter<Result<i32, &'static str>>,
}

impl ManualStream {
    fn new(items: Vec<Result<i32, &'static str>>) -> Self {
        Self { items: items.into_iter() }
    }
}

impl Stream for ManualStream {
    type Item = Result<i32, &'static str>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.next())
    }
}

impl futures::stream::FusedStream for ManualStream {
    fn is_terminated(&self) -> bool {
        self.items.len() == 0
    }
}

#[test]
fn map_err_with_count_two_oks_then_err() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let cx = &mut noop_context();
    // Mapper calls are recorded as (ok_count, error) pairs.
    let calls = Rc::new(RefCell::new(Vec::new()));
    let calls2 = Rc::clone(&calls);

    let mut s = ManualStream::new(vec![Ok(1), Ok(2), Err("boom")])
        .map_err_with_count(move |n, e| {
            calls2.borrow_mut().push((n, e));
            format!("mapped#{n}:{e}")
        });

    assert_eq!(Poll::Ready(Some(Ok(1))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Ok(2))), s.poll_next_unpin(cx));
    // The error is mapped exactly once, seeing the two successful items.
    assert_eq!(Poll::Ready(Some(Err("mapped#2:boom".to_string()))), s.poll_next_unpin(cx));
    assert_eq!(*calls.borrow(), vec![(2, "boom")]);
    // Input stream terminated: the adapter follows it and stays terminated.
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
    assert_eq!(calls.borrow().len(), 1);
}

#[test]
fn map_err_with_count_continues_after_error() {
    use futures::stream::FusedStream;

    let cx = &mut noop_context();
    let mut s = ManualStream::new(vec![Ok(1), Err("a"), Ok(2), Err("b"), Ok(3)])
        .map_err_with_count(|n, e| (n, e));

    assert!(!s.is_terminated());
    assert_eq!(Poll::Ready(Some(Ok(1))), s.poll_next_unpin(cx));
    // Error does not terminate the adapter; the count keeps the oks so far.
    assert_eq!(Poll::Ready(Some(Err((1, "a")))), s.poll_next_unpin(cx));
    assert!(!s.is_terminated());
    assert_eq!(Poll::Ready(Some(Ok(2))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Err((2, "b")))), s.poll_next_unpin(cx));
    assert_eq!(Poll::Ready(Some(Ok(3))), s.poll_next_unpin(cx));
    // Only once the input stream is exhausted does the adapter terminate.
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
    assert!(s.is_terminated());
    assert_eq!(Poll::Ready(None), s.poll_next_unpin(cx));
}

#[test]
fn map_err_with_count_matches_map_err_regression() {
    // Short regression check against the original `map_err` interface: both
    // adapters must produce identical output for the same input.
    let input = || vec![Ok(10), Ok(20), Err("x"), Ok(30), Err("y")];

    let via_map_err = block_on(
        ManualStream::new(input()).map_err(|e| format!("E:{e}")).collect::<Vec<_>>(),
    );
    let via_count = block_on(
        ManualStream::new(input())
            .map_err_with_count(|_n, e| format!("E:{e}"))
            .collect::<Vec<_>>(),
    );

    let expected = vec![
        Ok(10),
        Ok(20),
        Err("E:x".to_string()),
        Ok(30),
        Err("E:y".to_string()),
    ];
    assert_eq!(via_map_err, expected);
    assert_eq!(via_count, expected);
    println!("map_err           => {via_map_err:?}");
    println!("map_err_with_count=> {via_count:?}");
}

#[test]
fn map_err_with_count_auto_traits() {
    fn assert_send<T: Send>(_: &T) {}
    fn assert_unpin<T: Unpin>(_: &T) {}

    let s = stream::iter(vec![Ok::<i32, i32>(1)]).map_err_with_count(|_n, e| e);
    assert_send(&s);
    assert_unpin(&s);
}
