#![cfg(feature = "std")]

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use futures::{
    executor::block_on,
    future,
    stream::{self, Stream, StreamExt},
    task::noop_waker,
};

/// Shared state for the counting futures fixture.
#[derive(Default)]
struct Shared {
    /// Number of futures that have been polled at least once and have not
    /// completed yet.
    in_flight: usize,
    /// High-water mark of `in_flight`.
    max_in_flight: usize,
    /// Ids of futures that are allowed to complete.
    ready: Vec<usize>,
    wakers: Vec<Waker>,
}

/// A future that counts how many instances are concurrently in flight and
/// only completes once its id has been marked ready by the test.
struct CountingFut {
    id: usize,
    shared: Rc<RefCell<Shared>>,
    started: bool,
}

impl CountingFut {
    fn new(id: usize, shared: &Rc<RefCell<Shared>>) -> Self {
        Self { id, shared: Rc::clone(shared), started: false }
    }
}

impl Future for CountingFut {
    type Output = Result<usize, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut shared = this.shared.borrow_mut();
        if !this.started {
            this.started = true;
            shared.in_flight += 1;
            shared.max_in_flight = shared.max_in_flight.max(shared.in_flight);
        }
        if shared.ready.contains(&this.id) {
            shared.in_flight -= 1;
            Poll::Ready(Ok(this.id))
        } else {
            shared.wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Three counting futures become ready in (reverse) completion order 1, 2, 0,
/// yet the outputs must come out in input order 0, 1, 2, and no more than two
/// futures may be in flight at any time.
#[test]
fn ordered_output_with_bounded_in_flight() {
    let shared = Rc::new(RefCell::new(Shared::default()));
    let futs: Vec<_> = (0..3).map(|id| CountingFut::new(id, &shared)).collect();
    let mut stream = Box::pin(stream::iter(futs).buffered_ordered(2));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // First poll admits futures 0 and 1; the limit of 2 is reached.
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(shared.borrow().in_flight, 2);

    // Future 1 (not the head) becomes ready first: it completes internally,
    // but nothing is yielded while future 0 is still pending.
    shared.borrow_mut().ready.push(1);
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(shared.borrow().in_flight, 1);

    // The freed slot admits future 2; still at most 2 in flight.
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(shared.borrow().in_flight, 2);

    // Future 2 becomes ready next; future 0 is still pending, so no output.
    shared.borrow_mut().ready.push(2);
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());

    // Finally future 0 becomes ready: the outputs come out in input order.
    shared.borrow_mut().ready.push(0);
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(Ok(0))));
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(Ok(1))));
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(Ok(2))));
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(None));

    assert_eq!(shared.borrow().max_in_flight, 2);
}

/// The error of the second input future keeps its input index (1) and does
/// not stop the remaining futures from being admitted and yielded.
#[test]
fn error_keeps_input_index() {
    block_on(async {
        let futs = vec![
            future::ready(Ok::<_, &str>(0)),
            future::ready(Err("boom")),
            future::ready(Ok::<_, &str>(2)),
        ];
        let results: Vec<_> = stream::iter(futs).buffered_ordered(2).collect().await;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Ok(0));
        let err = results[1].as_ref().unwrap_err();
        assert_eq!(err.index, 1);
        assert_eq!(err.error, "boom");
        assert_eq!(results[2], Ok(2));
    });
}

/// Dropping the stream releases the futures it holds (both the in-flight ones
/// and the ones still buffered in the underlying stream).
#[test]
fn drop_releases_held_futures() {
    let dropped = Rc::new(RefCell::new(vec![false; 3]));

    struct DropFut {
        id: usize,
        dropped: Rc<RefCell<Vec<bool>>>,
    }

    impl Future for DropFut {
        type Output = Result<(), ()>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let _ = cx;
            Poll::Pending
        }
    }

    impl Drop for DropFut {
        fn drop(&mut self) {
            self.dropped.borrow_mut()[self.id] = true;
        }
    }

    let futs: Vec<_> =
        (0..3).map(|id| DropFut { id, dropped: Rc::clone(&dropped) }).collect();
    let mut stream = Box::pin(stream::iter(futs).buffered_ordered(2));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // Two futures are admitted and held in flight; the third is still held by
    // the underlying stream.
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(&*dropped.borrow(), &[false, false, false]);

    drop(stream);
    assert_eq!(&*dropped.borrow(), &[true, true, true]);
}
