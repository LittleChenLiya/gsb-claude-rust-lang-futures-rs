use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::future::Future;
use futures::stream::{self, StreamExt};

/// A "counting" future fixture.
///
/// * Becomes ready after `polls_before_ready` pending polls (it wakes itself
///   on every pending poll), so the readiness order of a set of futures is
///   fully deterministic.
/// * Tracks how many futures are simultaneously in flight (started but not
///   yet completed) and records the maximum.
/// * Records every drop exactly once, so tests can assert that dropping the
///   stream only releases the futures it holds.
struct CountingFuture {
    index: usize,
    polls_left: usize,
    result: Result<usize, &'static str>,
    started: bool,
    completed: bool,
    inflight: Rc<Cell<usize>>,
    max_inflight: Rc<Cell<usize>>,
    drops: Rc<RefCell<Vec<usize>>>,
}

impl CountingFuture {
    fn new(
        index: usize,
        polls_before_ready: usize,
        result: Result<usize, &'static str>,
        inflight: Rc<Cell<usize>>,
        max_inflight: Rc<Cell<usize>>,
        drops: Rc<RefCell<Vec<usize>>>,
    ) -> Self {
        Self {
            index,
            polls_left: polls_before_ready,
            result,
            started: false,
            completed: false,
            inflight,
            max_inflight,
            drops,
        }
    }
}

impl Future for CountingFuture {
    type Output = Result<usize, &'static str>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            let now = self.inflight.get() + 1;
            self.inflight.set(now);
            if now > self.max_inflight.get() {
                self.max_inflight.set(now);
            }
        }
        if self.polls_left > 0 {
            self.polls_left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            self.completed = true;
            self.inflight.set(self.inflight.get() - 1);
            Poll::Ready(self.result)
        }
    }
}

impl Drop for CountingFuture {
    fn drop(&mut self) {
        if self.started && !self.completed {
            self.inflight.set(self.inflight.get() - 1);
        }
        self.drops.borrow_mut().push(self.index);
    }
}

struct Fixture {
    inflight: Rc<Cell<usize>>,
    max_inflight: Rc<Cell<usize>>,
    drops: Rc<RefCell<Vec<usize>>>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            inflight: Rc::new(Cell::new(0)),
            max_inflight: Rc::new(Cell::new(0)),
            drops: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn future(&self, index: usize, polls_before_ready: usize, result: Result<usize, &'static str>) -> CountingFuture {
        CountingFuture::new(
            index,
            polls_before_ready,
            result,
            Rc::clone(&self.inflight),
            Rc::clone(&self.max_inflight),
            Rc::clone(&self.drops),
        )
    }
}

/// Three counting futures become ready in reverse order (the later a future
/// sits in the input, the fewer polls it needs), yet the results must be
/// yielded in the original input order, and at most 2 futures may be in
/// flight at any time.
#[test]
fn ordered_output_with_bounded_inflight() {
    let fx = Fixture::new();
    let futs = vec![
        fx.future(0, 3, Ok(0)),
        fx.future(1, 2, Ok(1)),
        fx.future(2, 1, Ok(2)),
    ];

    let out: Vec<_> = block_on(stream::iter(futs).buffer_ordered(2).collect());

    // Reverse readiness order, but input-order output.
    assert_eq!(out, vec![Ok(0), Ok(1), Ok(2)]);
    // Never more than 2 futures in flight.
    assert_eq!(fx.max_inflight.get(), 2);
    // Everything completed and was released.
    assert_eq!(fx.inflight.get(), 0);
    let mut drops = fx.drops.borrow().clone();
    drops.sort_unstable();
    assert_eq!(drops, vec![0, 1, 2]);
}

/// The error of the second input future must be tagged with input index 1,
/// without disturbing the order of the surrounding successes.
#[test]
fn error_keeps_input_index() {
    let fx = Fixture::new();
    let futs = vec![
        fx.future(0, 2, Ok(0)),
        fx.future(1, 1, Err("boom")),
        fx.future(2, 1, Ok(2)),
    ];

    let out: Vec<_> = block_on(stream::iter(futs).buffer_ordered(2).collect());

    assert_eq!(out.len(), 3);
    assert_eq!(out[0], Ok(0));
    let err = out[1].as_ref().unwrap_err();
    assert_eq!(err.index, 1);
    assert_eq!(err.error, "boom");
    assert_eq!(out[2], Ok(2));
}

/// Dropping the stream releases exactly the futures it holds (each exactly
/// once): the in-flight ones and the ones still owned by the underlying
/// stream. Already yielded futures are not dropped again.
#[test]
fn drop_releases_only_held_futures() {
    let fx = Fixture::new();
    let futs = vec![
        fx.future(0, 0, Ok(0)),
        fx.future(1, 100, Ok(1)),
        fx.future(2, 100, Ok(2)),
    ];

    {
        let mut buffered = stream::iter(futs).buffer_ordered(2);
        // First poll admits futures 0 and 1 (limit 2); future 0 is ready
        // immediately and is yielded. Future 2 has not been admitted yet and
        // is still owned by the underlying stream inside the adaptor.
        assert_eq!(block_on(buffered.next()), Some(Ok(0)));
        // Neither the in-flight future 1 nor the not-yet-admitted future 2
        // has been released.
        assert!(!fx.drops.borrow().contains(&1));
        assert!(!fx.drops.borrow().contains(&2));
        drop(buffered);
    }

    // Futures 1 (in flight) and 2 (still upstream) are released by the drop,
    // each exactly once; future 0 is not dropped twice.
    let mut drops = fx.drops.borrow().clone();
    drops.sort_unstable();
    assert_eq!(drops, vec![0, 1, 2]);
    assert_eq!(fx.inflight.get(), 0);
}
