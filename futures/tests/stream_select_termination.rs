//! Tests for the termination-state query (Select::termination() and
//! SelectWithStrategy::termination()) and the termination masking of
//! two-way select streams.
//!
//! All streams are manually controlled scripts with poll counters, so the
//! tests are fully deterministic and never wait on real time.

use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::stream::{self, FusedStream, SelectTermination, Stream, StreamExt};
use futures_test::task::noop_context;

/// A manually controlled stream: every poll_next bumps a shared poll
/// counter and pops the next scripted response. Once the script is
/// exhausted it keeps returning Ready(None) (like a fused stream).
struct ScriptedStream {
    script: VecDeque<Poll<Option<u32>>>,
    polls: Rc<Cell<usize>>,
}

impl fmt::Debug for ScriptedStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedStream").field("polls", &self.polls.get()).finish()
    }
}

impl ScriptedStream {
    fn new(script: Vec<Poll<Option<u32>>>, polls: Rc<Cell<usize>>) -> Self {
        Self { script: script.into_iter().collect(), polls }
    }
}

impl Stream for ScriptedStream {
    type Item = u32;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u32>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        this.script.pop_front().unwrap_or(Poll::Ready(None))
    }
}

fn counter() -> Rc<Cell<usize>> {
    Rc::new(Cell::new(0))
}

fn some(item: u32) -> Poll<Option<u32>> {
    Poll::Ready(Some(item))
}

const NONE: Poll<Option<u32>> = Poll::Ready(None);

/// One side (left) ends early: it must never be polled again, while the
/// right side keeps producing items.
#[test]
fn left_ends_early_right_continues() {
    let mut cx = noop_context();

    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![some(1), NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![some(10), some(20), some(30), NONE], right_polls.clone());

    let mut sel = Box::pin(stream::select(left, right));

    // Nothing has terminated yet.
    assert_eq!(sel.termination(), SelectTermination::Neither);
    assert!(!sel.termination().left_terminated());
    assert!(!sel.termination().right_terminated());
    assert!(!sel.termination().is_terminated());

    // Round-robin: left first, then right.
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(1));
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(10));
    assert_eq!(sel.termination(), SelectTermination::Neither);

    // This poll hits the end of the left stream: the state flips to Left
    // and the same poll still yields the next item of the right stream.
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(20));
    assert_eq!(sel.termination(), SelectTermination::Left);
    assert!(sel.termination().left_terminated());
    assert!(!sel.termination().right_terminated());

    // The terminated left side is masked: its poll count never moves again.
    let left_polls_at_end = left_polls.get();
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(30));
    assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    assert_eq!(left_polls.get(), left_polls_at_end, "terminated left stream was polled again");
    assert!(right_polls.get() > 0);

    // Both sides are done now.
    assert_eq!(sel.termination(), SelectTermination::Both);
    assert!(sel.termination().is_terminated());
}

/// Mirror image: the right side ends early and is masked afterwards.
#[test]
fn right_ends_early_left_continues() {
    let mut cx = noop_context();

    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![some(1), some(2), some(3), NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![NONE], right_polls.clone());

    let mut sel = Box::pin(stream::select(left, right));

    assert_eq!(sel.as_mut().poll_next(&mut cx), some(1));
    // Right ends on the second poll; the left item is still delivered.
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(2));
    assert_eq!(sel.termination(), SelectTermination::Right);
    assert!(sel.termination().right_terminated());
    assert!(!sel.termination().left_terminated());

    let right_polls_at_end = right_polls.get();
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(3));
    assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    assert_eq!(right_polls.get(), right_polls_at_end, "terminated right stream was polled again");

    assert_eq!(sel.termination(), SelectTermination::Both);
}

/// Once both sides have finished, repeated polls stably return None and
/// neither underlying stream is ever polled again.
#[test]
fn both_finished_repeated_poll_none() {
    let mut cx = noop_context();

    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![NONE], right_polls.clone());

    let mut sel = Box::pin(stream::select(left, right));

    // First poll drains both sides at once.
    assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    assert_eq!(sel.termination(), SelectTermination::Both);

    let left_total = left_polls.get();
    let right_total = right_polls.get();

    // Repeated polls: stable None, no extra polls of the underlying streams.
    for _ in 0..3 {
        assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
        assert_eq!(sel.termination(), SelectTermination::Both);
    }
    assert_eq!(left_polls.get(), left_total);
    assert_eq!(right_polls.get(), right_total);
}

/// While both sides are live, the fair round-robin selection is unchanged
/// and the state query keeps reporting Neither.
#[test]
fn fairness_unchanged_while_both_active() {
    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![some(1), some(2), some(3), NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![some(10), some(20), some(30), NONE], right_polls.clone());

    let sel = stream::select(left, right);
    assert_eq!(sel.termination(), SelectTermination::Neither);

    let collected: Vec<u32> = futures::executor::block_on(sel.collect::<Vec<_>>());
    // Strict alternation: the fairness of select is preserved.
    assert_eq!(collected, vec![1, 10, 2, 20, 3, 30]);
    // Each side was polled exactly until it returned None.
    assert_eq!(left_polls.get(), 4);
    assert_eq!(right_polls.get(), 4);
}

/// The new state query is also available on SelectWithStrategy and does
/// not disturb a custom (priority) strategy.
#[test]
fn select_with_strategy_exposes_termination() {
    let mut cx = noop_context();

    fn prio_left(_: &mut ()) -> stream::PollNext {
        stream::PollNext::Left
    }

    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![some(1), NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![some(2), NONE], right_polls.clone());

    let mut sel = Box::pin(stream::select_with_strategy(left, right, prio_left));
    assert_eq!(sel.termination(), SelectTermination::Neither);

    assert_eq!(sel.as_mut().poll_next(&mut cx), some(1));
    // Left ends; the same poll falls through to the right stream.
    assert_eq!(sel.as_mut().poll_next(&mut cx), some(2));
    assert_eq!(sel.termination(), SelectTermination::Left);

    let left_total = left_polls.get();
    assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    assert_eq!(sel.termination(), SelectTermination::Both);
    assert_eq!(left_polls.get(), left_total);
}

/// The original interfaces keep working unchanged when the new query is
/// never used (backwards compatibility).
#[test]
fn legacy_interface_untouched() {
    // select + StreamExt exactly as before the query existed.
    let left = stream::iter(vec![1, 2, 3]);
    let right = stream::iter(vec![4, 5, 6]);
    let vec = futures::executor::block_on(stream::select(left, right).collect::<Vec<_>>());
    assert_eq!(vec, vec![1, 4, 2, 5, 3, 6]);

    // select_next_some on a select stream, as in the doc example.
    futures::executor::block_on(async {
        let mut out = stream::select(stream::repeat(1), stream::repeat(2));
        for _ in 0..10 {
            assert_eq!(1, out.select_next_some().await);
            assert_eq!(2, out.select_next_some().await);
        }
    });

    // FusedStream::is_terminated still agrees with the new query.
    let mut cx = noop_context();
    let mut sel = Box::pin(stream::select(stream::empty::<u32>(), stream::empty::<u32>()));
    assert!(!FusedStream::is_terminated(&sel));
    assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    assert!(FusedStream::is_terminated(&sel));
    assert_eq!(sel.termination(), SelectTermination::Both);
}

/// Small in-memory + temp-dir fixture: record a run of a select stream as
/// an event log, assert the expected trace, and persist a tiny report to a
/// temporary directory to demonstrate the observed behavior.
#[test]
fn termination_report_fixture() {
    let mut cx = noop_context();

    let left_polls = counter();
    let right_polls = counter();
    let left = ScriptedStream::new(vec![some(1), NONE], left_polls.clone());
    let right = ScriptedStream::new(vec![some(10), some(20), NONE], right_polls.clone());

    let mut sel = Box::pin(stream::select(left, right));

    // In-memory fixture: the event log of the whole run.
    let mut log: Vec<String> = Vec::new();
    loop {
        let event = match sel.as_mut().poll_next(&mut cx) {
            Poll::Ready(Some(item)) => format!("yield {item}"),
            Poll::Ready(None) => "done".to_string(),
            Poll::Pending => unreachable!("scripted streams never pend in this fixture"),
        };
        let done = event == "done";
        log.push(format!("{event}; state={:?}", sel.termination()));
        if done {
            break;
        }
    }
    // A couple of extra polls to demonstrate the stable None.
    for _ in 0..2 {
        assert_eq!(sel.as_mut().poll_next(&mut cx), NONE);
    }
    log.push(format!(
        "polls: left={}, right={} (left masked after termination)",
        left_polls.get(),
        right_polls.get()
    ));

    let expected = vec![
        "yield 1; state=Neither".to_string(),
        "yield 10; state=Neither".to_string(),
        "yield 20; state=Left".to_string(),
        "done; state=Both".to_string(),
        "polls: left=2, right=3 (left masked after termination)".to_string(),
    ];
    assert_eq!(log, expected);

    // Temp-dir fixture: persist the tiny report and read it back.
    let dir = option_env!("CARGO_TARGET_TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("select_termination_report.txt");
    let report = log.join("\n") + "\n";
    std::fs::write(&path, &report).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), report);

    eprintln!("select termination report written to {}", path.display());
    eprintln!("{report}");
}
