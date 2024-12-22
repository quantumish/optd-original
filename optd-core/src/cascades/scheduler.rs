// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! A single-thread scheduler for the cascades tasks. The tasks are queued in a stack of `Vec` so that
//! we won't overflow the system stack. The cascades task are compute-only and don't have I/O.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Wake},
};

struct Task {
    // The task to be executed.
    inner: Pin<Box<dyn Future<Output = ()> + 'static>>,
}

pub struct Executor {}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        unreachable!("cascades tasks shouldn't yield");
    }
}

// This needs nightly feature and we use stable Rust, so we had to copy-paste it here. TODO: license

mod optd_futures_task {
    use std::{
        ptr,
        task::{RawWaker, RawWakerVTable, Waker},
    };
    const NOOP: RawWaker = {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            // Cloning just returns a new no-op raw waker
            |_| NOOP,
            // `wake` does nothing
            |_| {},
            // `wake_by_ref` does nothing
            |_| {},
            // Dropping does nothing as we don't allocate anything
            |_| {},
        );
        RawWaker::new(ptr::null(), &VTABLE)
    };

    #[inline]
    #[must_use]
    pub const fn noop() -> &'static Waker {
        const WAKER: &Waker = &unsafe { Waker::from_raw(NOOP) };
        WAKER
    }
}

thread_local! {
    pub static OPTD_SCHEDULER_QUEUE: RefCell<Vec<Task>> = RefCell::new(Vec::new());
}

pub fn spawn<F>(task: F)
where
    F: Future<Output = ()> + 'static,
{
    OPTD_SCHEDULER_QUEUE.with_borrow_mut(|tasks| {
        tasks.push(
            Task {
                inner: Box::pin(task),
            }
            .into(),
        )
    });
}

impl Executor {
    pub fn new() -> Self {
        Executor {}
    }

    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        spawn(task);
    }

    /// SAFETY: The caller must ensure all futures running on this runtime does not have I/O. Otherwise it will deadloop
    /// with all futures pending.
    pub fn run(&self) {
        let waker = optd_futures_task::noop();
        let mut cx: Context<'_> = Context::from_waker(&waker);

        while let Some(mut task) = OPTD_SCHEDULER_QUEUE.with_borrow_mut(|tasks| tasks.pop()) {
            if task.inner.as_mut().poll(&mut cx).is_pending() {
                OPTD_SCHEDULER_QUEUE.with_borrow_mut(|tasks| tasks.push(task))
            }
        }
    }
}
