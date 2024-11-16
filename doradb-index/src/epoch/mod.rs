#[macro_use]
mod macros;
mod atomic;
mod collector;
mod guard;
mod internal;
mod list;
mod queue;

mod sealed {
    pub trait Sealed {}
}

pub use atomic::{low_bits, Atomic, Inline, Owned, Pointable, PointerOrInline, Shared};
pub use guard::{unprotected, Guard};

use collector::{Collector, LocalHandle};
use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::sync::Lazy;

static COLLECTOR: Lazy<Collector> = Lazy::new(Collector::default);

thread_local! {
    static HANDLE: LocalHandle = COLLECTOR.register();
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Epoch {
    data: usize,
}

impl Epoch {
    #[inline]
    pub(crate) fn starting() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn wrapping_sub(self, rhs: Self) -> isize {
        self.data.wrapping_sub(rhs.data & !1) as isize >> 1
    }

    #[inline]
    pub(crate) fn is_pinned(self) -> bool {
        (self.data & 1) == 1
    }

    #[inline]
    pub(crate) fn pinned(self) -> Self {
        Epoch {
            data: self.data | 1,
        }
    }

    #[inline]
    pub(crate) fn unpinned(self) -> Self {
        Epoch {
            data: self.data & !1,
        }
    }

    #[inline]
    pub(crate) fn successor(self) -> Self {
        Epoch {
            data: self.data.wrapping_add(2),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct AtomicEpoch {
    data: AtomicUsize,
}

impl AtomicEpoch {
    #[inline]
    pub(crate) fn new(epoch: Epoch) -> Self {
        let data = AtomicUsize::new(epoch.data);
        AtomicEpoch { data }
    }

    #[inline]
    pub(crate) fn load(&self, ord: Ordering) -> Epoch {
        Epoch {
            data: self.data.load(ord),
        }
    }

    #[inline]
    pub(crate) fn store(&self, epoch: Epoch, ord: Ordering) {
        self.data.store(epoch.data, ord)
    }

    #[inline]
    pub(crate) fn compare_exchange(
        &self,
        current: Epoch,
        new: Epoch,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Epoch, Epoch> {
        match self
            .data
            .compare_exchange(current.data, new.data, success, failure)
        {
            Ok(data) => Ok(Epoch { data }),
            Err(data) => Err(Epoch { data }),
        }
    }
}

#[inline]
pub fn pin() -> Guard {
    with_handle(|h| h.pin())
}

#[inline]
pub fn is_pinned() -> bool {
    with_handle(|h| h.is_pinned())
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&LocalHandle) -> R,
{
    HANDLE
        .try_with(|h| f(h))
        .unwrap_or_else(|_| f(&COLLECTOR.register()))
}
