#[macro_use]
mod macros;
mod atomic;
mod collector;
mod epoch;
mod guard;
mod internal;
mod list;
mod queue;

mod sealed {
    pub trait Sealed {}
}

pub use atomic::{low_bits, Atomic, Inline, Owned, Pointable, PointerOrInline, Shared};
pub use epoch::Epoch;
pub use guard::{unprotected, Guard};

use collector::{Collector, LocalHandle};

use once_cell::sync::Lazy;

static COLLECTOR: Lazy<Collector> = Lazy::new(Collector::default);

thread_local! {
    static HANDLE: LocalHandle = COLLECTOR.register();
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
