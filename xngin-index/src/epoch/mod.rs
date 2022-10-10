#[macro_use]
mod macros;
mod atomic;
mod epoch;
mod internal;
mod list;
mod collector;
mod guard;
mod queue;

mod sealed {
    pub trait Sealed {}
}

pub use epoch::Epoch;
pub use guard::{Guard, unprotected};
pub use atomic::{Atomic, Owned, Shared, Inline, Pointable, PointerOrInline, low_bits};

use collector::{Collector, LocalHandle};

use once_cell::sync::Lazy;

static COLLECTOR: Lazy<Collector> = Lazy::new(Collector::default);

thread_local!{
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
    HANDLE.try_with(|h| f(h))
        .unwrap_or_else(|_| f(&COLLECTOR.register()))
}