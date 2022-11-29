use super::atomic::{Pointable, Shared};
use super::collector::Collector;
use super::internal::Deferred;
use super::internal::Local;
use scopeguard::defer;
use std::fmt;
use std::mem;
use std::ptr;

pub struct Guard {
    pub(crate) local: *const Local,
}

impl Guard {
    pub fn defer<F, R>(&self, f: F)
    where
        F: FnOnce() -> R,
        F: Send + 'static,
    {
        unsafe {
            self.defer_unchecked(f);
        }
    }

    pub unsafe fn defer_unchecked<F, R>(&self, f: F)
    where
        F: FnOnce() -> R,
    {
        if let Some(local) = self.local.as_ref() {
            local.defer(Deferred::new(move || drop(f())), self);
        } else {
            drop(f());
        }
    }

    pub unsafe fn defer_destroy<T: Pointable>(&self, ptr: Shared<'_, T>) {
        self.defer_unchecked(move || ptr.try_into_owned());
    }

    pub fn flush(&self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.flush(self);
        }
    }

    pub fn repin(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.repin();
        }
    }

    pub fn repin_after<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.acquire_handle();
            local.unpin();
        }

        defer! {
            if let Some(local) = unsafe { self.local.as_ref() } {
                mem::forget(local.pin());
                local.release_handle();
            }
        }

        f()
    }

    pub fn collector(&self) -> Option<&Collector> {
        unsafe { self.local.as_ref().map(|local| local.collector()) }
    }
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.unpin();
        }
    }
}

impl fmt::Debug for Guard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Guard { .. }")
    }
}

#[inline]
pub unsafe fn unprotected() -> &'static Guard {
    struct GuardWrapper(Guard);
    unsafe impl Sync for GuardWrapper {}
    static UNPROTECTED: GuardWrapper = GuardWrapper(Guard { local: ptr::null() });
    &UNPROTECTED.0
}
