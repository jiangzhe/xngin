use super::guard::Guard;
use super::internal::{Global, Local};
use std::fmt;
use std::sync::Arc;

pub struct Collector {
    pub(crate) global: Arc<Global>,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Default for Collector {
    fn default() -> Self {
        Self {
            global: Arc::new(Global::new()),
        }
    }
}

impl Collector {
    pub fn register(&self) -> LocalHandle {
        Local::register(self)
    }
}

impl Clone for Collector {
    fn clone(&self) -> Self {
        Collector {
            global: self.global.clone(),
        }
    }
}

pub struct LocalHandle {
    pub(crate) local: *const Local,
}

impl LocalHandle {
    #[inline]
    pub fn pin(&self) -> Guard {
        unsafe { (*self.local).pin() }
    }

    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }

    #[inline]
    pub fn collector(&self) -> &Collector {
        unsafe { (*self.local).collector() }
    }
}

impl Drop for LocalHandle {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            Local::release_handle(&*self.local);
        }
    }
}

impl fmt::Debug for LocalHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("LocalHandle { .. }")
    }
}
