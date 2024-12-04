use crate::latch::HybridGuard;
use crate::buffer::page::PageID;
use crate::buffer::frame::BufferFrame;
use crate::error::{Result, Validation, Validation::{Valid, Invalid}};
use crate::latch::GuardState;
use std::marker::PhantomData;

pub struct PageGuard<'a, T> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageGuard<'a, T> {
    #[inline]
    pub(super) fn new(bf: &'a BufferFrame, guard: HybridGuard<'a>) -> Self {
        Self {
            bf,
            guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<'a, T>> {
        self.guard.try_shared().map(|_| PageSharedGuard{
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<'a, T>> {
        self.guard.try_exclusive().map(|_| PageExclusiveGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    /// Returns page with optimistic read.
    /// All values must be validated before use.
    #[inline]
    pub fn page_unchecked(&self) -> &T {
        unsafe { &*(self.bf.page.get() as *const _ as *const T) }
    }

    /// Validates version not change.
    /// In optimistic mode, this means no other thread change
    /// the protected object inbetween.
    /// In shared/exclusive mode, the validation will always
    /// succeed because no one can change the protected object
    /// (acquire exclusive lock) at the same time.
    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if !self.guard.validate() {
            debug_assert!(self.guard.state == GuardState::Optimistic);
            return Invalid;
        }
        Valid(())
    }

    /// Returns a copy of optimistic guard.
    /// Otherwise fail.
    #[inline]
    pub fn copy_keepalive(&self) -> Result<Self> {
        let guard = self.guard.optimistic_clone()?;
        Ok(PageGuard {
            bf: self.bf,
            guard,
            _marker: PhantomData,
        })
    }

    /// Downgrade read/write lock to optimistic lock.
    #[inline]
    pub fn downgrade(&mut self) {
        self.guard.downgrade(); // convert read/write lock to optimistic lock
    }

    /// Returns whether the guard is in optimistic mode.
    #[inline]
    pub fn is_optimistic(&self) -> bool {
        self.guard.state == GuardState::Optimistic
    }
}

pub struct PageSharedGuard<'a, T> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageSharedGuard<'a, T> {
    /// Convert a page shared guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(&mut self) {
        self.guard.downgrade();
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    /// Returns shared page.
    #[inline]
    pub fn page(&self) -> &T {
        unsafe { &*(self.bf.page.get() as *const _ as *const T) }
    }
}

pub struct PageExclusiveGuard<'a, T> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> PageExclusiveGuard<'a, T> {
    /// Convert a page exclusive guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(&mut self) {
        self.guard.downgrade();
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    #[inline]
    pub fn page(&self) -> &T {
        unsafe { &*(self.bf.page.get() as *const T) }
    }

    #[inline]
    pub fn page_mut(&mut self) -> &mut T {
        unsafe { &mut *(self.bf.page.get() as *mut T) }
    }

    #[inline]
    pub fn set_next_free(&mut self, next_free: PageID) {
        unsafe {
            *self.bf.next_free.get() = next_free;
        }
    }
}
