use crate::buffer::frame::BufferFrame;
use crate::buffer::page::PageID;
use crate::error::{
    Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::GuardState;
use crate::latch::HybridGuard;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;

pub struct PageGuard<'a, T> {
    bf: &'a UnsafeCell<BufferFrame>,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageGuard<'a, T> {
    #[inline]
    pub(super) fn new(bf: &'a UnsafeCell<BufferFrame>, guard: HybridGuard<'a>) -> Self {
        Self {
            bf,
            guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn page_id(&self) -> PageID {
        (*self.bf.get()).page_id
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<'a, T>> {
        self.guard.try_shared().map(|_| PageSharedGuard {
            bf: unsafe { &*self.bf.get() },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn block_until_shared(self) -> PageSharedGuard<'a, T> {
        match self.guard.state {
            GuardState::Exclusive => PageSharedGuard {
                bf: unsafe { &*self.bf.get() },
                guard: self.guard,
                _marker: PhantomData,
            },
            GuardState::Shared => {
                unimplemented!("lock downgrade from exclusive to shared is not supported")
            }
            GuardState::Optimistic => {
                let guard = self.guard.block_until_shared();
                PageSharedGuard {
                    bf: unsafe { &*self.bf.get() },
                    guard,
                    _marker: PhantomData,
                }
            }
        }
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<'a, T>> {
        self.guard.try_exclusive().map(|_| PageExclusiveGuard {
            bf: unsafe { &mut *self.bf.get() },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn block_until_exclusive(self) -> PageExclusiveGuard<'a, T> {
        match self.guard.state {
            GuardState::Exclusive => PageExclusiveGuard {
                bf: unsafe { &mut *self.bf.get() },
                guard: self.guard,
                _marker: PhantomData,
            },
            GuardState::Shared => {
                unimplemented!("lock upgradate from shared to exclusive is not supported")
            }
            GuardState::Optimistic => {
                let guard = self.guard.block_until_exclusive();
                PageExclusiveGuard {
                    bf: unsafe { &mut *self.bf.get() },
                    guard,
                    _marker: PhantomData,
                }
            }
        }
    }

    /// Returns page with optimistic read.
    /// All values must be validated before use.
    #[inline]
    pub unsafe fn page_unchecked(&self) -> &T {
        let bf = self.bf.get();
        mem::transmute(&(*bf).page)
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

    /// Returns the buffer frame current page associated.
    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.bf
    }

    /// Returns current page id.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id
    }

    /// Returns shared page.
    #[inline]
    pub fn page(&self) -> &T {
        unsafe { mem::transmute(&self.bf.page) }
    }
}

pub struct PageExclusiveGuard<'a, T> {
    bf: &'a mut BufferFrame,
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

    /// Returns current page id.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id
    }

    /// Returns current page.
    #[inline]
    pub fn page(&self) -> &T {
        unsafe { mem::transmute(&self.bf.page) }
    }

    /// Returns mutable page.
    #[inline]
    pub fn page_mut(&mut self) -> &mut T {
        unsafe { mem::transmute(&mut self.bf.page) }
    }

    /// Returns current buffer frame.
    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        &self.bf
    }

    /// Returns mutable buffer frame.
    #[inline]
    pub fn bf_mut(&mut self) -> &mut BufferFrame {
        &mut self.bf
    }

    /// Set next free page.
    #[inline]
    pub fn set_next_free(&mut self, next_free: PageID) {
        self.bf.next_free = next_free;
    }
}
