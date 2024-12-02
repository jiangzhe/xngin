pub mod page;
pub mod ptr;

use super::latch::{HybridGuard, LatchFallbackMode};
use crate::buffer::page::{Page, PageID, INVALID_PAGE_ID};
use crate::error::{Error, Result};
use crate::latch::{GuardState, HybridLatch};
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_HUGEPAGE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE,
};
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct BufferFrame {
    pub page_id: Cell<PageID>,
    pub latch: HybridLatch, // lock proctects free list and page.
    pub next_free: UnsafeCell<PageID>,
    pub page: UnsafeCell<Page>,
}

pub struct PageGuard<'a, T> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageGuard<'a, T> {
    #[inline]
    fn new(bf: &'a BufferFrame, guard: HybridGuard<'a>) -> Self {
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
    pub fn shared(mut self) -> Result<PageSharedGuard<'a, T>> {
        if !self.guard.shared() {
            return Err(Error::RetryLatch);
        }
        Ok(PageSharedGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn exclusive(mut self) -> Result<PageExclusiveGuard<'a, T>> {
        if !self.guard.exclusive() {
            return Err(Error::RetryLatch);
        }
        Ok(PageExclusiveGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Result<PageExclusiveGuard<'a, T>> {
        if !self.guard.try_exclusive() {
            return Err(Error::RetryLatch);
        }
        Ok(PageExclusiveGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    /// Returns page with optimistic read.
    /// All values must be validated before use.
    #[inline]
    pub fn page(&self) -> &T {
        unsafe { &*(self.bf.page.get() as *const _ as *const T) }
    }

    /// Validates the optimistic version.
    #[inline]
    pub fn validate(&self) -> Result<()> {
        if !self.guard.validate() {
            return Err(Error::RetryLatch);
        }
        Ok(())
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

pub const SAFETY_PAGES: usize = 10;

pub struct FixedBufferPool {
    bfs: *mut BufferFrame,
    size: usize,
    allocated: AtomicU64,
    free_list: AtomicU64,
}

impl FixedBufferPool {
    /// Create a buffer pool with given capacity.
    #[inline]
    pub fn with_capacity(pool_size: usize) -> Result<Self> {
        let size = pool_size / mem::size_of::<BufferFrame>();
        let dram_total_size = mem::size_of::<BufferFrame>() * (size + SAFETY_PAGES);
        let bfs = unsafe {
            let big_memory_chunk = mmap(
                std::ptr::null_mut(),
                dram_total_size,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS,
                -1,
                0,
            );
            if big_memory_chunk == MAP_FAILED {
                return Err(Error::InsufficientMemory(dram_total_size));
            }
            madvise(big_memory_chunk, dram_total_size, MADV_HUGEPAGE);
            madvise(big_memory_chunk, dram_total_size, MADV_DONTFORK);
            big_memory_chunk
        } as *mut BufferFrame;
        Ok(FixedBufferPool {
            bfs,
            size,
            allocated: AtomicU64::new(0),
            free_list: AtomicU64::new(INVALID_PAGE_ID),
        })
    }

    /// Create a buffer pool with given capacity, leak it to heap
    /// and return the static reference.
    #[inline]
    pub fn with_capacity_static(pool_size: usize) -> Result<&'static Self> {
        let pool = Self::with_capacity(pool_size)?;
        let boxed = Box::new(pool);
        let leak = Box::leak(boxed);
        Ok(leak)
    }

    /// Drop static buffer pool.
    ///
    /// # Safety
    ///
    /// Caller must ensure no further use on the deallocated pool.
    pub unsafe fn drop_static(this: &'static Self) {
        drop(Box::from_raw(this as *const Self as *mut Self));
    }

    // allocate a new page with exclusive lock.
    #[inline]
    pub fn allocate_page<T>(&self) -> Result<PageGuard<'_, T>> {
        // try get from free list.
        loop {
            let page_id = self.free_list.load(Ordering::Acquire);
            if page_id == INVALID_PAGE_ID {
                break;
            }
            let bf = unsafe { &mut *self.bfs.offset(page_id as isize) };
            let new_free = unsafe { *bf.next_free.get() };
            if self
                .free_list
                .compare_exchange(page_id, new_free, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                *bf.next_free.get_mut() = INVALID_PAGE_ID;
                let guard = bf.latch.exclusive();
                return Ok(PageGuard::new(bf, guard));
            }
        }

        // try get from page pool.
        let page_id = self.allocated.fetch_add(1, Ordering::AcqRel);
        if page_id as usize >= self.size {
            return Err(Error::InsufficientBufferPool(page_id));
        }
        let bf = unsafe { &mut *self.bfs.offset(page_id as isize) };
        bf.page_id.set(page_id);
        *bf.next_free.get_mut() = INVALID_PAGE_ID; // only current thread hold the mutable ref.
        let guard = bf.latch.exclusive();
        Ok(PageGuard::new(bf, guard))
    }

    /// Returns the page guard with given page id.
    /// Caller should make sure page id is valid.
    #[inline]
    pub fn get_page<T>(
        &self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<PageGuard<'_, T>> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            return Err(Error::PageIdOutOfBound(page_id));
        }
        let bf = unsafe { &*self.bfs.offset(page_id as isize) };
        let guard = bf.latch.optimistic_fallback(mode)?;
        Ok(PageGuard::new(bf, guard))
    }

    #[inline]
    pub fn deallocate_page<T>(&self, g: PageGuard<'_, T>) {
        let mut g = g
            .exclusive()
            .expect("no one should hold lock on deallocating page");
        loop {
            let page_id = self.free_list.load(Ordering::Acquire);
            g.set_next_free(page_id);
            if self
                .free_list
                .compare_exchange(page_id, g.page_id(), Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Get child page by page id provided by parent page.
    /// The parent page guard should be provided because other thread may change page
    /// id concurrently, and the input page id may not be valid through the function
    /// call. So version must be validated before returning the buffer frame.
    #[inline]
    pub fn get_child_page<T>(
        &self,
        p_guard: PageGuard<'_, T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<PageGuard<'_, T>> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            return Err(Error::PageIdOutOfBound(page_id));
        }
        p_guard.validate()?;
        let bf = unsafe { &*self.bfs.offset(page_id as isize) };
        let c_guard = bf.latch.optimistic_fallback(mode)?;
        Ok(PageGuard::new(bf, c_guard))
    }
}

impl Drop for FixedBufferPool {
    fn drop(&mut self) {
        let dram_total_size = mem::size_of::<BufferFrame>() * (self.size + SAFETY_PAGES);
        unsafe {
            munmap(self.bfs as *mut c_void, dram_total_size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::block_index::BlockNode;

    #[test]
    fn test_fixed_buffer_pool() {
        let pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let g: PageGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 0);
        }
        {
            let g: PageGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
            pool.deallocate_page(g);
            let g: PageGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
        }
        {
            let g: PageGuard<'_, BlockNode> = pool.get_page(0, LatchFallbackMode::Jump).unwrap();
            assert_eq!(g.page_id(), 0);
        }
        assert!(pool
            .get_page::<BlockNode>(5, LatchFallbackMode::Jump)
            .is_err());

        unsafe {
            FixedBufferPool::drop_static(pool);
        }
    }
}
