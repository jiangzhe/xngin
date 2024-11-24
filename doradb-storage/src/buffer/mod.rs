pub mod page;
pub mod ptr;

use crate::latch::{HybridLatch, GuardState};
use crate::error::{Result, Error};
use crate::buffer::page::{Page, PageOps, PageID, INVALID_PAGE_ID};
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::cell::{Cell, UnsafeCell};
use libc::{mmap, munmap, madvise, c_void, PROT_READ, PROT_WRITE, MAP_PRIVATE, MAP_FAILED, MAP_ANONYMOUS, MADV_HUGEPAGE, MADV_DONTFORK};
use super::latch::{HybridGuard, LatchFallbackMode};

pub struct BufferFrame {
    pub page_id: Cell<PageID>,
    pub latch: HybridLatch, // lock proctects free list and page.
    pub next_free: UnsafeCell<PageID>,
    pub page: UnsafeCell<Page>,
}

pub struct PageGuard<'a> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
}

impl<'a> PageGuard<'a> {
    #[inline]
    fn new(bf: &'a BufferFrame, guard: HybridGuard<'a>) -> Self {
        Self{bf, guard}
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    #[inline]
    pub fn shared(mut self) -> Result<PageSharedGuard<'a>> {
        if !self.guard.shared() {
            return Err(Error::RetryLatch)
        }
        Ok(PageSharedGuard{bf: self.bf, guard: self.guard})
    }

    #[inline]
    pub fn exclusive(mut self) -> Result<PageExclusiveGuard<'a>> {
        if !self.guard.exclusive() {
            return Err(Error::RetryLatch)
        }
        Ok(PageExclusiveGuard{bf: self.bf, guard: self.guard})
    }

    #[inline]
    pub fn page<P: PageOps>(&self) -> &P {
        let page_data = unsafe { & *self.bf.page.get() };
        P::cast(page_data)
    }

    #[inline]
    pub fn validate(&self) -> bool {
        self.guard.validate()
    }
}

pub struct PageSharedGuard<'a> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
}

impl<'a> PageSharedGuard<'a> {
    /// Convert a page shared guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn keepalive(mut self) -> PageGuard<'a> {
        self.guard.keepalive();
        PageGuard{bf: self.bf, guard: self.guard}
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    /// Returns shared page.
    #[inline]
    pub fn page(&self) -> &Page {
        unsafe { &*self.bf.page.get() }
    }
}

pub struct PageExclusiveGuard<'a> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
}

impl<'a> PageExclusiveGuard<'a> {
    /// Convert a page exclusive guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn keepalive(mut self) -> PageGuard<'a> {
        self.guard.keepalive();
        PageGuard{bf: self.bf, guard: self.guard}
    }
    
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id.get()
    }

    #[inline]
    pub fn page(&self) -> &Page {
        unsafe { &*self.bf.page.get() }
    }

    #[inline]
    pub fn page_mut(&mut self) -> &mut Page {
        unsafe { &mut *self.bf.page.get() }
    }

    #[inline]
    pub fn set_next_free(&mut self, next_free: PageID) {
        unsafe { *self.bf.next_free.get() = next_free; }
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
            let big_memory_chunk = mmap(std::ptr::null_mut(), dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if big_memory_chunk == MAP_FAILED {
                return Err(Error::InsufficientMemory(dram_total_size));
            }
            madvise(big_memory_chunk, dram_total_size, MADV_HUGEPAGE);
            madvise(big_memory_chunk, dram_total_size, MADV_DONTFORK);
            big_memory_chunk
        } as *mut BufferFrame;
        Ok(FixedBufferPool{
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

    // allocate a new page with exclusive lock.
    #[inline]
    pub fn allocate_page(&self) -> Result<PageGuard> {
        // try get from free list.
        loop {
            let page_id = self.free_list.load(Ordering::Acquire);
            if page_id == INVALID_PAGE_ID {
                break;
            }
            let bf = unsafe { &mut *self.bfs.offset(page_id as isize) };
            let new_free = unsafe { *bf.next_free.get() };
            if self.free_list.compare_exchange(page_id, new_free, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
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

    // should return guard with optimisitc guard and let caller lock the page with read lock or write lock.
    #[inline]
    pub fn get_page(&self, page_id: PageID, mode: LatchFallbackMode) -> Result<PageGuard> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            return Err(Error::PageIdOutOfBound(page_id))
        }
        let bf = unsafe { &*self.bfs.offset(page_id as isize) };
        let guard = bf.latch.optimistic_fallback(mode)?;
        Ok(PageGuard::new(bf, guard))
    }

    #[inline]
    pub fn deallocate_page(&self, g: PageGuard) {
        let mut g = g.exclusive().expect("no one should hold lock on deallocating page");
        loop {
            let page_id = self.free_list.load(Ordering::Acquire);
            g.set_next_free(page_id);
            if self.free_list.compare_exchange(page_id, g.page_id(), Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                return;
            }
        }
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

    #[test]
    fn test_fixed_buffer_pool() {
        let pool = FixedBufferPool::with_capacity_static(64*1024*1024).unwrap();
        {
            let g = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 0);    
        }
        {
            let g = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
            pool.deallocate_page(g);
            let g = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
        }
        {
            let g = pool.get_page(0, LatchFallbackMode::Jump).unwrap();
            assert_eq!(g.page_id(), 0);
        }
        assert!(pool.get_page(5, LatchFallbackMode::Jump).is_err());
    }
}
