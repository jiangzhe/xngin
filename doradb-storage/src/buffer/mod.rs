pub mod frame;
pub mod guard;
pub mod page;
pub mod ptr;

use crate::buffer::frame::{BufferFrame, BufferFrameAware};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::{PageID, INVALID_PAGE_ID};
use crate::error::{Error, Result, Validation, Validation::Valid};
use crate::latch::LatchFallbackMode;
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_HUGEPAGE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

pub const SAFETY_PAGES: usize = 10;

pub struct FixedBufferPool {
    bfs: *mut BufferFrame,
    size: usize,
    allocated: AtomicU64,
    free_list: Mutex<PageID>,
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
            free_list: Mutex::new(INVALID_PAGE_ID),
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

    #[inline]
    fn try_get_page_from_free_list<T: BufferFrameAware>(
        &self,
    ) -> Option<PageExclusiveGuard<'_, T>> {
        unsafe {
            let bf = {
                let mut page_id = self.free_list.lock();
                if *page_id == INVALID_PAGE_ID {
                    return None;
                }
                let bf = self.get_bf(*page_id);
                *page_id = (*bf.get()).next_free;
                bf
            };
            (*bf.get()).next_free = INVALID_PAGE_ID;
            T::init_bf(self, &mut (*bf.get()));
            let g = init_bf_exclusive_guard(bf);
            Some(g)
        }
    }

    // allocate a new page with exclusive lock.
    #[inline]
    pub fn allocate_page<T: BufferFrameAware>(&self) -> Result<PageExclusiveGuard<'_, T>> {
        // try get from free list.
        if let Some(g) = self.try_get_page_from_free_list() {
            return Ok(g);
        }
        // try get from page pool.
        let page_id = self.allocated.fetch_add(1, Ordering::AcqRel);
        if page_id as usize >= self.size {
            return Err(Error::InsufficientBufferPool(page_id));
        }
        unsafe {
            let bf = self.get_bf(page_id);
            (*bf.get()).page_id = page_id;
            (*bf.get()).next_free = INVALID_PAGE_ID;
            T::init_bf(self, &mut *bf.get());
            let g = init_bf_exclusive_guard(bf);
            Ok(g)
        }
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
        Ok(self.get_page_internal(page_id, mode))
    }

    #[inline]
    fn get_page_internal<T>(&self, page_id: PageID, mode: LatchFallbackMode) -> PageGuard<'_, T> {
        unsafe {
            let bf = self.get_bf(page_id);
            let g = (*bf.get()).latch.optimistic_fallback(mode);
            PageGuard::new(bf, g)
        }
    }

    #[inline]
    unsafe fn get_bf(&self, page_id: PageID) -> &UnsafeCell<BufferFrame> {
        let bf_ptr = self.bfs.offset(page_id as isize);
        &*(bf_ptr as *mut UnsafeCell<BufferFrame>)
    }

    /// Deallocate page.
    #[inline]
    pub fn deallocate_page<T: BufferFrameAware>(&self, mut g: PageExclusiveGuard<'_, T>) {
        T::deinit_bf(self, g.bf_mut());
        let mut page_id = self.free_list.lock();
        g.bf_mut().next_free = *page_id;
        *page_id = g.page_id();
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
    ) -> Validation<Result<PageGuard<'_, T>>> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            return Valid(Err(Error::PageIdOutOfBound(page_id)));
        }
        let g = self.get_page_internal(page_id, mode);
        // apply lock coupling.
        // the validation make sure parent page does not change until child
        // page is acquired.
        p_guard.validate().and_then(|_| Valid(Ok(g)))
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

unsafe impl Sync for FixedBufferPool {}

#[inline]
fn init_bf_exclusive_guard<T: BufferFrameAware>(
    bf: &UnsafeCell<BufferFrame>,
) -> PageExclusiveGuard<'_, T> {
    unsafe {
        let g = (*bf.get()).latch.exclusive();
        PageGuard::new(bf, g)
            .try_exclusive()
            .expect("free page owns exclusive lock")
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
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 0);
        }
        {
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
            pool.deallocate_page(g);
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page().unwrap();
            assert_eq!(g.page_id(), 1);
        }
        {
            let g: PageGuard<'_, BlockNode> = pool.get_page(0, LatchFallbackMode::Spin).unwrap();
            assert_eq!(unsafe { g.page_id() }, 0);
        }
        assert!(pool
            .get_page::<BlockNode>(5, LatchFallbackMode::Spin)
            .is_err());

        unsafe {
            FixedBufferPool::drop_static(pool);
        }
    }
}
