use crate::buffer::page::{Page, PageID};
use crate::buffer::FixedBufferPool;
use crate::latch::HybridLatch;
use crate::trx::undo::UndoMap;

const _: () = assert!(
    { std::mem::size_of::<BufferFrame>() % 64 == 0 },
    "Size of BufferFrame must be multiply of 64"
);

const _: () = assert!(
    { std::mem::align_of::<BufferFrame>() % 64 == 0 },
    "Align of BufferFrame must be multiply of 64"
);

#[repr(C)]
pub struct BufferFrame {
    pub page_id: PageID,
    pub next_free: PageID,
    /// Undo Map is only maintained by RowPage.
    /// Once a RowPage is eliminated, the UndoMap is retained by BufferPool
    /// and when the page is reloaded, UndoMap is reattached to page.
    pub undo_map: Option<Box<UndoMap>>,
    pub latch: HybridLatch, // lock proctects free list and page.
    pub page: Page,
}

/// BufferFrameAware defines callbacks on lifecycle of buffer frame
/// for initialization and de-initialization.
pub trait BufferFrameAware {
    /// This callback is called when a page is just loaded into BufferFrame.
    fn init_bf(_pool: &FixedBufferPool, _bf: &mut BufferFrame) {}

    /// This callback is called when a page is cleaned and return to BufferPool.
    fn deinit_bf(_pool: &FixedBufferPool, _bf: &mut BufferFrame) {}
}
