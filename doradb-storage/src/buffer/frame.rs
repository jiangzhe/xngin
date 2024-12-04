use std::cell::{Cell, UnsafeCell};
use crate::buffer::page::{Page, PageID};
use crate::latch::HybridLatch;

pub struct BufferFrame {
    pub page_id: Cell<PageID>,
    pub latch: HybridLatch, // lock proctects free list and page.
    pub next_free: UnsafeCell<PageID>,
    pub page: UnsafeCell<Page>,
}