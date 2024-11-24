use crate::buffer::page::{LSN, Page, PageID, PageOps, PAGE_SIZE};
use crate::buffer::{FixedBufferPool, PageGuard};
use crate::error::{Result, Error};
use crate::latch::LatchFallbackMode;
use std::sync::atomic::AtomicU64;
use std::mem;

pub const BLOCK_SIZE: usize = 1272;
pub const NBR_BLOCKS_IN_LEAF: usize = 51;
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const NBR_PAGES_IN_ROW_BLOCK: usize = 78;
pub type RowID = u64;
pub type Block = [u8; BLOCK_SIZE];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum BlockKind {
    Row = 1,
    Col = 2,
}

/// The inner node of block index, including root.
#[repr(C)]
pub struct BlockBranch {
    // node type
    pub height: u64,
    // log sequence number
    pub lsn: LSN,
    // start row id of this inner node
    pub row_id: RowID,
    // count of entry.
    pub count: u64,
    // padding to make the total bytes to be 64K.
    padding: [u8; 16],
    // entries
    pub entries: [PageEntry; NBR_ENTRIES_IN_BRANCH],
}

impl PageOps for BlockBranch {
    #[inline]
    fn init(page: &mut [u8; PAGE_SIZE], height: usize) -> &mut BlockBranch {
        let branch = unsafe { &mut *(page as *mut _ as *mut BlockBranch) };
        branch.height = height as u64;
        branch.lsn = 0;
        branch.row_id = 0;
        branch.count = 0;
        branch
    }
}

/// The leaf node of block index.
#[repr(C)]
pub struct BlockLeaf {
    // height of the node
    pub height: u64,
    // log sequence number
    pub lsn: LSN,
    // start row id of this leaf
    pub row_id: RowID,
    // count of entry.
    pub count: u64,
    // padding to make the total bytes to be 64K.
    padding: [u8; 640],
    // list of block header.
    pub blocks: [Block; NBR_BLOCKS_IN_LEAF],
}

impl PageOps for BlockLeaf {
    #[inline]
    fn init(page: &mut Page, height: usize) -> &mut Self {
        let leaf = unsafe { &mut *(page as *mut _ as *mut BlockLeaf) };
        leaf.height = height as u64;
        leaf.lsn = 0;
        leaf.row_id = 0;
        leaf.count = 0;
        leaf
    }
}

impl BlockLeaf {
    #[inline]
    pub fn is_full(&self) -> bool {
        self.count as usize == NBR_BLOCKS_IN_LEAF
    }
}

pub struct PageEntry {
    pub row_id: RowID,
    pub page_id: PageID,
}

pub trait BlockOps: Sized {
    #[inline]
    fn cast(block: &Block) -> &Self {
        unsafe { &*(block as *const _ as *const Self) }
    }
    
    #[inline]
    fn cast_mut(block: &mut Block) -> &mut Self {
        unsafe { &mut *(block as *mut _ as *mut Self) }
    }
}

#[repr(C)]
pub struct RowBlock {
    pub kind: BlockKind,
    pub count: u32,
    pub start_row_id: RowID,
    pub end_row_id: RowID,
    pub entries: [PageEntry; NBR_PAGES_IN_ROW_BLOCK],
}

impl RowBlock {
    #[inline]
    pub fn is_full(&self) -> bool {
        self.count as usize == NBR_PAGES_IN_ROW_BLOCK
    }
}

impl BlockOps for RowBlock {}

#[repr(C)]
pub struct ColBlock {
    pub kind: BlockKind,
    padding1: [u8; 4],
    pub row_id: RowID,
    pub count: u64,
    pub entries: [ColSegmentMeta; 16],
    // maybe include some column statistics.
    padding2: [u8; 992],
}

impl BlockOps for ColBlock {}

#[repr(C)]
pub struct ColSegmentMeta {
    pub row_id: RowID,
    pub count: u64,
}

pub struct BlockIndex<'a> {
    buf_pool: &'a FixedBufferPool,
    root: PageID,
    max_row_id: AtomicU64, // maximum row id, exclusive.
}

impl<'a> BlockIndex<'a> {
    #[inline]
    pub fn new(buf_pool: &'a FixedBufferPool) -> Result<Self> {
        let root_page = buf_pool.allocate_page()?;
        let page_id = root_page.page_id();
        let mut g = root_page.exclusive()?;
        let page = g.page_mut();
        let _ = BlockLeaf::init(page, 0);
        Ok(BlockIndex{
            buf_pool,
            root: page_id,
            max_row_id: AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn insert_row_page(&self, count: u64) -> Result<PageGuard> {
        todo!()
    }

    #[inline]
    pub fn find_row_id(&self, row_id: RowID) -> Result<RowLocation> {
        let mut g = self.buf_pool.get_page(self.root, LatchFallbackMode::Spin)?;
        todo!()
    }

    
}

pub enum RowLocation {
    ColSegment(u64, u64),
    RowPage(PageID),
}