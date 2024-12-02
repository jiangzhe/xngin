use crate::buffer::page::{PageID, LSN, PAGE_SIZE};
use crate::buffer::{FixedBufferPool, PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::error::{Error, Result};
use crate::latch::LatchFallbackMode;
use crate::row::RowPage;
use parking_lot::Mutex;
use std::mem;

pub const BLOCK_PAGE_SIZE: usize = PAGE_SIZE;
pub const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockNodeHeader>();
pub const BLOCK_SIZE: usize = 1272;
pub const NBR_BLOCKS_IN_LEAF: usize = 51;
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const NBR_PAGES_IN_ROW_BLOCK: usize = 78;
pub const NBR_SEGMENTS_IN_COL_BLOCK: usize = 16;
pub type RowID = u64;
// pub type Block = [u8; BLOCK_SIZE];
// header 32 bytes, padding 16 bytes.
pub const BLOCK_BRANCH_ENTRY_START: usize = 48;
// header 32 bytes, padding 640 bytes.
pub const BLOCK_LEAF_ENTRY_START: usize = 672;

/// BlockKind can be Row or Col.
/// Row Block contains 78 row page ids.
/// Col block represent a columnar file and
/// stores segment information inside the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum BlockKind {
    Row = 1,
    Col = 2,
}

/// BlockNode is B-Tree node of block index.
/// It can be either branch or leaf.
/// Branch contains at most 4093 child node pointers.
/// Leaf contains at most 51 block headers.
#[repr(C)]
#[derive(Clone)]
pub struct BlockNode {
    pub header: BlockNodeHeader,
    padding: [u8; BLOCK_PAGE_SIZE - BLOCK_HEADER_SIZE],
}

impl BlockNode {
    /// Returns whether the block node is leaf.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns whether the block node is branch.
    #[inline]
    pub fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    #[inline]
    fn ptr(&self) -> *mut u8 {
        self as *const Self as *mut Self as *mut u8
    }

    /* branch methods */

    /// Returns whether the branch node is full.
    #[inline]
    pub fn branch_is_full(&self) -> bool {
        debug_assert!(self.is_branch());
        self.header.count as usize == NBR_ENTRIES_IN_BRANCH
    }

    /// Returns whether the branch node is empty.
    #[inline]
    pub fn branch_is_empty(&self) -> bool {
        debug_assert!(self.is_branch());
        self.header.count == 0
    }

    /// Returns the entry slice in branch node.
    #[inline]
    pub fn branch_entries(&self) -> &[PageEntry] {
        debug_assert!(self.is_branch());
        unsafe { std::slice::from_raw_parts(self.branch_entry_ptr(), self.header.count as usize) }
    }

    /// Returns mutable entry slice in branch node.
    #[inline]
    pub fn branch_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_branch());
        unsafe {
            std::slice::from_raw_parts_mut(self.branch_entry_ptr(), self.header.count as usize)
        }
    }

    /// Returns entry in branch node by given index.
    #[inline]
    pub fn branch_entry(&self, idx: usize) -> &PageEntry {
        &self.branch_entries()[idx]
    }

    /// Returns last entry in branch node.
    #[inline]
    pub fn branch_last_entry(&self) -> &PageEntry {
        debug_assert!(self.is_branch());
        &self.branch_entries()[self.header.count as usize - 1]
    }

    /// Add a new entry in branch node.
    #[inline]
    pub fn branch_add_entry(&mut self, entry: PageEntry) {
        debug_assert!(self.is_branch());
        assert!((self.header.count as usize) < NBR_ENTRIES_IN_BRANCH);
        let idx = self.header.count;
        self.header.count += 1;
        self.branch_entries_mut()[idx as usize] = entry;
    }

    #[inline]
    fn branch_entry_ptr(&self) -> *mut PageEntry {
        debug_assert!(self.is_branch());
        unsafe { self.ptr().add(BLOCK_BRANCH_ENTRY_START) as *mut PageEntry }
    }

    /* leaf methods */

    /// Returns whether the node is leaf.
    #[inline]
    pub fn leaf_is_full(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count as usize == NBR_BLOCKS_IN_LEAF
    }

    /// Returns whether the leaf node is empty.
    #[inline]
    pub fn leaf_is_empty(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count == 0
    }

    /// Returns block slice of leaf node.
    #[inline]
    pub fn leaf_blocks(&self) -> &[Block] {
        debug_assert!(self.is_leaf());
        unsafe { std::slice::from_raw_parts(self.leaf_block_ptr(), self.header.count as usize) }
    }

    /// Returns block in leaf node by given index.
    #[inline]
    pub fn leaf_block(&self, idx: usize) -> &Block {
        debug_assert!(self.is_leaf());
        &self.leaf_blocks()[idx]
    }

    /// Returns last block in leaf node.
    #[inline]
    pub fn leaf_last_block(&self) -> &Block {
        debug_assert!(self.is_leaf());
        &self.leaf_blocks()[self.header.count as usize - 1]
    }

    /// Returns mutable last block in leaf node.
    #[inline]
    pub fn leaf_last_block_mut(&self) -> &mut Block {
        debug_assert!(self.is_leaf());
        &mut self.leaf_blocks_mut()[self.header.count as usize - 1]
    }

    /// Returns mutable block slice in leaf node.
    #[inline]
    pub fn leaf_blocks_mut(&self) -> &mut [Block] {
        debug_assert!(self.is_leaf());
        unsafe { std::slice::from_raw_parts_mut(self.leaf_block_ptr(), self.header.count as usize) }
    }

    /// Add a new block in leaf node.
    #[inline]
    pub fn leaf_add_block(&mut self, count: u64, page_id: PageID) {
        debug_assert!(self.is_leaf());
        debug_assert!(!self.leaf_is_full());
        self.header.count += 1;
        let b = &mut self.leaf_blocks_mut()[self.header.count as usize - 1];
        b.init_row(self.header.end_row_id, count, page_id);
        self.header.end_row_id += count;
    }

    #[inline]
    fn leaf_block_ptr(&self) -> *mut Block {
        debug_assert!(self.is_leaf());
        unsafe { self.ptr().add(BLOCK_LEAF_ENTRY_START) as *mut Block }
    }
}

#[repr(C)]
#[derive(Clone)]
pub struct BlockNodeHeader {
    // height of the node
    pub height: u32,
    // count of entry.
    pub count: u32,
    // log sequence number
    pub lsn: LSN,
    // start row id of the node.
    pub start_row_id: RowID,
    // end row id of the node.
    // note: this value may not be valid if the node is branch.
    pub end_row_id: RowID,
}

#[repr(C)]
pub struct PageEntry {
    pub row_id: RowID,
    pub page_id: PageID,
}

impl PageEntry {
    #[inline]
    pub fn new(row_id: RowID, page_id: PageID) -> Self {
        PageEntry { row_id, page_id }
    }
}

#[repr(C)]
pub struct BlockHeader {
    pub kind: BlockKind,
    pub count: u32,
    pub start_row_id: RowID,
    pub end_row_id: RowID,
}

/// Block is an abstraction on data distribution.
/// Block has two kinds: row and column.
/// Row block contains at most 78 row page ids with
/// its associated min row id.
/// Column block represents one on-disk columnar file
/// with its row id range and statistics.
#[repr(C)]
pub struct Block {
    pub header: BlockHeader,
    padding: [u8; BLOCK_SIZE - mem::size_of::<BlockHeader>()],
}

impl Block {
    /// Initialize block with single row page info.
    #[inline]
    pub fn init_row(&mut self, row_id: RowID, count: u64, page_id: PageID) {
        self.header.kind = BlockKind::Row;
        self.header.count = 0;
        self.header.start_row_id = row_id;
        self.header.end_row_id = row_id;
        self.row_add_page(count, page_id);
    }

    /// Returns whether the block is row block.
    #[inline]
    pub fn is_row(&self) -> bool {
        self.header.kind == BlockKind::Row
    }

    /// Returns whether the block is column block.
    #[inline]
    pub fn is_col(&self) -> bool {
        !self.is_row()
    }

    #[inline]
    fn data_ptr(&self) -> *const u8 {
        self.padding.as_ptr()
    }

    #[inline]
    fn data_ptr_mut(&mut self) -> *mut u8 {
        self.padding.as_mut_ptr()
    }

    /* row block methods */

    /// Returns page entry slice in row block.
    #[inline]
    pub fn row_page_entries(&self) -> &[PageEntry] {
        let ptr = self.data_ptr() as *const PageEntry;
        unsafe { std::slice::from_raw_parts(ptr, self.header.count as usize) }
    }

    /// Returns mutable page entry slice in row block.
    #[inline]
    pub fn row_page_entries_mut(&mut self) -> &mut [PageEntry] {
        let ptr = self.data_ptr_mut() as *mut PageEntry;
        unsafe { std::slice::from_raw_parts_mut(ptr, self.header.count as usize) }
    }

    /// Add a new page entry in row block.
    #[inline]
    pub fn row_add_page(&mut self, count: u64, page_id: PageID) -> u64 {
        assert!((self.header.count as usize) < NBR_PAGES_IN_ROW_BLOCK);
        let entry = PageEntry {
            row_id: self.header.end_row_id,
            page_id,
        };
        let idx = self.header.count as usize;
        self.header.count += 1;
        self.row_page_entries_mut()[idx] = entry;
        self.header.end_row_id += count;
        self.header.end_row_id
    }

    /// Returns whether the row block is full.
    #[inline]
    pub fn row_is_full(&self) -> bool {
        self.header.count as usize == NBR_PAGES_IN_ROW_BLOCK
    }

    /* col block methods */
}

#[repr(C)]
pub struct ColSegmentMeta {
    pub row_id: RowID,
    pub count: u64,
}

/// The base index of blocks.
///
/// It controls block/page level storage information
/// of column-store and row-store.
///
/// The index is sorted by RowID, which is global unique identifier
/// of each row.
/// When inserting a new row, a row page must be located, either by
/// allocating a new page from buffer pool, or by reusing a non-full
/// page.
/// The row page determines its RowID range by increasing max row id
/// of the block index with estimated row count.
///
/// Old rows have smaller RowID, new rows have bigger RowID.
/// Once all data in one row page can be seen by all active transactions,
/// it is qualified to be persisted to disk in column format.
///
/// Multiple row pages are merged to be a column file.
/// Block index will also be updated to reflect the change.
///
/// The block index supports two operations.
///
/// 1. index search with row id: determine which column file or row page
///    one RowID belongs to.
/// 2. table scan: traverse all column files and row pages to perform
///    full table scan.
///
pub struct BlockIndex<'a> {
    buf_pool: &'a FixedBufferPool,
    root: PageID,
    insert_free_list: Mutex<Vec<PageID>>,
}

impl<'a> BlockIndex<'a> {
    /// Create a new block index backed by buffer pool.
    #[inline]
    pub fn new(buf_pool: &'a FixedBufferPool) -> Result<Self> {
        let root_page = buf_pool.allocate_page()?;
        let page_id = root_page.page_id();
        let mut g: PageExclusiveGuard<'a, BlockNode> = root_page.exclusive()?;
        let page = g.page_mut();
        page.header.height = 0;
        page.header.count = 0;
        page.header.lsn = 0;
        page.header.start_row_id = 0;
        page.header.end_row_id = 0;
        Ok(BlockIndex {
            buf_pool,
            root: page_id,
            insert_free_list: Mutex::new(Vec::with_capacity(64)),
        })
    }

    /// Get row page for insertion.
    /// Caller should cache insert page id to avoid invoking this method frequently.
    #[inline]
    pub fn get_insert_page(&self, row_len: usize) -> Result<PageExclusiveGuard<'a, RowPage>> {
        if let Ok(free_page) = self.get_insert_page_from_free_list() {
            return Ok(free_page);
        }
        let count = estimate_row_count_in_page(row_len);
        let new_page: PageExclusiveGuard<'a, RowPage> =
            self.buf_pool.allocate_page()?.exclusive()?;
        let new_page_id = new_page.page_id();
        loop {
            match self.insert_row_page(count, new_page_id) {
                Ok(_) => return Ok(new_page),
                Err(Error::RetryLatch) => (),
                Err(e) => return Err(e),
            }
        }
    }

    /// Find location of given row id, maybe in column file or row page.
    #[inline]
    pub fn find_row_id(&self, row_id: RowID) -> Result<RowLocation> {
        loop {
            match self.try_find_row_id(row_id) {
                Err(Error::RetryLatch) => (),
                res => return res,
            }
        }
    }

    /// Put given page into insert free list.
    #[inline]
    pub fn free_exclusive_insert_page(&self, guard: PageExclusiveGuard<'a, RowPage>) {
        let page_id = guard.page_id();
        drop(guard);
        let mut free_list = self.insert_free_list.lock();
        free_list.push(page_id);
    }

    /// Returns the cursor starts from given row id.
    #[inline]
    pub fn cursor_shared(&self, row_id: RowID) -> Result<CursorShared<'_>> {
        let mut stack = vec![];
        loop {
            match self.try_find_leaf_by_row_id(row_id, &mut stack) {
                Ok(_) => break,
                Err(Error::RetryLatch) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(CursorShared {
            blk_idx: self,
            stack,
        })
    }

    #[inline]
    fn get_insert_page_from_free_list(&self) -> Result<PageExclusiveGuard<'a, RowPage>> {
        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Err(Error::EmptyFreeListOfBufferPool);
            }
            g.pop().unwrap()
        };
        let page: PageGuard<'a, RowPage> = self
            .buf_pool
            .get_page(page_id, LatchFallbackMode::Exclusive)?;
        page.exclusive()
    }

    #[inline]
    fn insert_row_page_split_root(
        &self,
        mut p_guard: PageExclusiveGuard<'_, BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<()> {
        debug_assert!(p_guard.page_id() == self.root);
        debug_assert!({
            let p = p_guard.page();
            (p.is_leaf() && p.leaf_is_full()) || (p.is_branch() && p.branch_is_full())
        });
        debug_assert!({
            let p = p_guard.page();
            p.is_leaf()
                && (p.leaf_last_block().is_col() || p_guard.page().leaf_last_block().row_is_full())
        });
        let new_height = p_guard.page().header.height + 1;
        let l_row_id = p_guard.page().header.start_row_id;
        let r_row_id = row_id;
        let max_row_id = r_row_id + count;

        // create left child and copy all contents to it.
        let mut l_guard: PageExclusiveGuard<'_, BlockNode> =
            self.buf_pool.allocate_page()?.exclusive()?;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page()); // todo: LSN

        // create right child, add one row block with one page entry.
        let mut r_guard: PageExclusiveGuard<'_, BlockNode> =
            self.buf_pool.allocate_page()?.exclusive()?;
        let r_page_id = r_guard.page_id();
        {
            let r = r_guard.page_mut();
            r.header.height = 0; // leaf
            r.header.start_row_id = r_row_id;
            r.header.end_row_id = r_row_id;
            r.header.count = 0;
            // todo: LSN
            r.leaf_add_block(count, insert_page_id);
        }

        // initialize parent again.
        {
            let p = p_guard.page_mut();
            p.header.height = new_height; // branch
            p.header.start_row_id = l_row_id;
            p.header.end_row_id = max_row_id;
            // todo: LSN
            p.header.count = 0;
            p.branch_add_entry(PageEntry {
                row_id: l_row_id,
                page_id: l_page_id,
            });
            p.branch_add_entry(PageEntry {
                row_id: r_row_id,
                page_id: r_page_id,
            });
        }
        Ok(())
    }

    #[inline]
    fn insert_row_page_to_new_leaf(
        &self,
        stack: &mut Vec<PageGuard<'_, BlockNode>>,
        c_guard: PageExclusiveGuard<'_, BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<()> {
        debug_assert!(!stack.is_empty());
        let mut p_guard;
        loop {
            // try to lock parent.
            let g = stack.pop().unwrap();
            g.validate()?;
            debug_assert!(g.page().is_branch());
            // if lock failed, just retry the whole process.
            p_guard = g.try_exclusive()?;
            if !p_guard.page().branch_is_full() {
                break;
            } else if stack.is_empty() {
                // root is full, should split
                return self.insert_row_page_split_root(p_guard, row_id, count, insert_page_id);
            }
        }
        // create new leaf node with one insert page id
        let mut leaf: PageExclusiveGuard<'_, BlockNode> =
            self.buf_pool.allocate_page()?.exclusive()?;
        let leaf_page_id = leaf.page_id();
        {
            let b: &mut BlockNode = leaf.page_mut();
            b.header.height = 0;
            b.header.count = 0;
            b.header.start_row_id = row_id;
            b.header.end_row_id = row_id;
            // todo: LSN
            b.leaf_add_block(count, insert_page_id);
        }
        // attach new leaf to parent
        {
            let p = p_guard.page_mut();
            p.branch_add_entry(PageEntry::new(row_id, leaf_page_id));
            p.header.end_row_id = row_id + count;
        }
        Ok(())
    }

    #[inline]
    fn insert_row_page(&self, count: u64, insert_page_id: PageID) -> Result<()> {
        let mut stack = vec![];
        let mut p_guard = self
            .find_right_most_leaf(&mut stack, LatchFallbackMode::Exclusive)?
            .exclusive()?;
        debug_assert!(p_guard.page().is_leaf());
        let end_row_id = p_guard.page().header.end_row_id;
        if p_guard.page().leaf_is_full() {
            let block = p_guard.page().leaf_last_block_mut();
            if (block.is_row() && block.row_is_full()) || block.is_col() {
                // leaf is full and block is full, we must add new leaf to block index
                if stack.is_empty() {
                    // root is full and already exclusive locked
                    return self.insert_row_page_split_root(
                        p_guard,
                        end_row_id,
                        count,
                        insert_page_id,
                    );
                }
                return self.insert_row_page_to_new_leaf(
                    &mut stack,
                    p_guard,
                    end_row_id,
                    count,
                    insert_page_id,
                );
            }
            // insert to current row block
            block.row_add_page(count, insert_page_id);
            p_guard.page_mut().header.end_row_id += count;
            return Ok(());
        }
        if p_guard.page().leaf_is_empty()
            || p_guard.page().leaf_last_block().is_col()
            || p_guard.page().leaf_last_block().row_is_full()
        {
            p_guard.page_mut().leaf_add_block(count, insert_page_id);
            return Ok(());
        }
        p_guard
            .page_mut()
            .leaf_last_block_mut()
            .row_add_page(count, insert_page_id);
        p_guard.page_mut().header.end_row_id += count;
        Ok(())
    }

    #[inline]
    fn find_right_most_leaf(
        &self,
        stack: &mut Vec<PageGuard<'a, BlockNode>>,
        mode: LatchFallbackMode,
    ) -> Result<PageGuard<BlockNode>> {
        let mut p_guard: PageGuard<'_, BlockNode> =
            self.buf_pool.get_page(self.root, LatchFallbackMode::Spin)?;
        let mut node = p_guard.page();
        let height = node.header.height;
        let mut level = 1;
        while !node.is_leaf() {
            let count = node.header.count;
            let idx = 1.max(count as usize).min(NBR_ENTRIES_IN_BRANCH) - 1;
            let page_id = node.branch_entries()[idx].page_id;
            p_guard.validate()?;
            stack.push(p_guard.copy_keepalive()?);
            p_guard = if level == height {
                self.buf_pool.get_child_page(p_guard, page_id, mode)?
            } else {
                self.buf_pool
                    .get_child_page(p_guard, page_id, LatchFallbackMode::Spin)?
            };
            node = p_guard.page();
            level += 1;
        }
        Ok(p_guard)
    }

    #[inline]
    fn try_find_leaf_by_row_id(
        &self,
        row_id: RowID,
        stack: &mut Vec<CursorPosition<'a>>,
    ) -> Result<()> {
        let mut g: PageGuard<'a, BlockNode> =
            self.buf_pool.get_page(self.root, LatchFallbackMode::Spin)?;
        loop {
            let node = g.page();
            if node.is_leaf() {
                debug_assert!(g.is_optimistic());
                let row_id = node.header.start_row_id;
                g.validate()?;
                stack.push(CursorPosition {
                    g,
                    next: NextKind::Leaf(row_id),
                });
                return Ok(());
            }
            let entries = node.branch_entries();
            let idx = match entries.binary_search_by_key(&row_id, |block| block.row_id) {
                Ok(idx) => idx,
                Err(0) => 0, // even it's out of range, we assign first page.
                Err(idx) => idx - 1,
            };
            // let row_id = entries[idx].row_id;
            let page_id = entries[idx].page_id;
            let next_row_id = if idx + 1 == node.header.count as usize {
                node.header.end_row_id
            } else {
                entries[idx + 1].row_id
            };
            g.validate()?;
            g.downgrade();
            stack.push(CursorPosition {
                g,
                next: NextKind::Branch(idx + 1, next_row_id),
            });
            g = self.buf_pool.get_page(page_id, LatchFallbackMode::Spin)?;
        }
    }

    #[inline]
    fn try_find_row_id(&self, row_id: RowID) -> Result<RowLocation> {
        let mut g: PageGuard<'a, BlockNode> =
            self.buf_pool.get_page(self.root, LatchFallbackMode::Spin)?;
        loop {
            let node = g.page();
            // todo: ensure end_row_id is always correct.
            if row_id >= node.header.end_row_id {
                g.validate()?;
                return Ok(RowLocation::NotFound);
            }
            if node.is_leaf() {
                let blocks = node.leaf_blocks();
                let idx =
                    match blocks.binary_search_by_key(&row_id, |block| block.header.start_row_id) {
                        Ok(idx) => idx,
                        Err(0) => {
                            g.validate()?;
                            return Ok(RowLocation::NotFound);
                        }
                        Err(idx) => idx - 1,
                    };
                let block = &blocks[idx];
                if row_id >= block.header.end_row_id {
                    g.validate()?;
                    return Ok(RowLocation::NotFound);
                }
                if block.is_col() {
                    todo!();
                }
                let entries = block.row_page_entries();
                let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(0) => {
                        g.validate()?;
                        return Ok(RowLocation::NotFound);
                    }
                    Err(idx) => idx - 1,
                };
                g.validate()?;
                return Ok(RowLocation::RowPage(entries[idx].page_id));
            }
            let entries = node.branch_entries();
            let idx = match entries.binary_search_by_key(&row_id, |block| block.row_id) {
                Ok(idx) => idx,
                Err(0) => {
                    g.validate()?;
                    return Ok(RowLocation::NotFound);
                }
                Err(idx) => idx - 1,
            };
            let page_id = entries[idx].page_id;
            g = self
                .buf_pool
                .get_child_page(g, page_id, LatchFallbackMode::Spin)?;
        }
    }
}

pub enum RowLocation {
    ColSegment(u64, u64),
    RowPage(PageID),
    NotFound,
}

/// A cursor to
pub struct CursorShared<'a> {
    blk_idx: &'a BlockIndex<'a>,
    stack: Vec<CursorPosition<'a>>,
}

impl<'a> CursorShared<'a> {
    #[inline]
    fn fill_stack_by_row_id_search(&mut self, row_id: RowID) -> Result<()> {
        loop {
            self.stack.clear();
            match self
                .blk_idx
                .try_find_leaf_by_row_id(row_id, &mut self.stack)
            {
                Ok(_) => return Ok(()),
                Err(Error::RetryLatch) => (),
                Err(e) => {
                    self.stack.clear();
                    return Err(e);
                }
            }
        }
    }
}

impl<'a> Iterator for CursorShared<'a> {
    type Item = Result<PageSharedGuard<'a, BlockNode>>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        'LOOP: while let Some(pos) = self.stack.pop() {
            match pos.next {
                NextKind::Leaf(row_id) => {
                    if pos.g.validate().is_ok() {
                        let g = pos.g.shared().unwrap();
                        return Some(Ok(g));
                    }
                    // validation failed, must retry with row id
                    match self.fill_stack_by_row_id_search(row_id) {
                        Ok(_) => continue 'LOOP,
                        Err(e) => return Some(Err(e)),
                    }
                }
                NextKind::Branch(idx, row_id) => {
                    let branch = pos.g.page();
                    // all entries in branch have been traversed.
                    if idx == branch.header.count as usize {
                        if pos.g.validate().is_ok() {
                            // we can just ignore this element
                            // and start from its parent(in stack)
                            continue 'LOOP;
                        }
                        // validation failed, must retry with row id
                        match self.fill_stack_by_row_id_search(row_id) {
                            Ok(_) => continue 'LOOP,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    let next_row_id = if idx + 1 < branch.header.count as usize {
                        branch.branch_entries()[idx as usize + 1].row_id
                    } else {
                        branch.header.end_row_id
                    };
                    let page_id = branch.branch_entry(idx).page_id;
                    // try get child page, may fail due to version change.
                    let p_guard = pos.g.copy_keepalive().expect("optimistic parent guard");
                    match self.blk_idx.buf_pool.get_child_page(
                        p_guard,
                        page_id,
                        LatchFallbackMode::Shared,
                    ) {
                        Ok(g) => {
                            // push next entry to stack
                            self.stack.push(CursorPosition {
                                g: pos.g,
                                next: NextKind::Branch(idx + 1, next_row_id),
                            });
                            let g = g.shared().unwrap();
                            return Some(Ok(g));
                        }
                        Err(Error::RetryLatch) => {
                            // validation failed, must retry with row id
                            match self.fill_stack_by_row_id_search(row_id) {
                                Ok(_) => continue 'LOOP,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Err(e) => {
                            self.stack.clear();
                            return Some(Err(e));
                        }
                    }
                }
            }
        }
        None
    }
}

struct CursorPosition<'a> {
    g: PageGuard<'a, BlockNode>,
    // next position: branch entry index or leaf end row id.
    next: NextKind,
}

enum NextKind {
    // branch entry index
    Branch(usize, RowID),
    // leaf row id
    Leaf(RowID),
}

#[inline]
fn estimate_row_count_in_page(_row_len: usize) -> u64 {
    // todo: calculate based on page payload size
    100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_index_free_list() {
        let buf_pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            let p1 = blk_idx.get_insert_page(100).unwrap();
            let pid1 = p1.page_id();
            blk_idx.free_exclusive_insert_page(p1);
            assert!(blk_idx.insert_free_list.lock().len() == 1);
            let p2 = blk_idx.get_insert_page(100).unwrap();
            assert!(pid1 == p2.page_id());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }

    #[test]
    fn test_block_index_insert_row_page() {
        let buf_pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            let p1 = blk_idx.get_insert_page(100).unwrap();
            let pid1 = p1.page_id();
            blk_idx.free_exclusive_insert_page(p1);
            assert!(blk_idx.insert_free_list.lock().len() == 1);
            let p2 = blk_idx.get_insert_page(100).unwrap();
            assert!(pid1 == p2.page_id());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }

    #[test]
    fn test_block_index_cursor_shared() {
        let row_pages = 10240usize;
        // allocate 1GB buffer pool is enough: 10240 pages ~= 640MB
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024 * 1024).unwrap();
        {
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            for _ in 0..row_pages {
                let _ = blk_idx.get_insert_page(100).unwrap();
            }
            let mut count = 0usize;
            for res in blk_idx.cursor_shared(0).unwrap() {
                assert!(res.is_ok());
                count += 1;
                let g = res.unwrap();
                let node = g.page();
                assert!(node.is_leaf());
                let row_pages: usize = node.leaf_blocks().iter()
                    .map(|block| if block.is_row() {
                        block.row_page_entries().iter().count()
                    } else { 0usize })
                    .sum();
                println!("start_row_id={:?}, end_row_id={:?}, blocks={:?}, row_pages={:?}", 
                    node.header.start_row_id, node.header.end_row_id, node.header.count, row_pages);
            }
            let row_pages_per_leaf = NBR_BLOCKS_IN_LEAF * NBR_PAGES_IN_ROW_BLOCK;
            assert!(count == (row_pages + row_pages_per_leaf - 1) / row_pages_per_leaf);
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }
}
