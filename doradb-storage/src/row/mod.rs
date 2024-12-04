pub mod action;

use crate::buffer::page::PAGE_SIZE;
use crate::row::action::{RowAction, InsertRow, DeleteRow, UpdateRow};
use crate::error::{Result, Error};
use std::mem;
use doradb_datatype::Const;

pub type RowID = u64;

/// RowPage is the core data structure of row-store.
/// It is designed to be fast in both TP and AP scenarios.
/// It follows design of PAX format.
/// 
/// Header:
/// 
/// | field            | length(B) |
/// |------------------|-----------|
/// | start_row_id     | 8         |
/// | end_row_id       | 8         |
/// | count            | 4         |
/// | var_field_end    | 4         |
/// 
/// Data:
/// 
/// | field            | length(B)                                     |
/// |------------------|-----------------------------------------------|
/// | del_bitset       | (count + 63) / 64 * 8                         |
/// | null_bitmap_list | (col_count + 7) / 8 * count, align to 8 bytes |
/// | c_0              | depends on column type, align to 8 bytes      |
/// | c_1              | same as above                                 |
/// | ...              | ...                                           |
/// | c_n              | same as above                                 |
/// | free_space       | free space                                    |
/// | var_len_data     | data of var-len column                        |
/// 
pub struct RowPage {
    pub header: RowPageHeader,
    pub data: [u8; PAGE_SIZE - mem::size_of::<RowPageHeader>()],
}

impl RowPage {
    #[inline]
    pub fn init(&mut self, start_row_id: u64, end_row_id: u64) {
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = end_row_id;
        self.header.var_field_end = PAGE_SIZE as u32;
        self.header.count = 0;
    }


    /// Add row to page.
    #[inline]
    pub fn perform(&mut self, action: RowAction) -> Result<Option<RowID>> {
        match action {
            RowAction::Ins(ins) => self.insert(ins).map(Some),
            RowAction::Del(del) => if self.delete(del) {
                Ok(None)
            } else {
                // This may happen in MVCC scenario, e.g. we perform an update as
                // delete followed by an insert, this can happen for example the 
                // page is not enough to store the new version, so we just delete
                // the old version and require a new page to insert new version.
                // in such case, we have to update secondary index to point original
                // entry to new RowID, but there might be some active transaction
                // which should see the old version, so we still need to maintain
                // the entry pointing to old RowID.
                Err(Error::RowNotFound)
            }
            RowAction::Upd(upd) => if self.update_in_place(upd) {
                Ok(None)
            } else {
                Err(Error::InsufficientFreeSpaceForInplaceUpdate)
            }
        }
    }

    #[inline]
    pub fn insert(&mut self, insert: InsertRow) -> Result<RowID> {
        todo!()
    }

    /// delete row in page.
    /// This method will only make the row as deleted.
    #[inline]
    pub fn delete(&mut self, delete: DeleteRow) -> bool {
        todo!()
    }

    /// Update in-place in current page.
    #[inline]
    pub fn update_in_place(&mut self, update: UpdateRow) -> bool {
        todo!()
    }

    #[inline]
    fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr() as *const _
    }

    #[inline]
    fn del_bitset(&self) -> &[u8] {
        todo!()
    }

    #[inline]
    fn del_bitset_mut(&mut self) -> &mut [u8] {
        todo!()
    }

    #[inline]
    fn null_bitsets(&self) -> &[u8] {
        todo!()
    }

    #[inline]
    fn null_bitsets_mut(&mut self) -> &mut [u8] {
        todo!()
    }

    #[inline]
    fn null_bitset(&self, row_id: RowID) -> &[u8] {
        todo!()
    }

    #[inline]
    fn null_bitset_mut(&mut self, row_id: RowID) -> &[u8] {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct RowPageHeader {
    pub start_row_id: u64,
    pub end_row_id: u64,
    pub count: u32,
    pub var_field_end: u32,
}

#[inline]
const fn align8(len: usize) -> usize {
    (len + 7) / 8 * 8
}

#[inline]
const fn align64(len: usize) -> usize {
    (len + 63) / 64 * 64
}

/// delete bitset length, align to 8 bytes
#[inline]
const fn row_page_del_bitset_len(count: usize) -> usize {
    align64(count) / 8
}

