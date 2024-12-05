pub mod action;
pub mod layout;
pub mod value;

use crate::buffer::page::PAGE_SIZE;
use crate::row::action::{RowAction, InsertRow, DeleteRow, UpdateRow};
use crate::row::layout::ColLayout;
use crate::row::value::*;
use crate::error::{Result, Error};
use std::mem;
use std::slice;

pub type RowID = u64;
pub const INVALID_ROW_ID: RowID = !0;

const _: () = assert!({
    std::mem::size_of::<RowPageHeader>() % 8 == 0
}, "RowPageHeader should have size align to 8 bytes");

/// RowPage is the core data structure of row-store.
/// It is designed to be fast in both TP and AP scenarios.
/// It follows design of PAX format.
/// 
/// Header:
/// 
/// | field                   | length(B) |
/// |-------------------------|-----------|
/// | start_row_id            | 8         |
/// | max_row_count           | 2         |
/// | row_count               | 2         |
/// | col_count               | 2         |
/// | del_bitset_offset       | 2         |
/// | null_bitset_list_offset | 2         |
/// | col_offset_list_offset  | 2         |
/// | fix_field_offset        | 2         |
/// | fix_field_end           | 2         |
/// | var_field_offset        | 2         |
/// | padding                 | 6         |
/// 
/// Data:
/// 
/// | field            | length(B)                                     |
/// |------------------|-----------------------------------------------|
/// | del_bitset       | (count + 63) / 64 * 8                         |
/// | null_bitmap_list | (col_count + 7) / 8 * count, align to 8 bytes |
/// | col_offset_list  | col_count * 2, align to 8 bytes               |
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
    pub fn init(&mut self, start_row_id: u64, max_count: u16, cols: &[ColLayout]) {
        self.header.start_row_id = start_row_id;
        self.header.max_row_count = max_count;
        self.header.row_count = 0;
        let row_count = max_count as usize;
        let col_count = cols.len();
        // initialize offset fields.
        self.header.del_bitset_offset = 0; // always starts at data_ptr().
        self.header.null_bitset_list_offset = self.header.del_bitset_offset + del_bitset_len(row_count) as u16;
        self.header.col_offset_list_offset = self.header.null_bitset_list_offset + null_bitset_list_len(col_count, max_count as usize) as u16;
        self.header.fix_field_offset = self.header.col_offset_list_offset + col_offset_list_len(col_count) as u16;
        self.init_col_offset_list_and_fix_field_end(cols, max_count);
        self.header.var_field_offset = (PAGE_SIZE - mem::size_of::<RowPageHeader>()) as u16;
        self.init_data();
    }

    #[inline]
    fn init_col_offset_list_and_fix_field_end(&mut self, cols: &[ColLayout], row_count: u16) {
        debug_assert!(cols.len() >= 2); // at least RowID and one user column.
        debug_assert!(cols[0] == ColLayout::Byte8); // first column must be RowID, with 8-byte layout.
        debug_assert!(self.header.col_offset_list_offset != 0);
        debug_assert!(self.header.fix_field_offset != 0);
        let mut col_offset = self.header.fix_field_offset;
        for (i, col) in cols.iter().enumerate() {
            *self.col_offset_mut(i) = col_offset;
            col_offset += col_fix_len(col, row_count as usize) as u16;
        }
        self.header.fix_field_end = col_offset;
    }

    #[inline]
    fn init_data(&mut self) {
        unsafe {
            // zero del_bitset, null_bitset_list and col_offset_list
            let count = (self.header.fix_field_offset - self.header.del_bitset_offset) as usize;
            let ptr = self.data_ptr_mut().add(self.header.del_bitset_offset as usize);
            std::ptr::write_bytes(ptr, 0, count);
        
            // initialize all RowIDs to INVALID_ROW_ID
            let row_ids = self.values_mut_unchecked::<RowID>(0, self.header.max_row_count as usize);
            for row_id in row_ids {
                *row_id = INVALID_ROW_ID;
            }
        }
    }

    #[inline]
    pub fn row_ids(&self) -> &[RowID] {
        self.values::<RowID>(0)
    }

    #[inline]
    pub fn row_ids_mut(&mut self) -> &mut [RowID] {
        self.values_mut::<RowID>(0)
    }

    #[inline]
    pub fn values<V: Value>(&self, col_idx: usize) -> &[V] {
        let len = self.header.row_count as usize;
        unsafe {
            self.values_unchecked(col_idx, len)
        }
    }

    #[inline]
    pub unsafe fn values_unchecked<V: Value>(&self, col_idx: usize, len: usize) -> &[V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const V = mem::transmute(ptr);
        std::slice::from_raw_parts(data, len)
    }

    #[inline]
    pub fn values_mut<V: Value>(&mut self, col_idx: usize) -> &mut [V] {
        let len = self.header.row_count as usize;
        unsafe {
            self.values_mut_unchecked(col_idx, len)
        }
    }

    #[inline]
    pub unsafe fn values_mut_unchecked<V: Value>(&mut self, col_idx: usize, len: usize) -> &mut [V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr_mut().add(offset);
        let data: *mut V = mem::transmute(ptr);
        std::slice::from_raw_parts_mut(data, len)
    }

    /// Returns free space of current page.
    /// The free space is used to hold data of var-len columns.
    #[inline]
    pub fn free_space(&self) -> u16 {
        self.header.var_field_offset - self.header.fix_field_end
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
        self.data.as_ptr()
    }

    #[inline]
    fn data_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    #[inline]
    fn del_bitset(&self) -> &[u8] {
        let len = del_bitset_len(self.header.max_row_count as usize);
        unsafe {
            slice::from_raw_parts(self.data_ptr().add(self.header.del_bitset_offset as usize), len)
        }
    }

    #[inline]
    fn del_bitset_mut(&mut self) -> &mut [u8] {
        let len = del_bitset_len(self.header.max_row_count as usize);
        unsafe {
            slice::from_raw_parts_mut(self.data_ptr_mut().add(self.header.del_bitset_offset as usize), len)
        }
    }

    #[inline]
    fn null_bitset(&self, row_offset: usize) -> &[u8] {
        let len = align8(self.header.col_count as usize) / 8;
        let offset = self.header.null_bitset_list_offset as usize;
        unsafe {
            slice::from_raw_parts(self.data_ptr().add(offset + len * row_offset), len)
        }
    }

    #[inline]
    fn null_bitset_mut(&mut self, row_offset: usize) -> &[u8] {
        let len = align8(self.header.col_count as usize) / 8;
        let offset = self.header.null_bitset_list_offset as usize;
        unsafe {
            slice::from_raw_parts_mut(self.data_ptr_mut().add(offset + len * row_offset), len)
        }
    }

    #[inline]
    fn col_offset(&self, col_idx: usize) -> u16 {
        let offset = self.header.col_offset_list_offset as usize;
        unsafe {
            let ptr = self.data_ptr().add(offset) as *const u16;
            *ptr.add(col_idx)
        }
    }

    #[inline]
    fn col_offset_mut(&mut self, col_idx: usize) -> &mut u16 {
        let offset = self.header.col_offset_list_offset as usize;
        unsafe {
            let ptr = self.data_ptr_mut().add(offset) as *mut u16;
            &mut *ptr.add(col_idx)
        }
    }
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct RowPageHeader {
    pub start_row_id: u64,
    pub max_row_count: u16,
    pub row_count: u16,
    pub col_count: u16,
    pub del_bitset_offset: u16,
    pub null_bitset_list_offset: u16,
    pub col_offset_list_offset: u16,
    pub fix_field_offset: u16,
    pub fix_field_end: u16,
    pub var_field_offset: u16,
    padding: [u8; 6],
}

#[inline]
const fn align8(len: usize) -> usize {
    (len + 7) / 8 * 8
}

#[inline]
const fn align64(len: usize) -> usize {
    (len + 63) / 64 * 64
}

/// delete bitset length, align to 8 bytes.
#[inline]
const fn del_bitset_len(count: usize) -> usize {
    align64(count) / 8
}

// null bitset length, align to 8 bytes.
#[inline]
const fn null_bitset_list_len(col_count: usize, row_count: usize) -> usize {
    align8(align8(col_count) / 8 * row_count)
}

// column offset list len, align to 8 bytes.
#[inline]
const fn col_offset_list_len(col_count: usize) -> usize {
    align8(mem::size_of::<u16>() * col_count)
}

// column fixed length, align to 8 bytes.
#[inline]
const fn col_fix_len(col: &ColLayout, row_count: usize) -> usize {
    align8(col.fix_len() * row_count)
}

#[cfg(test)]
mod tests {
    use mem::MaybeUninit;

    use super::*;

    #[test]
    fn test_row_page_init() {
        let cols = vec![ColLayout::Byte8, ColLayout::Byte8];
        let mut page = create_row_page();
        page.init(100, 105, &cols);
        println!("page header={:?}", page.header);
        assert!(page.header.start_row_id == 100);
        assert!(page.header.max_row_count == 105);
        assert!(page.header.row_count == 0);
        assert!(page.header.del_bitset_offset == 0);
        assert!(page.header.null_bitset_list_offset % 8 == 0);
        assert!(page.header.col_offset_list_offset % 8 == 0);
        assert!(page.header.fix_field_offset % 8 == 0);
        assert!(page.header.fix_field_end % 8 == 0);
        assert!(page.header.var_field_offset % 8 == 0);
    }

    fn create_row_page() -> RowPage {
        unsafe {
            let new = MaybeUninit::<RowPage>::uninit();
            new.assume_init()
        }
    }
}
