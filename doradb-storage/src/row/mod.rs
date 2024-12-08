pub mod ops;
pub mod layout;
pub mod value;

use crate::buffer::page::PAGE_SIZE;
use crate::row::ops::{SelectResult, InsertRow, InsertResult, DeleteRow, DeleteResult, UpdateRow, UpdateCol, UpdateResult};
use crate::row::layout::Layout;
use crate::row::value::*;
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
#[derive(PartialEq, Eq)]
pub struct RowPage {
    pub header: RowPageHeader,
    pub data: [u8; PAGE_SIZE - mem::size_of::<RowPageHeader>()],
}

impl RowPage {
    #[inline]
    pub fn init(&mut self, start_row_id: u64, max_row_count: usize, cols: &[Layout]) {
        debug_assert!(max_row_count <= 0xffff);
        self.header.start_row_id = start_row_id;
        self.header.max_row_count = max_row_count as u16;
        self.header.row_count = 0;
        self.header.col_count = cols.len() as u16;
        // initialize offset fields.
        self.header.del_bitset_offset = 0; // always starts at data_ptr().
        self.header.null_bitset_list_offset = self.header.del_bitset_offset + del_bitset_len(max_row_count) as u16;
        self.header.col_offset_list_offset = self.header.null_bitset_list_offset + null_bitset_list_len(max_row_count, cols.len()) as u16;
        self.header.fix_field_offset = self.header.col_offset_list_offset + col_offset_list_len(cols.len()) as u16;
        self.init_col_offset_list_and_fix_field_end(cols, max_row_count as u16);
        self.header.var_field_offset = (PAGE_SIZE - mem::size_of::<RowPageHeader>()) as u16;
        self.init_bitsets_and_row_ids();
        debug_assert!({
            (self.header.row_count..self.header.max_row_count).all(|i| {
                let row = self.row(i as usize);
                !row.is_deleted() && row.is_invalid()
            })    
        });
    }

    #[inline]
    fn init_col_offset_list_and_fix_field_end(&mut self, cols: &[Layout], row_count: u16) {
        debug_assert!(cols.len() >= 2); // at least RowID and one user column.
        debug_assert!(cols[0] == Layout::Byte8); // first column must be RowID, with 8-byte layout.
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
    fn init_bitsets_and_row_ids(&mut self) {
        unsafe {
            // zero del_bitset and null_bitset_list
            let count = (self.header.col_offset_list_offset - self.header.del_bitset_offset) as usize;
            let ptr = self.data_ptr_mut().add(self.header.del_bitset_offset as usize);
            std::ptr::write_bytes(ptr, 0, count);
        
            // initialize all RowIDs to INVALID_ROW_ID
            let row_ids = self.vals_mut_unchecked::<RowID>(0, self.header.max_row_count as usize);
            for row_id in row_ids {
                *row_id = INVALID_ROW_ID;
            }
        }
    }

    #[inline]
    pub fn row_ids(&self) -> &[RowID] {
        self.vals::<RowID>(0)
    }

    #[inline]
    pub fn row_ids_mut(&mut self) -> &mut [RowID] {
        self.vals_mut::<RowID>(0)
    }

    /// Returns free space of current page.
    /// The free space is used to hold data of var-len columns.
    #[inline]
    pub fn free_space(&self) -> u16 {
        self.header.var_field_offset - self.header.fix_field_end
    }

    /// Insert a new row in page.
    #[inline]
    pub fn insert(&mut self, insert: InsertRow) -> InsertResult {
        // insert row does not include RowID, as RowID is auto-generated.
        debug_assert!(insert.0.len() + 1 == self.header.col_count as usize);
        if self.header.row_count == self.header.max_row_count {
            return InsertResult::RowIDExhausted;
        }
        if !self.free_space_enough_for_insert(&insert.0) {
            return InsertResult::NoFreeSpace;
        }
        let mut new_row = self.new_row();
        for v in &insert.0 {
            match v {
                Val::Byte1(v1) => new_row.add_val(*v1),
                Val::Byte2(v2) => new_row.add_val(*v2),
                Val::Byte4(v4) => new_row.add_val(*v4),
                Val::Byte8(v8) => new_row.add_val(*v8),
                Val::VarByte(var) => new_row.add_var(var),
            }
        }
        InsertResult::Ok(new_row.finish())
    }

    /// delete row in page.
    /// This method will only mark the row as deleted.
    #[inline]
    pub fn delete(&mut self, delete: DeleteRow) -> DeleteResult {
        let row_id = delete.0;
        if row_id < self.header.start_row_id || row_id >= self.header.max_row_count as u64 {
            return DeleteResult::RowNotFound;
        }
        let row_idx = (row_id - self.header.start_row_id) as usize;
        if self.row(row_idx).is_invalid() {
            return DeleteResult::RowInvalid;
        }
        if self.is_deleted(row_idx) {
            return DeleteResult::RowAlreadyDeleted;
        }
        self.set_deleted(row_idx);
        DeleteResult::Ok
    }

    /// Update in-place in current page.
    #[inline]
    pub fn update_in_place(&mut self, update: UpdateRow) -> UpdateResult {
        if update.row_id < self.header.start_row_id || update.row_id >= self.header.max_row_count as u64 {
            return UpdateResult::RowNotFound;
        }
        let row_idx = (update.row_id - self.header.start_row_id) as usize;
        if self.row(row_idx).is_deleted() {
            return UpdateResult::RowAlreadyDeleted;
        }
        if self.row(row_idx).is_invalid() {
            return UpdateResult::RowInvalid;
        }
        if !self.free_space_enough_for_update(row_idx, &update.cols) {
            return UpdateResult::NoFreeSpace;
        }
        let mut row = self.row_mut(row_idx);
        // todo: identify difference and skip if the same.
        for uc in &update.cols {
            row.update_col(uc.idx, &uc.val);
        }
        UpdateResult::Ok
    }

    /// Select single row by row id.
    #[inline]
    pub fn select(&self, row_id: RowID) -> SelectResult {
        if row_id < self.header.start_row_id || row_id >= self.header.max_row_count as u64 {
            return SelectResult::RowNotFound;
        }
        let row_idx = (row_id - self.header.start_row_id) as usize;
        let row = self.row(row_idx);
        if row.is_invalid() {
            return SelectResult::RowInvalid;
        }
        if row.is_deleted() {
            return SelectResult::Deleted(row);
        }
        SelectResult::Ok(row)
    }

    #[inline]
    fn free_space_enough_for_insert(&self, insert: &[Val<'_>]) -> bool {
        let var_len: usize = insert.iter().map(|v| match v {
            Val::VarByte(var) => if var.len() > VAR_LEN_INLINE {
                var.len()
            } else {
                0
            }
            _ => 0,
        }).sum();
        var_len <= self.free_space() as usize
    }

    #[inline]
    fn free_space_enough_for_update(&self, row_idx: usize, update: &[UpdateCol<'_>]) -> bool {
        let row = self.row(row_idx);
        let var_len: usize = update.iter().map(|uc| {
            match uc.val {
                Val::VarByte(var) => {
                    let col = row.var(uc.idx);
                    let orig_var_len =  VarByteVal::inpage_len(col);
                    let upd_var_len = VarByteVal::inpage_len(var);
                    if upd_var_len > orig_var_len {
                        upd_var_len
                    } else {
                        0
                    }
                }
                _ => 0
            }
        }).sum();
        var_len <= self.free_space() as usize
    }

    /// Creates a new row in page.
    #[inline]
    fn new_row(&mut self) -> NewRow {
        debug_assert!(self.header.row_count < self.header.max_row_count);
        let start_row_id = self.header.start_row_id;
        let row_idx = self.header.row_count as usize;
        let mut row = NewRow{page: self, row_idx, col_idx: 0};
        // always add RowID as first column
        row.add_val(start_row_id + row_idx as u64);
        row
    }

    /// Returns row by given index in page.
    #[inline]
    fn row(&self, row_idx: usize) -> Row {
        debug_assert!(row_idx < self.header.max_row_count as usize);
        Row{page: self, row_idx}
    }

    /// Returns mutable row by given index in page.
    #[inline]
    fn row_mut(&mut self, row_idx: usize) -> RowMut {
        debug_assert!(row_idx < self.header.row_count as usize);
        RowMut{page: self, row_idx}
    }

    /// Returns all values of given column.
    #[inline]
    fn vals<V: Value>(&self, col_idx: usize) -> &[V] {
        let len = self.header.row_count as usize;
        unsafe {
            self.vals_unchecked(col_idx, len)
        }
    }

    /// Returns all mutable values of given column.
    #[inline]
    fn vals_mut<V: Value>(&mut self, col_idx: usize) -> &mut [V] {
        let len = self.header.row_count as usize;
        unsafe {
            self.vals_mut_unchecked(col_idx, len)
        }
    }

    #[inline]
    unsafe fn vals_unchecked<V: Value>(&self, col_idx: usize, len: usize) -> &[V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const V = mem::transmute(ptr);
        std::slice::from_raw_parts(data, len)
    }

    #[inline]
    unsafe fn vals_mut_unchecked<V: Value>(&mut self, col_idx: usize, len: usize) -> &mut [V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr_mut().add(offset);
        let data: *mut V = mem::transmute(ptr);
        std::slice::from_raw_parts_mut(data, len)
    }

    #[inline]
    unsafe fn val_unchecked<V: Value>(&self, row_idx: usize, col_idx: usize) -> &V {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const V = mem::transmute(ptr);
        &*data.add(row_idx)
    }

    #[inline]
    unsafe fn val_mut_unchecked<V: Value>(&mut self, row_idx: usize, col_idx: usize) -> &mut V {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *mut V = mem::transmute(ptr);
        &mut *data.add(row_idx)
    }

    #[inline]
    unsafe fn var_unchecked(&self, row_idx: usize, col_idx: usize) -> &VarByteVal {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const VarByteVal = mem::transmute(ptr);
        &*data.add(row_idx)
    }

    #[inline]
    unsafe fn var_len_unchecked(&self, row_idx: usize, col_idx: usize) -> usize {
        let var = self.var_unchecked(row_idx, col_idx);
        var.len()
    }

    #[inline]
    unsafe fn var_mut_unchecked(&mut self, row_idx: usize, col_idx: usize) -> &mut VarByteVal {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *mut VarByteVal = mem::transmute(ptr);
        &mut *data.add(row_idx)
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
    fn is_deleted(&self, row_idx: usize) -> bool {
        unsafe {
            let ptr = self.data_ptr().add(self.header.del_bitset_offset as usize);
            *ptr.add(row_idx / 8) & (1 << (row_idx % 8)) != 0
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
    fn set_deleted(&mut self, row_idx: usize) {
        unsafe {
            let ptr = self.data_ptr_mut().add(self.header.del_bitset_offset as usize);
            *ptr.add(row_idx / 8) |= 1 << (row_idx % 8);
        }
    }

    /// Returns null bitset for given column
    #[inline]
    fn null_bitset(&self, col_idx: usize) -> &[u8] {
        let len = align8(self.header.max_row_count as usize) / 8;
        let offset = self.header.null_bitset_list_offset as usize;
        unsafe {
            slice::from_raw_parts(self.data_ptr().add(offset + len * col_idx), len)
        }
    }

    #[inline]
    fn is_null(&self, row_idx: usize, col_idx: usize) -> bool {
        let bitset = self.null_bitset(col_idx);
        (bitset[row_idx / 8] & (1 << (row_idx % 8))) != 0
    }

    #[inline]
    fn null_bitset_mut(&mut self, col_idx: usize) -> &mut [u8] {
        let len = align8(self.header.max_row_count as usize) / 8;
        let offset = self.header.null_bitset_list_offset as usize;
        unsafe {
            slice::from_raw_parts_mut(self.data_ptr_mut().add(offset + len * col_idx), len)
        }
    }

    #[inline]
    fn set_null(&mut self, row_idx: usize, col_idx: usize) {
        let bitset = self.null_bitset_mut(col_idx);
        bitset[row_idx / 8] |= !(1 << (row_idx % 8))
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

    #[inline]
    fn add_var(&mut self, input: &[u8]) -> VarByteVal {
        let len = input.len();
        if len <= VAR_LEN_INLINE {
            return VarByteVal::inline(input);
        }
        self.header.var_field_offset -= len as u16;
        unsafe {
            let ptr = self.data_ptr_mut().add(self.header.var_field_offset as usize);
            let target = slice::from_raw_parts_mut(ptr, len);
            target.copy_from_slice(input);
        }
        VarByteVal::inpage(len as u16, self.header.var_field_offset, &input[..VAR_LEN_PREFIX])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

/// NewRow wraps the page to provide convenient method
/// to add values to new row.
pub struct NewRow<'a> {
    page: &'a mut RowPage,
    row_idx: usize,
    col_idx: usize,
}

impl<'a> NewRow<'a> {
    /// add one value to current row.
    #[inline]
    pub fn add_val<T: ToValue>(&mut self, input: T) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        let val = input.to_val();
        unsafe {
            let target = self.page.val_mut_unchecked::<T::Target>(self.row_idx, self.col_idx);
            *target = val;
        }
        self.col_idx += 1;
    }

    /// Add variable-length value to current row.
    #[inline]
    pub fn add_var(&mut self, input: &[u8]) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        debug_assert!(self.page.free_space() as usize >= input.len());
        let var = self.page.add_var(input);
        unsafe {
            let target = self.page.var_mut_unchecked(self.row_idx, self.col_idx);
            *target = var;
        }
        self.col_idx += 1;
    }

    /// Add string value to current row, same as add_var().
    #[inline]
    pub fn add_str(&mut self, input: &str) {
        self.add_var(input.as_bytes())
    }

    /// Add null value to current row.
    #[inline]
    pub fn add_null(&mut self) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        self.page.set_null(self.row_idx, self.col_idx);
        self.col_idx += 1;
    }

    /// Finish current row.
    #[inline]
    pub fn finish(self) -> RowID {
        debug_assert!(self.col_idx == self.page.header.col_count as usize);
        self.page.header.row_count += 1;
        self.page.header.start_row_id + self.row_idx as u64
    }
}

/// Row abstract a logical row in the page.
#[derive(Clone, PartialEq, Eq)]
pub struct Row<'a> {
    page: &'a RowPage,
    row_idx: usize,
}

impl<'a> Row<'a> {
    /// Returns RowID of current row.
    #[inline]
    pub fn row_id(&self) -> RowID {
        *self.val::<RowID>(0)
    }

    /// Returns whether current row is deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.page.is_deleted(self.row_idx)
    }

    /// Returns whether current row is invalid.
    /// The may happen in recovery if a transaction
    /// insert a row and then delete it.
    /// The row id is used but redo log does not
    /// record it, as we use value logging, only latest 
    /// version is kept.
    /// Therefore, some rows may be untouched during
    /// recovery and lead to "holes" in the page.
    /// The table scan should discard such rows.
    #[inline]
    pub fn is_invalid(&self) -> bool {
        self.row_id() == INVALID_ROW_ID
    }

    /// Returns value by given column index.
    #[inline]
    pub fn val<T: Value>(&self, col_idx: usize) -> &T {
        unsafe {
            self.page.val_unchecked::<T>(self.row_idx, col_idx)
        }
    }

    /// Returns variable-length value by given column index.
    #[inline]
    pub fn var(&self, col_idx: usize) -> &[u8] {
        unsafe {
            let var = self.page.var_unchecked(self.row_idx, col_idx);
            var.as_bytes(self.page.data_ptr())
        }
    }

    /// Returns whether column by given index is null.
    #[inline]
    pub fn is_null(&self, col_idx: usize) -> bool {
        self.page.is_null(self.row_idx, col_idx)
    }
}

/// RowMut is mutable row in the page.
pub struct RowMut<'a> {
    page: &'a mut RowPage,
    row_idx: usize,
}

impl<'a> RowMut<'a> {
    /// Returns RowID of current row.
    #[inline]
    pub fn row_id(&self) -> RowID {
        *self.val::<RowID>(0)
    }

    /// Returns whether current row is deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.page.is_deleted(self.row_idx)
    }

    /// Returns whether current row is invalid.
    #[inline]
    pub fn is_invalid(&self) -> bool {
        self.row_id() == INVALID_ROW_ID
    }

    #[inline]
    pub fn update_col(&mut self, col_idx: usize, value: &Val) {
        match value {
            Val::Byte1(v1) => {
                self.update_val(col_idx, v1);
            }
            Val::Byte2(v2) => {
                self.update_val(col_idx, v2);
            }
            Val::Byte4(v4) => {
                self.update_val(col_idx, v4);
            }
            Val::Byte8(v8) => {
                self.update_val(col_idx, v8);
            }
            Val::VarByte(var) => {
                self.update_var(col_idx, var);
            }
        }
    }

    /// Returns value by given column index.
    #[inline]
    pub fn val<T: Value>(&self, col_idx: usize) -> &T {
        unsafe {
            self.page.val_unchecked::<T>(self.row_idx, col_idx)
        }
    }

    /// Returns mutable value by given column index.
    #[inline]
    pub fn val_mut<T: Value>(&mut self, col_idx: usize) -> &mut T {
        unsafe {
            self.page.val_mut_unchecked(self.row_idx, col_idx)
        }
    }

    /// Update fix-length value by givne column index.
    #[inline]
    pub fn update_val<T: ToValue>(&mut self, col_idx: usize, input: &T) {
        *self.val_mut::<T::Target>(col_idx) = input.to_val();
    }

    /// Returns variable-length value by given column index.
    #[inline]
    pub fn var(&self, col_idx: usize) -> &[u8] {
        unsafe {
            let var = self.page.var_unchecked(self.row_idx, col_idx);
            var.as_bytes(self.page.data_ptr())
        }
    }

    /// Update variable-length value.
    #[inline]
    pub fn update_var(&mut self, col_idx: usize, input: &[u8]) {
        // todo: reuse released space by update. 
        // if update value is longer than original value,
        // the original space is wasted.
        // there can be optimization that additionally record
        // the head free offset of released var-len space at the page header.
        // and any released space is at lest 7 bytes(larger than VAR_LEN_INLINE) 
        // long and is enough to connect the free list.
        unsafe {
            let origin_len = self.page.var_len_unchecked(self.row_idx, col_idx);
            if input.len() <= VAR_LEN_INLINE || input.len() <= origin_len {
                let ptr = self.page.data_ptr_mut();
                let target = self.page.var_mut_unchecked(self.row_idx, col_idx);
                target.update_in_place(ptr, input);
            } else {
                let val = self.page.add_var(input);
                let target = self.page.var_mut_unchecked(self.row_idx, col_idx);
                *target = val;
            }
        }
    }

    /// Returns whether column by given index is null.
    #[inline]
    pub fn is_null(&self, col_idx: usize) -> bool {
        self.page.is_null(self.row_idx, col_idx)
    }

    /// Set null by given column index.
    #[inline]
    pub fn set_null(&mut self, col_idx: usize) {
        self.page.set_null(self.row_idx, col_idx);
    }
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
const fn null_bitset_list_len(row_count: usize, col_count: usize) -> usize {
    align8(align8(row_count) / 8 * col_count)
}

// column offset list len, align to 8 bytes.
#[inline]
const fn col_offset_list_len(col_count: usize) -> usize {
    align8(mem::size_of::<u16>() * col_count)
}

// column fixed length, align to 8 bytes.
#[inline]
const fn col_fix_len(col: &Layout, row_count: usize) -> usize {
    align8(col.fix_len() * row_count)
}

#[cfg(test)]
mod tests {
    use core::str;

    use mem::MaybeUninit;

    use super::*;

    #[test]
    fn test_row_page_init() {
        let cols = vec![Layout::Byte8, Layout::Byte8];
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

    #[test]
    fn test_row_page_new_row() {
        let cols = vec![Layout::Byte8, Layout::Byte8];
        let mut page = create_row_page();
        page.init(100, 200, &cols);
        assert!(page.header.row_count == 0);
        assert!(page.header.col_count == 2);
        let mut new_row = page.new_row();
        new_row.add_val(1u64);
        new_row.finish();
        assert!(page.header.row_count == 1);
        let mut new_row = page.new_row();
        new_row.add_val(2u64);
        new_row.finish();
        assert!(page.header.row_count == 2);
    }

    #[test]
    fn test_row_page_read_write_row() {
        let cols = vec![Layout::Byte8, Layout::Byte4, Layout::VarByte];
        let mut page = create_row_page();
        page.init(100, 200, &cols);
        
        let mut new_row = page.new_row();
        new_row.add_val(1_000_000i32);
        new_row.add_str("hello"); // inline string
        new_row.finish();

        let row1 = page.row(0);
        assert!(row1.row_id() == 100);
        assert!(*row1.val::<Byte4Val>(1) as i32 == 1_000_000i32);
        assert!(row1.var(2) == b"hello");

        let mut new_row = page.new_row();
        new_row.add_val(2_000_000i32);
        new_row.add_var(b"this value is not inline");
        new_row.finish();

        let row2 = page.row(1);
        assert!(row2.row_id() == 101);
        assert!(*row2.val::<Byte4Val>(1) as i32 == 2_000_000i32);
        let s = row2.var(2);
        println!("len={:?}, s={:?}", s.len(), str::from_utf8(&s[..24]));
        assert!(row2.var(2) == b"this value is not inline");

        let free_space = page.free_space();
        let mut row1_mut = page.row_mut(0);
        row1_mut.set_null(1);
        row1_mut.update_var(2, b"update to non-inline value");
        assert!(free_space > page.free_space());

        let free_space = page.free_space();
        let mut row2_mut = page.row_mut(1);
        *row2_mut.val_mut(1) = 99i32.to_val();
        row2_mut.update_var(2, b"inline");
        assert!(free_space == page.free_space());
    }

    #[test]
    fn test_row_page_crud() {
        let cols = vec![Layout::Byte8, Layout::Byte1, Layout::Byte2, Layout::Byte4, Layout::Byte8, Layout::VarByte];
        let mut page = create_row_page();
        page.init(100, 200, &cols);
        let short = b"short";
        let long = b"very loooooooooooooooooong";

        let insert: InsertRow<'static> = InsertRow(vec![
            Val::Byte1(1), Val::Byte2(1000), Val::Byte4(1_000_000),
            Val::Byte8(1 << 35), Val::VarByte(short)]);
        let res = page.insert(insert);
        assert!(res == InsertResult::Ok(100));
        assert!(!page.row(0).is_deleted());

        let update: UpdateRow<'static> = UpdateRow{
            row_id: 100,
            cols: vec![
                UpdateCol{idx: 1, val: Val::Byte1(2)},
                UpdateCol{idx: 2, val: Val::Byte2(2000)},
                UpdateCol{idx: 3, val: Val::Byte4(2_000_000)},
                UpdateCol{idx: 4, val: Val::Byte8(2 << 35)},
                UpdateCol{idx: 5, val: Val::VarByte(long)},
                ],
        };
        let res = page.update_in_place(update);
        assert!(res == UpdateResult::Ok);

        let delete = DeleteRow(100);
        let res = page.delete(delete);
        assert!(res == DeleteResult::Ok);

        let select = page.select(100);
        assert!(matches!(select, SelectResult::Deleted(_)));
    }

    fn create_row_page() -> RowPage {
        unsafe {
            let new = MaybeUninit::<RowPage>::uninit();
            new.assume_init()
        }
    }
}
