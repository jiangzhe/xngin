use crate::buffer::page::PageID;
use crate::row::ops::UpdateCol;
use crate::row::RowID;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// UndoMap is page level hash map to store undo chain of rows.
/// If UndoMap is empty, this page has all data visible to all transactions.
pub type UndoMap = HashMap<RowID, SharedUndoEntry>;

pub enum UndoKind {
    /// Before-image is empty for insert, so we do not need to copy values.
    Insert,
    /// Delete must be the head of version chain and data is stored in row page.
    /// So no need to copy values.
    Delete,
    /// Copy old versions of updated columns.
    Update(Vec<UpdateCol>),
    /// This is special case when in-place update fails.
    /// The old row is marked as deleted, and new row is inserted into a new page.
    /// Because we always update secondary index to point to new version,
    /// there might be two index entries pointing to the same row id.
    /// In such case, index key validation is required to choose correct version.
    /// Meanwhile, table scan should take care of two versions existing in different pages.
    /// In current design, the previous version of DeleteUpdate is discarded.
    DeleteUpdate(Vec<UpdateCol>),
}

/// SharedUndoEntry is a reference-counted pointer to UndoEntry.
/// The transaction is the primary owner of undo log.
/// and page-level transaction map can also own undo log
/// to track all visible versions of modified rows.
pub type SharedUndoEntry = Arc<UndoEntry>;

pub struct UndoEntry {
    pub ts: Arc<AtomicU64>,
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: UndoKind,
    pub next: Mutex<Option<SharedUndoEntry>>,
}
