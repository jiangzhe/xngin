//! DoraDB's concurrency control protocol is an implmementation of MVCC + MV2PL(todo).
//!
//! The basic MVCC logic is described as below.
//! 1. When starting a transaction, a snapshot timestamp(STS) is generated, and transaction id
//!    is also derived from STS by setting highest bit to 1.
//! 2. When the transaction do any insert, update or delete, an undo log is generated with
//!    RowID and stored in a page-level transaction version map(TRX-MAP). The undo log records
//!    current transaction id at head.
//! 3. When the transaction commits, a commit timestamp(CTS) is generated, and all undo logs of
//!    this transaction will update CTS in its head.
//! 4. When a transaction query a row in one page,
//!    a) it first look at page-level TRX-MAP, if the map is empty, then all data on the page
//!       are latest. So directly read data and return.
//!    b) otherwise, check if queried RowID exists in the map. if not, same as a).
//!    c) If exists, check the timestamp in entry head. If it's larger than current STS, means
//!       it's invisible, undo change and go to next version in the chain...
//!    d) If less than current STS, return current version.
pub mod redo;
pub mod sys;
pub mod undo;

use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::FixedBufferPool;
use crate::latch::LatchFallbackMode;
use crate::row::ops::{InsertResult, InsertRow};
use crate::row::RowPage;
use crate::trx::redo::{RedoBin, RedoEntry, RedoLog};
use crate::trx::undo::{SharedUndoEntry, UndoEntry, UndoKind};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type TrxID = u64;
pub const INVALID_TRX_ID: TrxID = !0;
pub const MIN_SNAPSHOT_TS: TrxID = 1;
pub const MAX_SNAPSHOT_TS: TrxID = 1 << 63;
pub const MAX_COMMIT_TS: TrxID = 1 << 63;
// As active transaction id is always greater than STS, that means
// visibility check can be simplified to "STS is larger".
pub const MIN_ACTIVE_TRX_ID: TrxID = (1 << 63) + 1;

pub struct ActiveTrx {
    trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    // transaction-level undo logs.
    trx_undo: Vec<SharedUndoEntry>,
    // statement-level undo logs.
    stmt_undo: Vec<SharedUndoEntry>,
    // transaction-level redo logs.
    trx_redo: Vec<RedoEntry>,
    // statement-level redo logs.
    stmt_redo: Vec<RedoEntry>,
}

impl ActiveTrx {
    /// Create a new transaction.
    #[inline]
    pub fn new(trx_id: TrxID, sts: TrxID) -> Self {
        ActiveTrx {
            trx_id: Arc::new(AtomicU64::new(trx_id)),
            sts,
            trx_undo: vec![],
            stmt_undo: vec![],
            trx_redo: vec![],
            stmt_redo: vec![],
        }
    }

    /// Returns transaction id of current transaction.
    #[inline]
    pub fn trx_id(&self) -> TrxID {
        self.trx_id.load(Ordering::Acquire)
    }

    /// Starts a statement.
    #[inline]
    pub fn start_stmt(&mut self) {
        debug_assert!(self.stmt_undo.is_empty());
        debug_assert!(self.stmt_redo.is_empty());
    }

    /// Ends a statement.
    #[inline]
    pub fn end_stmt(&mut self) {
        self.trx_undo.extend(self.stmt_undo.drain(..));
        self.trx_redo.extend(self.stmt_redo.drain(..));
    }

    /// Rollback a statement.
    #[inline]
    pub fn rollback_stmt(&mut self, buf_pool: &FixedBufferPool) {
        while let Some(undo) = self.stmt_undo.pop() {
            let page_guard: PageGuard<'_, RowPage> = buf_pool
                .get_page(undo.page_id, LatchFallbackMode::Exclusive)
                .expect("get page for undo should not fail");
            let page_guard = page_guard.block_until_exclusive();
            todo!()
        }
    }

    /// Prepare current transaction for committing.
    #[inline]
    pub fn prepare(self) -> PreparedTrx {
        debug_assert!(self.stmt_undo.is_empty());
        debug_assert!(self.stmt_redo.is_empty());
        // use bincode to serialize redo log
        let redo_bin = if self.trx_redo.is_empty() {
            None
        } else {
            // todo: use customized serialization method, and keep CTS placeholder.
            let redo_log = RedoLog {
                cts: INVALID_TRX_ID,
                data: self.trx_redo,
            };
            let redo_bin = bincode::serde::encode_to_vec(&redo_log, bincode::config::standard())
                .expect("redo serialization should not fail");
            Some(redo_bin)
        };
        PreparedTrx {
            trx_id: self.trx_id,
            sts: self.sts,
            // cts,
            redo_bin,
            undo: self.trx_undo,
        }
    }

    /// Rollback current transaction.
    #[inline]
    pub fn rollback(self) {
        todo!()
    }

    #[inline]
    fn insert_row_into_page(
        &mut self,
        page_guard: &mut PageExclusiveGuard<'_, RowPage>,
        insert: &InsertRow,
    ) -> InsertResult {
        match page_guard.page_mut().insert(&insert) {
            InsertResult::Ok(row_id) => {
                let page_id = page_guard.page_id();
                // create undo log.
                let undo_entry = Arc::new(UndoEntry {
                    ts: Arc::clone(&self.trx_id),
                    page_id,
                    row_id,
                    kind: UndoKind::Insert,
                    next: Mutex::new(None),
                });
                // store undo log in undo map.
                let undo_map = page_guard
                    .bf_mut()
                    .undo_map
                    .get_or_insert_with(|| Box::new(HashMap::new()))
                    .as_mut();
                let res = undo_map.insert(row_id, Arc::clone(&undo_entry));
                debug_assert!(res.is_none()); // insert must not have old version.
                                              // store undo log into transaction undo buffer.
                self.stmt_undo.push(undo_entry);
                // create redo log.
                let redo_entry = RedoEntry {
                    page_id,
                    row_id,
                    kind: insert.create_redo(),
                };
                // store redo log into transaction redo buffer.
                self.stmt_redo.push(redo_entry);
                InsertResult::Ok(row_id)
            }
            err => err,
        }
    }
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PreparedTrx {
    pub trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    pub redo_bin: Option<RedoBin>,
    pub undo: Vec<SharedUndoEntry>,
}

impl PreparedTrx {
    #[inline]
    pub fn fill_cts(self, cts: TrxID) -> PrecommitTrx {
        let mut redo_bin = self.redo_bin;
        if let Some(redo_bin) = redo_bin.as_mut() {
            backfill_cts(redo_bin, cts);
        }
        PrecommitTrx {
            trx_id: self.trx_id,
            sts: self.sts,
            cts,
            redo_bin,
            undo: self.undo,
        }
    }
}

#[inline]
fn backfill_cts(redo_bin: &mut [u8], cts: TrxID) {
    // todo
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PrecommitTrx {
    pub trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    pub cts: TrxID,
    pub redo_bin: Option<RedoBin>,
    pub undo: Vec<SharedUndoEntry>,
}

pub struct CommittedTrx {
    pub sts: TrxID,
    pub cts: TrxID,
    pub undo: Vec<SharedUndoEntry>,
}
