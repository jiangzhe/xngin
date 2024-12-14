use crate::row::{Row, RowID};
use crate::trx::redo::RedoKind;
use crate::value::Val;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub enum SelectResult<'a> {
    Ok(Row<'a>),
    RowDeleted(Row<'a>),
    RowNotFound,
}

impl SelectResult<'_> {
    /// Returns if select succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectResult::Ok(_))
    }
}

#[derive(Debug)]
pub struct InsertRow(pub Vec<Val>);

impl InsertRow {
    /// Create redo log
    #[inline]
    pub fn create_redo(&self) -> RedoKind {
        RedoKind::Insert(self.0.clone())
    }
}

pub enum InsertResult {
    Ok(RowID),
    RowIDExhausted,
    NoFreeSpace,
}

impl InsertResult {
    /// Returns if insert succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertResult::Ok(_))
    }
}

#[derive(Debug)]
pub struct DeleteRow(pub RowID);

pub enum DeleteResult {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
}

impl DeleteResult {
    /// Returns if delete succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, DeleteResult::Ok)
    }
}

#[derive(PartialEq, Eq)]
pub struct UpdateRow {
    pub row_id: RowID,
    pub cols: Vec<UpdateCol>,
}

impl UpdateRow {
    /// Create redo log based on changed values.
    #[inline]
    pub fn create_redo(&self, undo: &[UpdateCol]) -> Option<RedoKind> {
        let hashset: HashSet<_> = undo.iter().map(|uc| uc.idx).collect();
        let vals: Vec<UpdateCol> = self
            .cols
            .iter()
            .filter(|uc| hashset.contains(&uc.idx))
            .cloned()
            .collect();
        if vals.is_empty() {
            return None;
        }
        Some(RedoKind::Update(vals))
    }
}

pub enum UpdateResult {
    Ok,
    RowNotFound,
    RowDeleted,
    NoFreeSpace,
}

impl UpdateResult {
    /// Returns if update succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateResult::Ok)
    }
}

#[derive(PartialEq, Eq)]
pub enum UpdateWithUndoResult {
    Ok(Vec<UpdateCol>),
    RowNotFound,
    RowDeleted,
    NoFreeSpace,
}

impl UpdateWithUndoResult {
    /// Returns if update with undo succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateWithUndoResult::Ok(_))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCol {
    pub idx: usize,
    pub val: Val,
}
