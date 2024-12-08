use crate::row::value::Val;
use crate::row::{RowID, Row};

#[derive(PartialEq, Eq)]
pub enum SelectResult<'a> {
    Ok(Row<'a>),
    Deleted(Row<'a>),
    RowNotFound,
    RowInvalid,
}

#[derive(Debug, Clone)]
pub struct InsertRow<'a>(pub Vec<Val<'a>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    Ok(RowID),
    RowIDExhausted,
    NoFreeSpace,
}

#[derive(Debug, Clone)]
pub struct DeleteRow(pub RowID);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteResult {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
    RowInvalid,
}

#[derive(Debug, Clone)]
pub struct UpdateRow<'a> {
    pub row_id: RowID,
    pub cols: Vec<UpdateCol<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResult {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
    RowInvalid,
    NoFreeSpace,
}

#[derive(Debug, Clone)]
pub struct UpdateCol<'a> {
    pub idx: usize,
    pub val: Val<'a>,
}