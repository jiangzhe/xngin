use doradb_datatype::Const;
use crate::row::RowID;

#[derive(Debug, Clone)]
pub enum RowAction {
    Ins(InsertRow),
    Del(DeleteRow),
    Upd(UpdateRow),
}

#[derive(Debug, Clone)]
pub struct InsertRow(pub Vec<Const>);

#[derive(Debug, Clone)]
pub struct DeleteRow(pub RowID);

#[derive(Debug, Clone)]
pub struct UpdateRow {
    pub row_id: RowID,
    pub cols: Vec<UpdateCol>,
}

#[derive(Debug, Clone)]
pub struct UpdateCol {
    pub idx: usize,
    pub val: Const,
}