use crate::expr::{Col, Expr, ExprKind, QueryID};
use std::hash::Hash;
use xngin_catalog::TableID;

pub trait DataSourceID: Clone + Copy + PartialEq + Eq + Hash + PartialOrd + Ord + Sized {
    /// resolve data source from expression
    fn from_expr(e: &Expr) -> Option<(Self, u32)>;
}

impl DataSourceID for QueryID {
    #[inline]
    fn from_expr(e: &Expr) -> Option<(Self, u32)> {
        match &e.kind {
            ExprKind::Col(Col::QueryCol(qid, idx)) => Some((*qid, *idx)),
            _ => None,
        }
    }
}

impl DataSourceID for TableID {
    #[inline]
    fn from_expr(e: &Expr) -> Option<(Self, u32)> {
        match &e.kind {
            ExprKind::Col(Col::TableCol(tid, idx)) => Some((*tid, *idx)),
            _ => None,
        }
    }
}
