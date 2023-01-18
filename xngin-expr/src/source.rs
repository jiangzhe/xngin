use crate::expr::{Col, ColIndex, ColKind, Expr, ExprKind, QueryID};
use std::hash::Hash;
use xngin_catalog::TableID;

pub trait DataSourceID: Clone + Copy + PartialEq + Eq + Hash + PartialOrd + Ord + Sized {
    /// resolve data source from expression
    fn from_expr(e: &Expr) -> Option<(Self, ColIndex)>;
}

impl DataSourceID for QueryID {
    #[inline]
    fn from_expr(e: &Expr) -> Option<(Self, ColIndex)> {
        match &e.kind {
            ExprKind::Col(Col {
                kind: ColKind::QueryCol(qid),
                idx,
                ..
            }) => Some((*qid, *idx)),
            _ => None,
        }
    }
}

impl DataSourceID for TableID {
    #[inline]
    fn from_expr(e: &Expr) -> Option<(Self, ColIndex)> {
        match &e.kind {
            ExprKind::Col(Col {
                kind: ColKind::TableCol(tid, _),
                idx,
                ..
            }) => Some((*tid, *idx)),
            _ => None,
        }
    }
}
