use crate::expr::{Col, ColKind, ExprKind};
use crate::id::{ColIndex, QueryID};
use std::hash::Hash;
use xngin_catalog::TableID;

pub trait DataSourceID: Clone + Copy + PartialEq + Eq + Hash + PartialOrd + Ord + Sized {
    /// resolve data source from expression
    fn from_expr(e: &ExprKind) -> Option<(Self, ColIndex)>;
}

impl DataSourceID for QueryID {
    #[inline]
    fn from_expr(e: &ExprKind) -> Option<(Self, ColIndex)> {
        match e {
            ExprKind::Col(Col {
                kind: ColKind::Query(qid),
                idx,
                ..
            }) => Some((*qid, *idx)),
            _ => None,
        }
    }
}

impl DataSourceID for TableID {
    #[inline]
    fn from_expr(e: &ExprKind) -> Option<(Self, ColIndex)> {
        match e {
            ExprKind::Col(Col {
                kind: ColKind::Table(tid, _, _),
                idx,
                ..
            }) => Some((*tid, *idx)),
            _ => None,
        }
    }
}
