use crate::error::Result;
use crate::lgc::QuerySet;
use crate::rule::assign_id::assign_id;
use std::collections::{HashMap, HashSet};
use xngin_expr::{ExprKind, GlobalID, QueryID};

/// Predicate Movearound
///
/// This is a combination of prediate pushdown(PPD) and predicate pullup(PPU).
/// There are two important components to move around predicates.
///
/// 1. sets of columns.
///    An equal set contains columns that are specified equal in join conditions.
///
///    Example 1: "SELECT * FROM t1 JOIN t2 ON t1.c1 = t2.c2"
///    Equal set: {t1.c1, t2.c2}
///
///    Example 2: "SELECT * FROM t1 JOIN t2 ON t1.c1 = t2.c2 JOIN t3 ON t1.c1 = t3.c3"
///    Equal set: {t1.c1, t2.c2, t3.c3}
///
///    Example 3: "SELECT * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.c2"
///    Equal set: {}, but predicate can be propagated from t1.c1 to t2.c2.
///
/// 2. sets of filter expressions.
///    New expressions can be derived from existing expressions and equal set of columns
///
///    Example 1: "SELECT * FROM t1 JOIN t2 ON t1.c1 = t2.c2 WHERE t1.c1 > 0"
///    exprs: t1.c1 > 0
///    derived exprs: t2.c2 > 0
///
#[inline]
pub fn pred_move(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    assign_id(qry_set, qry_id)?;
    todo!()
}
