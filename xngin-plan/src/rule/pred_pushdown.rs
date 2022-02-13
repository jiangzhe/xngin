use crate::error::{Error, Result};
use crate::op::{Aggr, Filt, Join, JoinKind, Op, OpMutVisitor, QualifiedJoin};
use crate::query::{QueryPlan, QuerySet};
use crate::rule::expr_simplify::simplify_single;
use indexmap::IndexSet;
use smol_str::SmolStr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;
use xngin_expr::fold::Fold;
use xngin_expr::{Col, Const, Expr, ExprMutVisitor, ExprVisitor, QueryID};

/// Pushdown predicates.
#[inline]
pub fn pred_pushdown(QueryPlan { queries, root }: &mut QueryPlan) -> Result<()> {
    pushdown_pred(queries, *root)
}

fn pushdown_pred(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, op| {
        let mut ppd = PredPushdown {
            qry_set,
            res: Ok(()),
        };
        let _ = op.walk_mut(&mut ppd);
        ppd.res
    })?
}

struct PredPushdown<'a> {
    qry_set: &'a mut QuerySet,
    res: Result<()>,
}

impl OpMutVisitor for PredPushdown<'_> {
    fn enter(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Filt(Filt { pred, source }) => {
                // 1. Collect QueryID from predicate
                // do not take care of const predicate, which is handle
                // by operator eliminate rule.
                let mut pred_item = ExprItem::new(mem::take(pred));
                let attr = pred_item.load_attr();
                if !attr.has_aggf && attr.qry_ids.len() == 1 {
                    // 2. Only involve single table and no aggregation function,
                    // try pushing down entire predicate
                    match push_single(self.qry_set, source, pred_item) {
                        Ok(None) => {
                            // the only predicate is pushed down,
                            // current filter can be removed.
                            let source = mem::take(source.as_mut());
                            *op = source;
                            return self.enter(op);
                        }
                        Ok(Some(p)) => {
                            // child rejects the predicate, update it back
                            *pred = p.e;
                        }
                        Err(e) => {
                            self.res = Err(e);
                            return false;
                        }
                    }
                } else {
                    // 3. Split CNF predicates and recursively push down each of them.
                    todo!()
                }
                true
            }
            _ => true,
        }
    }
}

#[derive(Default, Clone)]
struct ExprAttr {
    qry_ids: IndexSet<QueryID>,
    has_aggf: bool,
}

impl ExprVisitor for ExprAttr {
    #[inline]
    fn enter(&mut self, e: &Expr) -> bool {
        match e {
            Expr::Aggf(_) => self.has_aggf = true,
            Expr::Col(Col::QueryCol(qry_id, _)) => {
                self.qry_ids.insert(*qry_id);
            }
            _ => (),
        }
        true
    }
}

#[derive(Clone)]
struct ExprItem {
    e: Expr,
    // lazy field
    attr: Option<ExprAttr>,
    reject_nulls: Option<HashMap<QueryID, bool>>,
}

impl ExprItem {
    fn new(e: Expr) -> Self {
        ExprItem {
            e,
            attr: None,
            reject_nulls: None,
        }
    }

    fn load_attr(&mut self) -> &ExprAttr {
        if self.attr.is_none() {
            let mut attr = ExprAttr::default();
            let _ = self.e.walk(&mut attr);
            self.attr = Some(attr);
        }
        self.attr.as_ref().unwrap() // won't fail
    }

    fn reset(&mut self) {
        self.attr = None;
        self.reject_nulls = None;
    }

    // check if the expression rejects null on given query id
    fn load_reject_null(&mut self, qry_id: QueryID) -> Result<bool> {
        if self.reject_nulls.is_none() {
            let mut m = HashMap::new();
            let e = self.e.clone(); // current approach is to try folding entire expression, so clone() is necessary.
            let res = e.reject_null(|e| match e {
                Expr::Col(Col::QueryCol(qid, _)) if *qid == qry_id => *e = Expr::const_null(),
                _ => (),
            })?;
            m.insert(qry_id, res);
            Ok(res)
        } else {
            let m = self.reject_nulls.as_mut().unwrap();
            match m.entry(qry_id) {
                Entry::Occupied(ent) => Ok(*ent.get()),
                Entry::Vacant(vac) => {
                    let e = self.e.clone();
                    let res = e.reject_null(|e| match e {
                        Expr::Col(Col::QueryCol(qid, _)) if *qid == qry_id => {
                            *e = Expr::const_null()
                        }
                        _ => (),
                    })?;
                    vac.insert(res);
                    Ok(res)
                }
            }
        }
    }
}

// recursively push pred to its child.
fn push_single(
    qry_set: &mut QuerySet,
    op: &mut Op,
    mut pred: ExprItem,
) -> Result<Option<ExprItem>> {
    let res = match op {
        Op::Query(qry_id) => {
            if let Some(subq) = qry_set.get(qry_id) {
                rewrite_out_expr(&mut pred, &subq.scope.out_cols);
                // after rewriting, Simplify it before pushing
                simplify_single(&mut pred.e)?;
                match &pred.e {
                    Expr::Const(Const::Null) => {
                        *op = Op::Empty;
                        return Ok(None);
                    }
                    Expr::Const(c) => {
                        if c.is_zero().unwrap_or_default() {
                            *op = Op::Empty;
                            return Ok(None);
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => (),
                }
                qry_set.transform_op(*qry_id, |qry_set, op| {
                    assert!(push_single(qry_set, op, pred)?.is_none()); // this push must succeed
                    Ok::<_, Error>(())
                })??;
                None
            } else {
                Some(pred)
            }
        }
        // Table always rejects
        Op::Table(..) => Some(pred),
        // Empty just ignores
        Op::Empty => None,
        Op::Row(_) => todo!(), // todo: evaluate immediately
        Op::Apply(_) => todo!(),
        // Proj/Sort/Limit will try pushing pred, and if fails just accept.
        Op::Proj(_) | Op::Sort(_) | Op::Limit(_) => push_or_accept(qry_set, op, pred)?,
        Op::Aggr(aggr) => {
            if pred.load_attr().has_aggf {
                Some(pred)
            } else {
                // after the validation, all expression that does not hold aggregate
                // functions can be pushed down through Aggr operator, as they can
                // only be composite of group columns, constants and functions.
                match push_single(qry_set, &mut aggr.source, pred)? {
                    Some(pred) => {
                        // we cannot simply return None, because Aggr operator may be the root
                        // operator of a query, so we replace itself with the filter
                        let old = mem::take(op);
                        *op = Op::filt(pred.e, old);
                        None
                    }
                    None => None,
                }
            }
        }
        Op::Filt(filt) => match push_single(qry_set, &mut filt.source, pred)? {
            Some(pred) => {
                let old = mem::take(&mut filt.pred);
                let mut new = Expr::pred_and(old, pred.e);
                simplify_single(&mut new)?;
                filt.pred = new;
                None
            }
            None => None,
        },
        Op::Setop(so) => {
            // push to both side and won't fail
            assert!(push_single(qry_set, &mut so.left, pred.clone())?.is_none());
            assert!(push_single(qry_set, &mut so.right, pred)?.is_none());
            None
        }
        Op::Join(join) => {
            let attr = pred.load_attr();
            if attr.has_aggf {
                Some(pred)
            } else {
                match attr.qry_ids.len() {
                    0 => unreachable!(),
                    1 => {
                        let qry_id = attr.qry_ids.first().cloned().unwrap();
                        // push down to single table
                        match join.as_mut() {
                            Join::Cross(tbls) => {
                                let tbl = tbls
                                    .iter_mut()
                                    .find(|t| t.contains_qry(qry_id))
                                    .ok_or_else(|| {
                                        Error::InternalError(format!("Query {} not found", *qry_id))
                                    })?;
                                // push must succeed
                                assert!(push_single(qry_set, tbl, pred)?.is_none());
                                None
                            }
                            Join::Qualified(QualifiedJoin {
                                kind, left, right, ..
                            }) => match kind {
                                JoinKind::Inner => {
                                    if left.contains_qry(qry_id) {
                                        assert!(push_single(qry_set, left, pred)?.is_none());
                                        None
                                    } else if right.contains_qry(qry_id) {
                                        assert!(push_single(qry_set, right, pred)?.is_none());
                                        None
                                    } else {
                                        return Err(Error::InternalError(format!(
                                            "Query {} not found",
                                            *qry_id
                                        )));
                                    }
                                }
                                JoinKind::Left => {
                                    if left.contains_qry(qry_id) {
                                        // predicate on left table in left join can be directly pushed down.
                                        assert!(push_single(qry_set, left, pred)?.is_none());
                                        None
                                    } else if right.contains_qry(qry_id) {
                                        // predicate on right table in left join may or may not change the join type,
                                        // depending on whether the expression rejects null.
                                        if pred.load_reject_null(qry_id)? {
                                            // convert outer join to inner join and push to right side
                                            *kind = JoinKind::Inner;
                                            assert!(push_single(qry_set, right, pred)?.is_none());
                                            None
                                        } else {
                                            // can neither convert join type nor push
                                            Some(pred)
                                        }
                                    } else {
                                        return Err(Error::InternalError(format!(
                                            "Query {} not found",
                                            *qry_id
                                        )));
                                    }
                                }
                                JoinKind::Full => {
                                    if left.contains_qry(qry_id) {
                                        if pred.load_reject_null(qry_id)? {
                                            // convert full join to left join and push to left side
                                            *kind = JoinKind::Left;
                                            assert!(push_single(qry_set, left, pred)?.is_none());
                                            None
                                        } else {
                                            Some(pred)
                                        }
                                    } else if right.contains_qry(qry_id) {
                                        if pred.load_reject_null(qry_id)? {
                                            // convert full join to right join, but we don't have right join,
                                            // so convert to left join and swap left and right children, push to left side.
                                            *kind = JoinKind::Left;
                                            mem::swap(left, right);
                                            assert!(push_single(qry_set, left, pred)?.is_none());
                                            None
                                        } else {
                                            Some(pred)
                                        }
                                    } else {
                                        return Err(Error::InternalError(format!(
                                            "Query {} not found",
                                            *qry_id
                                        )));
                                    }
                                }
                                JoinKind::Semi
                                | JoinKind::AntiSemi
                                | JoinKind::Mark
                                | JoinKind::Single => todo!(),
                            },
                        }
                    }
                    2 => {
                        todo!()
                    }
                    _ => {
                        todo!()
                    }
                }
            }
        }
    };
    Ok(res)
}

fn push_or_accept(qry_set: &mut QuerySet, op: &mut Op, pred: ExprItem) -> Result<Option<ExprItem>> {
    let source = op.source_mut().unwrap(); // won't fail
    match push_single(qry_set, source, pred)? {
        Some(pred) => {
            let child = mem::take(source);
            let new_filt = Op::filt(pred.e, child);
            *source = new_filt;
            Ok(None)
        }
        None => Ok(None),
    }
}

fn rewrite_out_expr(item: &mut ExprItem, out: &[(Expr, SmolStr)]) {
    let mut roe = RewriteOutExpr { out };
    let _ = item.e.walk_mut(&mut roe);
    item.reset(); // must reset lazy field as the expression changed
}

struct RewriteOutExpr<'a> {
    out: &'a [(Expr, SmolStr)],
}

impl ExprMutVisitor for RewriteOutExpr<'_> {
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> bool {
        match e {
            Expr::Col(Col::QueryCol(_, idx)) => {
                let (new_e, _) = &self.out[*idx as usize];
                *e = new_e.clone();
            }
            _ => (),
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{
        assert_j_plan1, get_subq_by_location, get_subq_filt_expr, j_catalog, print_plan,
    };
    use crate::query::Location;

    // push single table
    #[test]
    fn test_pred_pushdown1() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 where c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 having c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select c1, count(*) from t1 group by c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 order by c0 limit 10) x1 where c1 > 0",
            assert_filt_on_disk_table1,
        );
    }

    // push cross join
    #[test]
    fn test_pred_pushdown2() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1, t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1, t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1, t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, count(*) from t1, t2 group by t1.c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
    }

    // push inner join
    #[test]
    fn test_pred_pushdown3() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1 join t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1 join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, c2, count(*) from t1 join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
    }

    fn assert_filt_on_disk_table1(s1: &str, mut q1: QueryPlan) {
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(get_subq_filt_expr(&subq1[0]).is_some());
    }

    fn assert_filt_on_disk_table1r(s1: &str, mut q1: QueryPlan) {
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(get_subq_filt_expr(&subq1[1]).is_some());
    }

    fn assert_filt_on_disk_table2(s1: &str, mut q1: QueryPlan) {
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(get_subq_filt_expr(&subq1[0]).is_some());
        assert!(get_subq_filt_expr(&subq1[1]).is_some());
    }

    fn assert_no_filt_on_disk_table(s1: &str, mut q1: QueryPlan) {
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_subq_filt_expr(subq).is_none()));
    }
}
