use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, QualifiedJoin};
use crate::op::{Filt, Op, OpMutVisitor};
use crate::query::{QryIDs, QueryPlan, QuerySet};
use crate::rule::expr_simplify::simplify_nested;
use smol_str::SmolStr;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::fold::Fold;
use xngin_expr::{Col, Const, Expr, ExprMutVisitor, ExprVisitor, QueryID};

/// Pushdown predicates.
#[inline]
pub fn pred_pushdown(QueryPlan { qry_set, root }: &mut QueryPlan) -> Result<()> {
    pushdown_pred(qry_set, *root)
}

fn pushdown_pred(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppd = PredPushdown { qry_set };
        op.walk_mut(&mut ppd).unbranch()
    })?
}

struct PredPushdown<'a> {
    qry_set: &'a mut QuerySet,
}

impl OpMutVisitor for PredPushdown<'_> {
    type Break = Error;
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            Op::Filt(Filt { pred, source }) => {
                let mut fallback = vec![];
                for e in mem::take(pred) {
                    let item = ExprItem::new(e);
                    match push_single(self.qry_set, source, item).branch()? {
                        None => (),
                        Some(p) => {
                            // child rejects the predicate, add it to fallback
                            fallback.push(p.e);
                        }
                    }
                }
                if fallback.is_empty() {
                    // all predicates are pushed down, current operator can be removed.
                    let source = mem::take(source.as_mut());
                    *op = source;
                    self.enter(op)
                } else {
                    // still have certain predicates can not be pushed down,
                    // update them back
                    *pred = fallback;
                    ControlFlow::Continue(())
                }
            }
            Op::Query(qry_id) => pushdown_pred(self.qry_set, *qry_id).branch(),
            _ => ControlFlow::Continue(()),
        }
    }
}

#[derive(Default, Clone)]
struct ExprAttr {
    qry_ids: QryIDs,
    has_aggf: bool,
}

impl ExprVisitor for ExprAttr {
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Aggf(_) => self.has_aggf = true,
            Expr::Col(Col::QueryCol(qry_id, _)) => {
                // self.qry_ids.insert(*qry_id);
                match &mut self.qry_ids {
                    QryIDs::Empty => {
                        self.qry_ids = QryIDs::Single(*qry_id);
                    }
                    QryIDs::Single(qid) => {
                        if qid != qry_id {
                            let mut hs = HashSet::new();
                            hs.insert(*qid);
                            hs.insert(*qry_id);
                            self.qry_ids = QryIDs::Multi(hs);
                        }
                    }
                    QryIDs::Multi(hs) => {
                        hs.insert(*qry_id);
                    }
                }
            }
            _ => (),
        }
        ControlFlow::Continue(())
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

    fn load_reject_null(&mut self, qry_id: QueryID) -> Result<bool> {
        if let Some(reject_nulls) = &mut self.reject_nulls {
            let res = match reject_nulls.entry(qry_id) {
                Entry::Occupied(occ) => *occ.get(),
                Entry::Vacant(vac) => {
                    let rn = self.e.clone().reject_null(|e| match e {
                        Expr::Col(Col::QueryCol(qid, _)) if *qid == qry_id => {
                            *e = Expr::const_null();
                        }
                        _ => (),
                    })?;
                    vac.insert(rn);
                    rn
                }
            };
            Ok(res)
        } else {
            let mut reject_nulls = HashMap::new();
            let rn = self.e.clone().reject_null(|e| match e {
                Expr::Col(Col::QueryCol(qid, _)) if *qid == qry_id => {
                    *e = Expr::const_null();
                }
                _ => (),
            })?;
            reject_nulls.insert(qry_id, rn);
            self.reject_nulls = Some(reject_nulls);
            Ok(rn)
        }
    }

    fn rewrite(&mut self, qry_id: QueryID, out: &[(Expr, SmolStr)]) {
        let mut roe = RewriteOutExpr { qry_id, out };
        let _ = self.e.walk_mut(&mut roe);
        // must reset lazy field as the expression changed
        self.attr = None;
        self.reject_nulls = None;
    }
}

fn push_single(
    qry_set: &mut QuerySet,
    op: &mut Op,
    mut pred: ExprItem,
) -> Result<Option<ExprItem>> {
    let res = match op {
        Op::Query(qry_id) => {
            if let Some(subq) = qry_set.get(qry_id) {
                pred.rewrite(*qry_id, subq.out_cols());
                // after rewriting, Simplify it before pushing
                simplify_nested(&mut pred.e)?;
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
                qry_set.transform_op(*qry_id, |qry_set, _, op| {
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
                // after the validation, all expressions containing no aggregate
                // functions can be pushed down through Aggr operator, as they can
                // only be composite of group columns, constants and functions.
                assert!(push_single(qry_set, &mut aggr.source, pred)?.is_none());
                None
            }
        }
        Op::Filt(filt) => match push_single(qry_set, &mut filt.source, pred)? {
            Some(pred) => {
                let mut old = mem::take(&mut filt.pred);
                old.push(pred.e);
                let mut new = Expr::pred_conj(old);
                simplify_nested(&mut new)?;
                filt.pred = new.into_conj();
                None
            }
            None => None,
        },
        Op::Setop(so) => {
            // push to both side and won't fail
            assert!(push_single(qry_set, so.left.as_mut(), pred.clone())?.is_none());
            assert!(push_single(qry_set, so.right.as_mut(), pred)?.is_none());
            None
        }
        Op::JoinGraph(graph) => {
            let extra = graph.add_single_filt(pred.e)?;
            for (e, qry_id) in extra {
                // try pushing extra expressions
                qry_set.transform_op(qry_id, |qry_set, _, op| {
                    match push_single(qry_set, op, ExprItem::new(e)) {
                        Ok(None) => Ok(()),
                        Err(e) => Err(e),
                        _ => Err(Error::InternalError(
                            "Predicate pushdown failed on subquery".to_string(),
                        )),
                    }
                })??;
            }
            None
        }
        Op::Join(join) => match join.as_mut() {
            Join::Cross(jos) => {
                if pred.load_attr().has_aggf {
                    // do not push predicates with aggregate functions
                    Some(pred)
                } else {
                    let qry_ids = &pred.load_attr().qry_ids;
                    match qry_ids {
                        QryIDs::Empty => unreachable!(), // Currently marked as unreachable
                        QryIDs::Single(qry_id) => {
                            // predicate of single table
                            let mut jo_qids = HashSet::new(); // reused container
                            for jo in jos {
                                jo_qids.clear();
                                jo.collect_qry_ids(&mut jo_qids);
                                if jo_qids.contains(qry_id) {
                                    assert!(push_single(qry_set, jo.as_mut(), pred)?.is_none());
                                    return Ok(None);
                                }
                            }
                            Some(pred)
                        }
                        _ => Some(pred), // if involved multiple tables, we delay it to push unpon join graph
                    }
                }
            }
            Join::Qualified(QualifiedJoin {
                kind,
                left,
                right,
                cond,
                filt,
            }) => {
                let qry_ids = &pred.load_attr().qry_ids;
                match qry_ids {
                    QryIDs::Empty => unreachable!(), // Currently marked as unreachable
                    QryIDs::Single(qry_id) => {
                        let qry_id = *qry_id;
                        // let mut jo_qids = HashSet::new();
                        // // handle on left side
                        // left.collect_qry_ids(&mut jo_qids);
                        if left.contains_qry_id(qry_id) {
                            match kind {
                                JoinKind::Inner | JoinKind::Left => {
                                    assert!(push_single(qry_set, left.as_mut(), pred)?.is_none());
                                    return Ok(None);
                                }
                                JoinKind::Full => {
                                    if pred.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert full join to right join, then to left join
                                        *kind = JoinKind::Left;
                                        mem::swap(left, right);
                                        // push to (original) left side
                                        assert!(
                                            push_single(qry_set, right.as_mut(), pred)?.is_none()
                                        );
                                        return Ok(None);
                                    } else {
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                }
                                _ => todo!(),
                            }
                        } else if right.contains_qry_id(qry_id) {
                            match kind {
                                JoinKind::Inner => {
                                    assert!(push_single(qry_set, right.as_mut(), pred)?.is_none());
                                    return Ok(None);
                                }
                                JoinKind::Left => {
                                    if pred.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert left join to inner join
                                        *kind = JoinKind::Inner;
                                        if !filt.is_empty() {
                                            cond.extend(mem::take(filt)) // put filters into condition
                                        }
                                        assert!(
                                            push_single(qry_set, right.as_mut(), pred)?.is_none()
                                        );
                                        return Ok(None);
                                    } else {
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                }
                                JoinKind::Full => {
                                    if pred.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert full join to left join
                                        *kind = JoinKind::Left;
                                        assert!(
                                            push_single(qry_set, right.as_mut(), pred)?.is_none()
                                        );
                                        return Ok(None);
                                    } else {
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                }
                                _ => todo!(),
                            }
                        } else {
                            // this should not happen, the predicate must belong to either side
                            unreachable!()
                        }
                    }
                    QryIDs::Multi(qry_ids) => {
                        let mut left_qids = HashSet::new();
                        left.collect_qry_ids(&mut left_qids);
                        let left_qids: HashSet<QueryID> =
                            qry_ids.intersection(&left_qids).cloned().collect();
                        let mut right_qids = HashSet::new();
                        right.collect_qry_ids(&mut right_qids);
                        let right_qids: HashSet<QueryID> =
                            qry_ids.intersection(&right_qids).cloned().collect();
                        match (left_qids.is_empty(), right_qids.is_empty()) {
                            (false, true) => {
                                // handle on left side
                                match kind {
                                    JoinKind::Inner | JoinKind::Left => {
                                        assert!(
                                            push_single(qry_set, left.as_mut(), pred)?.is_none()
                                        );
                                        return Ok(None);
                                    }
                                    JoinKind::Full => {
                                        for qry_id in left_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                // convert full join to right join, then to left join
                                                *kind = JoinKind::Left;
                                                mem::swap(left, right);
                                                // push to (original) left side
                                                assert!(push_single(
                                                    qry_set,
                                                    right.as_mut(),
                                                    pred
                                                )?
                                                .is_none());
                                                return Ok(None);
                                            }
                                        }
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                    _ => todo!(),
                                }
                            }
                            (true, false) => {
                                // handle on right side
                                match kind {
                                    JoinKind::Inner => {
                                        assert!(
                                            push_single(qry_set, right.as_mut(), pred)?.is_none()
                                        );
                                        return Ok(None);
                                    }
                                    JoinKind::Left => {
                                        for qry_id in right_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                // convert left join to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    cond.extend(mem::take(filt))
                                                    // put filters into condition
                                                }
                                                assert!(push_single(
                                                    qry_set,
                                                    right.as_mut(),
                                                    pred
                                                )?
                                                .is_none());
                                                return Ok(None);
                                            }
                                        }
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                    JoinKind::Full => {
                                        for qry_id in right_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                // convert full join to left join
                                                *kind = JoinKind::Left;
                                                assert!(push_single(
                                                    qry_set,
                                                    right.as_mut(),
                                                    pred
                                                )?
                                                .is_none());
                                                return Ok(None);
                                            }
                                        }
                                        // not reject null
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                    _ => todo!(),
                                }
                            }
                            (false, false) => {
                                // handle on both sides
                                match kind {
                                    JoinKind::Inner => {
                                        cond.push(pred.e);
                                        return Ok(None);
                                    }
                                    JoinKind::Left => {
                                        for qry_id in right_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                // convert left join to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    cond.extend(mem::take(filt))
                                                    // put filters into condition
                                                }
                                                cond.push(pred.e);
                                                return Ok(None);
                                            }
                                        }
                                        // not reject null on right side
                                        filt.push(pred.e);
                                        return Ok(None);
                                    }
                                    JoinKind::Full => {
                                        let mut left_reject_null = false;
                                        let mut right_reject_null = false;
                                        for qry_id in left_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                left_reject_null = true;
                                                break;
                                            }
                                        }
                                        for qry_id in right_qids {
                                            if pred.load_reject_null(qry_id)? {
                                                right_reject_null = true;
                                                break;
                                            }
                                        }
                                        match (left_reject_null, right_reject_null) {
                                            (true, true) => {
                                                // convert to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    cond.extend(mem::take(filt))
                                                }
                                                cond.push(pred.e);
                                                return Ok(None);
                                            }
                                            (true, false) => {
                                                // convert to right join then left join
                                                *kind = JoinKind::Left;
                                                mem::swap(left, right);
                                                filt.push(pred.e);
                                                return Ok(None);
                                            }
                                            (false, true) => {
                                                // convert to left join
                                                *kind = JoinKind::Left;
                                                filt.push(pred.e);
                                                return Ok(None);
                                            }
                                            (false, false) => {
                                                filt.push(pred.e);
                                                return Ok(None);
                                            }
                                        }
                                    }
                                    _ => todo!(),
                                }
                            }
                            (true, true) => unreachable!(),
                        }
                    }
                }
            }
        },
    };
    Ok(res)
}

fn push_or_accept(qry_set: &mut QuerySet, op: &mut Op, pred: ExprItem) -> Result<Option<ExprItem>> {
    let source = op.source_mut().unwrap(); // won't fail
    match push_single(qry_set, source, pred)? {
        Some(pred) => {
            let child = mem::take(source);
            let new_filt = Op::filt(vec![pred.e], child);
            *source = new_filt;
            Ok(None)
        }
        None => Ok(None),
    }
}

struct RewriteOutExpr<'a> {
    qry_id: QueryID,
    out: &'a [(Expr, SmolStr)],
}

impl ExprMutVisitor for RewriteOutExpr<'_> {
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::Col(Col::QueryCol(qry_id, idx)) = e {
            if *qry_id == self.qry_id {
                let (new_e, _) = &self.out[*idx as usize];
                *e = new_e.clone();
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{
        assert_j_dup_plan, assert_j_plan1, extract_join_graph, extract_join_kinds,
        get_subq_by_location, get_subq_filt_expr, j_catalog, print_plan,
    };
    use crate::join::JoinKind;
    use crate::query::Location;
    use crate::rule::joingraph_initialize;

    #[test]
    fn test_pred_pushdown_single_table() {
        let cat = j_catalog();
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 where c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 where c0 + c1 = c1 + c0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select c1 from t1 having c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select c1 from t1) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select c1 from t1 where c1 > 0) x1",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select c1, count(*) from t1 group by c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select c1 from t1 order by c0 limit 10) x1 where c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select 1 as c1 from t1) x1 where c1 > 0",
            assert_no_filt_on_disk_table,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select 1 as c1 from t1) x1 where c1 = 0",
            |s1, mut q1| {
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                if let Op::Proj(proj) = &subq.root {
                    assert_eq!(&Op::Empty, proj.source.as_ref());
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select null as c1 from t1) x1 where c1 = 0",
            |s1, mut q1| {
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                if let Op::Proj(proj) = &subq.root {
                    assert_eq!(&Op::Empty, proj.source.as_ref());
                }
            },
        )
    }

    #[test]
    fn test_pred_pushdown_cross_join() {
        let cat = j_catalog();
        assert_j_dup_plan(
            &cat,
            "select 1 from t1, t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1 from t1, t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select t1.c1 from t1, t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1, count(*) from t1, t2 group by t1.c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
    }

    #[test]
    fn test_pred_pushdown_inner_join() {
        let cat = j_catalog();
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1 from t1 join t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select t1.c1 from t1 join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1, c2, count(*) from t1 join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
    }

    #[test]
    fn test_pred_pushdown_left_join() {
        let cat = j_catalog();
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // filter expression NOT rejects null, so cannot be pushed
        // to table scan.
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 where c2 is null",
            assert_no_filt_on_disk_table,
        );
        // involve both sides, cannot be pushed to table scan,
        // join type will be converted to inner join.
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = c2",
            assert_no_filt_on_disk_table,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1 from t1 left join t2 having t1.c1 = 0 order by c1",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from (select t1.c1 from t1 left join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_dup_plan(
            &cat,
            "select t1.c1, c2, count(*) from t1 left join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
        // one left join converted to inner join
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t3.c3 where t1.c1 = t2.c2",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                // one inner join, one left join
                assert_eq!(2, g.edges.len());
                assert_eq!(
                    1,
                    g.edges
                        .values()
                        .filter(|e| e.kind == JoinKind::Inner)
                        .count()
                );
                assert_eq!(
                    1,
                    g.edges
                        .values()
                        .filter(|e| e.kind == JoinKind::Left)
                        .count()
                );

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["left", "inner"], jks);
            },
        );
        // both left joins converted to inner joins, and one more inner join added.
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t2.c2 and t1.c1 = t3.c3 where t2.c2 = t3.c3",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(3, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                // in this case, the predicate pushdown stops at the first join
                // so second join will not be converted to inner join
                // but will be fixed by predicate propagation rule.
                assert_eq!(vec!["inner", "left"], jks);
            }
        );
        // both left joins converted to inner joins, and remove as no join condition,
        // one more inner join added.
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 left join t2 left join t3 where t2.c2 = t3.c3",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                // stops at first join, second will not be converted to inner join
                assert_eq!(vec!["inner", "left"], jks);
            },
        );
        // one is pushed as join condition, one is pushed as filter
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 join t2 left join t3 left join t4 where t1.c1 = t2.c2 and t3.c3 is null",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(3, g.edges.len());
                assert_eq!(1usize, g.edges.values().map(|e| e.cond.len()).sum());
                assert_eq!(1usize, g.edges.values().map(|e| e.filt.len()).sum());

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["left", "left", "inner"], jks);
            }
        );
    }

    #[test]
    fn test_pred_pushdown_right_join() {
        let cat = j_catalog();
        // right join is replaced by left join, so right table 2 is t1.
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 right join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 right join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1,
        );
    }

    #[test]
    fn test_pred_pushdown_full_join() {
        let cat = j_catalog();
        // full join converted to left join
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = 0",
            |s1, mut q1, q2| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq1 = get_subq_by_location(&q1, Location::Disk);
                assert!(!get_subq_filt_expr(&subq1[0]).is_empty());

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq1 = get_subq_by_location(&q1, Location::Disk);
                // converted to right table, then left table
                // the underlying query postion is changed.
                assert!(!get_subq_filt_expr(&subq1[1]).is_empty());
            },
        );
        // full join converted to right join, then left join
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 full join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // full join converted to inner join
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = t2.c2",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["inner"], jks);
            },
        );
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c1 is null and t2.c2 is null",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert_eq!(2usize, g.edges.values().map(|e| e.filt.len()).sum());

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["full"], jks);
            },
        );
        // convert to left join and add one filt
        assert_j_dup_plan(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c0 > 0 and t2.c2 is null",
            |s1: &str, mut q1: QueryPlan, q2: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Left));
                assert_eq!(1usize, g.edges.values().map(|e| e.filt.len()).sum());

                q1 = q2;
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["left"], jks);
            },
        );
    }

    fn assert_filt_on_disk_table1(s1: &str, mut q1: QueryPlan, q2: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());

        q1 = q2;
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());
    }

    fn assert_filt_on_disk_table1r(s1: &str, mut q1: QueryPlan, q2: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());

        q1 = q2;
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_filt_on_disk_table2(s1: &str, mut q1: QueryPlan, q2: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());

        q1 = q2;
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_no_filt_on_disk_table(s1: &str, mut q1: QueryPlan, q2: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_subq_filt_expr(subq).is_empty()));

        q1 = q2;
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_subq_filt_expr(subq).is_empty()));
    }
}
