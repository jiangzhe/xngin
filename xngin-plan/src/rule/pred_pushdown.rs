use crate::error::{Error, Result};
use crate::op::{Filt, Op, OpMutVisitor};
use crate::query::{QueryPlan, QuerySet};
use crate::rule::expr_simplify::simplify_single;
use indexmap::IndexSet;
use smol_str::SmolStr;
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
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
    qry_ids: IndexSet<QueryID>,
    has_aggf: bool,
}

impl ExprVisitor for ExprAttr {
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Aggf(_) => self.has_aggf = true,
            Expr::Col(Col::QueryCol(qry_id, _)) => {
                self.qry_ids.insert(*qry_id);
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
}

impl ExprItem {
    fn new(e: Expr) -> Self {
        ExprItem { e, attr: None }
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
                rewrite_out_expr(&mut pred, subq.out_cols());
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
                match push_single(qry_set, &mut aggr.source, pred)? {
                    Some(pred) => {
                        // always accept the pushed predicates as post-filter
                        let mut old = mem::take(&mut aggr.filt);
                        old.push(pred.e);
                        if old.len() > 1 {
                            // todo: once simplify_conj is done, update below code
                            let mut new = Expr::pred_conj(old);
                            simplify_single(&mut new)?;
                            aggr.filt = new.into_conj();
                        } else {
                            aggr.filt = old;
                        }
                        None
                    }
                    None => None,
                }
            }
        }
        Op::Filt(filt) => match push_single(qry_set, &mut filt.source, pred)? {
            Some(pred) => {
                let mut old = mem::take(&mut filt.pred);
                old.push(pred.e);
                let mut new = Expr::pred_conj(old);
                simplify_single(&mut new)?;
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
        Op::Join(_) => {
            unreachable!("Directly push down to join operator not supported")
        }
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

fn rewrite_out_expr(item: &mut ExprItem, out: &[(Expr, SmolStr)]) {
    let mut roe = RewriteOutExpr { out };
    let _ = item.e.walk_mut(&mut roe);
    item.reset(); // must reset lazy field as the expression changed
}

struct RewriteOutExpr<'a> {
    out: &'a [(Expr, SmolStr)],
}

impl ExprMutVisitor for RewriteOutExpr<'_> {
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::Col(Col::QueryCol(_, idx)) = e {
            let (new_e, _) = &self.out[*idx as usize];
            *e = new_e.clone();
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{
        assert_j_plan1, get_subq_by_location, get_subq_filt_expr, j_catalog, print_plan,
    };
    use crate::join::{JoinGraph, JoinKind};
    use crate::op::OpVisitor;
    use crate::query::Location;
    use crate::rule::joingraph_initialize;

    #[test]
    fn test_pred_pushdown_single_table() {
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
            "select 1 from (select c1 from t1 where c1 > 0) x1",
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

    #[test]
    fn test_pred_pushdown_cross_join() {
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

    #[test]
    fn test_pred_pushdown_inner_join() {
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

    #[test]
    fn test_pred_pushdown_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // filter expression NOT rejects null, so cannot be pushed
        // to table scan.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where c2 is null",
            assert_no_filt_on_disk_table,
        );
        // involve both sides, cannot be pushed to table scan,
        // join type will be converted to inner join.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = c2",
            assert_no_filt_on_disk_table,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1 left join t2 having t1.c1 = 0 order by c1",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1 left join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, c2, count(*) from t1 left join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
        // one left join converted to inner join
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t3.c3 where t1.c1 = t2.c2",
            |s1: &str, mut q1: QueryPlan| {
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
            },
        );
        // both left joins converted to inner joins, and one more inner join added.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t2.c2 and t1.c1 = t3.c3 where t2.c2 = t3.c3",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                // one inner join, one left join
                assert_eq!(3, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));
            }
        );
        // both left joins converted to inner joins, and remove as no join condition,
        // one more inner join added.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 where t2.c2 = t3.c3",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                // one inner join, one left join
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));
            },
        );
        // one is pushed as join condition, one is pushed as filter
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 left join t3 left join t4 where t1.c1 = t2.c2 and t3.c3 is null",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                // one inner join, one left join
                assert_eq!(3, g.edges.len());
                assert_eq!(1usize, g.edges.values().map(|e| e.cond.len()).sum());
                assert_eq!(1usize, g.edges.values().map(|e| e.filt.len()).sum());
            }
        );
    }

    #[test]
    fn test_pred_pushdown_right_join() {
        let cat = j_catalog();
        // right join is replaced by left join, so right table 2 is t1.
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1,
        );
    }

    #[test]
    fn test_pred_pushdown_full_join() {
        let cat = j_catalog();
        // full join converted to left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        // full join converted to right join, then left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // full join converted to inner join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = t2.c2",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Inner));
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c1 is null and t2.c2 is null",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert_eq!(2usize, g.edges.values().map(|e| e.filt.len()).sum());
            },
        );
        // convert to left join and add one filt
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c0 > 0 and t2.c2 is null",
            |s1: &str, mut q1: QueryPlan| {
                joingraph_initialize(&mut q1).unwrap();
                pred_pushdown(&mut q1).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let g = extract_join_graph(&subq.root).unwrap();
                assert_eq!(1, g.edges.len());
                assert!(g.edges.values().all(|e| e.kind == JoinKind::Left));
                assert_eq!(1usize, g.edges.values().map(|e| e.filt.len()).sum());
            },
        );
    }

    fn assert_filt_on_disk_table1(s1: &str, mut q1: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());
    }

    fn assert_filt_on_disk_table1r(s1: &str, mut q1: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_filt_on_disk_table2(s1: &str, mut q1: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_subq_filt_expr(&subq1[0]).is_empty());
        assert!(!get_subq_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_no_filt_on_disk_table(s1: &str, mut q1: QueryPlan) {
        joingraph_initialize(&mut q1).unwrap();
        pred_pushdown(&mut q1).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_subq_filt_expr(subq).is_empty()));
    }

    fn extract_join_graph(op: &Op) -> Option<JoinGraph> {
        struct ExtractJoinGraph(Option<JoinGraph>);
        impl OpVisitor for ExtractJoinGraph {
            type Break = ();
            fn enter(&mut self, op: &Op) -> ControlFlow<()> {
                match op {
                    Op::JoinGraph(g) => {
                        self.0 = Some(g.as_ref().clone());
                        ControlFlow::Break(())
                    }
                    _ => ControlFlow::Continue(()),
                }
            }
        }
        let mut ex = ExtractJoinGraph(None);
        let _ = op.walk(&mut ex);
        ex.0
    }
}
