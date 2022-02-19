use crate::error::Result;
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::op::{Filt, Limit, Op, OpMutVisitor, Proj, Setop, SetopKind, Sort};
use crate::query::{QueryPlan, QuerySet};
use crate::rule::expr_simplify::update_simplify_single;
use std::collections::HashSet;
use std::mem;
use xngin_expr::{Col, Const, Expr, ExprMutVisitor, QueryID, Setq};

/// Eliminate redundant operators.
/// 1. Filter with true predicate can be removed.
/// 2. Join with some empty child can be rewritten.
/// 3. Setop with some empty child can be rewritten.
/// 4. Aggr with empty child can be directly evaluated. (todo)
/// 5. Any other operators except Join, Setop, Aggr with empty child can be replaced with Empty.
/// 6. LIMIT 0 can eliminiate entire tree.
/// 7. ORDER BY in subquery without LIMIT can be removed.
#[inline]
pub fn op_eliminate(QueryPlan { queries, root }: &mut QueryPlan) -> Result<()> {
    eliminate_op(queries, *root, false)
}

fn eliminate_op(qry_set: &mut QuerySet, qry_id: QueryID, is_subq: bool) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut eo = EliminateOp::new(qry_set, is_subq);
        let _ = op.walk_mut(&mut eo);
        eo.res
    })?
}

struct EliminateOp<'a> {
    qry_set: &'a mut QuerySet,
    is_subq: bool,
    has_limit: bool,
    empty_qs: HashSet<QueryID>,
    res: Result<()>,
}

impl<'a> EliminateOp<'a> {
    fn new(qry_set: &'a mut QuerySet, is_subq: bool) -> Self {
        EliminateOp {
            qry_set,
            is_subq,
            has_limit: false,
            empty_qs: HashSet::new(),
            res: Ok(()),
        }
    }

    fn bottom_up(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Join(j) => match j.as_mut() {
                Join::Cross(tbls) => {
                    if tbls.iter().any(|t| t.as_ref().is_empty()) {
                        // any child in cross join results in empty rows
                        *op = Op::Empty
                    }
                }
                Join::Qualified(QualifiedJoin {
                    kind,
                    left: JoinOp(Op::Empty),
                    right: JoinOp(right_op),
                    ..
                }) => match kind {
                    JoinKind::Inner
                    | JoinKind::Left
                    | JoinKind::Semi
                    | JoinKind::AntiSemi
                    | JoinKind::Mark
                    | JoinKind::Single => *op = Op::Empty,
                    JoinKind::Full => {
                        if right_op.is_empty() {
                            *op = Op::Empty;
                        } else {
                            // As left table is empty, we can convert full join to single table,
                            // when bottom up, rewrite all columns derived from right table to null
                            let new = mem::take(right_op);
                            *op = new;
                            return true; // skip the cleansing because left table won't contain right columns.
                        }
                    }
                },
                Join::Qualified(QualifiedJoin {
                    kind,
                    left: JoinOp(left_op),
                    right: JoinOp(Op::Empty),
                    ..
                }) => match kind {
                    JoinKind::Inner
                    | JoinKind::Semi
                    | JoinKind::AntiSemi
                    | JoinKind::Mark
                    | JoinKind::Single => *op = Op::Empty,
                    JoinKind::Left | JoinKind::Full => {
                        // similar to left table case, we replace current op with the other child,
                        // and clean up when bottom up.
                        let new = mem::take(left_op);
                        *op = new;
                        return true;
                    }
                },
                _ => (),
            },
            Op::Setop(so) => {
                let Setop {
                    kind,
                    q,
                    left,
                    right,
                } = so.as_mut();
                match (left, right) {
                    (Op::Empty, Op::Empty) => *op = Op::Empty,
                    (Op::Empty, right) => match (kind, q) {
                        (SetopKind::Union, Setq::All) => *op = mem::take(right),
                        (SetopKind::Except | SetopKind::Intersect, Setq::All) => *op = Op::Empty,
                        _ => (),
                    },
                    (left, Op::Empty) => {
                        if *q == Setq::All {
                            *op = mem::take(left);
                        }
                    }
                    _ => (),
                }
            }
            Op::Limit(Limit { source, .. }) => {
                if source.is_empty() {
                    *op = Op::Empty
                }
            }
            Op::Sort(Sort { source, .. }) => {
                if source.is_empty() {
                    *op = Op::Empty
                }
            }
            Op::Aggr(_) => (), // todo: leave aggr as is, and optimize later
            Op::Proj(Proj { source, .. }) => {
                if source.is_empty() {
                    *op = Op::Empty
                }
            }
            Op::Filt(Filt { source, .. }) => {
                if source.is_empty() {
                    *op = Op::Empty
                }
            }
            Op::Apply(_) => unimplemented!(),
            Op::Query(_) | Op::Table(..) | Op::Row(_) => unreachable!(),
            // JoinGraph should be transformed away before this rule executes,
            // or generated after the rule finishes.
            Op::JoinGraph(_) => unreachable!(),
            Op::Empty => (),
        }
        if !op.is_empty() && !self.empty_qs.is_empty() {
            // current operator is not eliminated but we have some child query set to empty,
            // all column references to it must be set to null
            let mut ucs = UpdateColAndSimplify {
                qs: &self.empty_qs,
                res: Ok(()),
            };
            for e in op.exprs_mut() {
                e.walk_mut(&mut ucs);
                if ucs.res.is_err() {
                    self.res = ucs.res;
                    return false;
                }
            }
        }
        true
    }
}

impl OpMutVisitor for EliminateOp<'_> {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Filt(Filt { pred, source }) => {
                match pred.as_slice() {
                    [Expr::Const(Const::Null)] => {
                        // impossible predicates, remove entire sub-tree
                        *op = Op::Empty;
                    }
                    [Expr::Const(c)] => {
                        if c.is_zero().unwrap_or_default() {
                            // impossible predicates, remove entire sub-tree
                            *op = Op::Empty;
                        } else {
                            // true predicates, remove filter
                            let source = mem::take(source.as_mut());
                            *op = source;
                            // as we replace current operator with its child, we need to
                            // perform enter() again
                            return self.enter(op);
                        }
                    }
                    _ => (),
                }
            }
            Op::Limit(Limit { start, end, .. }) => {
                if *start == *end {
                    *op = Op::Empty;
                } else {
                    self.has_limit = true;
                }
            }
            Op::Sort(Sort { source, .. }) => {
                if self.is_subq && !self.has_limit {
                    // in case subquery that does not have limit upon sort,
                    // sort can be eliminated.
                    let source = mem::take(source.as_mut());
                    *op = source;
                    return self.enter(op);
                }
            }
            Op::Query(query_id) => {
                self.res = eliminate_op(self.qry_set, *query_id, true);
                return self.res.is_ok();
            }
            _ => (),
        };
        // always returns true, even if current node is updated in-place, we still
        // need to traverse back to eliminate entire tree if possible.
        true
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Query(qry_id) => {
                if let Some(subq) = self.qry_set.get(qry_id) {
                    if subq.root.is_empty() {
                        self.empty_qs.insert(*qry_id);
                        *op = Op::Empty;
                    }
                }
            }
            Op::Table(..) | Op::Row(_) => (),
            _ => return self.bottom_up(op),
        }
        true
    }
}

struct UpdateColAndSimplify<'a> {
    qs: &'a HashSet<QueryID>,
    res: Result<()>,
}
impl ExprMutVisitor for UpdateColAndSimplify<'_> {
    fn leave(&mut self, e: &mut Expr) -> bool {
        self.res = update_simplify_single(e, |e| {
            if let Expr::Col(Col::QueryCol(qry_id, _)) = e {
                if self.qs.contains(qry_id) {
                    *e = Expr::const_null();
                }
            }
        });
        self.res.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, get_lvl_queries, print_plan};
    use crate::op::preorder;

    #[test]
    fn test_op_eliminate_false_pred() {
        assert_j_plan("select c1 from t1 where null", assert_empty_root);
        assert_j_plan("select c1 from t1 where false", assert_empty_root);
    }

    #[test]
    fn test_op_eliminate_true_pred() {
        assert_j_plan("select c1 from t1 where true", |s, mut q| {
            op_eliminate(&mut q).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            if let Op::Proj(proj) = &subq.root {
                assert!(matches!(proj.source.as_ref(), Op::Query(..)))
            } else {
                panic!("fail")
            }
        })
    }

    #[test]
    fn test_op_eliminate_derived_table() {
        assert_j_plan(
            "select c1 from (select c1 as c1 from t1 where null) x1",
            assert_empty_root,
        );
        assert_j_plan(
            "select c1 from (select c1 as c1 from (select c1 from t2 limit 0) x2) x1",
            assert_empty_root,
        );
    }

    #[test]
    fn test_op_eliminate_order_by() {
        // remove ORDER BY in subquery
        assert_j_plan(
            "select c1 from (select c1 from t1 order by c0) x1",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subqs = get_lvl_queries(&q, 1);
                assert_eq!(subqs.len(), 1);
                assert!(matches!(subqs[0].root, Op::Proj(_)))
            },
        );
        // do NOT remove ORDER BY because of LIMIT exists
        assert_j_plan(
            "select c1 from (select c1 from t1 order by c0 limit 1) x1",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subqs = get_lvl_queries(&q, 1);
                assert_eq!(subqs.len(), 1);
                if let Op::Limit(Limit { source, .. }) = &subqs[0].root {
                    assert!(matches!(source.as_ref(), Op::Sort(_)));
                } else {
                    panic!("fail")
                }
            },
        );
    }

    #[test]
    fn test_op_eliminate_limit() {
        // eliminate entire tree if LIMIT 0
        assert_j_plan("select c1 from t1 limit 0", |s, mut q| {
            op_eliminate(&mut q).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert!(matches!(subq.root, Op::Empty));
        });
        assert_j_plan("select c1 from t1 limit 0 offset 3", |s, mut q| {
            op_eliminate(&mut q).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert!(matches!(subq.root, Op::Empty));
        });
        // do NOT eliminate if LIMIT non-zero
        assert_j_plan("select c1 from t1 limit 1", |s, mut q| {
            op_eliminate(&mut q).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert!(matches!(subq.root, Op::Limit(_)));
        });
    }

    #[test]
    fn test_op_eliminate_union_all() {
        assert_j_plan(
            "select c1 from t1 where false union all select c1 from t1 limit 0",
            assert_empty_root,
        );
        assert_j_plan(
            "select c1 from t1 where false union all select c1 from t1",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
        assert_j_plan(
            "select c1 from t1 union all select c1 from t1 limit 0",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    #[test]
    fn test_op_eliminate_except_all() {
        assert_j_plan(
            "select c1 from t1 where false except all select c1 from t1",
            assert_empty_root,
        );
        assert_j_plan(
            "select c1 from t1 except all select c1 from t1 where null",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    #[test]
    fn test_op_eliminate_intersect_all() {
        assert_j_plan(
            "select c1 from t1 where false intersect all select c1 from t1",
            assert_empty_root,
        );
        assert_j_plan(
            "select c1 from t1 intersect all select c1 from t1 where null",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    // cross join
    #[test]
    fn test_op_eliminate_cross_join() {
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1, (select * from t2 where false) x2",
            assert_empty_root,
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1, (select * from t2 limit 0) x2",
            assert_empty_root,
        );
    }

    // inner join
    #[test]
    fn test_op_eliminate_inner_join() {
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1 join (select * from t2) x2",
            assert_empty_root,
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1 join (select * from t2 limit 0) x2",
            assert_empty_root,
        );
    }

    // left join
    #[test]
    fn test_op_eliminate_left_join() {
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1 left join (select * from t2) x2",
            assert_empty_root,
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1 left join (select * from t2 limit 0) x2",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let Op::Proj(proj) = &subq.root {
                    assert_eq!(&proj.cols[0].0, &Expr::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
    }

    // right join
    #[test]
    fn test_op_eliminate_right_join() {
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1 right join (select * from t2) x2",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let Op::Proj(proj) = &subq.root {
                    assert_ne!(&proj.cols[0].0, &Expr::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1 right join (select * from t2 where false) x2",
            assert_empty_root,
        );
    }

    // full join
    #[test]
    fn test_op_eliminate_full_join() {
        assert_j_plan(
            "select x1.c1, c2 from (select c1 from t1 where null) x1 full join (select * from t2) x2",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let Op::Proj(proj) = &subq.root {
                    assert_eq!(&proj.cols[0].0, &Expr::const_null());
                    assert_ne!(&proj.cols[1].0, &Expr::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan(
            "select x1.c1, c2 from (select c1 from t1) x1 full join (select * from t2 where false) x2",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let Op::Proj(proj) = &subq.root {
                    assert_ne!(&proj.cols[0].0, &Expr::const_null());
                    assert_eq!(&proj.cols[1].0, &Expr::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan(
            "select c2 from (select c1 from t1 limit 0) x1 right join (select * from t2 where false) x2",
            assert_empty_root,
        );
    }

    fn assert_empty_root(s1: &str, mut q1: QueryPlan) {
        op_eliminate(&mut q1).unwrap();
        print_plan(s1, &q1);
        let root = &q1.root_query().unwrap().root;
        assert_eq!(&Op::Empty, root);
    }
}
