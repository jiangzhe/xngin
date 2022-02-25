use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::op::{Op, OpMutVisitor, OpVisitor};
use crate::query::{Location, QueryPlan, QuerySet, Subquery};
use crate::rule::RuleEffect;
use crate::setop::{Setop, SubqOp};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{Col, Expr, ExprMutVisitor, QueryID};

/// Unfold derived table.
///
/// The goal is to flatten the query structure to provide more
/// flexibility to join reorder.
/// e.g.
/// "SELECT 1 FROM t1 JOIN (SELECT c2, c3 FROM t2, t3 WHERE t2.c2 = t3.c3) t ON t1.c1 = t.c2"
/// If we don't unfold the derived table `t`, the two joins cannot be reordered at all.
///
/// Additional case should be taken to unfold once the parent has outer joins.
#[inline]
pub fn derived_unfold(QueryPlan { qry_set, root }: &mut QueryPlan) -> Result<RuleEffect> {
    let mut mapping = HashMap::new();
    unfold_derived(qry_set, *root, &mut mapping, Mode::Full)
}

fn unfold_derived(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    mapping: &mut HashMap<Col, Expr>,
    mode: Mode,
) -> Result<RuleEffect> {
    qry_set.transform_op(qry_id, |qry_set, loc, op| {
        if loc == Location::Intermediate {
            // only unfold intermediate query
            let mut u = Unfold::new(qry_set, mapping, mode);
            op.walk_mut(&mut u).unbranch()
        } else {
            Ok(RuleEffect::NONE)
        }
    })?
}

/// In top down way,
/// 1. if left join, push right child to stack.
/// 2. if full join, push both children to stack.
///
/// In bottom up way,
/// 1. if left join, pop right child from stack and partial unfold
/// 2. if full join, pop both children and partial unfold
struct Unfold<'a> {
    qry_set: &'a mut QuerySet,
    stack: Vec<Op>,
    mapping: &'a mut HashMap<Col, Expr>,
    mode: Mode,
}

impl<'a> Unfold<'a> {
    fn new(qry_set: &'a mut QuerySet, mapping: &'a mut HashMap<Col, Expr>, mode: Mode) -> Self {
        Unfold {
            qry_set,
            stack: vec![],
            mapping,
            mode,
        }
    }
}

impl OpMutVisitor for Unfold<'_> {
    type Cont = RuleEffect;
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        match op {
            Op::Query(qry_id) => {
                // recursively unfold child query
                let mut mapping = HashMap::new();
                unfold_derived(self.qry_set, *qry_id, &mut mapping, Mode::Full).branch()
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(_) => ControlFlow::Continue(RuleEffect::NONE),
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Left,
                    right,
                    ..
                }) => {
                    // remove right, execute on right, and add back when leaving
                    let mut right = mem::take(right);
                    let mut u = Unfold::new(self.qry_set, self.mapping, Mode::Partial);
                    let eff = right.as_mut().walk_mut(&mut u)?;
                    self.stack.push(right.into());
                    ControlFlow::Continue(eff)
                }
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Full,
                    left,
                    right,
                    ..
                }) => {
                    // remove both side, and add back when leaving
                    let mut right = mem::take(right);
                    let mut u = Unfold::new(self.qry_set, self.mapping, Mode::Partial);
                    let mut eff = right.as_mut().walk_mut(&mut u)?;
                    self.stack.push(right.into());
                    let mut left = mem::take(left);
                    // reuse u
                    eff |= left.as_mut().walk_mut(&mut u)?;
                    self.stack.push(left.into());
                    ControlFlow::Continue(eff)
                }
                _ => ControlFlow::Continue(RuleEffect::NONE), // other joins is fine to bypass
            },
            Op::Setop(so) => {
                // setop does not support unfolding into current query
                let mut eff = RuleEffect::NONE;
                let Setop { left, right, .. } = so.as_mut();
                let right = mem::take(right);
                if let Op::Query(qry_id) = right.as_ref() {
                    let mut mapping = HashMap::new();
                    eff |=
                        unfold_derived(self.qry_set, *qry_id, &mut mapping, Mode::Full).branch()?;
                    self.stack.push(right.into());
                } else {
                    unreachable!()
                }
                let left = mem::take(left);
                if let Op::Query(qry_id) = left.as_ref() {
                    let mut mapping = HashMap::new();
                    eff |=
                        unfold_derived(self.qry_set, *qry_id, &mut mapping, Mode::Full).branch()?;
                    self.stack.push(left.into());
                }
                ControlFlow::Continue(eff)
            }
            Op::JoinGraph(_) => todo!(),
            Op::Proj(_) | Op::Filt(_) | Op::Aggr(_) | Op::Sort(_) | Op::Limit(_) => {
                ControlFlow::Continue(RuleEffect::NONE)
            } // fine to bypass
            Op::Empty => ControlFlow::Continue(RuleEffect::NONE), // as join op is set to empty, it's safe to bypass
            Op::Table(..) | Op::Row(_) => unreachable!(),
            Op::Apply(_) => todo!(),
        }
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        match op {
            Op::Query(qry_id) => {
                return match self.qry_set.get_mut(qry_id) {
                    Some(subq) => {
                        match try_unfold_subq(subq, self.mode) {
                            Some((new_op, out_cols)) => {
                                // unfold subquery as operator tree, we need to store
                                // the mapping between original columns to unfolded expressions
                                for (idx, (e, _)) in out_cols.into_iter().enumerate() {
                                    self.mapping.insert(Col::QueryCol(*qry_id, idx as u32), e);
                                }
                                *op = new_op;
                                ControlFlow::Continue(RuleEffect::OPEXPR)
                            }
                            None => ControlFlow::Continue(RuleEffect::NONE),
                        }
                    }
                    None => ControlFlow::Break(Error::QueryNotFound(*qry_id)),
                };
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(_) => (),
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Left,
                    right,
                    ..
                }) => {
                    let jo = JoinOp::try_from(self.stack.pop().unwrap()).branch()?;
                    *right = jo;
                }
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Full,
                    left,
                    right,
                    ..
                }) => {
                    let jo = JoinOp::try_from(self.stack.pop().unwrap()).branch()?;
                    *left = jo;
                    let jo = JoinOp::try_from(self.stack.pop().unwrap()).branch()?;
                    *right = jo;
                }
                _ => (),
            },
            Op::Setop(so) => {
                let Setop { left, right, .. } = so.as_mut();
                let sq = SubqOp::try_from(self.stack.pop().unwrap()).branch()?;
                *left = sq;
                let sq = SubqOp::try_from(self.stack.pop().unwrap()).branch()?;
                *right = sq;
            }
            Op::JoinGraph(_) => todo!(),
            _ => (),
        }
        // rewrite all expressions in current operator
        let eff = rewrite_exprs(op, self.mapping);
        ControlFlow::Continue(eff)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    // unfold all expressions of derived table
    Full,
    // only unfold column expressions of derived table
    Partial,
}

fn try_unfold_subq(subq: &mut Subquery, mode: Mode) -> Option<(Op, Vec<(Expr, SmolStr)>)> {
    if subq.location != Location::Intermediate {
        return None;
    }
    match mode {
        Mode::Full => {
            // detect whether this subquery supports unfolding
            // currently only plain query, which only contains Proj, Join, are allowed.
            let mut d = Detect::new();
            let _ = subq.root.walk(&mut d);
            if !d.res {
                return None;
            }
            Some(extract(&mut subq.root))
        }
        Mode::Partial => {
            if subq
                .out_cols()
                .iter()
                .any(|(e, _)| !matches!(e, Expr::Col(Col::QueryCol(..))))
            {
                return None;
            }
            // detect whether this subquery supports unfolding
            // currently only plain query, which only contains Proj, Join, Query, are allowed.
            let mut d = Detect::new();
            let _ = subq.root.walk(&mut d);
            if !d.res {
                return None;
            }
            Some(extract(&mut subq.root))
        }
    }
}

struct Detect {
    top_proj: bool,
    res: bool,
}

impl Detect {
    fn new() -> Self {
        Detect {
            top_proj: false,
            res: true,
        }
    }
}

impl OpVisitor for Detect {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &Op) -> ControlFlow<()> {
        match op {
            Op::Aggr(_) | Op::Filt(_) | Op::Sort(_) | Op::Limit(_) | Op::Setop(_) => {
                self.res = false;
                ControlFlow::Break(())
            }
            Op::Proj(_) => {
                if !self.top_proj {
                    self.top_proj = true;
                    ControlFlow::Continue(())
                } else {
                    self.res = false;
                    ControlFlow::Break(())
                }
            }
            Op::Join(_) | Op::JoinGraph(_) | Op::Query(_) => {
                if !self.top_proj {
                    self.res = false;
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
            Op::Apply(_) => todo!(),
            Op::Row(_) | Op::Table(..) | Op::Empty => unreachable!(),
        }
    }
}

fn extract(op: &mut Op) -> (Op, Vec<(Expr, SmolStr)>) {
    match mem::take(op) {
        Op::Proj(proj) => (*proj.source, proj.cols),
        _ => unreachable!(),
    }
}

fn rewrite_exprs(op: &mut Op, mapping: &HashMap<Col, Expr>) -> RuleEffect {
    struct Rewrite<'a>(&'a HashMap<Col, Expr>);
    impl ExprMutVisitor for Rewrite<'_> {
        type Cont = RuleEffect;
        type Break = ();
        #[inline]
        fn leave(&mut self, e: &mut Expr) -> ControlFlow<(), RuleEffect> {
            if let Expr::Col(c) = e {
                if let Some(new) = self.0.get(c) {
                    *e = new.clone();
                    return ControlFlow::Continue(RuleEffect::EXPR);
                }
            }
            ControlFlow::Continue(RuleEffect::NONE)
        }
    }
    let mut eff = RuleEffect::NONE;
    if mapping.is_empty() {
        return eff;
    }
    let mut r = Rewrite(mapping);
    for e in op.exprs_mut() {
        match e.walk_mut(&mut r) {
            ControlFlow::Break(_) => (),
            ControlFlow::Continue(ef) => eff |= ef,
        }
    }
    eff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{build_plan, j_catalog, print_plan};
    use crate::op::OpKind::*;
    use crate::rule::{col_prune, pred_pushdown};

    #[test]
    fn test_derived_unfold_single_table() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            (
                "select 1 from (select 1 from t1) t",
                vec![Proj, Proj, Table],
            ),
            (
                "select c1+1 from (select c1 from t1) t",
                vec![Proj, Proj, Table],
            ),
            (
                "select x from (select c1+1 as x from t1) t",
                vec![Proj, Proj, Table],
            ),
            (
                "select x from (select c1+1 as x from t1) t where x > 0",
                vec![Proj, Filt, Proj, Table],
            ),
            // as derived table contains Filt, unfold not available.
            (
                "select x from (select c1+1 as x from t1 where c1 > 0) t",
                vec![Proj, Proj, Filt, Proj, Table],
            ),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_push_filt() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            (
                "select c1+1 from (select c1 from t1) t where c1 > 0",
                vec![Proj, Proj, Filt, Table],
            ),
            (
                "select x from (select c0+c1 as x from t1) t where x > 0",
                vec![Proj, Proj, Filt, Table],
            ),
            (
                "select x from (select c1+1 as x from t1 where c1 > 0) t",
                vec![Proj, Proj, Filt, Table],
            ),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            pred_pushdown(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_cross_join() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            (
                "select x from (select t1.c1 as x from t1, t2) t",
                vec![Proj, Join, Proj, Table, Proj, Table],
            ),
            (
                "select c1 from t1, (select c2 from t2) t",
                vec![Proj, Join, Proj, Table, Proj, Table],
            ),
            (
                "select * from (select c1 from t1) t, (select c2 from t2) tt",
                vec![Proj, Join, Proj, Table, Proj, Table],
            ),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            pred_pushdown(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_left_join() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            (
                "select t1.c1 from (select * from t1) t1 left join t2",
                vec![Proj, Join, Proj, Table, Proj, Table],
            ),
            (
                "select t1.c1 from (select * from t1 where c1 > 0) t1 left join t2",
                vec![Proj, Join, Proj, Filt, Table, Proj, Table],
            ),
            (
                "select * from (select c1 from t1) t left join (select c2 from t2 where c2 > 0) tt",
                vec![Proj, Join, Proj, Table, Proj, Filt, Table],
            ),
            // computations on right table disable unfolding
            (
                "select * from t1 left join (select c2+1 from t2 where c2 > 0) tt",
                vec![Proj, Join, Proj, Table, Proj, Proj, Filt, Table],
            ),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            pred_pushdown(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_full_join() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            ("select t1.c1 from (select * from t1) t1 full join t2", vec![Proj, Join, Proj, Table, Proj, Table]),
            ("select t1.c1 from (select * from t1 where c1 > 0) t1 full join t2", vec![Proj, Join, Proj, Filt, Table, Proj, Table]),
            ("select * from (select c1 from t1) t full join (select c2 from t2 where c2 > 0) tt", vec![Proj, Join, Proj, Table, Proj, Filt, Table]),
            // computations on right table
            ("select * from t1 full join (select c2+1 from t2 where c2 > 0) tt", vec![Proj, Join, Proj, Table, Proj, Proj, Filt, Table]),
            // computations on left table
            ("select * from (select c1+1 from t1 where c1 > 0) tt full join t2", vec![Proj, Join, Proj, Proj, Filt, Table, Proj, Table]),
            // computations on both tables
            ("select * from (select c1+1 from t1 where c1 > 0) t1 full join (select c2+1 from t2 where c2 > 0) t2", vec![Proj, Join, Proj, Proj, Filt, Table, Proj, Proj, Filt, Table]),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            pred_pushdown(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_nested_join() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            ("select tt.c1 from (select t1.c1 from t1 join t2) tt join t3", vec![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table]),
            ("select t1.c1 from t1 join (select * from t2 join t3) tt", vec![Proj, Join, Proj, Table, Join, Proj, Table, Proj, Table]),
            ("select t1.c1 from (select t1.c1 from t1 join t2) t1 join (select t2.c2 from t2 join t3) t2", vec![Proj, Join, Join, Proj, Table, Proj, Table, Join, Proj, Table, Proj, Table]),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }

    #[test]
    fn test_derived_unfold_setop() {
        let cat = j_catalog();
        for (sql, shape) in vec![
            (
                "select t1.c1 from t1 union all select t2.c2 from t2",
                vec![Setop, Proj, Proj, Table, Proj, Proj, Table],
            ),
            (
                "select * from (select c1 from t1) tt union all select t2.c2 from t2",
                vec![Setop, Proj, Proj, Table, Proj, Proj, Table],
            ),
            (
                "select t1.c1 from t1 union all select * from (select c2 from t2) tt",
                vec![Setop, Proj, Proj, Table, Proj, Proj, Table],
            ),
        ] {
            let mut p = build_plan(&cat, sql);
            col_prune(&mut p).unwrap();
            derived_unfold(&mut p).unwrap();
            print_plan(sql, &p);
            assert_eq!(shape, p.shape());
        }
    }
}
