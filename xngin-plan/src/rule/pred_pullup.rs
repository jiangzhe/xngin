use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::op::{Filt, Op, OpMutVisitor};
use crate::query::QuerySet;
use crate::rule::expr_simplify::{update_simplify_nested, NullCoalesce, PartialExpr};
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{
    Col, Expr, ExprVisitor, Func, FuncKind, Pred, PredFunc, PredFuncKind, QueryCol, QueryID,
};

/// Pullup predicates.
/// The canonical optimizations is pushing down predicates, not pulling up,
/// in order to filter data as early as possible.
/// But in some scenarios, the pullup with propagation enables generate many
/// more predicates that can be pushed down other tables.
/// This is an interesting rationale: "pull to push more".
///
/// Pullup should be performed only when necessary:
/// 1. Join exists.
/// 2. Only columns present in join condition should be pulled up.
/// 3. full join is discarded as propagation is not available.
/// 4. left join has limitation that predicates can be propagated
///    only from left side to right side.
#[inline]
pub fn pred_pullup(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    let mut p_preds = HashMap::new();
    let _ = pullup_pred(qry_set, qry_id, HashSet::new(), &mut p_preds)?; // pass empty parent columns, so pulled preds must be empty
    Ok(())
}

fn pullup_pred(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    p_cols: HashSet<QueryCol>,
    p_preds: &mut HashMap<QueryCol, HashSet<PartialExpr>>,
) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppu = PredPullup::new(qry_set, p_cols, p_preds);
        op.walk_mut(&mut ppu).unbranch()?;
        Ok(())
    })?
}

struct PredPullup<'a> {
    qry_set: &'a mut QuerySet,
    // parent columns that predicates target
    p_cols: HashSet<QueryCol>,
    // predicates converted based on mapping, this field is passed by
    // parent oprator
    p_preds: &'a mut HashMap<QueryCol, HashSet<PartialExpr>>,
    // mapping current cols to parent cols
    mapping: HashMap<QueryCol, Expr>,
    // whether the parent columns have been translated.
    translated: bool,
    // current columns involved in current predicates.
    c_cols: HashSet<QueryCol>,
    // current preds, that will be passed to child query.
    c_preds: HashMap<QueryCol, HashSet<PartialExpr>>,
    // store join op that are temporarily removed, in order to restore back
    stack: Vec<JoinOp>,
}

impl<'a> PredPullup<'a> {
    fn new(
        qry_set: &'a mut QuerySet,
        p_cols: HashSet<QueryCol>,
        p_preds: &'a mut HashMap<QueryCol, HashSet<PartialExpr>>,
    ) -> Self {
        PredPullup {
            qry_set,
            p_cols,
            p_preds,
            mapping: HashMap::new(),
            translated: false,
            c_cols: HashSet::new(),
            c_preds: HashMap::new(),
            stack: vec![],
        }
    }

    // Translate columns based on the parent column reference and output sequence.
    // We have to build mapping between current column and parent column.
    // e.g. "SELECT c1 FROM (SELECT c0 + 1 as c1 FROM t1) t"
    // The outer query has column "c1", the inner query outputs "c0 + 1", so we
    // can build equation "c1 = c0 + 1", then we try to convert the expression
    // to "c0 = c1 - 1", and store it in mapping. Suppose we find a predicate
    // "c0 > 1" later, then we can replace "c0" based on the mapping and get
    // new predicate "c1 - 1 > 1" or simplified version "c1 > 2" which could be
    // propagated in parent scope.
    fn translate_p_cols(&mut self, out_cols: &[(Expr, SmolStr)]) {
        if self.translated {
            // translate at most once for each query block
            return;
        }
        for (p_qid, p_idx) in &self.p_cols {
            let (e, _) = &out_cols[*p_idx as usize];
            if let Some((c_col, new_e)) = translate_col(*p_qid, *p_idx, e) {
                self.mapping.insert(c_col, new_e);
            }
        }
        self.translated = true;
    }

    fn collect_p_preds(&mut self, c_preds: &[Expr]) -> Result<()> {
        if self.mapping.is_empty() || c_preds.is_empty() {
            return Ok(());
        }
        for p in c_preds {
            if let Some((qid, idx, e)) = translate_pred(p, &self.mapping)? {
                self.p_preds.entry((qid, idx)).or_default().insert(e);
            }
        }
        Ok(())
    }

    fn collect_c_cols(&mut self, c_preds: &[Expr]) {
        if c_preds.is_empty() {
            return;
        }
        for p in c_preds {
            collect_non_aggr_qry_cols(p, &mut self.c_cols);
        }
    }

    fn pull_inner_side(&mut self, jo: &mut JoinOp) -> ControlFlow<Error> {
        let p_cols = HashSet::new();
        let mut p_preds = HashMap::new();
        let mut ppu = PredPullup::new(self.qry_set, p_cols, &mut p_preds);
        jo.as_mut().walk_mut(&mut ppu)
    }

    // currently we only support propagate based on equation join condition.
    fn propagate_preds(&mut self, conds: &[Expr]) -> Vec<(QueryCol, PartialExpr)> {
        let mut res = vec![];
        for c in conds {
            if let Expr::Pred(Pred::Func(PredFunc { kind, args })) = c {
                if let (
                    PredFuncKind::Equal,
                    [Expr::Col(Col::QueryCol(l_qid, l_idx)), Expr::Col(Col::QueryCol(r_qid, r_idx))],
                ) = (kind, args.as_ref())
                {
                    if let Some(pes) = self.c_preds.get(&(*l_qid, *l_idx)) {
                        for pe in pes {
                            res.push(((*r_qid, *r_idx), pe.clone()));
                        }
                    }
                    if let Some(pes) = self.c_preds.get(&(*r_qid, *r_idx)) {
                        for pe in pes {
                            res.push(((*l_qid, *l_idx), pe.clone()));
                        }
                    }
                }
            }
        }
        res
    }
}

impl OpMutVisitor for PredPullup<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            // top down, translate parent cols to current cols, for first proj or aggr
            Op::Proj(proj) => {
                self.translate_p_cols(&proj.cols);
            }
            // top down , translate parent cols
            Op::Aggr(aggr) => {
                self.translate_p_cols(&aggr.proj);
                // Here we do not collect c_cols because all predicates that
                // can be pushed should have been pushed by rule pred pushdown.
                // So remaining filters should contain aggf that can not be
                // handled by current rule.
            }
            Op::Filt(filt) => {
                // collect columns in filter predicates, which can be passed to
                // child query to pull predicates
                self.collect_c_cols(&filt.pred);
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(_) => (),
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    ..
                }) => match kind {
                    JoinKind::Inner => {
                        // for inner join, from top down, we'd like to collect columns that we care about
                        // and let child to collect related predicates
                        self.collect_c_cols(cond);
                    }
                    JoinKind::Left => {
                        // for left join, from top down, we only need to collect columns of left side,
                        // because predicates from right side is not allowed to propagated to left side.
                        // Here we remove and process right side, when bottom up, we add it back.
                        let mut right = mem::take(right);
                        self.pull_inner_side(&mut right)?;
                        self.stack.push(right);
                        // still collect columns for pulling left side
                        self.collect_c_cols(cond);
                    }
                    JoinKind::Full => {
                        // for full join, from top down, we don't want any predicates because they can
                        // not be propagate to the other side.
                        let mut right = mem::take(right);
                        self.pull_inner_side(&mut right)?;
                        self.stack.push(right);
                        let mut left = mem::take(left);
                        self.pull_inner_side(&mut left)?;
                        self.stack.push(left);
                    }
                    _ => todo!(),
                },
            },
            Op::Query(qry_id) => {
                // Here we collect all p_cols according to the child query id,
                // and recursively call the predicate pullup.
                let p_cols: HashSet<_> = self
                    .c_cols
                    .iter()
                    .filter_map(|(qid, idx)| {
                        if qid == qry_id {
                            Some((*qid, *idx))
                        } else {
                            None
                        }
                    })
                    .collect();
                pullup_pred(self.qry_set, *qry_id, p_cols, &mut self.c_preds).branch()?;
            }
            Op::Sort(_)
            | Op::Limit(_)
            | Op::Setop(_)
            | Op::Attach(..)
            | Op::Table(..)
            | Op::JoinGraph(_)
            | Op::Row(_)
            | Op::Empty => (),
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            // bottom up to Proj means all predicates in current tree are pulled up
            // but no Filt operator to gather them, so add one.
            Op::Proj(proj) => {
                if !self.c_preds.is_empty() {
                    let mut pred = vec![];
                    for ((qid, idx), pes) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = Expr::pred_func(
                                pe.kind,
                                vec![Expr::query_col(qid, idx), Expr::Const(pe.r_arg)],
                            );
                            pred.push(new_e);
                        }
                    }
                    let source = mem::take(&mut proj.source);
                    let filt = Op::Filt(Filt { pred, source });
                    proj.source = Box::new(filt);
                }
            }
            Op::Aggr(aggr) => {
                if !self.c_preds.is_empty() {
                    for ((qid, idx), pes) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = Expr::pred_func(
                                pe.kind,
                                vec![Expr::query_col(qid, idx), Expr::Const(pe.r_arg)],
                            );
                            aggr.filt.push(new_e);
                        }
                    }
                }
            }
            // bottom up and append all predicates to Filt
            Op::Filt(filt) => {
                if !filt.pred.is_empty() && !self.c_preds.is_empty() {
                    let new_preds = self.propagate_preds(&filt.pred);
                    for ((qid, idx), pes) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = Expr::pred_func(
                                pe.kind,
                                vec![Expr::query_col(qid, idx), Expr::Const(pe.r_arg)],
                            );
                            filt.pred.push(new_e);
                        }
                    }
                    for ((qid, idx), pe) in new_preds {
                        let new_e = Expr::pred_func(
                            pe.kind,
                            vec![Expr::query_col(qid, idx), Expr::Const(pe.r_arg)],
                        );
                        filt.pred.push(new_e);
                    }
                }
                self.collect_p_preds(&filt.pred).branch()?;
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(_) => (), // bypass cross join
                // filt field in join is not processed, maybe enhance in future
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    ..
                }) => match kind {
                    JoinKind::Inner => {
                        // Here we propagate predicates based on transitivity.
                        // e.g. join condition: a1=b1, predicate: a1>0, propagated: b1 > 0
                        let new_preds = self.propagate_preds(cond);
                        for (qc, pe) in new_preds {
                            self.c_preds.entry(qc).or_default().insert(pe);
                        }
                    }
                    JoinKind::Left => {
                        let new_preds = self.propagate_preds(cond);
                        let r_jo = self.stack.pop().unwrap();
                        if !new_preds.is_empty() {
                            // we need to specify which one belongs to right side
                            let mut r_qids = HashSet::new();
                            r_jo.collect_qry_ids(&mut r_qids);
                            // add right predicates to join condition, add others to c_preds
                            for ((qid, idx), pe) in new_preds {
                                if r_qids.contains(&qid) {
                                    let new_e = Expr::pred_func(
                                        pe.kind,
                                        vec![Expr::query_col(qid, idx), Expr::Const(pe.r_arg)],
                                    );
                                    cond.push(new_e);
                                } else {
                                    // all others belong to left side and can be pulled up
                                    self.c_preds.entry((qid, idx)).or_default().insert(pe);
                                }
                            }
                        }
                        *right = r_jo;
                    }
                    JoinKind::Full => {
                        let l_jo = self.stack.pop().unwrap();
                        let r_jo = self.stack.pop().unwrap();
                        *left = l_jo;
                        *right = r_jo;
                    }
                    _ => todo!(),
                },
            },
            Op::Sort(_)
            | Op::Limit(_)
            | Op::Setop(_)
            | Op::Attach(..)
            | Op::Table(..)
            | Op::JoinGraph(_)
            | Op::Query(_)
            | Op::Row(_)
            | Op::Empty => (),
        }
        ControlFlow::Continue(())
    }
}

/// collect query columns in expression.
fn collect_non_aggr_qry_cols(e: &Expr, hs: &mut HashSet<QueryCol>) {
    let mut c = CollectQryCols(hs);
    let _ = e.walk(&mut c);
}
struct CollectQryCols<'a>(&'a mut HashSet<QueryCol>);

impl ExprVisitor for CollectQryCols<'_> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Col(Col::QueryCol(qry_id, idx)) => {
                self.0.insert((*qry_id, *idx));
                ControlFlow::Continue(())
            }
            Expr::Aggf(_) => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

// currently, we only support out column 4 patterns:
// col, col + const, col - const, const - col
#[inline]
fn translate_col(p_qid: QueryID, p_idx: u32, e: &Expr) -> Option<(QueryCol, Expr)> {
    let res = match e {
        Expr::Col(Col::QueryCol(c_qid, c_idx)) => {
            // direct mapping between columns: c_col -> p_col
            let new_e = Expr::query_col(p_qid, p_idx);
            ((*c_qid, *c_idx), new_e)
        }
        Expr::Func(Func { kind, args }) => match (kind, args.as_ref()) {
            (FuncKind::Add, [Expr::Col(Col::QueryCol(c_qid, c_idx)), c @ Expr::Const(_)]) => {
                // "p_col = c_col + const" => "c_col = p_col - const"
                let new_e = Expr::func(
                    FuncKind::Sub,
                    vec![Expr::query_col(p_qid, p_idx), c.clone()],
                );
                ((*c_qid, *c_idx), new_e)
            }
            (FuncKind::Sub, [Expr::Col(Col::QueryCol(c_qid, c_idx)), c @ Expr::Const(_)]) => {
                // "p_col = c_col - const" => "c_col = p_col + const"
                let new_e = Expr::func(
                    FuncKind::Add,
                    vec![Expr::query_col(p_qid, p_idx), c.clone()],
                );
                ((*c_qid, *c_idx), new_e)
            }
            (FuncKind::Sub, [c @ Expr::Const(_), Expr::Col(Col::QueryCol(c_qid, c_idx))]) => {
                // "p_col = const - c_col" => "c_col = const - p_col"
                let new_e = Expr::func(
                    FuncKind::Sub,
                    vec![c.clone(), Expr::query_col(p_qid, p_idx)],
                );
                ((*c_qid, *c_idx), new_e)
            }
            _ => return None,
        },
        _ => return None,
    };
    Some(res)
}

// currently, we only support out predicates with: =, !=, >, >=, <, <=
// todo: add in, between, like, regexp, etc.
#[inline]
fn translate_pred(
    c_pred: &Expr,
    mapping: &HashMap<QueryCol, Expr>,
) -> Result<Option<(QueryID, u32, PartialExpr)>> {
    let mut new_p = c_pred.clone();
    let res = update_simplify_nested(&mut new_p, NullCoalesce::False, |e| match e {
        Expr::Col(Col::QueryCol(qry_id, idx)) => {
            if let Some(new_e) = mapping.get(&(*qry_id, *idx)) {
                *e = new_e.clone();
                Ok(())
            } else {
                Err(Error::Break)
            }
        }
        _ => Ok(()),
    });
    match res {
        Ok(_) => match new_p {
            Expr::Pred(Pred::Func(PredFunc { kind, args })) => match (kind, args.as_ref()) {
                (
                    PredFuncKind::Equal
                    | PredFuncKind::Greater
                    | PredFuncKind::GreaterEqual
                    | PredFuncKind::Less
                    | PredFuncKind::LessEqual
                    | PredFuncKind::NotEqual,
                    [Expr::Col(Col::QueryCol(qry_id, idx)), Expr::Const(c)],
                ) => {
                    let partial_e = PartialExpr {
                        kind,
                        r_arg: c.clone(),
                    };
                    Ok(Some((*qry_id, *idx, partial_e)))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        },
        Err(Error::Break) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan1, get_filt_expr, j_catalog, print_plan};

    #[test]
    fn test_pred_pullup_cross_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0 and c0 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1+1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1-1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select 1-c1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(3, filt.len());
            },
        );
    }

    #[test]
    fn test_pred_pullup_inner_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(2, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join (select c1 from t2 where c1 = 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(2, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c0, c1 from t1 where c1 = 0 and c0 > 5) t1 join t2 on t1.c1 = t2.c1 and t1.c0 = t2.c0",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(4, filt.len());
            },
        );
    }

    #[test]
    fn test_pred_pullup_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 left join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(1, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join (select c1 from t2 where c1 = 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert!(filt.is_empty())
            },
        );
    }

    #[test]
    fn test_pred_pullup_full_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 full join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert!(filt.is_empty())
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 full join (select c1 from t2 where c1 > 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert!(filt.is_empty())
            },
        );
    }

    #[test]
    fn test_pred_pullup_aggr() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1, count(*) as c2 from t1 group by c1 having count(*) > 0 and c1 > 1) t1 join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_filt_expr(&plan);
                assert_eq!(2, filt.len())
            },
        );
    }
}
