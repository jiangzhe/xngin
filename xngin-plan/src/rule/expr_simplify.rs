use crate::error::Result;
use crate::op::Op;
use crate::op::OpMutVisitor;
use crate::query::{QueryPlan, QuerySet};
use std::mem;
use xngin_expr::fold::{ConstFold, FoldAdd, FoldNeg, FoldNot};
use xngin_expr::{Expr, ExprMutVisitor, Func, FuncKind, Pred, PredFunc, QueryID};

/// Simplify expressions.
#[inline]
pub fn expr_simplify(QueryPlan { queries, root }: &mut QueryPlan) -> Result<()> {
    let mut subqueries = vec![*root];
    while let Some(qry_id) = subqueries.pop() {
        simplify_single(queries, qry_id, &mut subqueries)?
    }
    Ok(())
}

fn simplify_single(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    subqueries: &mut Vec<QueryID>,
) -> Result<()> {
    if let Some(subq) = qry_set.get_mut(&qry_id) {
        for (_, subq_id) in subq.scope.query_aliases.iter().rev() {
            subqueries.push(*subq_id)
        }
        let mut es = ExprSimplify { res: Ok(()) };
        let _ = subq.root.walk_mut(&mut es);
        es.res?
    }
    Ok(())
}

struct ExprSimplify {
    res: Result<()>,
}

impl OpMutVisitor for ExprSimplify {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        for e in op.exprs_mut() {
            let _ = e.walk_mut(self);
        }
        self.res.is_ok()
    }

    #[inline]
    fn leave(&mut self, _op: &mut Op) -> bool {
        true
    }
}

impl ExprMutVisitor for ExprSimplify {
    #[inline]
    fn enter(&mut self, _e: &mut Expr) -> bool {
        true
    }

    /// simplify expression bottom up.
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> bool {
        self.res = const_fold(e);
        self.res.is_ok()
    }
}

/// Fold constants such as 1 + 1, 2 < 3, etc.
fn const_fold(e: &mut Expr) -> Result<()> {
    match e {
        Expr::Func(f) => {
            if let Some(new) = fold_func(f)? {
                *e = new
            }
        }
        Expr::Pred(p) => {
            if let Some(new) = fold_pred(p)? {
                *e = new
            }
        }
        _ => (), // All other kinds are skipped in constant folding
    }
    Ok(())
}

/// Fold constants in function.
///
/// 1. remove pair of negating, e.g.
/// --e <=> e
/// 2. compute negating constant, e.g.
/// -c <=> new_c
/// 3. compute addition of constants, e.g.
/// 1+1 <=> 2
/// 4. remote adding zero, e.g.
/// e+0 <=> e
/// 5. swap order of variable in addtion, e.g.
/// 1+e <=> e+1
/// 6. associative, e.g.
/// (e+1)+2 <=> e+3
/// Note: (1+e)+2 <=> e+3 -- won't happen after rule 5, only for add/mul
/// 7. commutative and associative, e.g.
/// 1+(e+2) <=> e+3
/// Note: 1+(2+e) <=> e+3 -- won't happen after rule 5, only for add/mul
/// 8. commutative and associative, e.g.
/// (e1+1)+(e2+2) <=> (e1+e2)+3
fn fold_func(f: &mut Func) -> Result<Option<Expr>> {
    let res = match f.kind {
        FuncKind::Neg => match &mut f.args[0] {
            // rule 1: --e <=> e
            // should cast to numeric value
            Expr::Func(Func { kind, args }) if *kind == FuncKind::Neg => {
                Some(mem::take(&mut args[0]))
            }
            // rule 2: -c <=> new_c
            Expr::Const(c) => FoldNeg(c).fold()?.map(Expr::Const),
            _ => None,
        },
        FuncKind::Add => match f.args.as_mut() {
            // rule 3: 1+1 <=> 2
            [Expr::Const(c0), Expr::Const(c1)] => FoldAdd(c0, c1).fold()?.map(Expr::Const),
            [e, Expr::Const(c1)] => {
                if c1.is_zero().unwrap_or_default() {
                    // rule 4: e+0 <=> e
                    Some(mem::take(e))
                } else {
                    // rule 6
                    match e {
                        // (e+1)+2 <=> e+3
                        Expr::Func(Func {
                            kind: FuncKind::Add,
                            args,
                        }) => match args.as_mut() {
                            [e, Expr::Const(c2)] => match FoldAdd(c1, c2).fold()? {
                                Some(res) => {
                                    if res.is_zero().unwrap_or_default() {
                                        Some(mem::take(e))
                                    } else {
                                        Some(Expr::func(
                                            FuncKind::Add,
                                            vec![mem::take(e), Expr::Const(res)],
                                        ))
                                    }
                                }
                                None => None,
                            },
                            _ => None,
                        },
                        // (e-1)+2 <=> e+1
                        Expr::Func(Func {
                            kind: FuncKind::Sub,
                            args,
                        }) => todo!(),
                        _ => None,
                    }
                }
            }
            [Expr::Const(c0), e] => {
                if c0.is_zero().unwrap_or_default() {
                    // rule 4: 0+e <=> e
                    Some(mem::take(e))
                } else {
                    // rule 7
                    match e {
                        // 1+(e+2) <=> e+3
                        Expr::Func(Func {
                            kind: FuncKind::Add,
                            args,
                        }) => {
                            if let [e, Expr::Const(c2)] = args.as_mut() {
                                let res = match FoldAdd(c0, c2).fold()? {
                                    Some(res) => {
                                        if res.is_zero().unwrap_or_default() {
                                            mem::take(e)
                                        } else {
                                            Expr::func(
                                                FuncKind::Add,
                                                vec![mem::take(e), Expr::Const(res)],
                                            )
                                        }
                                    }
                                    None => return Ok(None),
                                };
                                return Ok(Some(res));
                            }
                        }
                        // 1+(e-2) <=> e+(-1)
                        Expr::Func(Func {
                            kind: FuncKind::Sub,
                            args,
                        }) => todo!(),
                        _ => (),
                    }
                    // rule 5: 1+e <=> e+1
                    let e0 = Expr::Const(mem::take(c0));
                    let e1 = mem::take(e);
                    Some(Expr::func(FuncKind::Add, vec![e1, e0]))
                }
            }
            // rule 8: (e1+1)+(e2+2) <=> (e1+e2)+3
            [Expr::Func(Func { kind: k1, args: a1 }), Expr::Func(Func { kind: k2, args: a2 })] => {
                match (k1, k2) {
                    (FuncKind::Add, FuncKind::Add) => match (&a1[1], &a2[1]) {
                        (Expr::Const(c1), Expr::Const(c2)) => match FoldAdd(c1, c2).fold()? {
                            Some(c3) => {
                                let e1 = mem::take(&mut a1[0]);
                                let e2 = mem::take(&mut a2[0]);
                                let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                                Some(Expr::func(FuncKind::Add, vec![e, Expr::Const(c3)]))
                            }
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        },
        _ => todo!(),
    };
    Ok(res)
}

/// Fold constants in predicate.
///
/// 1. NOT
/// 1.1. not Exists <=> NotExists
/// 1.2. not NotExists <=> Exists
/// 1.3. not In <=> NotIn
/// 1.4. not NotIn <=> In
/// 1.5. not cmp <=> complement of cmp
/// 1.6. not const <=> new_const
/// 1.7. not not e <=> cast(e as bool)
///
/// 2. Comparison
///
/// 3. CNF
///
/// 4. DNF
///
/// 5. EXISTS/NOT EXISTS
///
/// 6. IN/NOT IN
fn fold_pred(p: &mut Pred) -> Result<Option<Expr>> {
    let res = match p {
        Pred::Not(e) => match e.as_mut() {
            // 1.1
            Expr::Pred(Pred::Exists(subq)) => Some(Expr::Pred(Pred::NotExists(mem::take(subq)))),
            // 1.2
            Expr::Pred(Pred::NotExists(subq)) => Some(Expr::Pred(Pred::Exists(mem::take(subq)))),
            // 1.3
            Expr::Pred(Pred::InSubquery(lhs, subq)) => Some(Expr::Pred(Pred::NotInSubquery(
                mem::take(lhs),
                mem::take(subq),
            ))),
            // 1.4
            Expr::Pred(Pred::NotInSubquery(lhs, subq)) => Some(Expr::Pred(Pred::InSubquery(
                mem::take(lhs),
                mem::take(subq),
            ))),
            // 1.5
            Expr::Pred(Pred::Func(PredFunc { kind, args })) => kind.flip().map(|kind| {
                let args = mem::take(args);
                Expr::Pred(Pred::Func(PredFunc { kind, args }))
            }),
            // 1.6
            Expr::Const(c) => FoldNot(c).fold()?.map(Expr::Const),
            // 1.7 todo
            Expr::Pred(Pred::Not(_e)) => None,
            _ => None,
        },
        _ => None, // todo: handle other predicates
    };
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, assert_j_plan2, get_filt_expr, print_plan};

    // fold func 1
    #[test]
    fn test_expr_simplify1() {
        assert_j_plan2(
            "select c1 from t1 where --c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        )
    }

    // fold func 2
    #[test]
    fn test_expr_simplify2() {
        assert_j_plan2(
            "select c1 from t1 where -(2)",
            "select c1 from t1 where -2",
            assert_eq_filt_expr,
        )
    }

    // fold func 3
    #[test]
    fn test_expr_simplify3() {
        assert_j_plan2(
            "select c1 from t1 where 1+1",
            "select c1 from t1 where 2",
            assert_eq_filt_expr,
        )
    }

    // fold func 4
    #[test]
    fn test_expr_simplify4() {
        assert_j_plan2(
            "select c1 from t1 where c1+0",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        )
    }

    // fold func 4
    #[test]
    fn test_expr_simplify5() {
        assert_j_plan2(
            "select c1 from t1 where 0+c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        )
    }

    // fold func 5
    #[test]
    fn test_expr_simplify6() {
        assert_j_plan2(
            "select c1 from t1 where 1+c1",
            "select c1 from t1 where c1+1",
            assert_eq_filt_expr,
        )
    }

    // fold func 6
    #[test]
    fn test_expr_simplify7() {
        assert_j_plan2(
            "select c1 from t1 where c1+1+2",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        )
    }

    // fold func 7
    #[test]
    fn test_expr_simplify8() {
        assert_j_plan2(
            "select c1 from t1 where 1+(c1+2)",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        )
    }

    // fold func 3 and 5
    #[test]
    fn test_expr_simplify9() {
        assert_j_plan2(
            "select c1 from t1 where 1+2+c1",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        )
    }

    // fold func 5 and 7
    #[test]
    fn test_expr_simplify10() {
        assert_j_plan2(
            "select c1 from t1 where 1+(2+c1)",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        )
    }

    // fold func 8
    #[test]
    fn test_expr_simplify11() {
        assert_j_plan2(
            "select c1 from t1 where (c1+1)+(c1+2)",
            "select c1 from t1 where (c1+c1)+3",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.1
    #[test]
    fn test_expr_simplify12() {
        assert_j_plan(
            "select c1 from t1 where not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::NotExists(_))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        )
    }

    // fold pred 1.2
    #[test]
    fn test_expr_simplify13() {
        assert_j_plan(
            "select c1 from t1 where not not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::Exists(_))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        )
    }

    // fold pred 1.3
    #[test]
    fn test_expr_simplify14() {
        assert_j_plan(
            "select c1 from t1 where not c1 in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::NotInSubquery(..))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            "select c1 from t1 where not c1 in (select c1 from t2)",
            "select c1 from t1 where c1 not in (select c1 from t2)",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.4
    #[test]
    fn test_expr_simplify15() {
        assert_j_plan(
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::InSubquery(..))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            "select c1 from t1 where c1 in (select c1 from t2)",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify16() {
        assert_j_plan2(
            "select c1 from t1 where not c0 = c1",
            "select c1 from t1 where c0 <> c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 > c1",
            "select c1 from t1 where c0 <= c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 >= c1",
            "select c1 from t1 where c0 < c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 < c1",
            "select c1 from t1 where c0 >= c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 <= c1",
            "select c1 from t1 where c0 > c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 <> c1",
            "select c1 from t1 where c0 = c1",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify17() {
        assert_j_plan2(
            "select c1 from t1 where not c0 is null",
            "select c1 from t1 where c0 is not null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not null",
            "select c1 from t1 where c0 is null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is true",
            "select c1 from t1 where c0 is not true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not true",
            "select c1 from t1 where c0 is true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is false",
            "select c1 from t1 where c0 is not false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not false",
            "select c1 from t1 where c0 is false",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify18() {
        assert_j_plan2(
            "select c1 from t1 where not c0 like '1'",
            "select c1 from t1 where c0 not like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not like '1'",
            "select c1 from t1 where c0 like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 regexp '1'",
            "select c1 from t1 where c0 not regexp '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not regexp '1'",
            "select c1 from t1 where c0 regexp '1'",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify19() {
        assert_j_plan2(
            "select c1 from t1 where not c0 in (1,2,3)",
            "select c1 from t1 where c0 not in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not in (1,2,3)",
            "select c1 from t1 where c0 in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 between 0 and 2",
            "select c1 from t1 where c0 not between 0 and 2",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not between 0 and 2",
            "select c1 from t1 where c0 between 0 and 2",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.6
    #[test]
    fn test_expr_simplify38() {
        assert_j_plan2(
            "select c1 from t1 where not 1",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 2",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 1.5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 1.5e5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0.0",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0.0e-1",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        )
    }

    fn assert_eq_filt_expr(s1: &str, mut q1: QueryPlan, _s2: &str, mut q2: QueryPlan) {
        expr_simplify(&mut q1).unwrap();
        expr_simplify(&mut q2).unwrap();
        print_plan(s1, &q1);
        assert_eq!(get_filt_expr(&q1), get_filt_expr(&q2));
    }
}
