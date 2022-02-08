use crate::error::Result;
use crate::op::{Op, OpMutVisitor, OpVisitor};
use crate::query::{QueryPlan, QuerySet};
use fnv::{FnvHashMap, FnvHashSet};
use smol_str::SmolStr;
use std::collections::BTreeMap;
use xngin_expr::{Col, Expr, ExprMutVisitor, ExprVisitor, QueryID};

/// Column pruning will remove unnecessary columns from the given plan.
/// It is invoked top down. First collect all output columns from current
/// query, then apply the pruning to each of its child query.
#[inline]
pub fn col_prune(QueryPlan { queries, root }: &mut QueryPlan) -> Result<()> {
    col_prune_rec(queries, *root);
    Ok(())
}

fn col_prune_rec(all_qry_set: &mut QuerySet, root: QueryID) {
    let (mut use_set, children) = if let Some(subq) = all_qry_set.get(&root) {
        let qry_set: FnvHashSet<_> = subq
            .scope
            .query_aliases
            .iter()
            .map(|(_, query_id)| *query_id)
            .collect();
        let children: Vec<QueryID> = subq
            .scope
            .query_aliases
            .iter()
            .map(|(_, query_id)| *query_id)
            .collect();
        let mut use_set: FnvHashMap<_, BTreeMap<_, _>> = FnvHashMap::default();
        for (query_id, idx) in &subq.scope.cor_vars {
            // should always be true
            assert!(qry_set.contains(query_id));
            use_set.entry(*query_id).or_default().insert(*idx, 0);
        }
        let mut cc = ColCollector {
            qry_set: &qry_set,
            use_set: &mut use_set,
        };
        subq.root.walk(&mut cc);
        (use_set, children)
    } else {
        return;
    };
    // update the mapping of old column to new column.
    update_use_set(&mut use_set);
    if !use_set.is_empty() {
        // recursively modify all column references in current query and its correlated subqueries.
        let mut subq_set = vec![];
        // process root query
        if let Some(subq) = all_qry_set.get_mut(&root) {
            let mut cm = ColModifier {
                use_set: &use_set,
                subq_set: &mut subq_set,
            };
            let _ = subq.root.walk_mut(&mut cm);
        }
        while let Some(query_id) = subq_set.pop() {
            if let Some(subq) = all_qry_set.get_mut(&query_id) {
                if !subq.scope.cor_cols.is_empty() {
                    // only process correlated subquery
                    let mut cm = ColModifier {
                        use_set: &use_set,
                        subq_set: &mut subq_set,
                    };
                    let _ = subq.root.walk_mut(&mut cm);
                }
            }
        }
    }
    // reset out cols to represent pruning result
    if let Some(subq) = all_qry_set.get_mut(&root) {
        subq.reset_out_cols()
    }
    // prune all children
    for child in children {
        let mapping = use_set.remove(&child).unwrap_or_default();
        if let Some(subq) = all_qry_set.get_mut(&child) {
            let mut cp = ColPruner { mapping };
            subq.root.walk_mut(&mut cp);
            // then prune recursively
            col_prune_rec(all_qry_set, child)
        }
    }
}

/// Column collector to current query
struct ColCollector<'a> {
    qry_set: &'a FnvHashSet<QueryID>,
    use_set: &'a mut FnvHashMap<QueryID, BTreeMap<u32, u32>>,
}

impl ExprVisitor for ColCollector<'_> {
    #[inline]
    fn enter(&mut self, e: &Expr) -> bool {
        if let Expr::Col(Col::QueryCol(query_id, idx)) = e {
            if self.qry_set.contains(query_id) {
                self.use_set.entry(*query_id).or_default().insert(*idx, 0);
            }
        }
        true
    }

    #[inline]
    fn leave(&mut self, _e: &xngin_expr::Expr) -> bool {
        true
    }
}

impl OpVisitor for ColCollector<'_> {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        for e in op.exprs() {
            let _ = e.walk(self);
        }
        true
    }

    #[inline]
    fn leave(&mut self, _op: &Op) -> bool {
        true
    }
}

/// Column modifier to current query
struct ColModifier<'a> {
    use_set: &'a FnvHashMap<QueryID, BTreeMap<u32, u32>>,
    subq_set: &'a mut Vec<QueryID>,
}

impl OpMutVisitor for ColModifier<'_> {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        for e in op.exprs_mut() {
            let _ = e.walk_mut(self);
        }
        true
    }

    #[inline]
    fn leave(&mut self, _op: &mut Op) -> bool {
        true
    }
}

impl ExprMutVisitor for ColModifier<'_> {
    #[inline]
    fn enter(&mut self, e: &mut Expr) -> bool {
        match e {
            Expr::Col(Col::QueryCol(query_id, idx))
            | Expr::Col(Col::CorrelatedCol(query_id, idx)) => {
                if let Some(new) = self.use_set.get(query_id).and_then(|m| m.get(idx).cloned()) {
                    *idx = new;
                }
            }
            Expr::Subq(_, query_id) => self.subq_set.push(*query_id),
            _ => (),
        }
        true
    }

    #[inline]
    fn leave(&mut self, _e: &mut Expr) -> bool {
        true
    }
}

/// Column pruner to child query
struct ColPruner {
    mapping: BTreeMap<u32, u32>,
}

impl OpMutVisitor for ColPruner {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Proj(proj) => {
                if self.mapping.is_empty() {
                    // no output needed, use Const(1) to replace all original output.
                    proj.cols.clear();
                    proj.cols.push((Expr::const_i64(1), SmolStr::new("1")));
                } else {
                    proj.cols = std::mem::take(&mut proj.cols)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, e)| {
                            if self.mapping.contains_key(&(i as u32)) {
                                Some(e)
                            } else {
                                None
                            }
                        })
                        .collect();
                }
                false
            }
            Op::Aggr(aggr) => {
                if self.mapping.is_empty() {
                    aggr.proj.clear();
                    aggr.proj.push((Expr::const_i64(1), SmolStr::new("1")));
                } else {
                    aggr.proj = std::mem::take(&mut aggr.proj)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, e)| {
                            if self.mapping.contains_key(&(i as u32)) {
                                Some(e)
                            } else {
                                None
                            }
                        })
                        .collect();
                }
                false
            }
            Op::Row(row) => {
                if self.mapping.is_empty() {
                    row.clear();
                    row.push((Expr::const_i64(1), SmolStr::new("1")));
                } else {
                    *row = std::mem::take(row)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, e)| {
                            if self.mapping.contains_key(&(i as u32)) {
                                Some(e)
                            } else {
                                None
                            }
                        })
                        .collect();
                }
                false
            }
            _ => true,
        }
    }

    #[inline]
    fn leave(&mut self, _op: &mut Op) -> bool {
        true
    }
}

fn update_use_set(use_set: &mut FnvHashMap<QueryID, BTreeMap<u32, u32>>) {
    for mapping in use_set.values_mut() {
        for (i, old) in mapping.values_mut().enumerate() {
            *old = i as u32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, get_lvl_queries, print_plan};

    #[test]
    fn test_col_prune1() {
        assert_j_plan("select 1 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].scope.out_cols.len());
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq.len());
            assert_eq!(1, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune2() {
        assert_j_plan("select c1, c3 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].scope.out_cols.len());
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq.len());
            assert_eq!(2, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune3() {
        assert_j_plan("select c3, c1 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].scope.out_cols.len());
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(2, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune4() {
        assert_j_plan("select c2, c2, c2 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].scope.out_cols.len());
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            // remove duplicates and keep 1 column from source
            assert_eq!(1, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune5() {
        assert_j_plan("select t2.c0 from t2, t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(2, subq.len());
            assert_eq!(3, subq[0].scope.out_cols.len());
            assert_eq!(4, subq[1].scope.out_cols.len());
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq[0].scope.out_cols.len());
            // although no columns required from t3, still
            // keep 1 const value.
            assert_eq!(1, subq[1].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune6() {
        assert_j_plan(
            "select t2.c0 from t2, t3 where t2.c1 = t3.c1",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                // additional column is requied from t2 for filter
                assert_eq!(2, subq[0].scope.out_cols.len());
                assert_eq!(1, subq[1].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune7() {
        assert_j_plan(
            "select t2.c0 from t2 join t3 on t2.c1 = t3.c1 and t2.c2 = t3.c2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                // additional column is required from t2 for join
                assert_eq!(3, subq[0].scope.out_cols.len());
                // additional column is required from t3 for join
                assert_eq!(2, subq[1].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune8() {
        assert_j_plan(
            "select t1.c0, tmp.x from t1, (select 1 as x, 2 as y) tmp",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                assert_eq!(1, subq[1].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune9() {
        assert_j_plan("select sum(c2) from t2", |sql, mut plan| {
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune10() {
        assert_j_plan("select count(*) from t2", |sql, mut plan| {
            col_prune(&mut plan).unwrap();
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq[0].scope.out_cols.len());
            print_plan(sql, &plan)
        })
    }

    #[test]
    fn test_col_prune11() {
        assert_j_plan(
            "select count(*) from t2, t3 where t2.c2 = t3.c2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                assert_eq!(1, subq[1].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune12() {
        assert_j_plan(
            "select (select c0 from t3 where t3.c1 = t2.c1) from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune13() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2) from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(2, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune14() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2), c0 from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(3, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune15() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2), c1 from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(2, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune16() {
        assert_j_plan(
            "select s1 from (select sum(c1) as s1, sum(c2) as s2 from t2) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune17() {
        assert_j_plan(
            "select s1 from (select 1 as s1, 2 as s2) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune18() {
        assert_j_plan(
            "select count(*) from (select count(*) as c from t1) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].scope.out_cols.len());
                assert!(subq[0].scope.out_cols[0].0.is_const());
                print_plan(sql, &plan)
            },
        )
    }
}
