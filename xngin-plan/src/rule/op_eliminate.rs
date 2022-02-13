use crate::error::Result;
use crate::op::{
    Filt, Join, JoinKind, Limit, Op, OpMutVisitor, Proj, QualifiedJoin, Setop, SetopKind, Sort,
};
use crate::query::QueryPlan;
use indexmap::IndexSet;
use std::collections::{HashMap, VecDeque};
use std::mem;
use xngin_expr::{Const, Expr, QueryID, Setq};

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
    if let Some(subq) = queries.get_mut(root) {
        let mut subq_ids = VecDeque::new();
        let mut parent_map = HashMap::new();
        let mut empty_subqs = IndexSet::new();
        // 1. walk root plan and see if it's empty now
        let mut eo = EliminateOp::new(*root, false, &mut subq_ids, &mut parent_map);
        let _ = subq.root.walk_mut(&mut eo);
        if subq.root.is_empty() {
            // entire tree is empty now
            return Ok(());
        }
        // 2. try all the child queries
        while let Some(subq_id) = subq_ids.pop_front() {
            if let Some(subq) = queries.get_mut(&subq_id) {
                let mut eo = EliminateOp::new(subq_id, true, &mut subq_ids, &mut parent_map);
                let _ = subq.root.walk_mut(&mut eo);
                if subq.root.is_empty() {
                    empty_subqs.insert(subq_id);
                }
            }
        }
        // 3. try parents of all empty queries
        while let Some(em_qid) = empty_subqs.pop() {
            if let Some(parent_qid) = parent_map.get(&em_qid) {
                if let Some(subq) = queries.get_mut(parent_qid) {
                    let mut ec = EliminateByEmptyChild::new(em_qid);
                    let _ = subq.root.walk_mut(&mut ec);
                    if subq.root.is_empty() {
                        empty_subqs.insert(*parent_qid);
                    }
                }
            }
        }
    }
    Ok(())
}

struct EliminateOp<'a> {
    query_id: QueryID,
    is_subq: bool,
    has_limit: bool,
    subq_ids: &'a mut VecDeque<QueryID>,
    parent_map: &'a mut HashMap<QueryID, QueryID>,
}

impl<'a> EliminateOp<'a> {
    fn new(
        query_id: QueryID,
        is_subq: bool,
        subq_ids: &'a mut VecDeque<QueryID>,
        parent_map: &'a mut HashMap<QueryID, QueryID>,
    ) -> Self {
        EliminateOp {
            query_id,
            is_subq,
            has_limit: false,
            subq_ids,
            parent_map,
        }
    }
}

impl OpMutVisitor for EliminateOp<'_> {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Filt(Filt { pred, source }) => {
                match pred {
                    Expr::Const(Const::Null) => {
                        // impossible predicates, remove entire sub-tree
                        *op = Op::Empty;
                    }
                    Expr::Const(c) => {
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
            Op::Subquery(query_id) => {
                self.parent_map.insert(*query_id, self.query_id);
                self.subq_ids.push_back(*query_id);
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
            Op::Subquery(_) | Op::Table(..) | Op::Row(_) => (),
            _ => eliminate_tree_bottom_up(op),
        }
        true
    }
}

fn eliminate_tree_bottom_up(op: &mut Op) {
    match op {
        Op::Join(j) => match j.as_mut() {
            Join::Cross(tbls) => {
                if tbls.iter().any(|t| t.is_empty()) {
                    // any child in cross join results in empty rows
                    *op = Op::Empty
                }
            }
            Join::Qualified(QualifiedJoin {
                kind,
                left: Op::Empty,
                ..
            }) => match kind {
                JoinKind::Inner
                | JoinKind::Left
                | JoinKind::Semi
                | JoinKind::AntiSemi
                | JoinKind::Mark
                | JoinKind::Single => *op = Op::Empty,
                JoinKind::Full => (),
            },
            Join::Qualified(QualifiedJoin {
                kind,
                right: Op::Empty,
                ..
            }) => match kind {
                JoinKind::Inner
                | JoinKind::Semi
                | JoinKind::AntiSemi
                | JoinKind::Mark
                | JoinKind::Single => *op = Op::Empty,
                JoinKind::Full | JoinKind::Left => (),
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
        Op::Subquery(_) | Op::Table(..) | Op::Row(_) => unreachable!(),
        Op::Empty => (),
    }
}

struct EliminateByEmptyChild {
    em_qid: QueryID,
}

impl EliminateByEmptyChild {
    fn new(em_qid: QueryID) -> Self {
        EliminateByEmptyChild { em_qid }
    }
}

impl OpMutVisitor for EliminateByEmptyChild {
    #[inline]
    fn enter(&mut self, _op: &mut Op) -> bool {
        true
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Subquery(query_id) => {
                if *query_id == self.em_qid {
                    *op = Op::Empty
                }
            }
            _ => eliminate_tree_bottom_up(op),
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, get_lvl_queries, op_visit_fn, print_plan};
    use crate::op::Join;

    #[test]
    fn test_op_eliminate1() {
        assert_j_plan("select c1 from t1 where null", assert_empty_root);
        assert_j_plan("select c1 from t1 where false", assert_empty_root);
    }

    #[test]
    fn test_op_eliminate2() {
        assert_j_plan("select c1 from t1 where true", |s, mut q| {
            op_eliminate(&mut q).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            if let Op::Proj(proj) = &subq.root {
                assert!(matches!(proj.source.as_ref(), Op::Subquery(..)))
            } else {
                panic!("fail")
            }
        })
    }

    #[test]
    fn test_op_eliminate3() {
        // derive table is empty
        assert_j_plan(
            "select c1 from (select c1 as c1 from t1 where null) x1",
            assert_empty_root,
        );
        assert_j_plan(
            "select c1 from (select c1 as c1 from (select c1 from t2 limit 0) x2) x1",
            assert_empty_root,
        );
        // cross join
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1, (select * from t2 where false) x2",
            assert_empty_root,
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1, (select * from t2 limit 0) x2",
            assert_empty_root,
        );
        // inner join
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1 join (select * from t2) x2",
            assert_empty_root,
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1 join (select * from t2 limit 0) x2",
            assert_empty_root,
        );
        // left join
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
                subq.root.walk(&mut op_visit_fn(|op| match op {
                    Op::Join(j) => match j.as_ref() {
                        Join::Qualified(QualifiedJoin { right, .. }) => {
                            assert_eq!(right, &Op::Empty);
                        }
                        _ => panic!("fail"),
                    },
                    _ => (),
                }));
            },
        );
        // right join
        assert_j_plan(
            "select c2 from (select c1 from t1 where null) x1 right join (select * from t2) x2",
            |s, mut q| {
                op_eliminate(&mut q).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                subq.root.walk(&mut op_visit_fn(|op| match op {
                    Op::Join(j) => match j.as_ref() {
                        Join::Qualified(QualifiedJoin { right, .. }) => {
                            assert_eq!(right, &Op::Empty);
                        }
                        _ => panic!("fail"),
                    },
                    _ => (),
                }));
            },
        );
        assert_j_plan(
            "select c2 from (select c1 from t1) x1 right join (select * from t2 where false) x2",
            assert_empty_root,
        );
    }

    #[test]
    fn test_op_eliminate4() {
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
    fn test_op_eliminate5() {
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
    fn test_op_eliminate6() {
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
                subq.root.walk(&mut op_visit_fn(|op| match op {
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
                subq.root.walk(&mut op_visit_fn(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
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
                subq.root.walk(&mut op_visit_fn(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
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
                subq.root.walk(&mut op_visit_fn(|op| match op {
                    Op::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    fn assert_empty_root(s1: &str, mut q1: QueryPlan) {
        op_eliminate(&mut q1).unwrap();
        print_plan(s1, &q1);
        let root = &q1.root_query().unwrap().root;
        assert_eq!(&Op::Empty, root);
    }
}
