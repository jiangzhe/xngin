use crate::error::{Error, Result};
use crate::join::vertex::VertexSet;
use crate::join::{Join, JoinGraph, JoinOp, QualifiedJoin};
use crate::op::{Op, OpMutVisitor};
use crate::query::{Location, QueryPlan, QuerySet};
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::QueryID;

/// Initialize join graph.
/// The process can be viewed as two actions:
/// 1. detect joins.
/// Traverse the operator tree down to find the first join operator.
/// Then start the initialization.
/// 2. initialize graph.
/// From the topmost join operator, collect all queries to be joined.
/// Then replace the topmost join with the generated graph.
#[inline]
pub fn joingraph_initialize(QueryPlan { qry_set, root }: &mut QueryPlan) -> Result<()> {
    init_joingraph(qry_set, *root)
}

fn init_joingraph(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, location, op| {
        if location == Location::Intermediate {
            // only build join graph in intermediate queries
            let mut init = InitJoinGraph { qry_set };
            op.walk_mut(&mut init).unbranch()
        } else {
            Ok(())
        }
    })?
}

struct InitJoinGraph<'a> {
    qry_set: &'a mut QuerySet,
}

impl OpMutVisitor for InitJoinGraph<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            Op::Join(join) => {
                let mut graph = JoinGraph::default();
                match update_graph(self.qry_set, join.as_mut(), &mut graph) {
                    Ok(_) => {
                        *op = Op::join_graph(graph);
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(e),
                }
            }
            Op::Query(qry_id) => {
                // no join in current tree, recursively detect all children
                init_joingraph(self.qry_set, *qry_id).branch()
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

fn update_graph(
    qry_set: &mut QuerySet,
    join: &mut Join,
    graph: &mut JoinGraph,
) -> Result<VertexSet> {
    match join {
        Join::Cross(jos) => {
            let mut vset = VertexSet::default();
            for jo in jos {
                let op_vset = process_op(qry_set, jo, graph)?;
                vset |= op_vset;
            }
            Ok(vset)
        }
        Join::Qualified(QualifiedJoin {
            kind,
            left,
            right,
            cond,
            filt,
        }) => {
            let l_vset = process_op(qry_set, left, graph)?;
            let r_vset = process_op(qry_set, right, graph)?;
            let cond = mem::take(cond);
            let filt = mem::take(filt);
            graph.add_edge(*kind, l_vset, r_vset, cond, filt)?;
            let vset = l_vset | r_vset;
            Ok(vset)
        }
    }
}

fn process_op(qry_set: &mut QuerySet, jo: &mut JoinOp, graph: &mut JoinGraph) -> Result<VertexSet> {
    let mut vset = VertexSet::default();
    match jo {
        JoinOp(Op::Query(qry_id)) => {
            // recursively build join group in derived table
            init_joingraph(qry_set, *qry_id)?;
            let vid = graph.add_query(*qry_id)?;
            vset |= vid;
        }
        JoinOp(Op::Join(join)) => {
            let child_vset = update_graph(qry_set, join.as_mut(), graph)?;
            vset |= child_vset;
        }
        _ => unreachable!(),
    }
    Ok(vset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan1, j_catalog, print_plan};
    use crate::op::OpVisitor;

    #[test]
    fn test_joingraph_init1() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select 1 from t1, t2", assert_join_graph_exists);
        assert_j_plan1(&cat, "select 1 from t1 join t2", assert_join_graph_exists);
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2",
            assert_join_graph_exists,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2",
            assert_join_graph_exists,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2",
            assert_join_graph_exists,
        );
    }

    fn assert_join_graph_exists(sql: &str, mut plan: QueryPlan) {
        joingraph_initialize(&mut plan).unwrap();
        print_plan(sql, &plan);
        struct CollectGraph(Option<JoinGraph>);
        impl OpVisitor for CollectGraph {
            type Cont = ();
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
        let subq = plan.root_query().unwrap();
        let mut c = CollectGraph(None);
        let _ = subq.root.walk(&mut c);
        assert!(c.0.is_some());
    }
}
