pub(crate) mod greedy;

use crate::error::{Error, Result};
use crate::join::graph::{vid_to_qid, Graph, VertexSet};
use crate::join::{JoinKind, JoinOp};
use crate::op::{Op, OpMutVisitor};
use crate::query::{QueryPlan, QuerySet};
use std::collections::HashMap;
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::QueryID;

// Export GOO algorithm
pub use greedy::Goo;

/// Entry to perform join reorder.
#[inline]
pub fn join_reorder<R, F>(plan: &mut QueryPlan, mut f: F) -> Result<()>
where
    R: Reorder,
    F: FnMut() -> R,
{
    for qid in &plan.attaches {
        reorder_join(&mut plan.qry_set, *qid, &mut f)?
    }
    reorder_join(&mut plan.qry_set, plan.root, &mut f)
}

fn reorder_join<R, F>(qry_set: &mut QuerySet, qry_id: QueryID, f: &mut F) -> Result<()>
where
    R: Reorder,
    F: FnMut() -> R,
{
    struct S<'a, F>(&'a mut QuerySet, &'a mut F);
    impl<'a, R, F> OpMutVisitor for S<'a, F>
    where
        R: Reorder,
        F: FnMut() -> R,
    {
        type Cont = ();
        type Break = Error;
        #[inline]
        fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
            match op {
                Op::Query(qry_id) => reorder_join(self.0, *qry_id, self.1).branch(),
                Op::JoinGraph(_) => {
                    let graph = mem::take(op);
                    if let Op::JoinGraph(g) = graph {
                        let r = (self.1)();
                        let new = r.reorder(g.as_ref()).branch()?;
                        *op = new;
                        ControlFlow::Continue(())
                    } else {
                        unreachable!()
                    }
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut s = S(qry_set, f);
        op.walk_mut(&mut s).unbranch()
    })?
}

/// Reorder the join graph and reconstruct as operator tree.
pub trait Reorder {
    fn reorder(self, graph: &Graph) -> Result<Op>;
}

/// Sequential converts the join graph back to join tree
/// in original order.
pub struct Sequential;

impl Reorder for Sequential {
    fn reorder(self, graph: &Graph) -> Result<Op> {
        // let mut vset = VertexSet::default();
        let mut joined = HashMap::new();
        // insert all vertexes back
        for vid in graph.vertexes {
            let qid = vid_to_qid(&graph.vmap, vid)?;
            joined.insert(VertexSet::from(vid), Op::Query(qid));
        }

        for (vset, eids) in graph.vset_eids() {
            if eids.len() == 1 {
                let edge = graph.edge(eids[0]);
                let l = joined
                    .remove(&edge.l_vset)
                    .ok_or(Error::InvalidJoinVertexSet)?;
                let r = joined
                    .remove(&edge.r_vset)
                    .ok_or(Error::InvalidJoinVertexSet)?;
                let op = Op::qualified_join(
                    edge.kind,
                    JoinOp::try_from(l)?,
                    JoinOp::try_from(r)?,
                    edge.cond
                        .iter()
                        .map(|cid| graph.pred(*cid).clone())
                        .collect(),
                    edge.filt
                        .iter()
                        .map(|fid| graph.pred(*fid).clone())
                        .collect(),
                );
                joined.insert(*vset, op);
            } else {
                // only inner join could have multiple join edges, we fold them into one.
                // e.g. A JOIN B ON a1 = b1 JOIN C ON a1 = c1 AND b1 = c1
                // In above case, we will have two edges to join A, B, C together.
                // vset(ABC) => [ edge(A <=> C), edge(B <=> C) ]
                // So we fold two edges into one, rebuild the join.
                debug_assert!(eids
                    .iter()
                    .all(|eid| graph.edge(*eid).kind == JoinKind::Inner));
                let e1 = graph.edge(eids[0]);
                // use first edge to retrieve both sides from joined list
                let mut l_vset = VertexSet::default();
                let mut r_vset = VertexSet::default();
                for vset in joined.keys() {
                    if vset.includes(e1.l_vset) {
                        l_vset = *vset;
                    } else if vset.includes(e1.r_vset) {
                        r_vset = *vset;
                    }
                }
                if l_vset.is_empty() || r_vset.is_empty() {
                    return Err(Error::InvalidJoinVertexSet);
                }
                // check if all edges are included in two sides
                assert!(eids.iter().all(|eid| {
                    let e = graph.edge(*eid);
                    (l_vset.includes(e.l_vset) && r_vset.includes(e.r_vset))
                        || (l_vset.includes(e.r_vset) && r_vset.includes(e.l_vset))
                }));
                let l = joined.remove(&l_vset).ok_or(Error::InvalidJoinVertexSet)?;
                let r = joined.remove(&r_vset).ok_or(Error::InvalidJoinVertexSet)?;
                let cond: Vec<_> = eids
                    .iter()
                    .flat_map(|eid| graph.preds(graph.edge(*eid).cond.clone()))
                    .cloned()
                    .collect();
                let op = Op::qualified_join(
                    JoinKind::Inner,
                    JoinOp::try_from(l)?,
                    JoinOp::try_from(r)?,
                    cond,
                    vec![],
                );
                joined.insert(*vset, op);
            }
        }
        // finally there should be only one single join operator and we retrieve it
        assert_eq!(1, joined.len());
        let op = joined.into_values().next().unwrap();
        Ok(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{
        assert_j_plan1, extract_join_kinds, get_join_graph, get_tbl_id, j_catalog, print_plan,
        table_map,
    };
    use crate::join::estimate::Estimate;
    use crate::join::graph::{Edge, Graph};
    use crate::rule::joingraph_initialize;
    use xngin_catalog::TableID;

    #[test]
    fn test_join_reorder_sequential() {
        let cat = j_catalog();
        for (s, jks) in vec![
            ("select 1 from t1 join t2 on t1.c1 = t2.c1", vec!["inner"]),
            ("select 1 from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1", vec!["inner", "inner"]),
            ("select 1 from t1 join t2 on t1.c1 = t2.c1 left join t3 on t1.c1 = t3.c1", vec!["left", "inner"]),
            ("select 1 from t1 left join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1", vec!["inner", "left"]),
            ("select 1 from t0 full join t1 on t0.c0 = t1.c0 join t2 on t1.c1 = t2.c1 left join t3 on t2.c2 = t3.c2", vec!["left", "inner", "full"]),
            ("select 1 from t1 left join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 and t2.c1 = t3.c1", vec!["inner", "left"]),
        ] {
            assert_j_plan1(&cat, s, |s, mut q| {
                joingraph_initialize(&mut q.qry_set, q.root).unwrap();
                join_reorder(&mut q, || Sequential).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                assert_eq!(jks, extract_join_kinds(&subq.root))
            })
        }
    }

    #[test]
    fn test_join_reorder_greedy() {
        use crate::op::OpKind::*;
        let cat = j_catalog();
        let tbl_map = table_map(&*cat, "j", vec!["t0", "t1", "t2", "t3"]);
        for (s, qrs, js, jss, shape) in vec![
            (
                "select 1 from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1",
                vec![("t1", 100.0), ("t2", 100.0), ("t3", 100.0)],
                vec![("t1", "t2", 0.1), ("t2", "t3", 0.2)],
                vec![],
                vec![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table],
            ),
            (
                "select 1 from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1",
                vec![("t1", 100.0), ("t2", 100.0), ("t3", 100.0)],
                vec![("t1", "t2", 0.2), ("t2", "t3", 0.1)],
                vec![],
                vec![Proj, Join, Proj, Table, Join, Proj, Table, Proj, Table],
            ),
            (
                "select 1 from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 and t1.c1 = t3.c1",
                vec![("t1", 100.0), ("t2", 100.0), ("t3", 100.0)],
                vec![("t1", "t2", 0.2), ("t2", "t3", 0.1), ("t1", "t3", 0.01)],
                // suppost final result 0.02
                vec![(vec!["t1"], vec!["t2", "t3"], 0.2), (vec!["t1", "t2"], vec!["t3"], 1.0), (vec!["t1", "t3"], vec!["t2"], 2.0)],
                vec![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table],
            ),
        ] {
            assert_j_plan1(&cat, s, |s, mut q| {
                joingraph_initialize(&mut q.qry_set, q.root).unwrap();
                let subq = q.root_query().unwrap();
                let graph = get_join_graph(subq).unwrap();
                let qmap = build_qry_map(&mut q.qry_set, &graph, &tbl_map);
                let est = preset_estimator(qmap, qrs, js,jss);
                join_reorder(&mut q, || Goo::new(est.clone())).unwrap();
                print_plan(s, &q);
                assert_eq!(shape, q.shape());
            })
        }
    }

    // Simple estimator with presettings based on query set.
    // The join kind, condition and filter predicates are all ignored.
    #[derive(Debug, Default, Clone)]
    struct PresetEstimator {
        // rows per query
        qmap: HashMap<VertexSet, f64>,
        // selectivity per join
        jmap: HashMap<(VertexSet, VertexSet), f64>,
    }

    impl PresetEstimator {
        fn add_qry(&mut self, vid: VertexSet, count: f64) {
            self.qmap.insert(vid, count);
        }

        fn add_join(&mut self, l_vset: VertexSet, r_vset: VertexSet, count: f64) {
            self.jmap.insert((l_vset, r_vset), count);
            self.jmap.insert((r_vset, l_vset), count);
        }
    }

    impl Estimate for PresetEstimator {
        #[inline]
        fn estimated_qry_rows(&mut self, vset: VertexSet) -> Result<f64> {
            self.qmap
                .get(&vset)
                .cloned()
                .ok_or(Error::JoinEstimationNotSupport)
        }

        #[inline]
        fn estimated_join_rows(
            &mut self,
            _graph: &Graph,
            l_vset: VertexSet,
            r_vset: VertexSet,
            edge: &Edge,
        ) -> Result<f64> {
            match (
                self.qmap.get(&l_vset).cloned(),
                self.qmap.get(&r_vset).cloned(),
            ) {
                (Some(l_rows), Some(r_rows)) => {
                    if let Some(sel) = self
                        .jmap
                        .get(&(edge.e_vset & l_vset, edge.e_vset & r_vset))
                        .cloned()
                    {
                        let rows = l_rows * r_rows * sel;
                        self.qmap.insert(l_vset | r_vset, rows);
                        Ok(rows)
                    } else {
                        Err(Error::JoinEstimationNotSupport)
                    }
                }
                _ => Err(Error::JoinEstimationNotSupport),
            }
        }
    }

    fn build_qry_map(
        qry_set: &mut QuerySet,
        graph: &Graph,
        tbl_map: &HashMap<&'static str, TableID>,
    ) -> HashMap<&'static str, VertexSet> {
        let mut t2v = HashMap::new();
        for vid in graph.vertexes {
            let qid = graph.vmap.get(&vid).cloned().unwrap();
            let tbl_id = get_tbl_id(qry_set, qid).unwrap();
            t2v.insert(tbl_id, VertexSet::from(vid));
        }
        let mut s2v = HashMap::new();
        for (tn, tbl_id) in tbl_map.iter() {
            if let Some(vset) = t2v.get(tbl_id) {
                s2v.insert(*tn, *vset);
            }
        }
        s2v
    }

    fn preset_estimator(
        map: HashMap<&'static str, VertexSet>,
        qry_rows: Vec<(&'static str, f64)>,
        join_sels: Vec<(&'static str, &'static str, f64)>,
        joinset_sels: Vec<(Vec<&'static str>, Vec<&'static str>, f64)>,
    ) -> PresetEstimator {
        let mut est = PresetEstimator::default();
        for (q, rows) in qry_rows {
            if let Some(vid) = map.get(q) {
                est.add_qry(*vid, rows);
            }
        }
        for (lq, rq, sel) in join_sels {
            let l_vset = map.get(lq).unwrap();
            let r_vset = map.get(rq).unwrap();
            est.add_join(*l_vset, *r_vset, sel);
        }
        for (lqs, rqs, sel) in joinset_sels {
            let l_vset: VertexSet = lqs
                .into_iter()
                .map(|q| map.get(q).cloned().unwrap())
                .collect();
            let r_vset: VertexSet = rqs
                .into_iter()
                .map(|q| map.get(q).cloned().unwrap())
                .collect();
            est.add_join(l_vset, r_vset, sel);
        }
        est
    }
}
