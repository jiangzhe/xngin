use crate::error::{Error, Result};
use crate::join::estimate::Estimate;
use crate::join::graph::{vid_to_qid, Edge, Graph, VertexSet};
use crate::join::reorder::Reorder;
use crate::join::{JoinKind, JoinOp};
use crate::op::Op;
use indexmap::IndexMap;
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem;

/// Goo stands for Greedy Operator Ordering.
/// It's a greedy algorithm to generate bushy tree.
/// The core of this algorithm is greedily combine join trees
/// such that intermediate result is minimal.
pub struct Goo<E>(E);

impl<E> Goo<E> {
    #[inline]
    pub fn new(est: E) -> Self {
        Goo(est)
    }
}

impl<E: Estimate> Reorder for Goo<E> {
    #[inline]
    fn reorder(mut self, graph: &Graph) -> Result<Op> {
        let mut joined = BTreeMap::new();
        let mut edges: IndexMap<VertexSet, Vec<&Edge>> = graph
            .edges
            .iter()
            .map(|(vset, es)| (*vset, es.iter().collect()))
            .collect();
        // initialize single queries.
        for vid in graph.vertexes {
            // trigger estimation on single table
            let _ = self.0.estimated_qry_rows(VertexSet::from(vid))?;
            let qid = vid_to_qid(&graph.vmap, vid)?;
            let op = Op::Query(qid);
            joined.insert(VertexSet::from(vid), op);
        }
        // combine join sets until only one join left.
        while joined.len() > 1 {
            if edges.is_empty() {
                return Err(Error::CrossJoinNotSupport);
            }
            let (e, pks) = min_res(&edges, &mut self.0, &joined)?;
            for (k0, k1) in pks {
                let es = edges.get_mut(&k0).unwrap();
                if es.len() == 1 {
                    edges.remove(&k0).unwrap();
                } else {
                    let pos = es.iter().position(|e| e.e_vset == k1).unwrap();
                    es.remove(pos);
                }
            }
            let vset = e.l_vset | e.r_vset;
            let l = joined.remove(&e.l_vset).unwrap();
            let r = joined.remove(&e.r_vset).unwrap();
            let edge = e.edge.into_owned();
            let op = Op::qualified_join(
                edge.kind,
                JoinOp::try_from(l)?,
                JoinOp::try_from(r)?,
                edge.cond,
                edge.filt,
            );
            joined.insert(vset, op);
        }
        joined
            .into_values()
            .next()
            .ok_or(Error::InvalidJoinTransformation)
    }
}

fn min_res<'a, E: Estimate>(
    edges: &IndexMap<VertexSet, Vec<&'a Edge>>,
    est: &mut E,
    join_map: &BTreeMap<VertexSet, Op>,
) -> Result<(MinEdge<'a>, PurgeKeys)> {
    let mut min_edge: Option<MinEdge> = None;
    let mut min_purge: PurgeKeys = smallvec![];
    let mut purge = smallvec![];
    for vi in join_map.keys() {
        for vj in join_map.keys().filter(|k| *k > vi) {
            for (vset, es) in edges {
                for new_edge in es {
                    if (*vi | *vj).includes(new_edge.e_vset) {
                        // pass join eligibility check
                        // Identify the exact left side and right side.
                        let (l_vset, r_vset) = {
                            let l = new_edge.e_vset & new_edge.l_vset;
                            let r = new_edge.e_vset & new_edge.r_vset;
                            if vi.includes(l) && vj.includes(r) {
                                (*vi, *vj)
                            } else if vi.includes(r) && vj.includes(l) {
                                (*vj, *vi)
                            } else {
                                return Err(Error::InvalidJoinTransformation);
                            }
                        };
                        let key = (*vset, new_edge.e_vset);
                        purge.push(key);
                        let mut join_edge = Cow::Borrowed(*new_edge);
                        // For inner join, there might be multiple edges that can join two sides.
                        // Traverse all edges again to find if any other edge can be merged into current one.
                        // e.g. "A JOIN B ON a1 = b1 JOIN C ON b1 = c1 AND a1 = c1"
                        // Such that we have three edges: [{A<=>B}, {B<=>C}, {A<=>C}]
                        // If we join A, B together first, then there are two edges can be merged in next join
                        // because all three table are present in second join.
                        if join_edge.kind == JoinKind::Inner {
                            for (merge_vset, mes) in &*edges {
                                for merge_edge in mes {
                                    if merge_edge.kind == JoinKind::Inner
                                        && (*merge_vset, merge_edge.e_vset) != key
                                        && (l_vset | r_vset).includes(merge_edge.e_vset)
                                    {
                                        // merge this edge into join edge, only update join condition and eligibility set.
                                        let je = join_edge.to_mut();
                                        je.cond.extend_from_slice(&merge_edge.cond);
                                        je.e_vset |= merge_edge.e_vset;
                                        purge.push((*merge_vset, merge_edge.e_vset));
                                    }
                                }
                            }
                        }
                        // now we can estimate join rows and choose the best plan
                        let rows = est.estimated_join_rows(
                            join_edge.kind,
                            l_vset,
                            r_vset,
                            join_edge.e_vset,
                            &join_edge.cond,
                            &join_edge.filt,
                        )?;
                        if let Some(min_edge) = min_edge.as_mut() {
                            if rows < min_edge.rows {
                                min_edge.l_vset = l_vset;
                                min_edge.r_vset = r_vset;
                                min_edge.edge = join_edge;
                                min_edge.rows = rows;
                                mem::swap(&mut min_purge, &mut purge);
                            }
                        } else {
                            min_edge = Some(MinEdge {
                                l_vset,
                                r_vset,
                                edge: join_edge,
                                rows,
                            });
                            mem::swap(&mut min_purge, &mut purge);
                        }
                        purge.clear();
                    }
                }
            }
        }
    }
    min_edge
        .map(|e| (e, min_purge))
        .ok_or(Error::InvalidJoinVertexSet)
}

type PurgeKey = (VertexSet, VertexSet);
type PurgeKeys = SmallVec<[PurgeKey; 2]>;

struct MinEdge<'a> {
    l_vset: VertexSet,
    r_vset: VertexSet,
    edge: Cow<'a, Edge>,
    rows: f64,
}
