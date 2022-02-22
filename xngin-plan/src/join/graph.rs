use crate::error::{Error, Result};
use crate::join::edge::{Edge, Feedback, Rebuild, Sides};
use crate::join::vertex::{VertexID, VertexSet};
use crate::join::JoinKind;
use crate::op::Op;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use xngin_expr::{Expr, QueryID};

// Support at most 31 tables in single join graph.
// The threshold is actually very high for DP algorithm
// used in join reorder.
// If number of tables to join is more than 20,
// it will be very slow to determine the correct join order.
pub const MAX_JOIN_QUERIES: usize = 31;

/// Graph is a container maintaining two or more queries
/// to be joined together, with a set of join types and
/// join conditions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Graph {
    pub vertexes: VertexSet,
    pub vmap: HashMap<VertexID, QueryID>,
    pub rev_vmap: HashMap<QueryID, VertexID>,
    /// Edges store the join conditions in insertion order.
    /// It is important to cover outer join cases, because outer
    /// joins cannot be reorder freely.
    pub edges: IndexMap<VertexSet, Edge>,
    pub queries: Vec<Op>,
}

impl Graph {
    #[inline]
    pub fn add_query(&mut self, qry_id: QueryID) -> Result<VertexID> {
        let v_idx = self.queries.len();
        if v_idx >= MAX_JOIN_QUERIES {
            return Err(Error::TooManyTablesToJoin);
        }
        let vid = VertexID(1u32 << v_idx);
        self.vertexes |= vid;
        self.vmap.insert(vid, qry_id);
        self.rev_vmap.insert(qry_id, vid);
        self.queries.push(Op::Query(qry_id));
        Ok(vid)
    }

    #[inline]
    pub fn add_edge(
        &mut self,
        kind: JoinKind,
        l_vset: VertexSet,
        r_vset: VertexSet,
        cond: Vec<Expr>,
        filt: Vec<Expr>,
    ) -> Result<Vec<(Expr, QueryID)>> {
        match kind {
            JoinKind::Inner => {
                // Inner join condition is treated as filter
                // and left vertex and right vertex is discarded.
                let mut res = vec![];
                for e in cond.into_iter().chain(filt) {
                    res.extend(self.add_single_filt(e)?);
                }
                Ok(res)
            }
            JoinKind::Left => {
                let edge = Edge::new_left(&self.vmap, &self.rev_vmap, l_vset, r_vset, cond)?;
                let vset = edge.union_vset();
                assert!(self.edges.insert(vset, edge).is_none());
                let mut res = vec![];
                for e in filt {
                    res.extend(self.add_single_filt(e)?)
                }
                Ok(res)
            }
            JoinKind::Full => {
                let edge = Edge::new_full(&self.vmap, l_vset, r_vset, cond)?;
                let vset = edge.union_vset();
                assert!(self.edges.insert(vset, edge).is_none());
                let mut res = vec![];
                for e in filt {
                    res.extend(self.add_single_filt(e)?)
                }
                Ok(res)
            }
            _ => todo!(),
        }
    }

    /// Add a single filter expression.
    ///
    /// One usual case is that user first specify a list of tables
    /// in FROM clause, e.g. "FROM t1, t2, t3, t4", and then
    /// specify join conditions and filters in WHERE clause, e.g.
    /// "WHERE t1.c1 = t2.c2 and t3.c3 > 0 and t1.c1 + t3.c3 = t2.c2 + t4.c4".
    ///
    /// In optimization, the cross join will be first converted to
    /// a join graph without any join conditions, then each CNF predicate
    /// should be splitted from the filter and pushed to join graph.
    ///
    /// If expression only involves single table, e.g. "t3.c3 > 0",
    /// it can be pushed directly to that table, except there are outer
    /// joins on the push path.
    ///
    /// Another use case is to specify exact join types in SQL.
    /// e.g. "FROM t1 JOIN t2 LEFT JOIN t3 FULL JOIN t4".
    /// Here we need to respect original join order because change outer join
    /// order may result in wrong answers.
    ///
    /// Returns the expression and associated query id if expression can be directly
    /// pushed down to the query.
    #[inline]
    pub fn add_single_filt(&mut self, mut expr: Expr) -> Result<Vec<(Expr, QueryID)>> {
        let mut qry_ids = HashSet::new();
        expr.collect_qry_ids(&mut qry_ids);
        let filt_vset = qids_to_vset(&self.rev_vmap, &qry_ids)?;
        let mut res = vec![];
        // first iteration is to try outer joins in reverse order
        // rebuild edge if the predicate pushdown changes the join type
        let mut rebuild: Option<(VertexSet, Rebuild)> = None;
        loop {
            for (vset, jc) in self.edges.iter_mut().rev() {
                // also skip if no intersection between edge and expression
                if matches!(jc.kind, JoinKind::Left | JoinKind::Full) && vset.intersects(filt_vset)
                {
                    match jc.push_filt(filt_vset, expr)? {
                        Feedback::Fallthrough(e) => expr = e,
                        Feedback::Rebuild(rb, e) => {
                            expr = e;
                            rebuild = Some((*vset, rb));
                            break;
                        }
                        Feedback::Accept => return Ok(res),
                    }
                }
            }

            if let Some((rb_vset, rb)) = rebuild.take() {
                // the rebuild process must reserve the original join ordering
                let mut stack = vec![];
                // pop all joins after the one to rebuild
                while let Some((vset, edge)) = self.edges.pop() {
                    if vset == rb_vset {
                        // rebuild it
                        match (rb, edge) {
                            (
                                Rebuild::Inner,
                                Edge {
                                    sides: Sides::Both(l, r),
                                    cond,
                                    filt,
                                    ..
                                },
                            ) => {
                                let extra =
                                    self.add_edge(JoinKind::Inner, l.vset, r.vset, cond, filt)?;
                                res.extend(extra);
                            }
                            (
                                Rebuild::Left,
                                Edge {
                                    sides: Sides::Both(l, r),
                                    cond,
                                    filt,
                                    ..
                                },
                            ) => {
                                let extra =
                                    self.add_edge(JoinKind::Left, l.vset, r.vset, cond, filt)?;
                                res.extend(extra);
                            }
                            (
                                Rebuild::Right,
                                Edge {
                                    sides: Sides::Both(l, r),
                                    cond,
                                    filt,
                                    ..
                                },
                            ) => {
                                // swap l_vset and r_vset
                                let extra =
                                    self.add_edge(JoinKind::Left, r.vset, l.vset, cond, filt)?;
                                res.extend(extra);
                            }
                            _ => unreachable!(),
                        }
                        break;
                    } else {
                        stack.push((vset, edge));
                    }
                }
                // add all edges back
                while let Some((vset, edge)) = stack.pop() {
                    self.edges.insert(vset, edge);
                }
            } else {
                break;
            }
        }

        // after checking all outer joins, we can run fast path to push expr that
        // only involves single query
        if qry_ids.len() == 1 {
            return Ok(vec![(expr, qry_ids.into_iter().next().unwrap())]);
        }
        // second iteration is to lookup inner joins, the order is not relevant
        for (vset, jc) in self.edges.iter_mut() {
            if !matches!(jc.kind, JoinKind::Left | JoinKind::Full) && vset.intersects(filt_vset) {
                match jc.push_filt(filt_vset, expr)? {
                    Feedback::Rebuild(..) => unreachable!(), // impossible to trigger rebuild on inner join
                    Feedback::Fallthrough(e) => expr = e,
                    Feedback::Accept => return Ok(res),
                }
            }
        }
        // finally, no edges accept the expression, created a new one
        let new = Edge::new_inner(&self.vmap, filt_vset, expr)?;
        assert!(self.edges.insert(filt_vset, new).is_none());
        Ok(vec![])
    }

    #[inline]
    pub fn queries(&self) -> Vec<QueryID> {
        self.queries
            .iter()
            .filter_map(|op| match op {
                Op::Query(qry_id) => Some(*qry_id),
                _ => None,
            })
            .collect()
    }
}

#[inline]
pub(super) fn qids_to_vset<'a, I>(map: &HashMap<QueryID, VertexID>, qry_ids: I) -> Result<VertexSet>
where
    I: IntoIterator<Item = &'a QueryID>,
{
    let mut vset = VertexSet::default();
    for qry_id in qry_ids {
        if let Some(vid) = map.get(qry_id) {
            vset |= *vid;
        } else {
            return Err(Error::QueryNotFound(*qry_id));
        }
    }
    Ok(vset)
}

#[inline]
pub(super) fn vset_to_qids<C>(map: &HashMap<VertexID, QueryID>, vset: VertexSet) -> Result<C>
where
    C: FromIterator<QueryID>,
{
    if let Some(vid) = vset.single() {
        map.get(&vid)
            .cloned()
            .ok_or(Error::InvalidJoinVertexSet)
            .map(|qid| std::iter::once(qid).collect::<C>())
    } else {
        vset.into_iter()
            .map(|vid| map.get(&vid).cloned().ok_or(Error::InvalidJoinVertexSet))
            .collect::<Result<C>>()
    }
}
