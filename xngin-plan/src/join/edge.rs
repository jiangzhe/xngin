use crate::error::Result;
use crate::join::graph::{qids_to_vset, vset_to_qids};
use crate::join::vertex::{VertexID, VertexSet};
use crate::join::JoinKind;
use std::collections::{HashMap, HashSet};
use xngin_expr::fold::Fold;
use xngin_expr::{
    Col, CollectQryIDs, Expr, ExprVisitor, Func, FuncKind, Pred, PredFunc, PredFuncKind, QueryID,
};

/// Edge of join graph.
/// This is the "hyperedge" introduced in paper
/// "Dynamic Programming Strikes Back".
/// The fields `left`, `right` and `arbitrary` has some
/// special constructing logic:
///
/// For any join, we only count equi-join condition.
///
/// Join condition will be treated as CNF expression
/// and splitted.
/// Then each non-equal condition will still remain in
/// `cond`, but won't be used to compute `left`, `right`
/// and `arbitrary`.
/// For each equi-join condition, check the transposability
/// of each column in the equal expression, and group by
/// their tables.
///
/// The initial version will not take consideration of
/// equation and transposability mentioned above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub kind: JoinKind,
    // sides stores the information about `left`, `right`
    // or `arbitrary`
    pub sides: Sides,
    // Join conditions that should be evaluated in join.
    // They are conjunctive.
    pub cond: Vec<Expr>,
    // Filt which should be applied after the join.
    // Inner join will always has this field empty because
    // all filters can be evaluated as join condition in join phase.
    // They are conjunctive.
    pub filt: Vec<Expr>,
}

impl Edge {
    /// Construct an inner join edge.
    ///
    /// All tables involved in the join condition will be added
    /// to `arbitrary`.
    ///
    /// The join efficiency is not computed at this point, that means
    /// the expression can be anything, although equation btween two
    /// tables are more effective for fast join implementation.
    #[inline]
    pub fn new_inner(vmap: &HashMap<VertexID, QueryID>, vset: VertexSet, e: Expr) -> Result<Self> {
        assert!(vset.len() > 1);
        let qids: HashSet<QueryID> = vset_to_qids(vmap, vset)?;
        Ok(Edge {
            kind: JoinKind::Inner,
            sides: Sides::Arbitrary(Side { vset, qids }),
            cond: vec![e],
            filt: vec![],
        })
    }

    /// Construct a left join edge.
    ///
    /// Initialize `left` with all tables at left side in original
    /// plan, initialize `right` with all tables at right side.
    /// Then, check existence of all tables in the join condition,
    /// if any table in `left` not exists in join conditions, remove it
    /// from `left`.
    ///
    #[inline]
    pub fn new_left(
        vmap: &HashMap<VertexID, QueryID>,
        rev_vmap: &HashMap<QueryID, VertexID>,
        mut l_vset: VertexSet,
        r_vset: VertexSet,
        cond: Vec<Expr>,
    ) -> Result<Self> {
        let expr_qids = collect_qry_ids(&cond);
        let expr_vset = qids_to_vset(rev_vmap, &expr_qids)?;
        l_vset &= expr_vset;
        let left = Side {
            vset: l_vset,
            qids: vset_to_qids(vmap, l_vset)?,
        };
        let right = Side {
            vset: r_vset,
            qids: vset_to_qids(vmap, r_vset)?,
        };
        Ok(Edge {
            kind: JoinKind::Left,
            sides: Sides::Both(left, right),
            cond,
            filt: vec![],
        })
    }

    /// Construct a full join edge.
    ///
    /// Initialize `left` with all tables at left side in origial
    /// plan, initialize `right` with all tables at right side,
    /// leave `arbitrary` as empty.
    /// No tables can be removed from either `left` or `right`.
    ///
    #[inline]
    pub fn new_full(
        vmap: &HashMap<VertexID, QueryID>,
        l_vset: VertexSet,
        r_vset: VertexSet,
        cond: Vec<Expr>,
    ) -> Result<Self> {
        let left = Side {
            vset: l_vset,
            qids: vset_to_qids(vmap, l_vset)?,
        };
        let right = Side {
            vset: r_vset,
            qids: vset_to_qids(vmap, r_vset)?,
        };
        Ok(Edge {
            kind: JoinKind::Full,
            sides: Sides::Both(left, right),
            cond,
            filt: vec![],
        })
    }

    #[inline]
    pub fn push_filt(&mut self, vset: VertexSet, expr: Expr) -> Result<Feedback> {
        match (self.kind, &self.sides) {
            (JoinKind::Left, Sides::Both(left, right)) => {
                match (left.vset.intersects(vset), right.vset.intersects(vset)) {
                    // let it go as no join table involved in the input expression
                    (false, false) => Ok(Feedback::Fallthrough(expr)),
                    (false, true) => {
                        // filter on right table, check if it rejects null
                        if reject_null(&expr, &right.qids)? {
                            Ok(Feedback::Rebuild(Rebuild::Inner, expr))
                        } else {
                            self.filt.push(expr);
                            Ok(Feedback::Accept)
                        }
                    }
                    (true, false) => {
                        // filter on left table, fallthrough
                        Ok(Feedback::Fallthrough(expr))
                    }
                    (true, true) => {
                        // filter on both sides, check if rejects null of right table
                        if reject_null(&expr, &right.qids)? {
                            Ok(Feedback::Rebuild(Rebuild::Inner, expr))
                        } else {
                            self.filt.push(expr);
                            Ok(Feedback::Accept)
                        }
                    }
                }
            }
            (JoinKind::Full, Sides::Both(left, right)) => {
                match (left.vset.intersects(vset), right.vset.intersects(vset)) {
                    (false, false) => Ok(Feedback::Fallthrough(expr)),
                    (false, true) => {
                        if reject_null(&expr, &right.qids)? {
                            Ok(Feedback::Rebuild(Rebuild::Right, expr))
                        } else {
                            self.filt.push(expr);
                            Ok(Feedback::Accept)
                        }
                    }
                    (true, false) => {
                        if reject_null(&expr, &left.qids)? {
                            Ok(Feedback::Rebuild(Rebuild::Left, expr))
                        } else {
                            self.filt.push(expr);
                            Ok(Feedback::Accept)
                        }
                    }
                    (true, true) => {
                        let left_reject_null = reject_null(&expr, &left.qids)?;
                        let right_reject_null = reject_null(&expr, &right.qids)?;
                        match (left_reject_null, right_reject_null) {
                            (false, false) => {
                                self.filt.push(expr);
                                Ok(Feedback::Accept)
                            }
                            (false, true) => Ok(Feedback::Rebuild(Rebuild::Left, expr)),
                            (true, false) => Ok(Feedback::Rebuild(Rebuild::Right, expr)),
                            (true, true) => Ok(Feedback::Rebuild(Rebuild::Inner, expr)),
                        }
                    }
                }
            }
            (JoinKind::Inner, Sides::Arbitrary(arbitrary)) => {
                if arbitrary.vset == vset {
                    // only merge if vset exact matched
                    self.cond.push(expr);
                    Ok(Feedback::Accept)
                } else {
                    Ok(Feedback::Fallthrough(expr))
                }
            }
            _ => todo!(),
        }
    }

    #[inline]
    pub fn union_vset(&self) -> VertexSet {
        match &self.sides {
            Sides::Both(l, r) => l.vset | r.vset,
            Sides::Arbitrary(a) => a.vset,
        }
    }

    #[inline]
    pub fn union_sides(&self) -> (VertexSet, HashSet<QueryID>) {
        match &self.sides {
            Sides::Both(l, r) => (l.vset | r.vset, l.qids.union(&r.qids).cloned().collect()),
            Sides::Arbitrary(a) => (a.vset, a.qids.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Side {
    pub vset: VertexSet,
    pub qids: HashSet<QueryID>,
}

impl Side {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.vset.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sides {
    Both(Side, Side),
    Arbitrary(Side),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Feedback {
    Rebuild(Rebuild, Expr),
    Fallthrough(Expr),
    Accept,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuild {
    Inner,
    Left,
    Right,
}

#[inline]
fn reject_null(expr: &Expr, qry_ids: &HashSet<QueryID>) -> Result<bool> {
    expr.clone()
        .reject_null(|e| {
            if let Expr::Col(Col::QueryCol(qry_id, _)) = e {
                if qry_ids.contains(qry_id) {
                    *e = Expr::const_null();
                }
            }
        })
        .map_err(Into::into)
}

/// Reserved for fine-grained analysis on join conditions in future.
#[allow(dead_code)]
#[inline]
fn all_transposable(qids: &HashSet<QueryID>, e: &Expr) -> bool {
    let trans_map = transposable(e);
    for qid in qids {
        if let Some(trans) = trans_map.get(qid) {
            if !*trans {
                return false;
            }
        }
    }
    true
}

#[allow(dead_code)]
#[inline]
fn transposable(e: &Expr) -> HashMap<QueryID, bool> {
    struct Trans(HashMap<QueryID, bool>);
    impl ExprVisitor for Trans {
        fn leave(&mut self, e: &Expr) -> bool {
            match e {
                Expr::Col(Col::QueryCol(qry_id, _)) => {
                    self.0.entry(*qry_id).or_insert(true);
                }
                Expr::Func(Func {
                    kind: FuncKind::Add | FuncKind::Sub | FuncKind::Neg,
                    ..
                })
                | Expr::Pred(Pred::Func(PredFunc {
                    kind: PredFuncKind::Equal,
                    ..
                }))
                | Expr::Const(_) => {
                    // allow constants and Add, Sub, Neg, Equal to be transposable
                }
                _ => {
                    // all others are not allowed, mark all queries existing in the map to false
                    for v in self.0.values_mut() {
                        *v = false;
                    }
                }
            }
            true
        }
    }
    let mut trans = Trans(HashMap::new());
    let _ = e.walk(&mut trans);
    trans.0
}

#[inline]
fn collect_qry_ids(exprs: &[Expr]) -> HashSet<QueryID> {
    let mut c = CollectQryIDs::default();
    for e in exprs {
        let _ = e.walk(&mut c);
    }
    c.res()
}
