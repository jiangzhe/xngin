use crate::error::{Error, Result};
use crate::join::JoinKind;
use crate::op::Op;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};
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
    pub edges: IndexMap<VertexSet, Vec<Edge>>,
    pub queries: Vec<Op>,
}

impl Graph {
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

#[allow(dead_code)]
#[inline]
pub(crate) fn qid_to_vid(map: &HashMap<QueryID, VertexID>, qid: QueryID) -> Result<VertexID> {
    map.get(&qid).cloned().ok_or(Error::QueryNotFound(qid))
}

#[inline]
pub(crate) fn vid_to_qid(map: &HashMap<VertexID, QueryID>, vid: VertexID) -> Result<QueryID> {
    map.get(&vid).cloned().ok_or(Error::InvalidJoinVertexSet)
}

#[inline]
pub(crate) fn qids_to_vset<'a, I>(map: &HashMap<QueryID, VertexID>, qry_ids: I) -> Result<VertexSet>
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

#[allow(dead_code)]
#[inline]
pub(crate) fn vset_to_qids<C>(map: &HashMap<VertexID, QueryID>, vset: VertexSet) -> Result<C>
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

/// Edge of join graph.
/// This is the "hyperedge" introduced in paper
/// "Dynamic Programming Strikes Back".
/// Field `l_vset` contains all vertexes at left side.
/// Field `r_vset` contains all vertexes at right side.
/// Field `e_vset` is join eligibility set, which contains
/// all required vertexes to perform this join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub kind: JoinKind,
    // All nodes on left side.
    pub l_vset: VertexSet,
    // All nodes on right side.
    pub r_vset: VertexSet,
    // Eligibility set, to validate two table sets can be
    // joined or not.
    pub e_vset: VertexSet,
    // Join conditions that should be evaluated in join.
    // They are conjunctive.
    pub cond: Vec<Expr>,
    // Filt which should be applied after the join.
    // Inner join will always has this field empty because
    // all filters can be evaluated as join condition in join phase.
    // They are conjunctive.
    pub filt: Vec<Expr>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VertexSet {
    // Use bitmap to encode query id as vertexes in the graph
    bits: u32,
}

impl VertexSet {
    #[inline]
    pub fn includes(&self, other: Self) -> bool {
        self.bits & other.bits == other.bits
    }

    #[inline]
    pub fn intersects(&self, other: Self) -> bool {
        self.bits & other.bits != 0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bits.count_ones() as usize
    }

    /// fast path to get vid if set only contains one element
    #[inline]
    pub fn single(&self) -> Option<VertexID> {
        if self.len() == 1 {
            Some(VertexID(self.bits))
        } else {
            None
        }
    }

    #[inline]
    pub fn min(&self) -> Option<VertexID> {
        if self.is_empty() {
            None
        } else {
            Some(VertexID(1u32 << self.bits.trailing_zeros()))
        }
    }
}

impl IntoIterator for VertexSet {
    type Item = VertexID;
    type IntoIter = Iter;
    #[inline]
    fn into_iter(self) -> Iter {
        Iter {
            bits: self.bits,
            value: 1,
            count: self.bits.count_ones(),
        }
    }
}

impl FromIterator<VertexSet> for VertexSet {
    #[inline]
    fn from_iter<T: IntoIterator<Item = VertexSet>>(iter: T) -> Self {
        let mut res = VertexSet::default();
        for it in iter {
            res |= it;
        }
        res
    }
}

impl From<VertexID> for VertexSet {
    #[inline]
    fn from(src: VertexID) -> Self {
        VertexSet { bits: src.0 }
    }
}

pub struct Iter {
    bits: u32,
    value: u32,
    count: u32,
}

impl Iterator for Iter {
    type Item = VertexID;
    #[inline]
    fn next(&mut self) -> Option<VertexID> {
        if self.count == 0 {
            None
        } else {
            loop {
                if self.bits & self.value != 0 {
                    self.count -= 1;
                    let vid = VertexID(self.value);
                    self.value <<= 1;
                    return Some(vid);
                } else {
                    self.value <<= 1;
                }
            }
        }
    }
}

impl BitOrAssign<VertexID> for VertexSet {
    #[inline]
    fn bitor_assign(&mut self, rhs: VertexID) {
        self.bits |= rhs.0;
    }
}

impl BitOrAssign for VertexSet {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.bits |= rhs.bits;
    }
}

impl BitOr for VertexSet {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        VertexSet {
            bits: self.bits | rhs.bits,
        }
    }
}

impl BitAnd for VertexSet {
    type Output = Self;
    #[inline]
    fn bitand(self, rhs: Self) -> Self {
        VertexSet {
            bits: self.bits & rhs.bits,
        }
    }
}

impl BitAndAssign for VertexSet {
    #[inline]
    fn bitand_assign(&mut self, rhs: Self) {
        self.bits &= rhs.bits
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VertexID(pub(crate) u32);
