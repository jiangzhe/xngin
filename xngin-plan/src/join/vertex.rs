use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexID(pub(crate) u32);
