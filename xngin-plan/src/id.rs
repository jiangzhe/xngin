use std::ops::Deref;

/// QueryID wraps u32 to be the identifier of subqueries in single query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryID(u32);

impl From<u32> for QueryID {
    fn from(src: u32) -> Self {
        debug_assert!(src != !0, "Constructing QueryID from !0 is not allowed");
        QueryID(src)
    }
}

impl Deref for QueryID {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub const INVALID_QUERY_ID: QueryID = QueryID(!0);
