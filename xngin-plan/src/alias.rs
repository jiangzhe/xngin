use crate::error::{Error, Result};
use indexmap::{map, IndexMap};
use smol_str::SmolStr;
use std::ops::{Deref, DerefMut};
use xngin_expr::QueryID;

/// PlanAliases stores the aliases of tables.
/// MySQL allows subquery and table has same alias.
/// This has rare use cases, but causes very
/// confusing name resolution.
///
/// For example:
/// SELECT db1.t3.val FROM db1.t1 AS t3, (SELECT * FROM db1.t2) AS t3
/// SELECT     t3.val FROM db1.t1 AS t3, (SELECT * FROM db1.t2) AS t3
/// The two SQL may have different output according to order/priority
/// of column reference resolution.
///
/// I've decided not to keep the compatibility.
/// If user does not use duplicated aliases, it's all fine.
///
/// Let aliases within single query block to be unique.
#[derive(Debug, Clone, Default)]
pub struct QueryAliases(IndexMap<SmolStr, QueryID>);

impl Deref for QueryAliases {
    type Target = IndexMap<SmolStr, QueryID>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryAliases {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl QueryAliases {
    #[inline]
    pub fn new(plans: IndexMap<SmolStr, QueryID>) -> Self {
        QueryAliases(plans)
    }

    #[inline]
    pub fn insert_query(&mut self, name_or_alias: SmolStr, query_id: QueryID) -> Result<()> {
        match self.0.entry(name_or_alias) {
            map::Entry::Vacant(ent) => {
                ent.insert(query_id);
                Ok(())
            }
            map::Entry::Occupied(ent) => Err(Error::NotUniqueAliasOrTable(ent.key().to_string())),
        }
    }
}
