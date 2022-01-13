use crate::alias::QueryAliases;
use crate::expr::Expr;
use crate::id::QueryID;
use indexmap::IndexMap;
use smol_str::SmolStr;
use std::ops::{Deref, DerefMut};

// Scopes is stack-like environment for query blocks.
#[derive(Debug, Default)]
pub struct Scopes(Vec<Scope>);

impl Deref for Scopes {
    type Target = [Scope];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Scopes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Scopes {
    #[inline]
    pub fn curr_scope(&self) -> &Scope {
        self.0.last().unwrap()
    }

    #[inline]
    pub fn curr_scope_mut(&mut self) -> &mut Scope {
        self.0.last_mut().unwrap()
    }

    #[inline]
    pub fn push(&mut self, scope: Scope) {
        self.0.push(scope)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<Scope> {
        self.0.pop()
    }
}

/// Scope represents the namespace of query block.
/// There are two alias tables:
/// 1. table aliases
/// 2. column aliases
#[derive(Debug, Clone, Default)]
pub struct Scope {
    // CTE aliases will only contain row and subquery.
    pub cte_aliases: QueryAliases,
    // FROM aliases will only contain table and subquery.
    pub query_aliases: QueryAliases,
    // output column list
    pub out_cols: Vec<(Expr, SmolStr)>,
}

impl Scope {
    #[inline]
    pub fn position_out_col(&self, alias: &str) -> Option<usize> {
        self.out_cols.iter().position(|(_, a)| a == alias)
    }

    #[inline]
    pub fn find_out_col(&self, alias: &str) -> Option<(Expr, SmolStr)> {
        self.out_cols.iter().find_map(|(e, a)| {
            if a == alias {
                Some((e.clone(), a.clone()))
            } else {
                None
            }
        })
    }

    #[inline]
    pub fn single_from(&self) -> bool {
        self.query_aliases.len() == 1
    }

    #[inline]
    pub fn restrict_from_aliases(&self, aliases: &[SmolStr]) -> QueryAliases {
        let m: IndexMap<SmolStr, QueryID> = aliases
            .iter()
            .filter_map(|a| self.query_aliases.get(a).cloned().map(|p| (a.clone(), p)))
            .collect();
        QueryAliases::new(m)
    }
}
