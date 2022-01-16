use crate::error::{Error, Result};
use crate::op::{Join, JoinOp, Op, OpMutVisitor, OpVisitor};
use crate::scope::Scope;
use fnv::FnvHashMap;
use slab::Slab;
use smol_str::SmolStr;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::{Col, Expr, QueryID};

/// QueryPlan represents a self-contained query plan with
/// complete information about all its nodes.
pub struct QueryPlan {
    pub queries: QuerySet,
    pub root: QueryID,
}

/// Query wraps logical operator with additional syntax information.
/// group operators as a tree, with output column list.
/// it's equivalent to a simple SELECT statement, like below:
///
/// ```sql
/// SELECT ...
/// FROM ...
/// WHERE ...
/// GROUP BY ... HAVING ...
/// ORDER BY ... LIMIT ...
/// ```
/// The operator tree will be like(from top to bottom):
/// Limit -> Sort -> Proj -> Filt -> Aggr -> Filt -> Table/Join
#[derive(Debug, Clone)]
pub struct Subquery {
    // root operator
    pub root: Op,
    // correlated or not
    pub correlated: bool,
    // scope contains information incrementally collected during
    // build phase
    pub scope: Scope,
}

impl Subquery {
    /// Construct a subquery using given root operator and scope.
    #[inline]
    pub fn new(root: Op, correlated: bool, scope: Scope) -> Self {
        Subquery {
            root,
            correlated,
            scope,
        }
    }

    #[inline]
    pub fn proj_table(&self) -> Option<(SchemaID, TableID)> {
        match &self.root {
            Op::Table(schema_id, table_id) => Some((*schema_id, *table_id)),
            Op::Proj(proj) => match proj.source.as_ref() {
                Op::Table(schema_id, table_id) => Some((*schema_id, *table_id)),
                _ => None,
            },
            _ => None,
        }
    }

    // This method will search down the operator tree and find first Aggr or Proj operator.
    // Use its columns as out cols.
    #[inline]
    pub fn reset_out_cols(&mut self) {
        struct CollectOutCols<'a>(&'a mut Vec<(Expr, SmolStr)>);
        impl OpVisitor for CollectOutCols<'_> {
            fn enter(&mut self, op: &Op) -> bool {
                match op {
                    Op::Aggr(aggr) => {
                        for p in &aggr.proj {
                            self.0.push(p.clone())
                        }
                        false
                    }
                    Op::Proj(proj) => {
                        for p in &proj.cols {
                            self.0.push(p.clone())
                        }
                        false
                    }
                    Op::Row(row) => {
                        for c in row {
                            self.0.push(c.clone())
                        }
                        false
                    }
                    _ => true,
                }
            }
        }
        let out_cols = &mut self.scope.out_cols;
        out_cols.clear();
        let _ = self.root.walk(&mut CollectOutCols(out_cols));
    }
}

/// QuerySet stores all sub-subqeries and provide lookup and update methods.
#[derive(Debug, Default)]
pub struct QuerySet(Slab<Subquery>);

impl QuerySet {
    #[inline]
    pub fn insert(&mut self, query: Subquery) -> QueryID {
        let query_id = self.0.insert(query);
        QueryID::from(query_id as u32)
    }

    #[inline]
    pub fn get(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.0.get(**query_id as usize)
    }

    #[inline]
    pub fn get_mut(&mut self, query_id: &QueryID) -> Option<&mut Subquery> {
        self.0.get_mut(**query_id as usize)
    }

    /// Deep copy a query given its id.
    /// The logic is to find all subqueries in it,
    /// then recursively copy them one by one, replacing
    /// original id with new generated id.
    /// NOTE: Inner scope contains variables referring changed query ids.
    ///       We should also change them.
    #[inline]
    pub fn copy_query(&mut self, query_id: &QueryID) -> Result<QueryID> {
        if let Some(sq) = self.get(query_id) {
            let sq = sq.clone();
            Ok(self.upsert_query(sq))
        } else {
            Err(Error::InternalError("Query not found".to_string()))
        }
    }

    #[inline]
    fn upsert_query(&mut self, mut sq: Subquery) -> QueryID {
        let mut mapping = FnvHashMap::default();
        let mut upsert = UpsertQuery {
            qs: self,
            mapping: &mut mapping,
        };
        sq.root.walk_mut(&mut upsert);
        // update from aliases in subquery's scope
        for (_, query_id) in sq.scope.query_aliases.iter_mut() {
            if let Some(new_query_id) = mapping.get(query_id) {
                *query_id = *new_query_id;
            }
        }
        // update out_cols in subquery's scope
        for (e, _) in sq.scope.out_cols.iter_mut() {
            if let Expr::Col(Col::QueryCol(query_id, _)) = e {
                if let Some(new_query_id) = mapping.get(query_id) {
                    *query_id = *new_query_id;
                }
            }
        }
        self.insert(sq)
    }
}

struct UpsertQuery<'a> {
    qs: &'a mut QuerySet,
    mapping: &'a mut FnvHashMap<QueryID, QueryID>,
}

impl UpsertQuery<'_> {
    #[inline]
    fn modify_join_op(&mut self, jo: &mut JoinOp) {
        match jo {
            JoinOp::Subquery(query_id) => {
                let query = self.qs.get(query_id).cloned().unwrap(); // won't fail
                let new_query_id = self.qs.upsert_query(query);
                self.mapping.insert(*query_id, new_query_id);
                *query_id = new_query_id;
            }
            JoinOp::Join(j) => {
                self.modify_join(j);
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    fn modify_join(&mut self, j: &mut Join) {
        match j {
            Join::Cross(cj) => {
                for jo in cj {
                    self.modify_join_op(jo)
                }
            }
            Join::Qualified(qj) => {
                self.modify_join_op(&mut qj.left);
                self.modify_join_op(&mut qj.right);
            }
            Join::Dependent(dj) => {
                self.modify_join_op(&mut dj.left);
                self.modify_join_op(&mut dj.right);
            }
        }
    }
}

impl<'a> OpMutVisitor for UpsertQuery<'a> {
    fn enter(&mut self, _op: &mut Op) -> bool {
        true
    }

    fn leave(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Subquery(query_id) => {
                // only perform additional copy to subquery
                let query = self.qs.get(query_id).cloned().unwrap(); // won't fail
                let new_query_id = self.qs.upsert_query(query);
                self.mapping.insert(*query_id, new_query_id);
                *query_id = new_query_id;
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(cj) => {
                    for jo in cj {
                        self.modify_join_op(jo)
                    }
                }
                Join::Qualified(qj) => {
                    self.modify_join_op(&mut qj.left);
                    self.modify_join_op(&mut qj.right);
                }
                Join::Dependent(dj) => {
                    self.modify_join_op(&mut dj.left);
                    self.modify_join_op(&mut dj.right);
                }
            },
            _ => (), // others are safe to copy
        }
        true
    }
}
