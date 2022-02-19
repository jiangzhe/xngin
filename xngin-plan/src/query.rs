use crate::error::{Error, Result};
use crate::join::{Join, JoinOp};
use crate::op::{Op, OpKind, OpMutVisitor, OpVisitor};
use crate::scope::Scope;
use fnv::FnvHashMap;
use slab::Slab;
use smol_str::SmolStr;
use std::mem;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::{Col, Expr, QueryID};

/// QueryPlan represents a self-contained query plan with
/// complete information about all its nodes.
pub struct QueryPlan {
    pub queries: QuerySet,
    pub root: QueryID,
}

impl QueryPlan {
    /// Returns the shape of the query plan.
    /// The shape is presented by a sequence of Operators
    /// generated by preorder traversal on current plan.
    /// For example, The plan of "SELECT c0 FROM t1 JOIN t2"
    /// is represented as "[Proj, Join, Proj, Scan, Proj, Scan]"
    #[inline]
    pub fn shape(&self) -> Vec<OpKind> {
        let mut shape = vec![];
        generate_shape(&self.queries, &self.root, &mut shape);
        shape
    }

    #[inline]
    pub fn root_query(&self) -> Option<&Subquery> {
        self.queries.get(&self.root)
    }
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
    // scope contains information incrementally collected during
    // build phase
    pub scope: Scope,
    // location of the subquery
    pub location: Location,
}

impl Subquery {
    /// Construct a subquery using given root operator and scope.
    #[inline]
    pub fn new(root: Op, scope: Scope, location: Location) -> Self {
        Subquery {
            root,
            scope,
            location,
        }
    }

    #[inline]
    pub fn find_table(&self) -> Option<(SchemaID, TableID)> {
        struct FindTable(Option<(SchemaID, TableID)>);
        impl OpVisitor for FindTable {
            fn enter(&mut self, op: &Op) -> bool {
                match op {
                    Op::Table(schema_id, table_id) => {
                        self.0 = Some((*schema_id, *table_id));
                        false
                    }
                    _ => true,
                }
            }
        }
        let mut ft = FindTable(None);
        let _ = self.root.walk(&mut ft);
        ft.0
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Location {
    Intermediate,
    Disk,
    Memory,
    Network,
    // virtual table which is computed directly by expression
    Virtual,
}

/// QuerySet stores all sub-subqeries and provide lookup and update methods.
#[derive(Debug, Default)]
pub struct QuerySet(Slab<Subquery>);

impl QuerySet {
    #[inline]
    pub fn insert(&mut self, query: Subquery) -> QueryID {
        let qry_id = self.0.insert(query);
        QueryID::from(qry_id as u32)
    }

    #[inline]
    pub fn get(&self, qry_id: &QueryID) -> Option<&Subquery> {
        self.0.get(**qry_id as usize)
    }

    #[inline]
    pub fn transform_scope<T, F>(&mut self, qry_id: QueryID, f: F) -> Result<T>
    where
        F: FnOnce(&mut Scope) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            return Ok(f(&mut subq.scope));
        }
        Err(Error::QueryNotFound(qry_id))
    }

    #[inline]
    pub fn transform_subq<T, F>(&mut self, qry_id: QueryID, f: F) -> Result<T>
    where
        F: FnOnce(&mut Subquery) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            return Ok(f(subq));
        }
        Err(Error::QueryNotFound(qry_id))
    }

    #[inline]
    pub fn transform_op<T, F>(&mut self, qry_id: QueryID, f: F) -> Result<T>
    where
        F: FnOnce(&mut QuerySet, Location, &mut Op) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            let location = subq.location;
            let mut root = mem::take(&mut subq.root);
            let res = f(self, location, &mut root);
            // update root back, this always succeed because query set does not allow deletion.
            self.0[*qry_id as usize].root = root;
            return Ok(res);
        }
        Err(Error::QueryNotFound(qry_id))
    }

    #[inline]
    pub fn transform<T, F1, F2>(&mut self, qry_id: QueryID, f1: F1, f2: F2) -> Result<T>
    where
        F1: FnOnce(&mut Subquery),
        F2: FnOnce(&mut QuerySet, &mut Op) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            f1(subq);
            let mut root = mem::take(&mut subq.root);
            let res = f2(self, &mut root);
            // update root back, this always succeed because query set does not allow deletion.
            self.0[*qry_id as usize].root = root;
            return Ok(res);
        }
        Err(Error::QueryNotFound(qry_id))
    }

    /// Deep copy a query given its id.
    /// The logic is to find all subqueries in it,
    /// then recursively copy them one by one, replacing
    /// original id with new generated id.
    /// NOTE: Inner scope contains variables referring changed query ids.
    ///       We should also change them.
    #[inline]
    pub fn copy_query(&mut self, qry_id: &QueryID) -> Result<QueryID> {
        if let Some(sq) = self.get(qry_id) {
            let sq = sq.clone();
            Ok(self.upsert_query(sq))
        } else {
            Err(Error::QueryNotFound(*qry_id))
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
        match jo.as_mut() {
            Op::Query(query_id) => {
                let query = self.qs.get(query_id).cloned().unwrap(); // won't fail
                let new_query_id = self.qs.upsert_query(query);
                self.mapping.insert(*query_id, new_query_id);
                *query_id = new_query_id;
            }
            Op::Join(j) => {
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
        }
    }
}

impl<'a> OpMutVisitor for UpsertQuery<'a> {
    #[inline]
    fn leave(&mut self, op: &mut Op) -> bool {
        match op {
            Op::Query(query_id) => {
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
            },
            _ => (), // others are safe to copy
        }
        true
    }
}

fn generate_shape(qs: &QuerySet, root: &QueryID, shape: &mut Vec<OpKind>) {
    if let Some(subq) = qs.get(root) {
        let mut sg = ShapeGen { qs, shape };
        subq.root.walk(&mut sg);
    }
}

struct ShapeGen<'a> {
    qs: &'a QuerySet,
    shape: &'a mut Vec<OpKind>,
}

impl OpVisitor for ShapeGen<'_> {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        if let Op::Query(query_id) = op {
            generate_shape(self.qs, query_id, self.shape);
        } else {
            self.shape.push(op.kind());
        }
        true
    }
}
