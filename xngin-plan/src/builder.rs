#[cfg(test)]
pub(crate) mod tests;

use crate::alias::QueryAliases;
use crate::error::{Error, Result, ToResult};
use crate::join::{Join, JoinKind, JoinOp};
use crate::op::{Op, OpMutVisitor, SortItem};
use crate::query::{Location, QueryPlan, QuerySet, Subquery};
use crate::resolv::{ExprResolve, PlaceholderCollector, PlaceholderQuery, Resolution};
use crate::rule::expr_simplify::simplify_nested;
use crate::scope::{Scope, Scopes};
use crate::setop::{SetopKind, SubqOp};
use smol_str::SmolStr;
use std::sync::Arc;
use xngin_catalog::{QueryCatalog, SchemaID, TableID};
use xngin_expr::controlflow::ControlFlow;
use xngin_expr::{self as expr, Col, ExprMutVisitor, Plhd, PredFuncKind, QueryID, Setq, SubqKind};
use xngin_frontend::ast::*;

pub struct PlanBuilder {
    catalog: Arc<dyn QueryCatalog>,
    default_schema: SchemaID,
    qs: QuerySet,
    scopes: Scopes,
}

impl PlanBuilder {
    #[inline]
    pub fn new(catalog: Arc<dyn QueryCatalog>, default_schema: &str) -> Result<Self> {
        let qs = QuerySet::default();
        let default_schema = if let Some(s) = catalog.find_schema_by_name(default_schema) {
            s.id
        } else {
            return Err(Error::SchemaNotExists(default_schema.to_string()));
        };
        Ok(PlanBuilder {
            catalog,
            default_schema,
            qs,
            scopes: Scopes::default(),
        })
    }

    #[inline]
    pub fn build_plan(mut self, QueryExpr { with, query }: &QueryExpr<'_>) -> Result<QueryPlan> {
        let (root, _) = self.build_subquery(with, query, false)?;
        Ok(QueryPlan {
            qry_set: self.qs,
            root,
        })
    }

    #[inline]
    fn build_subquery<'a>(
        &mut self,
        with: &'a Option<With<'a>>,
        query: &'a Query<'a>,
        transitive: bool,
    ) -> Result<(QueryID, bool)> {
        // create new scope to collect aliases.
        self.scopes.push(Scope::new(transitive));
        if let Some(with) = with {
            // with clause is within current scope
            self.setup_with(with, transitive)?
        }
        let mut phc = PlaceholderCollector::new(transitive);
        let (mut root, location) = self.setup_query(query, &mut phc)?;
        // first setup subqueries
        for PlaceholderQuery { uid, kind, qry, .. } in phc.subqueries {
            let QueryExpr { with, query } = qry;
            let (query_id, _) = self.build_subquery(with, query, true)?;
            let _ = root.walk_mut(&mut ReplaceSubq {
                uid,
                kind,
                query_id,
                updated: false,
            });
        }
        // then resolve correlated columns against outer scopes.
        let mut scope = self.scopes.pop().unwrap(); // won't fail
        let cor_cols = self.setup_correlated_cols(&mut root, phc.idents)?;
        // We can identify whether current subquery is correlated by check the size of cor_cols
        let correlated = !cor_cols.is_empty();
        scope.cor_cols = cor_cols;
        let subquery = Subquery::new(root, scope, location);
        let query_id = self.qs.insert(subquery);
        Ok((query_id, correlated))
    }

    /// Setup correlated columns.
    /// This method is only used in correlated subquery context, in which
    /// the column reference can not be mapped to any columns in the context.
    /// Then traverse outer context and match through all query sources.
    /// Returns true if correlated column idendified.
    #[inline]
    fn setup_correlated_cols(
        &mut self,
        root: &mut Op,
        idents: Vec<(u32, Vec<SmolStr>, &'static str)>,
    ) -> Result<Vec<expr::Expr>> {
        if idents.is_empty() {
            return Ok(vec![]);
        }
        let mut ccols = Vec::with_capacity(idents.len());
        for (uid, ident, location) in &idents {
            let pair = match &ident[..] {
                [schema_name, tbl_alias, col_alias] => self.find_correlated_schema_tbl_col(
                    schema_name,
                    tbl_alias,
                    col_alias,
                    location,
                )?,
                [tbl_alias, col_alias] => {
                    self.find_correlated_tbl_col(tbl_alias, col_alias, location)?
                }
                [col_alias] => self.find_correlated_col(col_alias, location)?,
                _ => return Err(Error::unknown_column_idents(ident, location)),
            };
            ccols.push(pair.clone());
            // Replace placeholder with found correlated column
            let _ = root.walk_mut(&mut ReplaceCorrelatedCol(*uid, pair));
        }
        Ok(ccols)
    }

    #[inline]
    fn find_correlated_schema_tbl_col(
        &mut self,
        schema_name: &str,
        tbl_alias: &str,
        col_alias: &str,
        location: &'static str,
    ) -> Result<expr::Expr> {
        let schema = self.catalog.find_schema_by_name(schema_name).must_ok()?;
        for s in self.scopes.iter_mut().rev() {
            for (from_alias, query_id) in s.query_aliases.iter() {
                if from_alias == tbl_alias {
                    // found table by alias
                    let subquery = self.qs.get(query_id).must_ok()?;
                    let (schema_id, _) = subquery.find_table().ok_or_else(|| {
                        Error::unknown_column_full_name(schema_name, tbl_alias, col_alias, location)
                    })?;
                    if schema_id != schema.id {
                        return Err(Error::unknown_column_full_name(
                            schema_name,
                            tbl_alias,
                            col_alias,
                            location,
                        ));
                    }
                    let idx = subquery.position_out_col(col_alias).ok_or_else(|| {
                        Error::unknown_column_full_name(schema_name, tbl_alias, col_alias, location)
                    })?;
                    // mark cor_vars in the matched scope, so column pruning will also take them
                    // into consideration.
                    s.cor_vars.insert((*query_id, idx as u32));
                    return Ok(expr::Expr::correlated_col(*query_id, idx as u32));
                }
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_full_name(
            schema_name,
            tbl_alias,
            col_alias,
            location,
        ))
    }

    #[inline]
    fn find_correlated_tbl_col(
        &mut self,
        tbl_alias: &str,
        col_alias: &str,
        location: &'static str,
    ) -> Result<expr::Expr> {
        for s in self.scopes.iter_mut().rev() {
            for (from_alias, query_id) in s.query_aliases.iter() {
                if from_alias == tbl_alias {
                    // found table by alias
                    let subquery = self.qs.get(query_id).must_ok()?;
                    let idx = subquery.position_out_col(col_alias).ok_or_else(|| {
                        Error::unknown_column_partial_name(tbl_alias, col_alias, location)
                    })?;
                    // mark cor_vars in the matched scope, so column pruning will also take them
                    // into consideration.
                    s.cor_vars.insert((*query_id, idx as u32));
                    return Ok(expr::Expr::correlated_col(*query_id, idx as u32));
                }
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_partial_name(
            tbl_alias, col_alias, location,
        ))
    }

    #[inline]
    fn find_correlated_col(
        &mut self,
        col_alias: &str,
        location: &'static str,
    ) -> Result<expr::Expr> {
        for s in self.scopes.iter_mut().rev() {
            let mut matched = None;
            for (_, query_id) in s.query_aliases.iter() {
                let subquery = self.qs.get(query_id).must_ok()?;
                if let Some(idx) = subquery.position_out_col(col_alias) {
                    if matched.is_some() {
                        return Err(Error::DuplicatedColumnAlias(col_alias.to_string()));
                    }
                    let ccol = expr::Expr::correlated_col(*query_id, idx as u32);
                    let cvar = (*query_id, idx as u32);
                    matched = Some((ccol, cvar))
                }
            }
            if let Some((c, v)) = matched {
                // mark cor_vars in the matched scope, so column pruning will also take them
                // into consideration.
                s.cor_vars.insert(v);
                return Ok(c);
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_name(col_alias, location))
    }

    /// Setup with clause.
    ///
    /// Recursive CTE is not supported.
    /// All CTEs are stored as normal queries.
    ///
    /// if correlated is set to true, column can refer to sources in outer scopes.
    #[inline]
    fn setup_with<'a>(&mut self, with: &With<'a>, allow_unknown_ident: bool) -> Result<()> {
        if with.recursive {
            return Err(Error::UnsupportedSqlSyntax("Recursive CTE".to_string()));
        }
        for elem in &with.elements {
            let alias = elem.name.to_lower();
            // check first no duplicated table alias within current scope.
            if self.scopes.curr_scope().cte_aliases.contains_key(&alias) {
                return Err(Error::DuplicatedTableAlias(alias));
            }
            // For each CTE declaration, we inherit the placeholder strategy and process separately.
            // todo: handle correlated CTE
            let (query_id, _) = {
                let QueryExpr { with, query } = &elem.query_expr;
                self.build_subquery(with, query, allow_unknown_ident)?
            };
            if !elem.cols.is_empty() {
                // as output columns aliases are explicit specified,
                // we need to update the output alias list of generated tree.
                self.qs.transform_subq(query_id, |subq| {
                    let out_cols = subq.out_cols_mut();
                    if elem.cols.len() != out_cols.len() {
                        return Err(Error::ColumnCountMismatch);
                    }
                    for ((_, alias), new) in out_cols.iter_mut().zip(elem.cols.iter()) {
                        *alias = new.to_lower();
                    }
                    Ok(())
                })??
            }
            // add to CTE aliases of current scope, with duplication check
            self.scopes
                .curr_scope_mut()
                .cte_aliases
                .insert_query(alias, query_id)?;
        }
        Ok(())
    }

    #[inline]
    fn setup_query<'a>(
        &mut self,
        query: &'a Query<'a>,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<(Op, Location)> {
        match query {
            Query::Row(row) => {
                // todo: support correlated row
                let mut cols = Vec::with_capacity(row.len());
                let resolver = ResolveNone;
                for c in row {
                    match c {
                        DerivedCol::Asterisk(_) => {
                            return Err(Error::InvalidSingleRowData(
                                "Asterisk not allowed".to_string(),
                            ))
                        }
                        DerivedCol::Expr(expr, alias) => {
                            let e = resolver.resolve_expr(expr, "field list", phc, false)?;
                            cols.push((e.clone(), alias.to_lower()));
                            // alias conflict is detected when referring, here no need to check duplication
                        }
                    }
                }
                Ok((Op::Row(cols), Location::Virtual))
            }
            Query::Table(select_table) => self.setup_select_table(select_table, phc),
            Query::Set(select_set) => self.setup_select_set(select_set, phc),
        }
    }

    /// Build operator tree from a comment SELECT statement, includes following steps:
    /// 1. translate FROM clause
    /// 2. translate SELECT clause
    /// 3. translate WHERE clause
    /// 4. translate GROUP BY clause
    /// 5. translate HAVING clause
    /// 6. translate ORDER BY clause
    /// 7. translate LIMIT clause
    /// The final operator tree will conditinally reorder and merge them.
    #[inline]
    fn setup_select_table<'a>(
        &mut self,
        select_table: &'a SelectTable<'a>,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<(Op, Location)> {
        // 1. translate FROM clause
        let from = self.setup_table_refs(&select_table.from, phc)?;
        // After analyzing from clause, we have from aliases populated, then we need to make a copy
        // and pass it to other clauses, due to the limitation of borrowing on structual data.
        // We have to make sure the copy in sync with the source.
        let query_aliases = &self.scopes.curr_scope().query_aliases;
        // 2. translate SELECT clause
        let (proj_cols, distinct) = {
            let resolver = ResolveProjOrFilt {
                catalog: self.catalog.as_ref(),
                qs: &self.qs,
                query_aliases,
            };
            let proj_cols =
                self.setup_proj_cols(&resolver, &select_table.cols, "field list", phc)?;
            (proj_cols, select_table.q == SetQuantifier::Distinct)
        };
        // 3. translate WHERE clause
        let filter = if let Some(filter) = &select_table.filter {
            let resolver = ResolveProjOrFilt {
                catalog: self.catalog.as_ref(),
                qs: &self.qs,
                query_aliases,
            };
            let pred = resolver.resolve_expr(filter, "where clause", phc, false)?;
            Some(pred)
        } else {
            None
        };
        // 4. translate GROUP BY clause
        let (mut groups, scalar_aggr) = if !select_table.group_by.is_empty() {
            let resolver = ResolveGroup {
                catalog: self.catalog.as_ref(),
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
            };
            let gs = self.setup_group(&resolver, &select_table.group_by, phc)?;
            let scalar_aggr = validate_proj_aggr(&proj_cols, &gs)?;
            (gs, scalar_aggr)
        } else {
            (vec![], validate_proj_aggr(&proj_cols, &[])?)
        };
        if groups.is_empty() && !scalar_aggr && distinct {
            // convert DISTINCT to GROUP BY
            groups.extend(proj_cols.iter().map(|(e, _)| e.clone()));
        }
        // 5. translate HAVING clause
        let having = if let Some(having) = &select_table.having {
            let group_cols = if groups.is_empty() {
                None
            } else {
                Some(groups.as_slice())
            };
            let resolver = ResolveHavingOrOrder {
                catalog: self.catalog.as_ref(),
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
                group_cols,
            };
            let having = resolver.resolve_expr(having, "having clause", phc, false)?;
            validate_having(&proj_cols, &groups, scalar_aggr, &having)?;
            Some(having)
        } else {
            None
        };
        // 6. translate ORDER BY clause
        let order = if !select_table.order_by.is_empty() {
            let resolver = ResolveHavingOrOrder {
                catalog: self.catalog.as_ref(),
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
                group_cols: None,
            };
            let order = self.setup_order(&resolver, &select_table.order_by, phc)?;
            validate_order(&groups, scalar_aggr, &order)?;
            order
        } else {
            vec![]
        };
        // 7. translate LIMIT clause
        let limit = if let Some(limit) = &select_table.limit {
            // pushdown to sort if possible
            let start = limit.offset.unwrap_or(0);
            let end = start + limit.limit;
            Some((start, end))
        } else {
            None
        };
        // build the whole operator tree
        // a) build root operator
        let mut root = if from.len() == 1 {
            from.into_iter().next().unwrap().into()
        } else {
            Op::cross_join(from)
        };
        // b) build Filt operator
        if let Some(pred) = filter {
            root = Op::filt(pred.into_conj(), root);
        }
        // c) build Aggr operator
        if !groups.is_empty() || scalar_aggr {
            root = Op::aggr(groups, root);
        }
        // d) build Proj operator or merge into Aggr
        match &mut root {
            Op::Aggr(aggr) => aggr.proj = proj_cols,
            _ => {
                root = Op::proj(proj_cols, root);
            }
        }
        // e) build Filter operator from HAVING clause
        if let Some(pred) = having {
            root = Op::filt(pred.into_conj(), root);
        }
        // f) build Sort operator
        if !order.is_empty() {
            // scalar aggr query only returns 1 row, ingore SORT operation
            root = Op::sort(order, root);
        }
        // g) build Limit operator
        if let Some((start, end)) = limit {
            root = Op::limit(start, end, root);
        }
        Ok((root, Location::Intermediate))
    }

    #[inline]
    fn setup_select_set<'a>(
        &mut self,
        select_set: &'a SelectSet<'a>,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<(Op, Location)> {
        let kind = match select_set.op {
            SetOp::Union => SetopKind::Union,
            SetOp::Except => SetopKind::Except,
            SetOp::Intersect => SetopKind::Intersect,
        };
        let q = if select_set.distinct {
            Setq::Distinct
        } else {
            Setq::All
        };
        // todo: handle correlated subquery
        let (query_id, _) =
            self.build_subquery(&None, &select_set.left, phc.allow_unknown_ident)?;
        let left = SubqOp::query(query_id);
        let (query_id, _) =
            self.build_subquery(&None, &select_set.right, phc.allow_unknown_ident)?;
        let right = SubqOp::query(query_id);
        Ok((Op::setop(kind, q, left, right), Location::Intermediate))
    }

    /// process ORDER BY clause and generate sort items.
    /// ORDER BY is different from WHERE or GROUP BY,
    /// as it can refer to aliases in SELECT clause.
    /// e.g. "SELECT c0 as c1, c1 as c0 FROM t1 ORDER BY c0"
    /// The order item is actually the column `c1` in table `t1`,
    /// because the projection assigns the alias `c0` to it and
    /// ORDER BY pick it as higher priority to column names in
    /// original table.
    #[inline]
    fn setup_order<'a>(
        &self,
        resolver: &dyn ExprResolve,
        elems: &'a [OrderElement<'a>],
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<Vec<SortItem>> {
        let mut res = Vec::with_capacity(elems.len());
        for elem in elems {
            let expr = resolver.resolve_expr(&elem.expr, "order by", phc, false)?;
            let item = SortItem {
                expr,
                desc: elem.desc,
            };
            res.push(item)
        }
        Ok(res)
    }

    /// Setup columns in projection.
    /// a) Expand asterisk.
    /// b) Collect aggr functions.
    /// c) Collect expression list with aliases.
    ///
    /// To expand asterisk, we need to consider of different scenarios:
    ///
    /// 1. Single source.
    /// a). Table: "SELECT t.* FROM t"
    /// Derive projection by looking up from catalog
    ///
    /// b). Subquery: "SELECT * FROM (SELECT c0 FROM t) t2"
    /// Derive projection from output list.
    ///
    /// 2. query from multiple sources, including explicit joins.
    /// e.g.
    /// "SELECT * FROM t0, t1"
    /// "SELECT t0.* FROM t0 JOIN (SELECT * FROM t1) t2"
    /// "SELECT * FROM t0, t0 as t1 WHERE t0.c0 = t1.c1"
    /// In this case, we need to
    /// a) Convert table to subquery, then derive projection from it.
    /// b) For each subquery, derive its output.
    /// NOTE: The order of output columns is important, because later we will
    ///       reorder the sources(join reorder).
    #[inline]
    fn setup_proj_cols<'a>(
        &self,
        resolver: &dyn ExprResolve,
        cols: &'a [DerivedCol<'a>],
        location: &'static str,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<Vec<(expr::Expr, SmolStr)>> {
        let mut proj_cols = vec![];
        let mut wildcard = false; // multiple unqualified asterisk are not allowed in projection.
        for dc in cols {
            match dc {
                DerivedCol::Asterisk(q) => {
                    if q.is_empty() {
                        // wildcard without qualifier can only appear once.
                        if wildcard {
                            return Err(Error::DulicatedAsterisksInFieldList);
                        } else {
                            wildcard = true;
                        }
                    }
                    let es = resolver.resolve_asterisk(q, location)?;
                    proj_cols.extend(es);
                }
                DerivedCol::Expr(e, alias) => {
                    let e = resolver.resolve_expr(e, location, phc, false)?;
                    proj_cols.push((e, alias.to_lower()))
                }
            }
        }
        Ok(proj_cols)
    }

    /// Process table references in FROM clauase.
    /// All table refs are initialized as sub-trees in cross join, and will be
    /// optimized to qualified joins in canonical rewrite phase.
    /// As MySQL does not support correlated derived table, we won't handle it.
    ///
    /// Example of correlated derived table:
    ///
    /// ```sql
    /// SELECT t0.c0, t1.c1
    /// FROM
    ///     t0,
    ///     (SELECT c1 FROM t1 WHERE t0.c0 = t1.c0) derived
    /// ```
    #[inline]
    fn setup_table_refs<'a>(
        &mut self,
        table_refs: &'a [TableRef<'a>],
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<Vec<JoinOp>> {
        let mut jos = vec![];
        for tr in table_refs {
            let (jo, _) = self.setup_table_ref(tr, phc)?;
            jos.push(jo)
        }
        Ok(jos)
    }

    /// Setups each table reference and returns an operator.
    /// The operator returned should be either plan or join.
    fn setup_table_ref<'a>(
        &mut self,
        table_ref: &'a TableRef<'a>,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<(JoinOp, Vec<SmolStr>)> {
        let (plan, aliases) = match table_ref {
            TableRef::Primitive(tp) => self.setup_table_primitive(tp, phc)?,
            TableRef::Joined(tj) => match tj.as_ref() {
                TableJoin::Cross(cj) => {
                    let (left, mut aliases) = self.setup_table_ref(&cj.left, phc)?;
                    let (right, right_aliases) = self.setup_table_primitive(&cj.right, phc)?;
                    aliases.extend(right_aliases);
                    (JoinOp::cross(vec![left, right]), aliases)
                }
                TableJoin::Natural(_) => {
                    // Currently natural join not supported.
                    // In order to support natural join,
                    // we need to do following:
                    // 1. validate join condition.
                    // a) reject if join key is not unique.
                    // b) convert to cross join if no identical column
                    //    found in both side.
                    // c) otherwise, convert to inner join.
                    // 2. expand non-qualified asterisk for natural join
                    //    because natural join eliminate identical column
                    //    and change the output sequence of original columns.
                    return Err(Error::InternalError(
                        "natural join not supported".to_string(),
                    ));
                }
                TableJoin::Qualified(qj) => {
                    let (left, mut left_aliases) = self.setup_table_ref(&qj.left, phc)?;
                    let (right, right_aliases) = self.setup_table_primitive(&qj.right, phc)?;
                    let (cond, aliases) = if let Some(cond) = &qj.cond {
                        match cond {
                            JoinCondition::NamedCols(ncs) => {
                                let mut left_queries = Vec::with_capacity(left_aliases.len());
                                for qry_alias in &left_aliases {
                                    let query_id = self
                                        .scopes
                                        .curr_scope()
                                        .query_aliases
                                        .get(qry_alias)
                                        .must_ok()?;
                                    let subq = self.qs.get(query_id).must_ok()?;
                                    left_queries.push((*query_id, subq));
                                }
                                let mut right_queries = Vec::with_capacity(right_aliases.len());
                                for qry_alias in &right_aliases {
                                    let query_id = self
                                        .scopes
                                        .curr_scope()
                                        .query_aliases
                                        .get(qry_alias)
                                        .must_ok()?;
                                    let subq = self.qs.get(query_id).must_ok()?;
                                    right_queries.push((*query_id, subq));
                                }
                                let mut preds = Vec::with_capacity(ncs.len());
                                let mut left_col = None;
                                let mut right_col = None;
                                for nc in ncs {
                                    let col_alias = nc.to_lower();
                                    // find named column from left side
                                    for (query_id, subq) in &left_queries {
                                        if let Some(idx) = subq.position_out_col(&col_alias) {
                                            if left_col.is_some() {
                                                return Err(Error::DuplicatedColumnAlias(
                                                    col_alias.to_string(),
                                                ));
                                            }
                                            left_col =
                                                Some(expr::Expr::query_col(*query_id, idx as u32));
                                        }
                                    }
                                    if left_col.is_none() {
                                        return Err(Error::unknown_column_name(
                                            &col_alias,
                                            "join condition",
                                        ));
                                    }
                                    // find named column from right side
                                    for (query_id, subq) in &right_queries {
                                        if let Some(idx) = subq.position_out_col(&col_alias) {
                                            if right_col.is_some() {
                                                return Err(Error::DuplicatedColumnAlias(
                                                    col_alias.to_string(),
                                                ));
                                            }
                                            right_col =
                                                Some(expr::Expr::query_col(*query_id, idx as u32));
                                        }
                                    }
                                    if right_col.is_none() {
                                        return Err(Error::unknown_column_name(
                                            &col_alias,
                                            "join condition",
                                        ));
                                    }
                                    preds.push(expr::Expr::pred_func(
                                        PredFuncKind::Equal,
                                        vec![left_col.take().unwrap(), right_col.take().unwrap()],
                                    ));
                                }
                                left_aliases.extend(right_aliases);
                                (preds, left_aliases)
                            }
                            JoinCondition::Conds(e) => {
                                left_aliases.extend(right_aliases);
                                let from_aliases = self
                                    .scopes
                                    .curr_scope()
                                    .restrict_from_aliases(&left_aliases);
                                let resolver = ResolveProjOrFilt {
                                    catalog: self.catalog.as_ref(),
                                    qs: &self.qs,
                                    query_aliases: &from_aliases,
                                };
                                let pred =
                                    resolver.resolve_expr(e, "join condition", phc, false)?;
                                (pred.into_conj(), left_aliases)
                            }
                        }
                    } else {
                        left_aliases.extend(right_aliases);
                        (vec![], left_aliases)
                    };
                    // join type in JOIN clause only support INNER, LEFT, RIGHT, FULL.
                    let op = match qj.ty {
                        JoinType::Inner => {
                            JoinOp::qualified(JoinKind::Inner, left, right, cond, vec![])
                        }
                        JoinType::Left => {
                            JoinOp::qualified(JoinKind::Left, left, right, cond, vec![])
                        }
                        // It's safe to convert right join to left join because the query aliases stores tables in original sequence,
                        // and the projection list is generated using that sequence.
                        JoinType::Right => {
                            JoinOp::qualified(JoinKind::Left, right, left, cond, vec![])
                        }
                        JoinType::Full => {
                            JoinOp::qualified(JoinKind::Full, left, right, cond, vec![])
                        }
                    };
                    (op, aliases)
                }
            },
        };
        Ok((plan, aliases))
    }

    /// Currently lateral derived table is not supported, so PlaceholderCollector is not used.
    #[inline]
    fn setup_table_primitive<'a>(
        &mut self,
        tp: &TablePrimitive<'a>,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<(JoinOp, Vec<SmolStr>)> {
        let (plan, alias) = match tp {
            TablePrimitive::Named(tn, alias) => {
                let tbl_name = tn.table.to_lower();
                let tbl_alias = alias
                    .map(|a| a.to_lower())
                    .unwrap_or_else(|| tbl_name.clone());
                let query_id = if let Some(schema_name) = &tn.schema {
                    // schema is present, must be a concrete table
                    let schema_name = schema_name.to_lower();
                    let schema_id = if let Some(s) = self.catalog.find_schema_by_name(&schema_name)
                    {
                        s.id
                    } else {
                        return Err(Error::SchemaNotExists(schema_name.to_string()));
                    };
                    let table_id =
                        if let Some(t) = self.catalog.find_table_by_name(&schema_id, &tbl_name) {
                            t.id
                        } else {
                            return Err(Error::TableNotExists(format!(
                                "{}.{}",
                                schema_name, tbl_name
                            )));
                        };
                    // now manually construct a subquery to expose all columns in the table
                    self.table_to_subquery(schema_id, table_id)?
                } else if let Some(query_id) = self.find_cte(&tbl_name) {
                    // schema not present, first check CTEs
                    let query_id = *query_id;
                    // CTE matches:
                    // currently make a new copy.
                    // todo: maybe a share(copy on write) is better for future optimization.
                    self.qs.copy_query(&query_id)?
                } else if let Some(t) = self
                    .catalog
                    .find_table_by_name(&self.default_schema, &tbl_name)
                {
                    self.table_to_subquery(t.schema_id, t.id)?
                } else {
                    return Err(Error::TableNotExists(tbl_name.to_string()));
                };
                // update from aliases
                self.scopes
                    .curr_scope_mut()
                    .query_aliases
                    .insert_query(tbl_alias.clone(), query_id)?;
                (JoinOp::query(query_id), tbl_alias)
            }
            TablePrimitive::Derived(subq, alias) => {
                // Non-lateral derived table can not refer to outer values.
                // But a derived table within subquery could refer to outer values.
                // We need to inherit allow_unknown_ident flag in phc
                let QueryExpr { with, query } = subq.as_ref();
                // todo: handle correlated derived table in subquery
                let (query_id, _) = self.build_subquery(with, query, phc.allow_unknown_ident)?;
                let alias = alias.to_lower();
                self.scopes
                    .curr_scope_mut()
                    .query_aliases
                    .insert_query(alias.clone(), query_id)?;
                (JoinOp::query(query_id), alias)
            }
        };
        Ok((plan, vec![alias]))
    }

    #[inline]
    fn setup_group<'a>(
        &self,
        resolver: &dyn ExprResolve,
        group_by: &'a [Expr<'a>],
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<Vec<expr::Expr>> {
        let mut items = Vec::with_capacity(group_by.len());
        for g in group_by {
            let e = resolver.resolve_expr(g, "group by", phc, false)?;
            if e.contains_aggr_func() {
                return Err(Error::InvalidUsageOfAggrFunc);
            }
            // the alias is temporary, will be updated in select clause
            items.push(e)
        }
        Ok(items)
    }

    #[inline]
    fn table_to_subquery(&mut self, schema_id: SchemaID, table_id: TableID) -> Result<QueryID> {
        let all_cols = self.catalog.all_columns_in_table(&table_id);
        let mut proj_cols = Vec::with_capacity(all_cols.len());
        for c in all_cols {
            proj_cols.push((
                expr::Expr::Col(Col::TableCol(table_id, c.idx as u32)),
                c.name,
            ))
        }
        let proj = Op::proj(proj_cols, Op::Table(schema_id, table_id));
        // todo: currently we assume all tables are located on disk.
        let subquery = Subquery::new(proj, Scope::default(), Location::Disk);
        let query_id = self.qs.insert(subquery);
        Ok(query_id)
    }

    // CTE is like scoped view, we need to search from current scope until top
    #[inline]
    fn find_cte(&self, alias: &str) -> Option<&QueryID> {
        self.scopes
            .iter()
            .rev()
            .find_map(|s| s.cte_aliases.get(alias))
    }
}

/// Validate proj and aggr.
/// The rules are:
/// 1. aggr-funcs are not allowed as group items in aggr.
/// 2. all non-aggr expressions in proj must be group items in aggr.
/// 3. proj can have constant values, can also eliminate group items in aggr.
/// Returns true if the aggregation is scalar aggregation.
#[inline]
fn validate_proj_aggr(
    proj_cols: &[(expr::Expr, SmolStr)],
    aggr_groups: &[expr::Expr],
) -> Result<bool> {
    // Rule 1
    if aggr_groups.iter().any(|e| e.contains_aggr_func()) {
        return Err(Error::AggrFuncInGroupBy);
    }
    // Rule 2
    // We need to identify columns outside aggregate function and group item in projection.
    // e.g. SELECT a, a + sum(1) FROM t GROUP BY a
    // From projection, save first "a" as group item, save second "a" in "a + sum(1)" as
    // column outside aggregate function.
    // Then we require all columns outside aggregate function must be group item.
    // This means following SQL is illegal: SELECT a+1, a+1 + sum(1) FROM t GROUP BY a+1
    // which is logically valid.
    let mut proj_cols_outside_aggr = Vec::new();
    let mut proj_groups = Vec::new();
    let mut proj_has_aggr = false;
    for (e, _) in proj_cols {
        // collect column outside aggr
        let (cols, ag) = e.collect_non_aggr_cols();
        if ag {
            proj_cols_outside_aggr.extend(cols)
        } else {
            // constants can also be present in SELECT list
            let mut new_e = e.clone();
            simplify_nested(&mut new_e)?;
            if !new_e.is_const() {
                proj_groups.push(e);
            }
        }
        proj_has_aggr |= ag;
    }
    if aggr_groups.is_empty() {
        // No GROUP BY specified, so projection can either has no aggr functions,
        // or has all aggr functions with optional constants.
        if !proj_has_aggr {
            return Ok(false);
        }
        if !proj_groups.is_empty() || !proj_cols_outside_aggr.is_empty() {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        // scalar aggregation: no group by, all projection are aggregation function without column reference outside
        return Ok(true);
    }
    // all proj_groups must exist in aggr_groups, constants already excluded
    for pg in &proj_groups {
        if !aggr_groups.contains(pg) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
    }
    // all proj_cols_outside_aggr must exist in aggr_groups
    for pc in proj_cols_outside_aggr {
        if !aggr_groups.contains(&expr::Expr::Col(pc)) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
    }
    Ok(false)
}

/// Validate HAVING.
/// 1. For scalar aggregation, non-aggr columns are disallowed.
/// 2. For other aggregation, all non-aggr columns must exist in aggr groups.
/// 3. For flat projection, aggr functions are disallowed.
#[inline]
fn validate_having(
    proj_cols: &[(expr::Expr, SmolStr)],
    aggr_groups: &[expr::Expr],
    scalar_aggr: bool,
    having: &expr::Expr,
) -> Result<()> {
    if scalar_aggr {
        // disallow non-aggr columns in HAVING
        if having.contains_non_aggr_cols() {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        return Ok(());
    }
    if !aggr_groups.is_empty() {
        // all non-aggr columns must exist in aggr groups
        let (non_aggr_cols, _) = having.collect_non_aggr_cols();
        for c in non_aggr_cols {
            let ce = expr::Expr::Col(c);
            if aggr_groups.iter().all(|e| e != &ce) {
                return Err(Error::FieldsSelectedNotInGroupBy);
            }
        }
        return Ok(());
    }
    // aggr functions are not allowed.
    let (non_aggr_cols, aggr) = having.collect_non_aggr_cols();
    if aggr {
        return Err(Error::FieldsSelectedNotInGroupBy);
    }
    // columns must match projected expressions
    for nac in non_aggr_cols {
        let ce = &expr::Expr::Col(nac);
        if !proj_cols.iter().any(|(e, _)| e == ce) {
            // todo: notify column name
            return Err(Error::UnknownColumn("Unknown column".to_string()));
        }
    }
    Ok(())
}

/// Validate ORDER BY.
/// 1. For scalar aggregation, non-aggr columns are disallowed.
/// 2. For other aggregation, all non-aggr columns must exist in aggr groups.
/// 3. For flat projection, aggr functions are disallowed.
#[inline]
fn validate_order(aggr_groups: &[expr::Expr], scalar_aggr: bool, order: &[SortItem]) -> Result<()> {
    if scalar_aggr {
        // disallow non-aggr columns in ORDER BY
        if order.iter().any(|si| si.expr.contains_non_aggr_cols()) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        return Ok(());
    }
    if !aggr_groups.is_empty() {
        // all non-aggr columns must exist in aggr groups
        let mut non_aggr_cols = vec![];
        for si in order {
            si.expr.collect_non_aggr_cols_into(&mut non_aggr_cols);
        }
        for c in non_aggr_cols {
            let ce = expr::Expr::Col(c);
            if aggr_groups.iter().all(|e| e != &ce) {
                return Err(Error::FieldsSelectedNotInGroupBy);
            }
        }
        return Ok(());
    }
    // aggr functions are not allowed.
    if order.iter().any(|si| si.expr.contains_aggr_func()) {
        return Err(Error::FieldsSelectedNotInGroupBy);
    }
    Ok(())
}

/// no-op resolver for row subquery.
pub struct ResolveNone;

impl ExprResolve for ResolveNone {
    fn catalog(&self) -> Option<&dyn QueryCatalog> {
        None
    }

    fn find_query(&self, _tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        None
    }

    fn get_query(&self, _query_id: &QueryID) -> Option<&Subquery> {
        None
    }

    fn queries(&self) -> Vec<(SmolStr, QueryID)> {
        vec![]
    }
}

/// Resolver for SELECT, WHERE
pub struct ResolveProjOrFilt<'a> {
    catalog: &'a dyn QueryCatalog,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
}

impl<'a> ExprResolve for ResolveProjOrFilt<'a> {
    fn catalog(&self) -> Option<&dyn QueryCatalog> {
        Some(self.catalog)
    }

    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    fn queries(&self) -> Vec<(SmolStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }
}

/// Resolver for GROUP BY
pub struct ResolveGroup<'a> {
    catalog: &'a dyn QueryCatalog,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
    proj_cols: &'a [(expr::Expr, SmolStr)],
}

impl ExprResolve for ResolveGroup<'_> {
    fn catalog(&self) -> Option<&dyn QueryCatalog> {
        Some(self.catalog)
    }

    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    fn queries(&self) -> Vec<(SmolStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }

    /// try to match proj alias first.
    fn find_col(&self, col_alias: SmolStr, _location: &str) -> Result<Resolution> {
        for (e, a) in self.proj_cols {
            if a == &col_alias {
                return Ok(Resolution::Expr(e.clone()));
            }
        }
        self.find_col_by_default(col_alias)
    }
}

/// Resolver for HAVING or ORDER BY
/// The difference between them is HAVING does not care about
/// tables/queries in FROM clause.
pub struct ResolveHavingOrOrder<'a> {
    catalog: &'a dyn QueryCatalog,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
    proj_cols: &'a [(expr::Expr, SmolStr)],
    group_cols: Option<&'a [expr::Expr]>,
}

impl ExprResolve for ResolveHavingOrOrder<'_> {
    fn catalog(&self) -> Option<&dyn QueryCatalog> {
        Some(self.catalog)
    }

    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    fn queries(&self) -> Vec<(SmolStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }

    fn find_col(&self, col_alias: SmolStr, _location: &str) -> Result<Resolution> {
        // proj alias has higher priority
        for (e, a) in self.proj_cols {
            if a == &col_alias {
                return Ok(Resolution::Expr(e.clone()));
            }
        }
        // if we find no match in proj alias,
        // then match alias to outputs of queries, but require matched
        // alias is unique, or it exists in proj alias list as expression.
        let mut matched = vec![];
        for (_, query_id) in self.query_aliases.iter() {
            let subquery = self.qs.get(query_id).must_ok()?;
            if let Some(idx) = subquery.position_out_col(&col_alias) {
                let e = expr::Expr::query_col(*query_id, idx as u32);
                // check existence in proj aliases
                for (pe, _) in self.proj_cols {
                    if pe == &e {
                        return Ok(Resolution::Expr(e));
                    }
                }
                // then try group cols if exists, this is only for HAVING case,
                // which also respect GROUP BY clause for search
                if let Some(group_cols) = self.group_cols {
                    for ge in group_cols {
                        if ge == &e {
                            return Ok(Resolution::Expr(e));
                        }
                    }
                }
                // not exists, save to check uniqueness
                matched.push(e);
            }
        }
        match matched.len() {
            0 => Ok(Resolution::Unknown(vec![col_alias])),
            1 => Ok(Resolution::Expr(matched.pop().unwrap())),
            _ => Err(Error::DuplicatedColumnAlias(col_alias.to_string())),
        }
    }
}

struct ReplaceCorrelatedCol(u32, expr::Expr);

impl OpMutVisitor for ReplaceCorrelatedCol {
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<()> {
        for e in op.exprs_mut() {
            if let expr::Expr::Plhd(Plhd::Ident(uid)) = e {
                if *uid == self.0 {
                    *e = self.1.clone();
                }
            }
        }
        ControlFlow::Continue(())
    }
}

struct ReplaceSubq {
    uid: u32,
    kind: SubqKind,
    query_id: QueryID,
    updated: bool,
}

impl OpMutVisitor for ReplaceSubq {
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<()> {
        match op {
            Op::Proj(proj) => {
                for (e, _) in &mut proj.cols {
                    let _ = e.walk_mut(self);
                    if self.updated {
                        return ControlFlow::Break(());
                    }
                }
            }
            Op::Filt(filt) => {
                for e in &mut filt.pred {
                    let _ = e.walk_mut(self);
                }
                if self.updated {
                    return ControlFlow::Break(());
                }
            }
            Op::Aggr(aggr) => {
                for e in aggr
                    .proj
                    .iter_mut()
                    .map(|(e, _)| e)
                    .chain(aggr.filt.iter_mut())
                {
                    let _ = e.walk_mut(self);
                    if self.updated {
                        return ControlFlow::Break(());
                    }
                }
            }
            Op::Join(join) => {
                if let Join::Qualified(qj) = join.as_mut() {
                    for e in qj.cond.iter_mut().chain(qj.filt.iter_mut()) {
                        let _ = e.walk_mut(self);
                    }
                    if self.updated {
                        return ControlFlow::Break(());
                    }
                }
            }
            _ => (), // do not try others
        }
        ControlFlow::Continue(())
    }
}

impl ExprMutVisitor for ReplaceSubq {
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &mut expr::Expr) -> ControlFlow<()> {
        match e {
            expr::Expr::Plhd(Plhd::Subquery(_, uid)) if *uid == self.uid => {
                *e = expr::Expr::Subq(self.kind, self.query_id);
                return ControlFlow::Break(());
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}
