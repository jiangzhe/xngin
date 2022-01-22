use crate::error::{Error, Result, ToResult};
use crate::query::Subquery;
use smol_str::SmolStr;
use std::sync::Arc;
use xngin_catalog::QueryCatalog;
use xngin_datatype::{Date, Decimal, TimeUnit, DEFAULT_DATE_FORMAT};
use xngin_expr::{self as expr, Farg, FuncKind, Pred, PredFuncKind, QueryID, SubqKind};
use xngin_frontend::ast::*;

#[derive(Debug)]
pub enum Resolution {
    Expr(expr::Expr),
    Unknown(Vec<SmolStr>),
}

/// ExprResolve defines interface to resolve expressions in SQL context.
/// Different strategies should be implemented to resolve column references
/// in SELECT, WHERE, GROUP BY, HAVING and ORDER BY.
pub trait ExprResolve {
    /// Returns catalog for metadata lookup.
    /// It should be implemented in all scenarios, except row subquery.
    fn catalog(&self) -> Option<&dyn QueryCatalog>;

    /// Search visible subquery(table treated as subquery) by alias.
    /// It should be implemented in all scenarios, except row subquery.
    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)>;

    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery>;

    /// Returns all visiable subqueries for iterative search by alias.
    /// It should be implemented in all scenarios, except row subquery.
    fn queries(&self) -> Vec<(SmolStr, QueryID)>;

    /// Find column by alias, this may result in a column or a aliased expression,
    /// depends on the context.
    /// Default implementation should be overriden for GROUP BY, HAVING, ORDER BY.
    fn find_col(&self, col_alias: SmolStr, _location: &'static str) -> Result<Resolution> {
        self.find_col_by_default(col_alias)
    }

    /// Find column by partial qualified alias.
    /// Default implementation should be overriden for HAVING, ORDER BY.
    fn find_tbl_col(
        &self,
        tbl_alias: SmolStr,
        col_alias: SmolStr,
        location: &'static str,
    ) -> Result<Resolution> {
        self.find_tbl_col_by_default(tbl_alias, col_alias, location)
    }

    /// Find column by full qualified alias.
    /// Default implementation should be overriden for HAVING, ORDER BY.
    fn find_schema_tbl_col(
        &self,
        schema_name: SmolStr,
        tbl_alias: SmolStr,
        col_alias: SmolStr,
        location: &'static str,
    ) -> Result<Resolution> {
        self.find_schema_tbl_col_by_default(schema_name, tbl_alias, col_alias, location)
    }

    #[inline]
    fn find_col_by_default(&self, col_alias: SmolStr) -> Result<Resolution> {
        let queries = self.queries();
        if queries.is_empty() {
            return Ok(Resolution::Unknown(vec![col_alias]));
        }
        let mut col = None;
        for (_, query_id) in queries {
            let subquery = self.get_query(&query_id).must_ok()?;
            if let Some(idx) = subquery.scope.position_out_col(&col_alias) {
                if col.is_some() {
                    return Err(Error::DuplicatedColumnAlias(col_alias.to_string()));
                }
                col = Some(expr::Expr::query_col(query_id, idx as u32))
            }
        }
        match col {
            Some(e) => Ok(Resolution::Expr(e)),
            None => Ok(Resolution::Unknown(vec![col_alias])),
        }
    }

    #[inline]
    fn find_tbl_col_by_default(
        &self,
        tbl_alias: SmolStr,
        col_alias: SmolStr,
        location: &'static str,
    ) -> Result<Resolution> {
        let (query_id, subquery) = match self.find_query(&tbl_alias) {
            Some(query_id) => query_id,
            None => return Ok(Resolution::Unknown(vec![tbl_alias, col_alias])),
        };
        if let Some(idx) = subquery.scope.position_out_col(&col_alias) {
            Ok(Resolution::Expr(expr::Expr::query_col(
                query_id, idx as u32,
            )))
        } else {
            Err(Error::unknown_column_partial_name(
                &tbl_alias, &col_alias, location,
            ))
        }
    }

    #[inline]
    fn find_schema_tbl_col_by_default(
        &self,
        schema_name: SmolStr,
        tbl_alias: SmolStr,
        col_alias: SmolStr,
        location: &'static str,
    ) -> Result<Resolution> {
        let schema = self
            .catalog()
            .and_then(|cat| cat.find_schema_by_name(&schema_name))
            .ok_or_else(|| {
                Error::unknown_column_full_name(&schema_name, &tbl_alias, &col_alias, location)
            })?;
        let (query_id, subquery) = match self.find_query(&tbl_alias) {
            Some(query_id) => query_id,
            None => return Ok(Resolution::Unknown(vec![schema_name, tbl_alias, col_alias])),
        };
        if let Some((schema_id, _)) = subquery.proj_table() {
            if schema_id != schema.id {
                return Err(Error::unknown_column_full_name(
                    &schema_name,
                    &tbl_alias,
                    &col_alias,
                    location,
                ));
            }
            if let Some(idx) = subquery
                .scope
                .out_cols
                .iter()
                .position(|(_, alias)| alias == &col_alias)
            {
                return Ok(Resolution::Expr(expr::Expr::query_col(
                    query_id, idx as u32,
                )));
            }
        }
        Err(Error::unknown_column_full_name(
            &schema_name,
            &tbl_alias,
            &col_alias,
            location,
        ))
    }

    /// The logical to resolve column reference is as below(differs from MySQL):
    /// a). Full qualified column reference: <db>.<tb_or_alias>.<col>.
    ///     1) <db> will be first checked against catalog, fail if not found.
    ///     2) <tb_or_alias> will be checked against alias list of FROM statement,
    ///        only table name or table alias matches and <db> of that table also matches,
    ///        go to step 3, otherwise fail. That means subquery are not considered
    ///        in full qualified column name.
    ///     3) <col> will be checked against the column list of found table.
    /// b). Partial qualified column reference: <tb_or_alias>.<col>.
    ///     1) <tb_or_alias> will be checked against alias list of FROM statement.
    ///        In our system, no duplicated aliases, so if not found, just fail.
    ///     2) <col> will be checked against the column list of found table or
    ///        output aliased column list of found subquery.
    /// c). Non-qualified column reference: <col>.
    ///     1) <col> will be searched against an optional provided alias list,
    ///        if matches, use it.
    ///        This is specific for ORDER BY clause, which will use aliases in SELECT
    ///        clause if possible.
    ///     2) then search among alias list of FROM statement,
    ///        If no matched column, fail.
    ///        If any duplicates found, fail.
    #[inline]
    fn resolve_col_ref(&self, cr: &[Ident<'_>], location: &'static str) -> Result<Resolution> {
        match cr {
            [schema_name, tbl_alias, col_alias] => {
                // full qualified column reference
                self.find_schema_tbl_col(
                    schema_name.to_lower(),
                    tbl_alias.to_lower(),
                    col_alias.to_lower(),
                    location,
                )
            }
            [tbl_alias, col_alias] => {
                // partial qualified column reference
                self.find_tbl_col(tbl_alias.to_lower(), col_alias.to_lower(), location)
            }
            [col_alias] => {
                // non-qualified column reference
                self.find_col(col_alias.to_lower(), location)
            }
            _ => Err(Error::unknown_column(cr, location)),
        }
    }

    /// Resolve asterisk in SELECT clause.
    #[inline]
    fn resolve_asterisk(
        &self,
        q: &[Ident<'_>],
        location: &'static str,
    ) -> Result<Vec<(expr::Expr, SmolStr)>> {
        let res = match q {
            [schema_name, tbl_alias] => {
                let schema_name = schema_name.to_lower();
                let schema = self
                    .catalog()
                    .and_then(|cat| cat.find_schema_by_name(&schema_name))
                    .ok_or_else(|| Error::unknown_asterisk_column(q, location))?;
                let tbl_alias = tbl_alias.to_lower();
                let (query_id, subquery) = self
                    .find_query(&tbl_alias)
                    .ok_or_else(|| Error::unknown_asterisk_column(q, location))?;
                if let Some((schema_id, _)) = subquery.proj_table() {
                    if schema_id != schema.id {
                        return Err(Error::UnknownTable(format!(
                            "{}.{}",
                            schema_name, tbl_alias
                        )));
                    }
                    // match table to simple projection, use its output list to resolve asterisk
                    let mut res = Vec::with_capacity(subquery.scope.out_cols.len());
                    for (i, (_, alias)) in subquery.scope.out_cols.iter().enumerate() {
                        let e = expr::Expr::query_col(query_id, i as u32);
                        res.push((e, alias.clone()))
                    }
                    res
                } else {
                    return Err(Error::UnknownTable(format!(
                        "{}.{}",
                        schema_name, tbl_alias
                    )));
                }
            }
            [tbl_alias] => {
                let tbl_alias = tbl_alias.to_lower();
                let (query_id, subquery) = self
                    .find_query(&tbl_alias)
                    .ok_or_else(|| Error::unknown_asterisk_column(q, location))?;
                let mut res = Vec::with_capacity(subquery.scope.out_cols.len());
                for (i, (_, alias)) in subquery.scope.out_cols.iter().enumerate() {
                    let col = expr::Expr::query_col(query_id, i as u32);
                    res.push((col, alias.clone()))
                }
                res
            }
            [] => {
                // todo: remove duplicate columns for natural join
                let qrs = self.queries();
                if qrs.is_empty() {
                    return Err(Error::unknown_asterisk_column(q, location));
                }
                let mut res = vec![];
                for (_, query_id) in qrs {
                    let subquery = self.get_query(&query_id).must_ok()?;
                    for (i, (_, alias)) in subquery.scope.out_cols.iter().enumerate() {
                        let col = expr::Expr::query_col(query_id, i as u32);
                        res.push((col, alias.clone()))
                    }
                }
                res
            }
            _ => return Err(Error::unknown_asterisk_column(q, location)),
        };
        Ok(res)
    }

    #[inline]
    fn resolve_expr<'a>(
        &self,
        e: &'a Expr<'a>,
        location: &'static str,
        phc: &mut PlaceholderCollector<'a>,
        within_aggr: bool,
    ) -> Result<expr::Expr> {
        self.resolve_expr_by_default(e, location, phc, within_aggr)
    }

    #[inline]
    fn resolve_expr_by_default<'a>(
        &self,
        e: &'a Expr<'a>,
        location: &'static str,
        phc: &mut PlaceholderCollector<'a>,
        within_aggr: bool,
    ) -> Result<expr::Expr> {
        let res = match e {
            Expr::Literal(lit) => self.resolve_lit(lit)?,
            Expr::ColumnRef(cr) => match self.resolve_col_ref(cr, location)? {
                Resolution::Expr(e) => e,
                Resolution::Unknown(ident) => {
                    if phc.allow_unknown_ident {
                        phc.add_ident(ident, location)
                    } else {
                        return Err(Error::unknown_column_idents(&ident, location));
                    }
                }
            },
            Expr::AggrFunc(af) => {
                if within_aggr {
                    return Err(Error::InvalidUsageOfAggrFunc);
                } else {
                    self.resolve_aggr_func(af, location, phc)?
                }
            }
            Expr::Unary(ue) => match ue.op {
                UnaryOp::Neg => {
                    let e = self.resolve_expr(&ue.arg, location, phc, within_aggr)?;
                    expr::Expr::func(FuncKind::Neg, vec![e])
                }
                UnaryOp::BitInv => {
                    let e = self.resolve_expr(&ue.arg, location, phc, within_aggr)?;
                    expr::Expr::func(FuncKind::BitInv, vec![e])
                }
                UnaryOp::LogicalNot => {
                    let e = self.resolve_expr(&ue.arg, location, phc, within_aggr)?;
                    match e {
                        // handle not(not(e))
                        expr::Expr::Pred(Pred::Not(e)) => *e,
                        // handle not(true)
                        expr::Expr::Pred(Pred::True) => expr::Expr::pred_false(),
                        // handle not(false)
                        expr::Expr::Pred(Pred::False) => expr::Expr::pred_true(),
                        // handle not(exists)
                        expr::Expr::Pred(Pred::Exists(subq)) => {
                            expr::Expr::Pred(Pred::NotExists(subq))
                        }
                        // handle not(notExists)
                        expr::Expr::Pred(Pred::NotExists(subq)) => {
                            expr::Expr::Pred(Pred::Exists(subq))
                        }
                        // handle not(inSubq)
                        expr::Expr::Pred(Pred::InSubquery(lhs, subq)) => {
                            expr::Expr::Pred(Pred::NotInSubquery(lhs, subq))
                        }
                        // handle not(notInSubq)
                        expr::Expr::Pred(Pred::NotInSubquery(lhs, subq)) => {
                            expr::Expr::Pred(Pred::InSubquery(lhs, subq))
                        }
                        _ => expr::Expr::pred_not(e),
                    }
                }
            },
            Expr::Binary(be) => {
                let lhs = self.resolve_expr(&be.lhs, location, phc, within_aggr)?;
                let rhs = self.resolve_expr(&be.rhs, location, phc, within_aggr)?;
                let kind = match be.op {
                    BinaryOp::Add => FuncKind::Add,
                    BinaryOp::Sub => FuncKind::Sub,
                    BinaryOp::Mul => FuncKind::Mul,
                    BinaryOp::Div => FuncKind::Div,
                    BinaryOp::BitAnd => FuncKind::BitAnd,
                    BinaryOp::BitOr => FuncKind::BitOr,
                    BinaryOp::BitXor => FuncKind::BitXor,
                    BinaryOp::BitShl => FuncKind::BitShl,
                    BinaryOp::BitShr => FuncKind::BitShr,
                };
                expr::Expr::func(kind, vec![lhs, rhs])
            }
            Expr::Predicate(pred) => match pred.as_ref() {
                Predicate::Cmp(op, lhs, rhs) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let rhs = self.resolve_expr(rhs, location, phc, within_aggr)?;
                    let kind = match op {
                        CompareOp::Equal => PredFuncKind::Equal,
                        CompareOp::Greater => PredFuncKind::Greater,
                        CompareOp::GreaterEqual => PredFuncKind::GreaterEqual,
                        CompareOp::Less => PredFuncKind::Less,
                        CompareOp::LessEqual => PredFuncKind::LessEqual,
                        CompareOp::NotEqual => PredFuncKind::NotEqual,
                    };
                    expr::Expr::pred_func(kind, vec![lhs, rhs])
                }
                Predicate::QuantCmp(..) => todo!(),
                Predicate::Is(op, arg) => {
                    let arg = self.resolve_expr(arg, location, phc, within_aggr)?;
                    let kind = match op {
                        IsOp::Null => PredFuncKind::IsNull,
                        IsOp::NotNull => PredFuncKind::IsNotNull,
                        IsOp::True => PredFuncKind::IsTrue,
                        IsOp::NotTrue => PredFuncKind::IsNotTrue,
                        IsOp::False => PredFuncKind::IsFalse,
                        IsOp::NotFalse => PredFuncKind::IsNotFalse,
                    };
                    expr::Expr::pred_func(kind, vec![arg])
                }
                Predicate::Match(op, lhs, rhs) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let rhs = self.resolve_expr(rhs, location, phc, within_aggr)?;
                    let kind = match op {
                        MatchOp::SafeEqual => PredFuncKind::SafeEqual,
                        MatchOp::Like => PredFuncKind::Like,
                        MatchOp::NotLike => PredFuncKind::NotLike,
                        MatchOp::Regexp => PredFuncKind::Regexp,
                        MatchOp::NotRegexp => PredFuncKind::NotRegexp,
                    };
                    expr::Expr::pred_func(kind, vec![lhs, rhs])
                }
                Predicate::InValues(lhs, vals) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let mut list = Vec::with_capacity(vals.len() + 1);
                    list.push(lhs);
                    for v in vals {
                        let e = self.resolve_expr(v, location, phc, within_aggr)?;
                        list.push(e);
                    }
                    expr::Expr::pred_func(PredFuncKind::InValues, list)
                }
                Predicate::NotInValues(lhs, vals) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let mut list = Vec::with_capacity(vals.len() + 1);
                    list.push(lhs);
                    for v in vals {
                        let e = self.resolve_expr(v, location, phc, within_aggr)?;
                        list.push(e);
                    }
                    expr::Expr::pred_func(PredFuncKind::NotInValues, list)
                }
                Predicate::InSubquery(lhs, subq) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let ph = phc.add_subquery(SubqKind::In, subq.as_ref(), location);
                    expr::Expr::pred_in_subq(lhs, ph)
                }
                Predicate::NotInSubquery(lhs, subq) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let ph = phc.add_subquery(SubqKind::In, subq.as_ref(), location);
                    expr::Expr::pred_not_in_subq(lhs, ph)
                }
                Predicate::Between(lhs, mhs, rhs) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let mhs = self.resolve_expr(mhs, location, phc, within_aggr)?;
                    let rhs = self.resolve_expr(rhs, location, phc, within_aggr)?;
                    expr::Expr::pred_func(PredFuncKind::Between, vec![lhs, mhs, rhs])
                }
                Predicate::NotBetween(lhs, mhs, rhs) => {
                    let lhs = self.resolve_expr(lhs, location, phc, within_aggr)?;
                    let mhs = self.resolve_expr(mhs, location, phc, within_aggr)?;
                    let rhs = self.resolve_expr(rhs, location, phc, within_aggr)?;
                    expr::Expr::pred_func(PredFuncKind::NotBetween, vec![lhs, mhs, rhs])
                }
                Predicate::Exists(subq) => {
                    let ph = phc.add_subquery(SubqKind::Exists, subq.as_ref(), location);
                    expr::Expr::pred_exists(ph)
                }
                Predicate::Conj(cs) => {
                    let mut list = Vec::with_capacity(cs.len());
                    for c in cs {
                        let e = self.resolve_expr(c, location, phc, within_aggr)?;
                        list.push(e);
                    }
                    expr::Expr::pred(Pred::Conj(list))
                }
                Predicate::Disj(ds) => {
                    let mut list = Vec::with_capacity(ds.len());
                    for d in ds {
                        let e = self.resolve_expr(d, location, phc, within_aggr)?;
                        list.push(e);
                    }
                    expr::Expr::pred(Pred::Disj(list))
                }
                Predicate::LogicalXor(xs) => {
                    let mut list = Vec::with_capacity(xs.len());
                    for x in xs {
                        let e = self.resolve_expr(x, location, phc, within_aggr)?;
                        list.push(e);
                    }
                    expr::Expr::pred(Pred::Xor(list))
                }
            },
            Expr::ScalarSubquery(subq) => {
                phc.add_subquery(expr::SubqKind::Scalar, subq.as_ref(), location)
            }
            Expr::Builtin(bi) => match bi {
                Builtin::Extract(unit, arg) => {
                    let unit = time_unit_from_ast(*unit);
                    let e = self.resolve_expr(arg, location, phc, within_aggr)?;
                    expr::Expr::func(
                        expr::FuncKind::Extract,
                        vec![expr::Expr::Farg(Farg::TimeUnit(unit)), e],
                    )
                }
                Builtin::Substring(arg, start, end) => {
                    let arg = self.resolve_expr(arg, location, phc, within_aggr)?;
                    let start = self.resolve_expr(start, location, phc, within_aggr)?;
                    let end = match end {
                        Some(e) => self.resolve_expr(e, location, phc, within_aggr)?,
                        None => expr::Expr::farg_none(),
                    };
                    expr::Expr::func(expr::FuncKind::Substring, vec![arg, start, end])
                }
            },
            Expr::CaseWhen(CaseWhen {
                operand,
                branches,
                fallback,
            }) => {
                let mut args = Vec::with_capacity(branches.len() * 2 + 2);
                let node = match operand {
                    Some(e) => self.resolve_expr(e, location, phc, within_aggr)?,
                    None => expr::Expr::farg_none(),
                };
                args.push(node);
                let fb = match fallback {
                    Some(e) => self.resolve_expr(e, location, phc, within_aggr)?,
                    None => expr::Expr::farg_none(),
                };
                args.push(fb);
                for (when, then) in branches {
                    let when = self.resolve_expr(when, location, phc, within_aggr)?;
                    args.push(when);
                    let then = self.resolve_expr(then, location, phc, within_aggr)?;
                    args.push(then);
                }
                expr::Expr::func(expr::FuncKind::Case, args)
            }
            _ => todo!("exprs"),
        };
        Ok(res)
    }

    /// Map SQL literal to constant value of storage type.
    /// Validation are also performed here, e.g.
    /// date/time format, hex-str, binary-str, interval, etc.
    #[inline]
    fn resolve_lit(&self, lit: &Literal<'_>) -> Result<expr::Expr> {
        let res = match lit {
            Literal::Null => expr::Expr::const_null(),
            Literal::Numeric(n) => {
                if n.contains(|c| c == 'e' || c == 'E') {
                    // float64
                    let f: f64 = n.parse()?;
                    expr::Expr::const_f64(f)
                } else if n.contains('.') {
                    // decimal
                    let d: Decimal = n.parse()?;
                    expr::Expr::const_decimal(d)
                } else if let Ok(i) = n.parse::<i64>() {
                    // i64
                    expr::Expr::const_i64(i)
                } else if let Ok(u) = n.parse::<u64>() {
                    // u64
                    expr::Expr::const_u64(u)
                } else {
                    // decimal
                    let d: Decimal = n.parse()?;
                    expr::Expr::const_decimal(d)
                }
            }
            Literal::CharStr(cs) => {
                let s = if cs.rest.is_empty() {
                    Arc::from(cs.first)
                } else {
                    let mut s = String::from(cs.first);
                    for r in &cs.rest {
                        s.push_str(r);
                    }
                    Arc::from(s)
                };
                expr::Expr::const_str(s)
            }
            Literal::Bool(b) => {
                if *b {
                    expr::Expr::pred(Pred::True)
                } else {
                    expr::Expr::pred(Pred::False)
                }
            }
            Literal::Date(dt) => {
                let dt = Date::parse(dt, &DEFAULT_DATE_FORMAT)?;
                expr::Expr::const_date(dt)
            }
            Literal::Interval(Interval { unit, value }) => {
                let unit = time_unit_from_ast(*unit);
                let value: i32 = value.parse()?;
                expr::Expr::const_interval(unit, value)
            }
            _ => todo!("hexstr, bitstr, time, datetime, interval"),
        };
        Ok(res)
    }

    #[inline]
    fn resolve_aggr_func<'a>(
        &self,
        af: &'a AggrFunc<'a>,
        location: &'static str,
        phc: &mut PlaceholderCollector<'a>,
    ) -> Result<expr::Expr> {
        let e = self.resolve_expr(&af.expr, location, phc, true)?;
        let q = match af.q {
            SetQuantifier::All => expr::Setq::All,
            SetQuantifier::Distinct => expr::Setq::Distinct,
        };
        let res = match af.kind {
            AggrFuncKind::CountAsterisk => expr::Expr::count_asterisk(),
            AggrFuncKind::Count => expr::Expr::count(q, e),
            AggrFuncKind::Sum => expr::Expr::sum(q, e),
            AggrFuncKind::Avg => expr::Expr::avg(q, e),
            AggrFuncKind::Max => expr::Expr::max(q, e),
            AggrFuncKind::Min => expr::Expr::min(q, e),
        };
        Ok(res)
    }
}

/// PlaceholderCollector is a component to collect placeholder
/// during resolution of expression.
/// In simple query, there should be no such intermediate
/// values that cannot be resolved in current context,
/// but in more complex queries that contains subqueries and
/// correlated subqueries, this is necessary to collect
/// and resolve in a later phase, to make the name resolution
/// simple and clear.
pub struct PlaceholderCollector<'a> {
    pub allow_unknown_ident: bool,
    pub idents: Vec<(u32, Vec<SmolStr>, &'static str)>,
    pub subqueries: Vec<PlaceholderQuery<'a>>,
    ident_id_gen: u32,
    subquery_id_gen: u32,
}

impl<'a> PlaceholderCollector<'a> {
    #[inline]
    pub fn new(allow_unknown_ident: bool) -> Self {
        PlaceholderCollector {
            allow_unknown_ident,
            idents: vec![],
            subqueries: vec![],
            ident_id_gen: 0,
            subquery_id_gen: 0,
        }
    }

    #[inline]
    pub fn add_ident(&mut self, ident: Vec<SmolStr>, location: &'static str) -> expr::Expr {
        let uid = self.ident_id_gen;
        self.idents.push((uid, ident, location));
        self.ident_id_gen += 1;
        expr::Expr::ph_ident(uid)
    }

    // #[inline]
    // pub fn add_subquery(
    //     &mut self,
    //     kind: expr::SubqKind,
    //     subquery: &'a QueryExpr<'a>,
    //     location: &'static str,
    // ) -> expr::Expr {
    //     let uid = self.subquery_id_gen;
    //     self.subqueries.push((uid, kind, subquery, location));
    //     self.subquery_id_gen += 1;
    //     expr::Expr::ph_subquery(uid)
    // }

    #[inline]
    pub fn add_subquery(
        &mut self,
        kind: expr::SubqKind,
        qry: &'a QueryExpr<'a>,
        location: &'static str,
    ) -> expr::Expr {
        let uid = self.subquery_id_gen;
        self.subqueries.push(PlaceholderQuery {
            uid,
            kind,
            qry,
            location,
        });
        self.subquery_id_gen += 1;
        expr::Expr::ph_subquery(kind, uid)
    }
}

pub struct PlaceholderQuery<'a> {
    pub uid: u32,
    pub kind: expr::SubqKind,
    pub qry: &'a QueryExpr<'a>,
    pub location: &'static str,
}

/* helper functions */

fn time_unit_from_ast(unit: DatetimeUnit) -> TimeUnit {
    match unit {
        DatetimeUnit::Microsecond => TimeUnit::Microsecond,
        DatetimeUnit::Second => TimeUnit::Second,
        DatetimeUnit::Minute => TimeUnit::Minute,
        DatetimeUnit::Hour => TimeUnit::Hour,
        DatetimeUnit::Day => TimeUnit::Day,
        DatetimeUnit::Week => TimeUnit::Week,
        DatetimeUnit::Month => TimeUnit::Month,
        DatetimeUnit::Quarter => TimeUnit::Quarter,
        DatetimeUnit::Year => TimeUnit::Year,
    }
}
