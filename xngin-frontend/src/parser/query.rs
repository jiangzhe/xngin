use crate::ast::*;
use crate::parser::expr::{char_sp0, expr_sp0, sp0_char};
use crate::parser::{
    derived_col, ident, is_reserved_keyword, next, next_cut, spcmt0, spcmt1, table_name, ParseInput,
};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{alphanumeric1, char, u64};
use nom::combinator::{cut, map, not, opt, peek, success, value};
use nom::error::ParseError;
use nom::multi::{fold_many0, separated_list1};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;

pub(crate) struct PartialSetQuery<'a> {
    op: SetOp,
    distinct: bool,
    operand: Query<'a>,
}

parse!(
    /// Parse a query expression.
    fn query_expr -> 'a QueryExpr<'a> = {
        map(
            pair(
                opt(with),
                next_cut(select, |_, i, query| {
                    let mut input = i;
                    let mut res = query;
                    loop {
                        match preceded(spcmt0, set_op)(input) {
                            Ok((i, ps)) => {
                                res = Query::Set(SelectSet{op: ps.op, distinct: ps.distinct, children: vec![res, ps.operand]});
                                input = i;
                            }
                            Err(nom::Err::Error(_)) => {
                                return Ok((input, res));
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                }),
            ),
            |(with, query)| QueryExpr{with, query},
        )
    }
);

parse!(
    /// Parse a union clause.
    fn set_op -> 'a PartialSetQuery<'a> = {
        map(
            pair(
                terminated(
                    alt((
                        value(SetOp::Union, tag_no_case("union")),
                        value(SetOp::Except, tag_no_case("except")),
                        value(SetOp::Intersect, tag_no_case("intersect")),
                    )),
                    terminated(peek(not(alphanumeric1)), spcmt0)),
                cut(pair(
                    alt((
                        value(false, terminated(tag_no_case("all"), terminated(peek(not(alphanumeric1)), spcmt0))),
                        value(true, terminated(tag_no_case("distinct"), terminated(peek(not(alphanumeric1)), spcmt0))),
                        success(true),
                    )),
                    select,
                )),
            ),
            |(op, (distinct, operand))| PartialSetQuery{op, distinct, operand},
        )
    }
);

parse!(
    /// Parse a with clause.
    fn with -> 'a With<'a> = {
        preceded(
            terminated(tag_no_case("with"), spcmt0),
            cut(alt((
                map(
                    preceded(
                        terminated(tag_no_case("recursive"), spcmt1),
                        separated_list1(
                            terminated(char_sp0(','), spcmt0),
                            terminated(with_element, spcmt0),
                        )
                    ),
                    |elements| With{recursive: true, elements},
                ),
                map(
                    separated_list1(
                        terminated(char_sp0(','), spcmt0),
                        terminated(with_element, spcmt0),
                    ),
                    |elements| With{recursive: false, elements},
                )
            )))
        )
    }
);

parse!(
    /// Parse a with element.
    /// ```bnf
    /// <with_element> ::= <query_name> [ <left_paren> <with_col_list> <right_paren> ]
    ///                    AS <left_paren> <query_expr> <right_paren>
    /// ```
    fn with_element -> 'a WithElement<'a> = {
        map(
            tuple((
                terminated(ident, spcmt0),
                opt(
                    preceded(
                        char_sp0('('),
                        cut(terminated(
                            separated_list1(char_sp0(','), terminated(ident, spcmt0)),
                            char_sp0(')'),
                        )),
                    )
                ),
                cut(delimited(
                    preceded(tag_no_case("as"), sp0_char('(')),
                    preceded(spcmt0, query_expr),
                    preceded(spcmt0, char(')')),
                )),
            )),
            |(name, cols, query_expr)| WithElement{name, cols: cols.unwrap_or_default(), query_expr},
        )

    }
);

parse!(
    /// Parse a select clause
    /// ```bnf
    /// <select_clause> ::= SELECT <column_list> FROM <table_expr> [ <where_clause> ]
    ///                     [ <group_by_clause> ] [ <having_clause> ] [ <order_by_clause> ]
    ///                     [ <limit_clause> ]
    /// ```
    fn select -> 'a Query<'a> = {
        map(
            pair(
                pair(
                    terminated(
                        alt((
                            value(SetQuantifier::Distinct, delimited(tag_no_case("select"), spcmt1, tag_no_case("distinct"))),
                            value(SetQuantifier::All, delimited(tag_no_case("select"), spcmt1, tag_no_case("all"))),
                            value(SetQuantifier::All, preceded(tag_no_case("select"), spcmt1)),
                        )),
                        spcmt0,
                    ),
                    cut(separated_list1(char_sp0(','), terminated(derived_col, spcmt0))),
                ),
                opt(
                    pair(
                        from,
                        cut(tuple((
                            opt(filter),
                            opt(group_by),
                            opt(having),
                            opt(order_by),
                            opt(limit),
                        )))
                    )
                ),
            ),
            |((q, cols), other)| match other {
                Some((from, (filter, group_by, having, order_by, limit))) => {
                    let group_by = group_by.unwrap_or_default();
                    let order_by = order_by.unwrap_or_default();
                    Query::table(SelectTable{q, cols, from, filter, group_by, having, order_by, limit})
                }
                None => Query::Row(cols), // for row query, set quantifier can be safely ignored
            }
        )
    }
);

parse!(
    /// Parse a from clause
    /// todo: alias, subquery, etc.
    /// ```bnf
    /// <from_clause> ::= FROM <table_name>
    /// ```
    fn from -> 'a Vec<TableRef<'a>> = {
        preceded(
            preceded(tag_no_case("from"), terminated(peek(not(alphanumeric1)), spcmt0)),
            // cut(terminated(table_name, multispace0)),
            cut(separated_list1(char_sp0(','), terminated(table_ref, spcmt0)))
        )
    }
);

parse!(
    /// Parse a where clause
    /// ```bnf
    /// <where_clause> ::= WHERE <expr>
    /// ```
    fn filter -> 'a Expr<'a> = {
        preceded(terminated(tag_no_case("where"), peek(not(alphanumeric1))), cut(preceded(spcmt0, expr_sp0)))
    }
);

parse!(
    /// Parse a group by clause
    /// ```bnf
    /// <group_by_clause> ::= GROUP BY <grouping_element_list>
    /// ```
    fn group_by -> 'a Vec<Expr<'a>> = {
        preceded(
            delimited(tag_no_case("group"), spcmt1, terminated(tag_no_case("by"), spcmt0)),
            cut(separated_list1(terminated(char(','), spcmt0), expr_sp0)),
        )
    }
);

parse!(
    /// Parse a having clause
    /// ```bnf
    /// <having_clause> ::= HAVING <expr>
    /// ```
    fn having -> 'a Expr<'a> = {
        preceded(terminated(tag_no_case("having"), peek(not(alphanumeric1))), cut(preceded(spcmt0, expr_sp0)))
    }
);

parse!(
    /// Parse an order by clause
    /// ```bnf
    /// <order_by_clause> ::= ORDER BY <sort_spec_list>
    /// ```
    fn order_by -> 'a Vec<OrderElement<'a>> = {
        preceded(
            preceded(
                preceded(tag_no_case("order"), spcmt1),
                tag_no_case("by"),
            ),
            cut(separated_list1(char(','), order_element)),
        )
    }
);

parse!(
    /// Parse an order element
    /// ```bnf
    /// <order_element> ::= <expr> [ ASC | DESC ]
    /// ```
    fn order_element -> 'a OrderElement<'a> = {
        map(
            pair(
                preceded(spcmt0, expr_sp0),
                map(
                    opt(alt((
                        value(false, preceded(tag_no_case("asc"), spcmt0)),
                        value(true, preceded(tag_no_case("desc"), spcmt0)),
                    ))),
                    |ordering| ordering.unwrap_or(false),
                )
            ),
            |(expr, desc)| OrderElement{expr, desc},
        )
    }
);

parse!(
    /// Parse a limit clause
    /// ```bnf
    /// <limit_clause> ::= LIMIT [ <offset_limit> | <limit> OFFSET <offset>  ]
    /// ```
    fn limit -> Limit = {
        preceded(
            preceded(tag_no_case("limit"), spcmt1),
            cut(alt((
                map(
                    pair(terminated(u64, sp0_char(',')), delimited(spcmt0, u64, spcmt0)),
                    |(offset, limit)| Limit{limit, offset: Some(offset)},
                ),
                map(
                    pair(terminated(u64, spcmt0), opt(preceded(tag_no_case("offset"), preceded(spcmt0, u64)))),
                    |(limit, offset)| Limit{limit, offset},
                )
            )))
        )
    }
);

parse!(
    /// Parse a table primitive (named table/query or derived table)
    /// todo: add derived table
    /// ```bnf
    /// <table_or_query_name> ::= <table_name> | <query_name>
    /// <table_primitive> ::= <table_or_query_name> [ AS <alias> ]
    /// ```
    fn table_primitive -> 'a TablePrimitive<'a> = {
        alt((
            next(
                terminated(table_name, spcmt0),
                |i, table| {
                    match ident::<'_, I, E>(i) {
                        Ok((ri, Ident::Regular(alias))) => {
                            if alias.eq_ignore_ascii_case("as") {
                                let (ri, alias) = cut(preceded(spcmt0, ident))(ri)?;
                                Ok((ri, TablePrimitive::Named(table, Some(alias))))
                            } else if !is_reserved_keyword(alias) {
                                Ok((ri, TablePrimitive::Named(table, Some(alias.into()))))
                            } else {
                                // reserved keyword found, break this branch
                                Ok((i, TablePrimitive::Named(table, None)))
                            }
                        }
                        Ok((i, alias)) => {
                            Ok((i, TablePrimitive::Named(table, Some(alias))))
                        }
                        Err(_) => Ok((i, TablePrimitive::Named(table, None)))
                    }
                }
            ),
            map(
                pair(
                    preceded(
                        char_sp0('('),
                        cut(terminated(
                            query_expr,
                            char_sp0(')'),
                        ))
                    ),
                    cut(preceded(
                        opt(preceded(tag_no_case("as"), peek(not(alphanumeric1)))),
                        preceded(spcmt0, ident))),
                ),
                |(query, alias)| TablePrimitive::derived(query, alias),
            )
        ))
    }
);

parse!(
    /// Parse a subquery.
    fn subquery -> 'a QueryExpr<'a> = {
        preceded(
            char_sp0('('),
            cut(terminated(
                query_expr,
                char_sp0(')'),
            ))
        )
    }
);

parse!(
    /// Parse a table ref clause.
    /// Table ref is defined as a table primitive with zero or more joined table.
    fn table_ref -> 'a TableRef<'a> = {
        next_cut(table_primitive, |_, i, tp| {
            let (i, _) = spcmt0(i)?;
            let init = || TableRef::Primitive(Box::new(tp.clone()));
            fold_many0(
                alt((
                    cross_join,
                    natural_join,
                    qualified_join,
                )),
                init,
                |tr, pj| match pj {
                    PartialJoin::Cross(tp) => TableRef::Joined(Box::new(TableJoin::cross(tr, tp))),
                    PartialJoin::Natural(jt, tp) => TableRef::Joined(Box::new(TableJoin::natural(tr, tp, jt))),
                    PartialJoin::Qualified(jt, tp, cond) => TableRef::Joined(Box::new(TableJoin::qualified(tr, tp, jt, cond))),
                },
            )(i)
        })
    }
);

// intermediate struct to hold partial join criteria.
pub enum PartialJoin<'a> {
    Cross(TablePrimitive<'a>),
    Natural(JoinType, TablePrimitive<'a>),
    Qualified(JoinType, TablePrimitive<'a>, Option<JoinCondition<'a>>),
}

parse!(
    /// Parse a cross join clause.
    /// ```bnf
    /// <cross_join> ::= CROSS JOIN <table_primitive>
    /// ```
    fn cross_join -> 'a PartialJoin<'a> = {
        map(
            preceded(
                preceded(
                    terminated(tag_no_case("cross"), spcmt1),
                    terminated(tag_no_case("join"), spcmt1),
                ),
                cut(table_primitive),
            ),
            PartialJoin::Cross,
        )
    }
);

parse!(
    /// Parse a natual join clause.
    fn natural_join -> 'a PartialJoin<'a> = {
        map(
            pair(
                preceded(
                    terminated(tag_no_case("natural"), spcmt1),
                    cut(terminated(join_type, spcmt1)),
                ),
                table_primitive,
            ),
            |(jt, tp)| PartialJoin::Natural(jt, tp),
        )
    }
);

parse!(
    /// Parse a qualified join clause.
    /// does different join type have different precedences?
    fn qualified_join -> 'a PartialJoin<'a> = {
        map(
            pair(
                terminated(join_type, spcmt0),
                cut(pair(
                    terminated(table_primitive, spcmt0),
                    opt(join_condition),
                ))
            ),
            |(jt, (tp, jc))| PartialJoin::Qualified(jt, tp, jc),
        )
    }
);

parse!(
    /// Parse a join type.
    fn join_type -> JoinType = {
        alt((
            value(JoinType::Inner,
                delimited(tag_no_case("inner"), spcmt1, tag_no_case("join"))),
            terminated(
                alt((
                    value(JoinType::Left, terminated(tag_no_case("left"), spcmt1)),
                    value(JoinType::Right, terminated(tag_no_case("right"), spcmt1)),
                    value(JoinType::Full, terminated(tag_no_case("full"), spcmt1)),
                )),
                preceded(
                    opt(terminated(tag_no_case("outer"), spcmt1)),
                    tag_no_case("join"),
                )),
            value(JoinType::Inner, tag_no_case("join")),
        ))
    }
);

parse!(
    /// Parse a join condition clause.
    fn join_condition -> 'a JoinCondition<'a> = {
        alt((
            map(
                preceded(
                    terminated(tag_no_case("on"), spcmt1),
                    cut(expr_sp0),
                ),
                JoinCondition::Conds,
            ),
            map(
                preceded(
                    delimited(tag_no_case("using"), spcmt0, char('(')),
                    cut(terminated(
                        separated_list1(sp0_char(','), preceded(spcmt0, ident)),
                        sp0_char(')')
                    )),
                ),
                JoinCondition::NamedCols,
            ),
        ))
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::Ansi;
    use nom::error::Error;

    #[test]
    fn test_parse_where() -> anyhow::Result<()> {
        for c in vec![
            ("where a", Expr::column_ref(vec!["a".into()])),
            (
                "where a > 1",
                Expr::cmp_gt(Expr::column_ref(vec!["a".into()]), Expr::numeric_lit("1")),
            ),
        ] {
            let (i, o) = filter::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_group_by() -> anyhow::Result<()> {
        for c in vec![
            ("group by a", vec![Expr::column_ref(vec!["a".into()])]),
            (
                "group by a, b",
                vec![
                    Expr::column_ref(vec!["a".into()]),
                    Expr::column_ref(vec!["b".into()]),
                ],
            ),
        ] {
            let (i, o) = group_by::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_having() -> anyhow::Result<()> {
        for c in vec![
            ("having a", Expr::column_ref(vec!["a".into()])),
            ("having 1", Expr::numeric_lit("1")),
            (
                "having a = 1",
                Expr::cmp_eq(Expr::column_ref(vec!["a".into()]), Expr::numeric_lit("1")),
            ),
        ] {
            let (i, o) = having::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_order_by() -> anyhow::Result<()> {
        for c in vec![
            (
                "order by a",
                vec![OrderElement::asc(Expr::column_ref(vec!["a".into()]))],
            ),
            (
                "order by a, b",
                vec![
                    OrderElement::new(Expr::column_ref(vec!["a".into()]), false),
                    OrderElement::new(Expr::column_ref(vec!["b".into()]), false),
                ],
            ),
            (
                "order by a desc, b asc",
                vec![
                    OrderElement::desc(Expr::column_ref(vec!["a".into()])),
                    OrderElement::asc(Expr::column_ref(vec!["b".into()])),
                ],
            ),
        ] {
            let (i, o) = order_by::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_limit() -> anyhow::Result<()> {
        for c in vec![
            (
                "limit 1",
                Limit {
                    limit: 1,
                    offset: None,
                },
            ),
            (
                "limit 1, 2",
                Limit {
                    limit: 2,
                    offset: Some(1),
                },
            ),
            (
                "limit 1 offset 5",
                Limit {
                    limit: 1,
                    offset: Some(5),
                },
            ),
        ] {
            let (i, o) = limit::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_table_ref() -> anyhow::Result<()> {
        for c in vec![
            (
                "a cross join b",
                TableRef::joined(TableJoin::cross(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                )),
            ),
            (
                "a natural join b",
                TableRef::joined(TableJoin::natural(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Inner,
                )),
            ),
            (
                "a natural left join b",
                TableRef::joined(TableJoin::natural(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Left,
                )),
            ),
            (
                "a natural left outer join b",
                TableRef::joined(TableJoin::natural(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Left,
                )),
            ),
            (
                "a natural right join b",
                TableRef::joined(TableJoin::natural(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Right,
                )),
            ),
            (
                "a natural full join b",
                TableRef::joined(TableJoin::natural(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Full,
                )),
            ),
            (
                "a join b",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Inner,
                    None,
                )),
            ),
            (
                "a inner join b",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Inner,
                    None,
                )),
            ),
            (
                "a left join b",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Left,
                    None,
                )),
            ),
            (
                "a right join b",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Right,
                    None,
                )),
            ),
            (
                "a full join b",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Full,
                    None,
                )),
            ),
            (
                "a join b on a.c0 = b.c0",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Inner,
                    Some(JoinCondition::Conds(Expr::cmp_eq(
                        Expr::column_ref(vec!["a".into(), "c0".into()]),
                        Expr::column_ref(vec!["b".into(), "c0".into()]),
                    ))),
                )),
            ),
            (
                "a join b using(c0)",
                TableRef::joined(TableJoin::qualified(
                    TableRef::primitive(TablePrimitive::Named("a".into(), None)),
                    TablePrimitive::Named("b".into(), None),
                    JoinType::Inner,
                    Some(JoinCondition::NamedCols(vec!["c0".into()])),
                )),
            ),
            (
                "(select 1) as a",
                TableRef::primitive(TablePrimitive::derived(
                    QueryExpr {
                        with: None,
                        query: Query::row(vec![DerivedCol::auto_alias(
                            Expr::numeric_lit("1"),
                            "1",
                        )]),
                    },
                    "a".into(),
                )),
            ),
        ] {
            let (i, o) = table_ref::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_select() -> anyhow::Result<()> {
        for c in vec![
            (
                "select 1",
                Query::row(vec![DerivedCol::auto_alias(Expr::numeric_lit("1"), "1")]),
            ),
            (
                "select 1 as a",
                Query::row(vec![DerivedCol::new(Expr::numeric_lit("1"), "a".into())]),
            ),
            (
                "select a from b",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            ),
            (
                "select a from b where 1",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: Some(Expr::numeric_lit("1")),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            ),
            (
                "select a from b group by a",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: None,
                    group_by: vec![Expr::column_ref(vec!["a".into()])],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            ),
            (
                "select a from b having a > 1",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: None,
                    group_by: vec![],
                    having: Some(Expr::cmp_gt(
                        Expr::column_ref(vec!["a".into()]),
                        Expr::numeric_lit("1"),
                    )),
                    order_by: vec![],
                    limit: None,
                }),
            ),
            (
                "select a from b order by a",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![OrderElement::new(Expr::column_ref(vec!["a".into()]), false)],
                    limit: None,
                }),
            ),
            (
                "select a from b limit 1",
                Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![DerivedCol::auto_alias(
                        Expr::column_ref(vec!["a".into()]),
                        "a",
                    )],
                    from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                        TableName::new(None, "b".into()),
                        None,
                    )))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: Some(Limit {
                        limit: 1,
                        offset: None,
                    }),
                }),
            ),
        ] {
            let (i, o) = select::<'_, _, Error<_>>(Ansi(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_query_expr() -> anyhow::Result<()> {
        fn simple_select<'a>() -> SelectTable<'a> {
            SelectTable {
                q: SetQuantifier::All,
                cols: vec![],
                from: vec![TableRef::Primitive(Box::new(TablePrimitive::Named(
                    TableName::new(None, "t".into()),
                    None,
                )))],
                filter: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }
        }
        for c in vec![
            (
                "select 1",
                QueryExpr {
                    with: None,
                    query: Query::row(vec![DerivedCol::auto_alias(Expr::numeric_lit("1"), "1")]),
                },
            ),
            (
                "select distinct 1",
                QueryExpr {
                    with: None,
                    query: Query::row(vec![DerivedCol::auto_alias(Expr::numeric_lit("1"), "1")]),
                },
            ),
            (
                "select 1 as a",
                QueryExpr {
                    with: None,
                    query: Query::row(vec![DerivedCol::new(Expr::numeric_lit("1"), "a".into())]),
                },
            ),
            (
                "select a from t",
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        cols: vec![DerivedCol::auto_alias(
                            Expr::column_ref(vec!["a".into()]),
                            "a",
                        )],
                        ..simple_select()
                    }),
                },
            ),
            (
                "select distinct a from t",
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::Distinct,
                        cols: vec![DerivedCol::auto_alias(
                            Expr::column_ref(vec!["a".into()]),
                            "a",
                        )],
                        ..simple_select()
                    }),
                },
            ),
            (
                "with x as (select 1) select a from t",
                QueryExpr {
                    with: Some(With {
                        recursive: false,
                        elements: vec![WithElement {
                            name: "x".into(),
                            cols: vec![],
                            query_expr: QueryExpr {
                                with: None,
                                query: Query::row(vec![DerivedCol::auto_alias(
                                    Expr::numeric_lit("1"),
                                    "1",
                                )]),
                            },
                        }],
                    }),
                    query: Query::table(SelectTable {
                        cols: vec![DerivedCol::auto_alias(
                            Expr::column_ref(vec!["a".into()]),
                            "a",
                        )],
                        ..simple_select()
                    }),
                },
            ),
            (
                "with x(y) as (select 1) select a from t",
                QueryExpr {
                    with: Some(With {
                        recursive: false,
                        elements: vec![WithElement {
                            name: "x".into(),
                            cols: vec!["y".into()],
                            query_expr: QueryExpr {
                                with: None,
                                query: Query::row(vec![DerivedCol::auto_alias(
                                    Expr::numeric_lit("1"),
                                    "1",
                                )]),
                            },
                        }],
                    }),
                    query: Query::table(SelectTable {
                        cols: vec![DerivedCol::auto_alias(
                            Expr::column_ref(vec!["a".into()]),
                            "a",
                        )],
                        ..simple_select()
                    }),
                },
            ),
            (
                "select a from t union select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Union,
                        distinct: true,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t union all select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Union,
                        distinct: false,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t except select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Except,
                        distinct: true,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t except all select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Except,
                        distinct: false,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t intersect select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Intersect,
                        distinct: true,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t intersect all select b from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Intersect,
                        distinct: false,
                        children: vec![
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["a".into()]),
                                    "a",
                                )],
                                ..simple_select()
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["b".into()]),
                                    "b",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
            (
                "select a from t union all select b from t union all select c from t",
                QueryExpr {
                    with: None,
                    query: Query::Set(SelectSet {
                        op: SetOp::Union,
                        distinct: false,
                        children: vec![
                            Query::Set(SelectSet {
                                op: SetOp::Union,
                                distinct: false,
                                children: vec![
                                    Query::table(SelectTable {
                                        cols: vec![DerivedCol::auto_alias(
                                            Expr::column_ref(vec!["a".into()]),
                                            "a",
                                        )],
                                        ..simple_select()
                                    }),
                                    Query::table(SelectTable {
                                        cols: vec![DerivedCol::auto_alias(
                                            Expr::column_ref(vec!["b".into()]),
                                            "b",
                                        )],
                                        ..simple_select()
                                    }),
                                ],
                            }),
                            Query::table(SelectTable {
                                cols: vec![DerivedCol::auto_alias(
                                    Expr::column_ref(vec!["c".into()]),
                                    "c",
                                )],
                                ..simple_select()
                            }),
                        ],
                    }),
                },
            ),
        ] {
            check_query(c)?;
        }
        Ok(())
    }

    #[inline]
    fn check_query(c: (&'static str, QueryExpr<'_>)) -> anyhow::Result<()> {
        let (i, o) = query_expr::<'_, _, Error<_>>(Ansi(c.0))?;
        assert!(i.is_empty());
        assert_eq!(c.1, o);
        Ok(())
    }
}
