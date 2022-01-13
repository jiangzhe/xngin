use crate::ast::*;
use crate::parser::expr::{char_sp0, expr_sp0};
use crate::parser::query::{filter, query_expr};
use crate::parser::{ident, ident_tag, spcmt0, spcmt1, table_name, ParseInput};
use nom::branch::alt;
use nom::combinator::{cut, map, opt};
use nom::error::ParseError;
use nom::multi::separated_list1;
use nom::sequence::{pair, preceded, terminated, tuple};
use nom::IResult;

parse!(
    /// Parse an insert statement.
    fn insert -> 'a InsertExpr<'a> = {
        preceded(
            preceded(terminated(ident_tag("insert"), spcmt1), cut(ident_tag("into"))),
            cut(map(pair(
                preceded(spcmt0,
                    pair(
                        terminated(table_name, spcmt0),
                        opt(
                            preceded(
                                char_sp0('('),
                                terminated(
                                    separated_list1(char_sp0(','), terminated(ident, spcmt0)),
                                    char_sp0(')'),
                                )
                            )
                        ),
                    ),
                ),
                preceded(spcmt0, insert_source),
            ), |((target, cols), source)| InsertExpr{target, cols: cols.unwrap_or_default(), source})),
        )
    }
);

parse!(
    /// Parse an insert source.
    fn insert_source -> 'a InsertSource<'a> = {
        alt((
            map(insert_values, InsertSource::Values),
            map(query_expr, |q| InsertSource::Query(Box::new(q))),
        ))
    }
);

parse!(
    /// Parse insert values.
    fn insert_values -> 'a Vec<Expr<'a>> = {
        preceded(
            terminated(ident_tag("values"), cut(preceded(spcmt0, char_sp0('(')))),
            cut(terminated(
                separated_list1(char_sp0(','), expr_sp0),
                char_sp0(')'),
            )),
        )
    }
);

parse!(
    /// Parse a delete statement.
    fn delete -> 'a DeleteExpr<'a> = {
        preceded(
            preceded(terminated(ident_tag("delete"), spcmt1), cut(terminated(ident_tag("from"), spcmt0))),
            cut(map(
                pair(
                    terminated(table_name, spcmt0),
                    opt(filter),
                ),
                |(target, cond)| DeleteExpr{target, cond},
            ))
        )
    }
);

parse!(
    /// Parse an update statement.
    fn update -> 'a UpdateExpr<'a> = {
        preceded(
            terminated(ident_tag("update"), spcmt0),
            cut(map(
                tuple((
                    terminated(table_name, spcmt0),
                    update_acts,
                    opt(filter),
                )),
                |(target, acts, cond)| UpdateExpr{target, acts, cond},
            ))
        )
    }
);

parse!(
    /// Parse update set clause
    fn update_acts -> 'a Vec<(ColumnName<'a>, Expr<'a>)> = {
        preceded(
            terminated(ident_tag("set"), spcmt0),
            cut(separated_list1(
                char_sp0(','),
                pair(
                    terminated(ident, spcmt0),
                    preceded(char_sp0('='), expr_sp0),
                ),
            )),
        )
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::MySQL;
    use nom::error::Error;

    #[test]
    fn test_parse_insert() -> anyhow::Result<()> {
        for c in vec![
            (
                "insert into a values(1)",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec![],
                    source: InsertSource::values(vec![Expr::numeric_lit("1")]),
                },
            ),
            (
                "insert into a values(1, 2, 3)",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec![],
                    source: InsertSource::values(vec![
                        Expr::numeric_lit("1"),
                        Expr::numeric_lit("2"),
                        Expr::numeric_lit("3"),
                    ]),
                },
            ),
            (
                "insert into a (c0) values(1)",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec!["c0".into()],
                    source: InsertSource::values(vec![Expr::numeric_lit("1")]),
                },
            ),
            (
                "insert into a (c0, c1, c2) values(1, 2, 3)",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec!["c0".into(), "c1".into(), "c2".into()],
                    source: InsertSource::values(vec![
                        Expr::numeric_lit("1"),
                        Expr::numeric_lit("2"),
                        Expr::numeric_lit("3"),
                    ]),
                },
            ),
            (
                "insert into a select 1",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec![],
                    source: InsertSource::query(QueryExpr {
                        with: None,
                        query: Query::row(vec![DerivedCol::auto_alias(
                            Expr::numeric_lit("1"),
                            "1",
                        )]),
                    }),
                },
            ),
            (
                "insert into a(c0)select 1",
                InsertExpr {
                    target: TableName::new(None, "a".into()),
                    cols: vec!["c0".into()],
                    source: InsertSource::query(QueryExpr {
                        with: None,
                        query: Query::row(vec![DerivedCol::auto_alias(
                            Expr::numeric_lit("1"),
                            "1",
                        )]),
                    }),
                },
            ),
        ] {
            let (i, o) = insert::<'_, _, Error<_>>(MySQL(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_delete() -> anyhow::Result<()> {
        for c in vec![
            (
                "delete from t ",
                DeleteExpr {
                    target: TableName::new(None, "t".into()),
                    cond: None,
                },
            ),
            (
                "delete from t where c0 > 1",
                DeleteExpr {
                    target: TableName::new(None, "t".into()),
                    cond: Some(Expr::cmp_gt(
                        Expr::column_ref(vec!["c0".into()]),
                        Expr::numeric_lit("1"),
                    )),
                },
            ),
        ] {
            let (i, o) = delete::<'_, _, Error<_>>(MySQL(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_update() -> anyhow::Result<()> {
        for c in vec![
            (
                "update t set c0 = 1",
                UpdateExpr {
                    target: TableName::new(None, "t".into()),
                    acts: vec![("c0".into(), Expr::numeric_lit("1"))],
                    cond: None,
                },
            ),
            (
                "update t set c0 = 1 where c0 > 1",
                UpdateExpr {
                    target: TableName::new(None, "t".into()),
                    acts: vec![("c0".into(), Expr::numeric_lit("1"))],
                    cond: Some(Expr::cmp_gt(
                        Expr::column_ref(vec!["c0".into()]),
                        Expr::numeric_lit("1"),
                    )),
                },
            ),
        ] {
            let (i, o) = update::<'_, _, Error<_>>(MySQL(c.0))?;
            assert!(i.is_empty());
            assert_eq!(c.1, o);
        }
        Ok(())
    }
}
