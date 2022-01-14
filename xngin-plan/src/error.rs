use smol_str::SmolStr;
use thiserror::Error;
use xngin_frontend::ast::Ident;

pub type Result<T> = std::result::Result<T, Error>;

pub trait ToResult {
    type Output;

    fn must_ok(self) -> Result<Self::Output>;
}

impl<T> ToResult for Option<T> {
    type Output = T;

    fn must_ok(self) -> Result<Self::Output> {
        self.ok_or(Error::MustOK)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unsupported SQL syntax '{0}'")]
    UnsupportedSqlSyntax(String),
    #[error("Duplicated table alias '{0}'")]
    DuplicatedTableAlias(SmolStr),
    #[error("Duplicated column alias '{0}'")]
    DuplicatedColumnAlias(String),
    #[error("Invalid single row data '{0}'")]
    InvalidSingleRowData(String),
    #[error("Invalid Identifier '{0}'")]
    InvalidIdentifier(String),
    #[error("Schema not exists '{0}'")]
    SchemaNotExists(String),
    #[error("Table not exists '{0}'")]
    TableNotExists(String),
    #[error("Column not exists '{0}'")]
    ColumnNotExists(String),
    #[error("Column count mismatch")]
    ColumnCountMismatch,
    #[error("Not unique alias or table '{0}'")]
    NotUniqueAliasOrTable(String),
    #[error("Not unique table '{0}'")]
    NotUniqueTable(String),
    #[error("Unknown table: '{0}'")]
    UnknownTable(String),
    #[error("{0}")]
    UnknownColumn(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Fields selected not in GROUP BY clause")]
    FieldsSelectedNotInGroupBy,
    #[error("ORDER BY contains non-selected fields")]
    OrderByNonSelectedFields,
    #[error("Duplicated asterisks in field list")]
    DulicatedAsterisksInFieldList,
    #[error("Aggregate functions in GROUP BY clause")]
    AggrFuncInGroupBy,
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("Internal error MustOK")]
    MustOK,
}

impl Error {
    #[inline]
    pub fn unknown_column(ids: &[Ident<'_>], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            let (head, tail) = ids.split_first().unwrap();
            s.push_str(&head.as_str());
            for id in tail {
                s.push('.');
                s.push_str(&id.as_str());
            }
        })
    }

    #[inline]
    pub fn unknown_column_full_name(schema: &str, tbl: &str, col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(schema);
            s.push('.');
            s.push_str(tbl);
            s.push('.');
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_partial_name(tbl: &str, col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(tbl);
            s.push('.');
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_name(col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_idents(ids: &[SmolStr], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            let (head, tail) = ids.split_first().unwrap();
            s.push_str(head);
            for id in tail {
                s.push('.');
                s.push_str(id.as_str());
            }
        })
    }

    #[inline]
    fn unknown_column_apply<F: FnOnce(&mut String)>(location: &str, f: F) -> Self {
        let mut s = String::from("Unknown column '");
        f(&mut s);
        s.push_str("' in '");
        s.push_str(location);
        s.push('\'');
        Error::UnknownColumn(s)
    }

    #[inline]
    pub fn unknown_asterisk_column(qualifiers: &[Ident<'_>], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            for q in qualifiers {
                s.push_str(&q.as_str());
                s.push('.');
            }
            s.push('*');
        })
    }
}
