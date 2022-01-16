use crate::error::{Error, Result};
use crate::{Column, ColumnAttr, ColumnID, QueryCatalog, Schema, SchemaID, Table, TableID};
use indexmap::IndexMap;
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use xngin_datatype::DataType;

#[derive(Debug, Clone)]
pub struct MemCatalog {
    inner: Arc<MemCatalogInner>,
}

#[derive(Debug)]
struct MemCatalogInner {
    schemas: IndexMap<SmolStr, Schema>,
    tables: IndexMap<SchemaID, Vec<Table>>,
    table_columns: HashMap<TableID, TableWithColumns>,
}

#[derive(Debug)]
pub struct TableWithColumns {
    table: Table,
    columns: Vec<Column>,
}

impl QueryCatalog for MemCatalog {
    fn all_schemas(&self) -> Vec<Schema> {
        self.inner.schemas.values().cloned().collect()
    }

    fn exists_schema(&self, schema_name: &str) -> bool {
        self.inner.schemas.contains_key(schema_name)
    }

    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema> {
        self.inner.schemas.get(schema_name).cloned()
    }

    fn find_schema(&self, schema_id: &SchemaID) -> Option<Schema> {
        self.inner
            .schemas
            .values()
            .find(|s| &s.id == schema_id)
            .cloned()
    }

    fn all_tables_in_schema(&self, schema_id: &SchemaID) -> Vec<Table> {
        self.inner
            .tables
            .get(schema_id)
            .cloned()
            .unwrap_or_default()
    }

    fn exists_table(&self, schema_id: &SchemaID, table_name: &str) -> bool {
        self.inner
            .tables
            .get(schema_id)
            .map(|ts| ts.iter().any(|t| t.name == table_name))
            .unwrap_or_default()
    }

    fn find_table_by_name(&self, schema_id: &SchemaID, table_name: &str) -> Option<Table> {
        self.inner
            .tables
            .get(schema_id)
            .and_then(|ts| ts.iter().find(|t| t.name == table_name).cloned())
    }

    fn find_table(&self, table_id: &TableID) -> Option<Table> {
        self.inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.table.clone())
    }

    fn all_columns_in_table(&self, table_id: &TableID) -> Vec<Column> {
        self.inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.columns.clone())
            .unwrap_or_default()
    }

    fn exists_column(&self, table_id: &TableID, column_name: &str) -> bool {
        self.inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.columns.iter().any(|c| c.name == column_name))
            .unwrap_or_default()
    }

    fn find_column_by_name(&self, table_id: &TableID, column_name: &str) -> Option<Column> {
        self.inner
            .table_columns
            .get(table_id)
            .and_then(|twc| twc.columns.iter().find(|c| c.name == column_name).cloned())
    }
}

#[derive(Debug, Default)]
pub struct MemCatalogBuilder {
    schemas: IndexMap<SmolStr, Schema>,
    tables: IndexMap<SchemaID, Vec<Table>>,
    table_columns: HashMap<TableID, TableWithColumns>,
    schema_id_gen: u32,
    table_id_gen: u32,
    column_id_gen: u32,
}

impl MemCatalogBuilder {
    #[inline]
    pub fn add_schema(&mut self, name: &str) -> Result<SchemaID> {
        if self.schemas.contains_key(name) {
            return Err(Error::SchemaAlreadyExists(name.to_string()));
        }
        self.schema_id_gen += 1;
        let id = SchemaID::new(self.schema_id_gen);
        let name = SmolStr::new(name);
        let schema = Schema {
            id,
            name: name.clone(),
        };
        self.schemas.insert(name, schema);
        Ok(id)
    }

    #[inline]
    pub fn add_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        cols: &[ColumnSpec],
    ) -> Result<TableID> {
        if let Some(schema) = self.schemas.get(schema_name) {
            let schema_id = schema.id;
            // check table name conflicts
            if self
                .tables
                .get(&schema_id)
                .and_then(|ts| ts.iter().find(|t| t.name == table_name))
                .is_some()
            {
                return Err(Error::TableAlreadyExists(table_name.to_string()));
            }
            // check column name conflicts
            let mut col_names = HashSet::with_capacity(cols.len());
            for c in cols {
                if !col_names.insert(c.name.clone()) {
                    return Err(Error::ColumnNameNotUnique(c.name.to_string()));
                }
            }
            // create table first
            self.table_id_gen += 1;
            let table_id = TableID::new(self.table_id_gen);
            let table_name = SmolStr::new(table_name);
            let table = Table {
                id: table_id,
                schema_id,
                name: table_name,
            };
            self.tables
                .entry(schema_id)
                .or_insert(vec![])
                .push(table.clone());
            let mut columns = Vec::with_capacity(cols.len());
            for (i, c) in cols.iter().enumerate() {
                self.column_id_gen += 1;
                let id = ColumnID::new(self.column_id_gen);
                let column = Column {
                    id,
                    table_id,
                    name: c.name.clone(),
                    ty: c.ty,
                    attr: c.attr,
                    idx: i as u32,
                };
                columns.push(column);
            }
            self.table_columns
                .insert(table_id, TableWithColumns { table, columns });
            Ok(table_id)
        } else {
            Err(Error::SchemaNotExists(schema_name.to_string()))
        }
    }

    #[inline]
    pub fn build(self) -> MemCatalog {
        let inner = MemCatalogInner {
            schemas: self.schemas,
            tables: self.tables,
            table_columns: self.table_columns,
        };
        MemCatalog {
            inner: Arc::new(inner),
        }
    }
}

pub struct ColumnSpec {
    pub name: SmolStr,
    pub ty: DataType,
    pub attr: ColumnAttr,
}

impl ColumnSpec {
    #[inline]
    pub fn new(name: &str, ty: DataType, attr: ColumnAttr) -> Self {
        ColumnSpec {
            name: SmolStr::new(name),
            ty,
            attr,
        }
    }
}
