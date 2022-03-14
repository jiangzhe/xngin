pub mod error;
pub mod mem_impl;

use bitflags::bitflags;
use smol_str::SmolStr;
use std::hash::Hash;
use std::marker::PhantomData;
use xngin_datatype::PreciseType;

pub trait QueryCatalog {
    fn all_schemas(&self) -> Vec<Schema>;

    fn exists_schema(&self, schema_name: &str) -> bool;

    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema>;

    fn find_schema(&self, schema_id: &SchemaID) -> Option<Schema>;

    fn all_tables_in_schema(&self, schema_id: &SchemaID) -> Vec<Table>;

    fn exists_table(&self, schema_id: &SchemaID, table_name: &str) -> bool;

    fn find_table_by_name(&self, schema_id: &SchemaID, table_name: &str) -> Option<Table>;

    fn find_table(&self, table_id: &TableID) -> Option<Table>;

    fn all_columns_in_table(&self, table_id: &TableID) -> Vec<Column>;

    fn exists_column(&self, table_id: &TableID, column_name: &str) -> bool;

    fn find_column_by_name(&self, table_id: &TableID, column_name: &str) -> Option<Column>;
}

// pub type ObjectID = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectID<T> {
    id: u32,
    _marker: PhantomData<T>,
}

impl<T> ObjectID<T> {
    /// Required to create object only within the catalog module.
    pub(crate) fn new(id: u32) -> Self {
        ObjectID {
            id,
            _marker: PhantomData,
        }
    }
}

impl<T> ObjectID<T> {
    #[inline]
    pub fn value(&self) -> u32 {
        self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct S;
pub type SchemaID = ObjectID<S>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct T;
pub type TableID = ObjectID<T>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct C;
pub type ColumnID = ObjectID<C>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub id: SchemaID,
    pub name: SmolStr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableID,
    pub schema_id: SchemaID,
    pub name: SmolStr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: ColumnID,
    pub table_id: TableID,
    pub name: SmolStr,
    pub pty: PreciseType,
    pub idx: u32,
    pub attr: ColumnAttr,
}

bitflags! {
    pub struct ColumnAttr: u8 {
        const PK = 0x01; // primary key
        const UK = 0x02; // unique key
        const FK = 0x04; // foreign key
        const SK = 0x08; // shard key
    }
}
