use crate::mysql::col::ColumnDefinition;
use crate::mysql::flag::StatusFlags;
use flume::Receiver;

pub struct ResultSet {
    pub col_defs: Vec<ColumnDefinition<'static>>,
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    pub info: String,
    pub rows: Vec<Row>,
    pub next: Option<Receiver<Vec<Row>>>,
}

pub struct Row(Vec<u8>);
