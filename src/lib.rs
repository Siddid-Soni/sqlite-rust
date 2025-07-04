pub mod database;
pub mod record;
pub mod cell;
pub mod schema;
pub mod varint;
pub mod commands;

// Common constants
pub const DB_HEADER_SIZE: usize = 100;
pub const BTREE_HEADER_SIZE: usize = 8;

// Re-export main types for convenience
pub use database::{Database, TableRow, TableRows};
pub use record::{Record, RecordValue, RecordHeader};
pub use cell::Cell;
pub use schema::{TableSchema, ColumnInfo};
pub use commands::execute_command;
