pub mod database;
pub mod query;
pub mod ui;

// Common constants
pub const DB_HEADER_SIZE: usize = 100;
pub const BTREE_HEADER_SIZE: usize = 8;

// Re-export main types for convenience
pub use database::{Database, TableRow, TableRows, RecordValue, Cell, TableSchema, ColumnInfo};
pub use query::execute_command;
pub use ui::{run_tui, App};
