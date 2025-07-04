# SQLite Implementation in Rust

A high-performance SQLite database implementation written in Rust, featuring B-tree navigation, index optimization, and SQL query processing.

## Features

- **Complete SQLite File Format Support**: Reads and parses SQLite database files according to the official specification
- **B-tree Navigation**: Efficient traversal of both table and index B-tree structures
- **Index Optimization**: Automatic index usage for WHERE clauses with equality conditions
- **SQL Query Support**: 
  - `SELECT` statements with column selection
  - `WHERE` clauses with equality and inequality operators
  - `COUNT(*)` aggregation
  - Case-insensitive SQL keywords
- **Schema Introspection**: Support for `.tables`, `.schema`, and `.dbinfo` commands
- **Memory Efficient**: Direct row lookup using B-tree search instead of loading entire tables

## Architecture

### Core Components

- **`database.rs`**: Main database engine with B-tree traversal and index optimization
- **`commands.rs`**: SQL query parser and command dispatcher
- **`record.rs`**: SQLite record format parsing and value handling
- **`cell.rs`**: B-tree cell structure parsing
- **`schema.rs`**: Table schema parsing from CREATE statements
- **`varint.rs`**: Variable-length integer encoding/decoding

### Key Optimizations

1. **Index Scanning**: Automatically detects and uses indexes for WHERE clauses
2. **Direct Row Lookup**: Uses B-tree navigation to fetch specific rows by ID
3. **Smart Page Traversal**: Only visits relevant B-tree pages based on search criteria
4. **Memory Efficient**: Avoids loading entire tables when only specific rows are needed

## Usage

### Basic Commands

```bash
# Get database information
cargo run <database.db> ".dbinfo"

# List all tables
cargo run <database.db> ".tables"

# Show schema information
cargo run <database.db> ".schema"
```

### SQL Queries

```bash
# Select all columns
cargo run <database.db> "SELECT * FROM table_name"

# Select specific columns
cargo run <database.db> "SELECT id, name FROM table_name"

# Count rows
cargo run <database.db> "SELECT COUNT(*) FROM table_name"

# WHERE clauses (with automatic index usage)
cargo run <database.db> "SELECT * FROM table_name WHERE column = 'value'"
cargo run <database.db> "SELECT id, name FROM companies WHERE country = 'eritrea'"
```

## Performance Features

### Index Optimization

The implementation automatically detects when indexes can be used for queries:

- **Equality conditions**: `WHERE column = 'value'` uses index if available
- **Smart traversal**: Only visits B-tree pages that contain relevant data
- **Direct row lookup**: Fetches specific rows by ID without loading entire table

### B-tree Navigation

- **Recursive traversal**: Clean, readable tree navigation
- **Page-level optimization**: Efficient reading of database pages
- **Interior page handling**: Proper navigation through interior B-tree nodes

## Implementation Details

### SQLite File Format

The implementation correctly handles:
- Database header parsing (page size, schema version, etc.)
- B-tree page types (leaf/interior, table/index)
- Cell pointer arrays and cell data
- Variable-length integer encoding (varints)
- Record format with typed columns

### Data Types Supported

- **Integers**: 8, 16, 24, 32, 48, 64-bit signed integers
- **Floats**: 64-bit IEEE floating point
- **Text**: UTF-8 strings
- **Blobs**: Binary data
- **Null values**
- **Special values**: Zero, One

### Query Processing

1. **Parse SQL**: Extract query type, columns, table, and WHERE conditions
2. **Schema lookup**: Get table structure and column information
3. **Index detection**: Check for usable indexes on WHERE columns
4. **Execution**: Use index scanning or full table scan as appropriate
5. **Result formatting**: Display results in pipe-separated format

## Error Handling

Comprehensive error handling for:
- Invalid database files
- Corrupted B-tree structures
- Malformed SQL queries
- Missing tables or columns
- Type conversion errors

## Testing

The implementation has been tested with:
- Real SQLite database files
- Complex queries with WHERE clauses
- Index usage scenarios
- Large datasets with performance optimization

## Building and Running

```bash
# Build the project
cargo build --release

# Run with a database file
cargo run <database_file> "<SQL_query>"

# Example
cargo run companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"
```

## Dependencies

- `anyhow`: Error handling
- Standard Rust library for file I/O and data structures

## Performance Characteristics

- **Index queries**: O(log n) lookup time when indexes are available
- **Full table scans**: O(n) when no suitable index exists
- **Memory usage**: Minimal - only loads necessary pages and rows
- **B-tree traversal**: Efficient recursive navigation

## Future Enhancements

Potential areas for expansion:
- Support for more SQL operations (JOIN, ORDER BY, GROUP BY)
- Additional comparison operators (>, <, >=, <=, LIKE)
- Write operations (INSERT, UPDATE, DELETE)
- Transaction support
- Concurrent access

