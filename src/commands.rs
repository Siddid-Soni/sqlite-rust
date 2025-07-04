use anyhow::{bail, Result};
use crate::{Database, TableRows, RecordValue};

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column_name: String,
    pub operator: ComparisonOperator,
    pub value: String,
}

impl WhereCondition {
    pub fn parse(where_clause: &str) -> Result<Self> {
        let where_clause = where_clause.trim();
        
        if let Some(pos) = where_clause.find(" = ") {
            let column_name = where_clause[..pos].trim().to_string();
            let value = where_clause[pos + 3..].trim();
            let value = Self::parse_value(value)?;
            return Ok(WhereCondition {
                column_name,
                operator: ComparisonOperator::Equal,
                value,
            });
        }
        
        if let Some(pos) = where_clause.find(" != ") {
            let column_name = where_clause[..pos].trim().to_string();
            let value = where_clause[pos + 4..].trim();
            let value = Self::parse_value(value)?;
            return Ok(WhereCondition {
                column_name,
                operator: ComparisonOperator::NotEqual,
                value,
            });
        }
        
        bail!("Unsupported WHERE clause format: {}", where_clause);
    }
    
    fn parse_value(value: &str) -> Result<String> {
        let value = value.trim();
        if value.starts_with('\'') && value.ends_with('\'') {
            // Remove quotes for string values
            Ok(value[1..value.len()-1].to_string())
        } else if value.starts_with('"') && value.ends_with('"') {
            // Remove double quotes for string values
            Ok(value[1..value.len()-1].to_string())
        } else {
            // Numeric or unquoted value
            Ok(value.to_string())
        }
    }
    
    pub fn matches(&self, record_value: &RecordValue) -> bool {
        let value_str = record_value.to_display_string();
        match self.operator {
            ComparisonOperator::Equal => value_str == self.value,
            ComparisonOperator::NotEqual => value_str != self.value,
            // Add more operators as needed
            _ => false,
        }
    }
}

pub fn execute_command(database_path: &str, command: &str) -> Result<()> {
    match command {
        ".dbinfo" => handle_dbinfo(database_path),
        ".tables" => handle_tables(database_path),
        ".schema" => handle_schema(database_path),
        query => handle_sql_query(database_path, query),
    }
}

fn handle_dbinfo(database_path: &str) -> Result<()> {
    let mut db = Database::new(database_path)?;
    
    println!("database page size: {}", db.get_page_size());
    println!("number of tables: {}", db.get_num_tables()?);
    
    Ok(())
}

fn handle_tables(database_path: &str) -> Result<()> {
    let mut db = Database::new(database_path)?;
    let table_names = db.get_table_names()?;
    
    for table_name in table_names {
        println!("{}", table_name);
    }
    
    Ok(())
}

fn handle_schema(database_path: &str) -> Result<()> {
    let mut db = Database::new(database_path)?;
    let objects = db.get_all_schema_objects()?;
    
    for obj in objects {
        println!("{}: {} (table: {}, page: {})", 
            obj.object_type, obj.name, obj.tbl_name, obj.rootpage);
        if let Some(sql) = &obj.sql {
            println!("  SQL: {}", sql);
        }
    }
    
    Ok(())
}

fn handle_sql_query(database_path: &str, query: &str) -> Result<()> {
    let mut query_parts = query.split_whitespace();
    
    match query_parts.next().map(|s| s.to_lowercase()).as_deref() {
        Some("select") => handle_select_query(database_path, query),
        _ => bail!("Unsupported SQL command: {}", query),
    }
}

fn handle_select_query(database_path: &str, query: &str) -> Result<()> {
    // Skip the "SELECT" keyword (case insensitive)
    let query_after_select = if query.to_lowercase().starts_with("select ") {
        &query[7..] // Skip "SELECT "
    } else {
        bail!("Query must start with SELECT");
    };
    
    let query_lower = query_after_select.to_lowercase();
    
    if query_lower.starts_with("count(*)") {
        let mut parts = query_after_select.split_whitespace();
        parts.next(); // skip "count(*)"
        handle_select_count_from_remaining(database_path, &mut parts)
    } else if query_lower.starts_with("*") {
        let mut parts = query_after_select.split_whitespace();
        parts.next(); // skip "*"
        handle_select_all_from_remaining(database_path, &mut parts)
    } else {
        // Handle specific column selection with case preservation
        handle_select_columns(database_path, query_after_select)
    }
}

fn handle_select_count_from_remaining(database_path: &str, query_parts: &mut std::str::SplitWhitespace) -> Result<()> {
    match query_parts.next().map(|s| s.to_lowercase()).as_deref() {
        Some("from") => {
            let table_name = query_parts.next()
                .ok_or_else(|| anyhow::anyhow!("Missing table name in SELECT COUNT(*) FROM query"))?;
            
            let mut db = Database::new(database_path)?;
            let count = db.count_table_rows(table_name)?;
            println!("{}", count);
            Ok(())
        }
        _ => bail!("Expected FROM after SELECT COUNT(*)"),
    }
}

fn handle_select_all_from_remaining(database_path: &str, query_parts: &mut std::str::SplitWhitespace) -> Result<()> {
    match query_parts.next().map(|s| s.to_lowercase()).as_deref() {
        Some("from") => {
            let remaining_parts: Vec<&str> = query_parts.collect();
            let (table_name, where_condition) = parse_table_and_where(&remaining_parts)?;
            
            let mut db = Database::new(database_path)?;
            let table_data = db.get_table_rows(table_name)?;
            
            if table_data.columns.is_empty() {
                bail!("Table {} not found or has no columns", table_name);
            }
            
            // Apply WHERE filter if present
            let final_data = if let Some(condition) = where_condition {
                apply_where_filter(&table_data, &condition)?
            } else {
                table_data
            };
            
            display_table_data(&final_data);
            Ok(())
        }
        _ => bail!("Expected FROM after SELECT *"),
    }
}

fn handle_select_columns(database_path: &str, query_str: &str) -> Result<()> {
    // Parse column1, column2, ... FROM table [WHERE condition] format
    // Use case-insensitive matching for keywords but preserve case for values
    let query_lower = query_str.to_lowercase();
    let from_pos = query_lower.find(" from ")
        .ok_or_else(|| anyhow::anyhow!("Missing FROM clause in SELECT query"))?;
    
    let columns_str = query_str[..from_pos].trim();
    let from_and_where = query_str[from_pos + 6..].trim(); // Skip " from "
    
    // Check if there's a WHERE clause (case-insensitive)
    let from_and_where_lower = from_and_where.to_lowercase();
    let (table_name, where_condition) = if let Some(where_pos) = from_and_where_lower.find(" where ") {
        let table_name = from_and_where[..where_pos].trim();
        let where_clause = from_and_where[where_pos + 7..].trim(); // Preserve case for WHERE clause
        let condition = WhereCondition::parse(where_clause)?;
        (table_name, Some(condition))
    } else {
        (from_and_where, None)
    };
    
    // Parse column names (handle comma-separated)
    let column_names: Vec<&str> = columns_str
        .split(',')
        .map(|s| s.trim())
        .collect();
    
    let mut db = Database::new(database_path)?;
    
    // Try to use index scanning if there's a WHERE condition
    let table_data = if let Some(ref condition) = where_condition {
        // Check if we can use an index for this condition
        if condition.operator == ComparisonOperator::Equal {
            if let Ok(Some(index)) = db.find_index_for_column(table_name, &condition.column_name) {
                // Use index scanning
                let row_ids = db.search_index(&index, &condition.value)?;
                
                // Get only the rows with matching IDs
                db.get_table_rows_by_ids(table_name, &row_ids)?
            } else {
                // Fall back to full table scan
                let all_data = db.get_table_rows(table_name)?;
                apply_where_filter(&all_data, condition)?
            }
        } else {
            // Non-equality conditions: use full table scan
            let all_data = db.get_table_rows(table_name)?;
            apply_where_filter(&all_data, condition)?
        }
    } else {
        // No WHERE clause: get all data
        db.get_table_rows(table_name)?
    };
    
    if table_data.columns.is_empty() {
        bail!("Table {} not found or has no columns", table_name);
    }
    
    // Extract specific columns
    let column_values = extract_columns(&table_data, &column_names)?;
    
    // Display column headers
    println!("{}", column_names.join("|"));
    
    // Display data rows
    for row in column_values {
        let row_values: Vec<String> = row.iter()
            .map(|val| val.to_display_string())
            .collect();
        println!("{}", row_values.join("|"));
    }
    
    Ok(())
}

/// Apply WHERE filter to table data
fn apply_where_filter(table_data: &TableRows, condition: &WhereCondition) -> Result<TableRows> {
    // Find the column index for the condition
    let column_index = table_data.columns.iter()
        .position(|col| col.name.eq_ignore_ascii_case(&condition.column_name))
        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", condition.column_name))?;
    
    // Filter rows based on the condition
    let filtered_rows: Vec<_> = table_data.rows.iter()
        .filter(|row| {
            if let Some(value) = row.values.get(column_index) {
                condition.matches(value)
            } else {
                false
            }
        })
        .cloned()
        .collect();
    
    Ok(TableRows {
        columns: table_data.columns.clone(),
        rows: filtered_rows,
    })
}

/// Extract specific columns from table data
fn extract_columns(table_data: &TableRows, column_names: &[&str]) -> Result<Vec<Vec<RecordValue>>> {
    let mut column_indices = Vec::new();
    
    // Find indices for the requested columns
    for col_name in column_names {
        let index = table_data.columns.iter()
            .position(|col| col.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", col_name))?;
        column_indices.push(index);
    }
    
    // Extract values for the specified columns
    let mut results = Vec::new();
    for row in &table_data.rows {
        let mut row_values = Vec::new();
        for &col_index in &column_indices {
            if let Some(value) = row.values.get(col_index) {
                row_values.push(value.clone());
            } else {
                row_values.push(RecordValue::Null);
            }
        }
        results.push(row_values);
    }
    
    Ok(results)
}

/// Display table data in a formatted way
fn display_table_data(table_data: &TableRows) {
    // Print column headers
    let headers: Vec<String> = table_data.columns.iter()
        .map(|col| col.name.clone())
        .collect();
    println!("{}", headers.join("|"));
    
    // Print separator
    let separator = headers.iter()
        .map(|h| "-".repeat(h.len().max(10)))
        .collect::<Vec<_>>()
        .join("|");
    println!("{}", separator);
    
    // Print data rows
    for row in &table_data.rows {
        let row_values: Vec<String> = row.values.iter()
            .map(|val| val.to_display_string())
            .collect();
        println!("{}", row_values.join("|"));
    }
}

/// Parse table name and WHERE condition from query parts
fn parse_table_and_where<'a>(parts: &'a [&str]) -> Result<(&'a str, Option<WhereCondition>)> {
    if parts.is_empty() {
        bail!("Missing table name in SELECT query");
    }
    
    // Convert to lowercase for keyword matching but preserve original case
    let parts_lower: Vec<String> = parts.iter().map(|s| s.to_lowercase()).collect();
    
    // Check if there's a WHERE clause (case-insensitive)
    if let Some(where_pos) = parts_lower.iter().position(|part| part == "where") {
        if where_pos == 0 {
            bail!("Missing table name before WHERE clause");
        }
        let table_name = parts[0];
        let where_clause = parts[where_pos + 1..].join(" ");
        let condition = WhereCondition::parse(&where_clause)?;
        Ok((table_name, Some(condition)))
    } else {
        Ok((parts[0], None))
    }
} 