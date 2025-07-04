use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub index: usize,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
}

impl TableSchema {
    pub fn from_create_sql(sql: &str) -> Result<Self> {
        // Extract column definitions between parentheses
        let start = sql
            .find('(')
            .ok_or_else(|| anyhow::anyhow!("No opening parenthesis found in CREATE TABLE statement"))?;
        let end = sql
            .rfind(')')
            .ok_or_else(|| anyhow::anyhow!("No closing parenthesis found in CREATE TABLE statement"))?;

        if start >= end {
            bail!("Invalid parentheses in CREATE TABLE statement");
        }

        let columns_def = &sql[start + 1..end];
        let column_parts: Vec<&str> = columns_def.split(',').collect();

        let mut columns = Vec::new();
        for (index, part) in column_parts.iter().enumerate() {
            let trimmed = part.trim();
            let words: Vec<&str> = trimmed.split_whitespace().collect();
            if !words.is_empty() {
                let is_primary_key = trimmed.to_lowercase().contains("primary key");
                columns.push(ColumnInfo {
                    name: words[0].to_string(),
                    index,
                    is_primary_key,
                });
            }
        }

        Ok(TableSchema { columns })
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .find(|col| col.name.eq_ignore_ascii_case(name))
            .map(|col| col.index)
    }
} 